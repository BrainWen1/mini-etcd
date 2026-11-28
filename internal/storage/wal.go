package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	defaultSegmentSize = 64 * 1024 * 1024 // 每个 WAL 分段文件的默认最大大小，64MB
	walFileExt         = ".wal"           // WAL 文件扩展名
)

// WALRecord 定义一个 WAL 记录，将 HardState 和 Entries 打包
type WALRecord struct {
	HardState raftpb.HardState
	Entries   []raftpb.Entry
}

// walMaxIndex 返回一批日志和一条 HardState 中较大的索引。
// 纯 HardState 记录（commit 推进但本批没有新日志）也覆盖了索引，
// 计算分段文件的覆盖范围时不能漏掉它，否则 Truncate 可能删掉仍然需要的分段。
func walMaxIndex(entries []raftpb.Entry, hardState raftpb.HardState) uint64 {
	var maxIdx uint64
	if len(entries) > 0 {
		maxIdx = entries[len(entries)-1].Index
	}
	if !raft.IsEmptyHardState(hardState) && hardState.Commit > maxIdx {
		maxIdx = hardState.Commit
	}
	return maxIdx
}

// firstEntryIndex 返回 WAL 中第一条日志的索引；没有日志时返回 0。
func firstEntryIndex(entriesList [][]raftpb.Entry) uint64 {
	for _, entries := range entriesList {
		if len(entries) > 0 {
			return entries[0].Index
		}
	}
	return 0
}

type WAL struct {
	*raft.MemoryStorage            // 因为 Raft 算法需要高频调用 Entries() 和 Term()，每次查磁盘会卡死，所以要用内存存储来缓存
	mu                  sync.Mutex // 保护并发写入

	dir         string   // WAL 文件夹路径
	snapDir     string   // 快照文件目录
	segmentSize int64    // 单个分段文件的最大字节数
	activeSize  int64    // 当前活跃文件的大小（字节）
	activeFile  *os.File // 当前正在写入的文件句柄
	// 记录每个 WAL 文件对应的最大日志索引，方便 Truncate 时极速判断。
	// key 一律是文件名（filepath.Base），不能是完整路径。
	fileMaxIdx map[string]uint64
	// 最近一次写入非空 HardState 的分段文件名。Truncate 必须保留它：
	// 快照只持久化 Term/Commit，Vote 只能从 WAL 恢复，删掉会破坏“同一任期只投一票”。
	lastHardStateFile string
}

// OpenWAL 打开 WAL 文件，如果文件不存在则创建一个新的文件，并返回一个 WAL 实例
func OpenWAL(walPath string, snapPath string) (*WAL, error) {
	return OpenWALWithSegmentSize(walPath, snapPath, defaultSegmentSize)
}

// OpenWALWithSegmentSize 与 OpenWAL 相同，但允许指定单个分段文件的最大字节数。
// segmentSize <= 0 时回退到默认的 64MB。测试和小内存环境可以调小它。
func OpenWALWithSegmentSize(walPath string, snapPath string, segmentSize int64) (*WAL, error) {
	if segmentSize <= 0 {
		segmentSize = defaultSegmentSize
	}
	// 创建 WAL 实例
	w := &WAL{
		MemoryStorage: raft.NewMemoryStorage(),
		dir:           walPath,
		snapDir:       snapPath,
		segmentSize:   segmentSize,
		fileMaxIdx:    make(map[string]uint64),
	}

	// 读取 WAL 文件夹下的所有文件
	files, err := os.ReadDir(walPath)
	if err != nil {
		return nil, err
	}
	var walFiles []string
	for _, f := range files {
		if !f.IsDir() && filepath.Ext(f.Name()) == walFileExt {
			walFiles = append(walFiles, f.Name())
		}
	}
	sort.Strings(walFiles) // 按文件名（如 0000000000000001.wal）字典序排序

	var allHardStates []raftpb.HardState
	var allEntriesList [][]raftpb.Entry

	// 依次读取所有历史 WAL 文件的内容
	for _, name := range walFiles {
		// 拼接文件路径，打开文件流
		path := filepath.Join(walPath, name)
		file, err := os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		// 读取文件内容
		hs, entries, err := w.readFromFile(file)
		if err != nil {
			file.Close()
			return nil, err
		}
		// 将读取到的 HardState 和 Entries 追加到总列表中
		allHardStates = append(allHardStates, hs...)
		allEntriesList = append(allEntriesList, entries...)

		// 记录该文件最大的日志 Index，方便后续 Truncate。
		// 注意必须同时考虑 HardState.Commit：只有 HardState 的记录也占用了索引，
		// 只用 entries 计算会低估文件覆盖范围，导致误删仍有用的分段。
		var lastEntries []raftpb.Entry
		if len(entries) > 0 {
			lastEntries = entries[len(entries)-1]
		}
		var lastHS raftpb.HardState
		if len(hs) > 0 {
			lastHS = hs[len(hs)-1]
		}
		w.fileMaxIdx[name] = walMaxIndex(lastEntries, lastHS)
		if len(hs) > 0 {
			// 扫描按文件名顺序进行，这里最终会停在整个 WAL 里最后一条非空 HardState 所在的文件。
			w.lastHardStateFile = name
		}
		file.Close()
	}

	// 先从独立 .snap 文件加载最新快照
	snap, err := w.LoadSnapshot()
	if err != nil {
		return nil, err
	}
	if snap != nil {
		if err := w.MemoryStorage.ApplySnapshot(*snap); err != nil {
			return nil, fmt.Errorf("failed to apply snapshot to memory storage: %w", err)
		}
	}

	// 日志连续性检查。
	// MemoryStorage.Append 要求新日志与当前末尾连续；遇到缺口会直接 panic
	// （missing log entry [last: X, append at: Y]）。接收过 Leader 快照的节点，
	// WAL 是从快照之后的索引开始写的，一旦快照文件丢失/损坏，重启时就会出现
	// “WAL 从 101 开始，但没有快照”的缺口。这里提前返回可操作的错误。
	if firstWAL := firstEntryIndex(allEntriesList); firstWAL > 0 {
		if snap == nil && firstWAL > 1 {
			// 如果 WAL 里有日志，但没有快照文件，说明快照目录丢失或损坏
			return nil, fmt.Errorf("log starts at index %d but no snapshot file was found; "+
				"the snapshot directory is missing or corrupted", firstWAL)
		}
		if snap != nil && firstWAL > snap.Metadata.Index+1 {
			// 如果 WAL 里第一条日志的索引大于快照索引 + 1，说明快照文件丢失或损坏
			return nil, fmt.Errorf("log gap: latest snapshot is at index %d, but WAL starts at index %d; "+
				"the snapshot file is missing or corrupted", snap.Metadata.Index, firstWAL)
		}
	}

	// 将读取到的数据恢复到内存存储中
	for _, entries := range allEntriesList {
		// 将日志条目追加到内存存储中
		if err := w.MemoryStorage.Append(entries); err != nil {
			return nil, fmt.Errorf("failed to append entries to memory storage: %w", err)
		}
	}
	var hs raftpb.HardState
	if len(allHardStates) > 0 {
		// 如果有 HardState，取最后一条作为最新的 HardState
		hs = allHardStates[len(allHardStates)-1]
	} else if snap != nil {
		// 走到这里说明：磁盘上有快照，但 wal.log 里一条记录都没有（HardState 自然也读不到）。
		// 真实场景有两种：
		//   1. Bootstrap 时刚写完初始快照（index=1），进程还没来得及处理任何 Ready 就挂了；
		//   2. 快照文件是从别处拷贝/恢复过来的，而 WAL 是新建的空文件。
		// 此时如果什么都不做，hs 就是零值，raft 会以 Term=0 启动 —— 而磁盘上的快照明明
		// 已经证明「Term 到过 snap.Term、index 到过 snap.Index」。用 0 起步会让节点
		// 在旧任期里重新选举，属于安全性瑕疵。
		// 快照本身就是这两项的持久化证据，所以直接以快照为准合成 HardState。
		// Vote 留空（raft.None）：本地没有任何投票记录，且合成出来的 Term 下也不该凭空有票。
		hs = raftpb.HardState{
			Term:   snap.Metadata.Term,
			Commit: snap.Metadata.Index,
		}
	}
	// 修复「快照比 HardState 新」的崩溃窗口：
	// Ready 的处理顺序是「先落盘快照，再落盘 HardState」，如果进程在两者之间挂掉，
	// 磁盘上就会出现「快照 index=100，但 WAL 里 HardState.Commit 还停留在 33」的状态。
	// 直接 SetHardState 会让 raft.loadState 直接 panic：
	//   panic: 1 state.commit 33 is out of range [100, 100]
	// 快照本身就是「index 100 已提交」的证据，所以以快照为准把 Commit 补齐；
	// Term 落后时一并抬到快照的 Term，此时旧的 Vote 属于旧 Term，必须清空。
	if snap != nil && hs.Commit < snap.Metadata.Index {
		log.Printf("Warning: hard state commit %d is behind snapshot index %d, repairing from snapshot", hs.Commit, snap.Metadata.Index)
		hs.Commit = snap.Metadata.Index // 以快照为准把 Commit 补齐
		if hs.Term < snap.Metadata.Term {
			hs.Term = snap.Metadata.Term // Term 落后时一并抬到快照的 Term
			hs.Vote = raft.None          // 旧的 Vote 属于旧 Term，必须清空
		}
	}

	// 将最新的 HardState 设置到内存存储中
	if err := w.MemoryStorage.SetHardState(hs); err != nil {
		return nil, fmt.Errorf("failed to set hard state in memory storage: %w", err)
	}

	// 校检快照和 WAL 文件的一致性
	// 注意：不能用 w.MemoryStorage.LastIndex() 来比较 —— ApplySnapshot 之后它至少等于快照索引，
	// 快照索引 > LastIndex 恒不成立，这个校验永远不会触发。这里改用 WAL 文件里真实的最后一条日志索引。
	if snap != nil {
		var walLastIndex uint64
		if n := len(allEntriesList); n > 0 {
			if last := allEntriesList[n-1]; len(last) > 0 {
				walLastIndex = last[len(last)-1].Index // WAL 最后一条日志的索引
			}
		}
		// 快照索引大于 WAL 最后一条日志是合法的：follower 落后太多时，leader 直接发快照帮它追赶，
		// 本地 WAL 里本来就不会有快照覆盖的那段日志，所以这里只提示，不当作错误。
		if walLastIndex > 0 && snap.Metadata.Index > walLastIndex {
			log.Printf("Warning: snapshot index %d is ahead of WAL last entry index %d (entries covered by the snapshot were never written to this WAL)", snap.Metadata.Index, walLastIndex)
		}
	}

	// 确定当前的 activeFile 和 activeSize
	if len(walFiles) == 0 {
		// 全新的节点，创建 1 号分段（文件序号，不是日志索引）
		if err := w.createNewSegment(1); err != nil {
			return nil, err
		}
	} else {
		// 打开最后一个文件用于追加写入
		lastFile := walFiles[len(walFiles)-1]
		path := filepath.Join(walPath, lastFile)
		file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, err
		}
		w.activeFile = file
		w.activeSize = info.Size()
	}

	return w, nil
}

// createNewSegment 创建一个新的 WAL 分段文件。
// 文件按创建顺序编号（1、2、3...），统一格式化成 16 位十六进制，
// 这样按文件名字典序排序就是写入顺序，且不会因为十进制超过 9 而错位。
func (w *WAL) createNewSegment(seq uint64) error {
	filename := fmt.Sprintf("%016x%s", seq, walFileExt)
	path := filepath.Join(w.dir, filename)
	// 打开文件用于写入，如果文件不存在则创建
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	// 新建文件后必须 fsync 目录：否则崩溃后目录项可能丢失，已经 fsync 过文件内容的
	// 记录会连同文件名一起消失，破坏 WAL 的持久化保证。
	if err := syncDir(w.dir); err != nil {
		file.Close()
		return err
	}

	// 更新当前活跃文件句柄和大小，并记录该文件的最大索引为 0（新文件还没有写入任何日志）
	w.activeFile = file
	w.activeSize = 0
	w.fileMaxIdx[filename] = 0
	return nil
}

// syncDir 对目录执行 fsync，确保新建/删除的文件目录项落盘。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Close 关闭 WAL 文件
func (w *WAL) Close() error {
	// 依旧要加锁保护并发写入，防止在关闭时还有 goroutine 在写入
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.activeFile.Close()
}

// Save 将整个 Ready 打包成一条记录追加写入文件
// 注意：官方 Ready 里 HardState 可能为空，Entries 也可能为空，只要有一个不为空，就写一条记录
func (w *WAL) Save(hardState raftpb.HardState, entries []raftpb.Entry) error {
	// 先落盘再更新内存，保证 WAL 的持久化特性
	// 落盘
	if raft.IsEmptyHardState(hardState) && len(entries) == 0 {
		// 如果 HardState 和 Entries 都为空，就不写入
		return nil
	}

	// 加锁保护并发写入
	w.mu.Lock()
	defer w.mu.Unlock()

	// 组装 WAL 记录
	record := WALRecord{
		HardState: hardState,
		Entries:   entries,
	}

	// 1. 先把这条记录编码到内存里（每次创建新的 Encoder，避免跨进程类型重复）
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(record); err != nil {
		return fmt.Errorf("gob encode failed: %w", err)
	}
	data := buf.Bytes()
	// 2. 检查当前活跃文件大小，如果超过阈值就切换到新文件。
	// activeSize == 0 时不轮转：单条记录本身就超过阈值时，换文件也没有意义，
	// 只会白白留下一个空分段。
	frameSize := int64(4 + len(data))
	if w.activeSize > 0 && w.activeSize+frameSize > w.segmentSize {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("rotate WAL failed: %w", err)
		}
	}
	// 3. 写入 4 字节的长度头 (大端序)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := w.activeFile.Write(hdr[:]); err != nil {
		return err
	}
	// 4. 写入实际数据
	if _, err := w.activeFile.Write(data); err != nil {
		return err
	}
	// 5. 强制刷盘
	if err := w.activeFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}

	w.activeSize += frameSize

	// 更新内存
	if !raft.IsEmptyHardState(hardState) {
		if err := w.MemoryStorage.SetHardState(hardState); err != nil {
			return fmt.Errorf("failed to set hard state in memory storage: %w", err)
		}
	}
	if len(entries) > 0 {
		if err := w.MemoryStorage.Append(entries); err != nil {
			return fmt.Errorf("failed to append entries to memory storage: %w", err)
		}
	}

	// 更新当前活跃文件的最大索引。
	// key 必须用文件名（与 OpenWAL 扫描时一致），不能用 activeFile.Name() 返回的完整路径，
	// 否则 Truncate 会拿着一堆对不上的 key，既漏删旧文件、又可能拼出错误的删除路径。
	// 取 max 而不是直接覆盖：日志冲突覆盖时新写入的条目索引可能比文件里已有的更小，
	// 分段覆盖范围只能单调不减。
	name := filepath.Base(w.activeFile.Name())
	if idx := walMaxIndex(entries, hardState); idx > w.fileMaxIdx[name] {
		w.fileMaxIdx[name] = idx
	}
	if !raft.IsEmptyHardState(hardState) {
		w.lastHardStateFile = name
	}

	return nil
}

// rotate 关闭当前活跃文件，并创建一个新的 WAL 分段文件
func (w *WAL) rotate() error {
	// 文件名是 16 位十六进制序号，解析时必须用 base 16。
	// 用 base 10 解析会在序号 10（a）之后失败，随后重新创建 1 号文件，
	// 造成文件覆盖和重启时的日志乱序/丢失。
	fileName := filepath.Base(w.activeFile.Name())
	if err := w.activeFile.Close(); err != nil {
		return err
	}
	seq, err := strconv.ParseUint(strings.TrimSuffix(fileName, walFileExt), 16, 64)
	if err != nil {
		return fmt.Errorf("parse WAL segment name %q: %w", fileName, err)
	}

	return w.createNewSegment(seq + 1)
}

// readFromFile 从指定的 WAL 文件中读取所有记录，并返回 HardState 和 Entries 列表
func (w *WAL) readFromFile(file *os.File) ([]raftpb.HardState, [][]raftpb.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 将文件指针归位到开头，准备读取
	if _, err := file.Seek(0, 0); err != nil {
		return nil, nil, err
	}

	var hardStates []raftpb.HardState
	var entriesList [][]raftpb.Entry
	var lastValidOffset int64 = 0 // 记录最后一条合法记录的结束位置

	for {
		// 1. 读取 4 字节长度头
		var hdr [4]byte
		if _, err := io.ReadFull(file, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break // 正常读完
			}
			log.Printf("Warning: incomplete header read: %v. Truncating.", err)
			break // 头没读满，说明写一半断电了
		}

		length := binary.BigEndian.Uint32(hdr[:])
		// 防御性检查：防止读到乱码导致申请超大内存
		if length > 10*1024*1024 { // 10MB
			log.Printf("Warning: invalid record length: %d. Truncating.", length)
			break
		}

		// 2. 根据长度读取数据体
		payload := make([]byte, length)
		if _, err := io.ReadFull(file, payload); err != nil {
			log.Printf("Warning: incomplete payload read: %v. Truncating.", err)
			break // 数据体没读满，说明写一半断电了
		}

		// 3. 独立解码
		var record WALRecord
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&record); err != nil {
			log.Printf("Warning: gob decode failed: %v. Truncating.", err)
			break
		}

		// 读成功了，更新合法偏移量
		lastValidOffset += int64(4 + length) // 4 字节长度头 + 数据体长度

		// 记录 HardState 和 Entries
		if !raft.IsEmptyHardState(record.HardState) {
			hardStates = append(hardStates, record.HardState)
		}
		if len(record.Entries) > 0 {
			entriesList = append(entriesList, record.Entries)
		}
	}

	// 4. 如果发现文件有坏尾巴（比如崩溃了），截断它
	fileInfo, err := file.Stat()
	if err != nil {
		// 如果连文件信息都获取不到，说明文件可能被删除了，直接返回错误
		log.Printf("Warning: failed to stat WAL file: %v. Returning error.", err)
		return nil, nil, err
	}
	if fileInfo.Size() > lastValidOffset {
		log.Printf("WAL file corrupted or incomplete. Truncating from %d to %d bytes.", fileInfo.Size(), lastValidOffset)
		if err := file.Truncate(lastValidOffset); err != nil {
			return nil, nil, fmt.Errorf("failed to truncate WAL: %w", err)
		}
	}

	// 5. 指针回到最后一条合法记录的末尾，准备追加写入
	if _, err := file.Seek(lastValidOffset, io.SeekStart); err != nil {
		return nil, nil, err
	}

	return hardStates, entriesList, nil
}

// Truncate 删除所有最大索引小于 snapshotIndex 的 WAL 分段文件。
//
// 安全约束：
//  1. 正在写入的活跃分段永远保留；
//  2. 最近一次写入非空 HardState 的分段永远保留 —— 快照只记录 Term/Commit，
//     Vote 只能从 WAL 恢复，删掉它可能导致同一任期重复投票；
//  3. 只有整个分段的日志都被快照覆盖（maxIdx < snapshotIndex）才删除，
//     恰好等于快照索引的分段也保留，避免索引越界。
//
// 删除单个文件失败不会中断其余文件的清理，函数返回第一个错误。
func (w *WAL) Truncate(snapshotIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 获取当前活跃文件的文件名，避免误删
	activeName := filepath.Base(w.activeFile.Name())

	// 按文件名排序，保证删除顺序稳定、日志可读
	names := make([]string, 0, len(w.fileMaxIdx))
	for name := range w.fileMaxIdx {
		names = append(names, name)
	}
	sort.Strings(names)

	var firstErr error
	for _, name := range names {
		if name == activeName {
			continue // 绝对不能删正在写的文件
		}
		if name == w.lastHardStateFile {
			continue // 最新 HardState 还要用来恢复 Term/Vote
		}
		maxIdx := w.fileMaxIdx[name]
		if maxIdx == 0 || maxIdx >= snapshotIndex {
			// maxIdx == 0：空分段或纯空 HardState，没有可直接判断的覆盖范围，保守保留
			continue
		}
		// 整个分段的日志都小于快照索引，说明已被快照完全覆盖，可以删除
		path := filepath.Join(w.dir, name)
		log.Printf("Truncating WAL segment %s (max index: %d < snapshot index: %d)", name, maxIdx, snapshotIndex)
		if err := os.Remove(path); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			log.Printf("Warning: failed to remove WAL segment %s: %v", name, err)
			continue
		}
		// 从 fileMaxIdx 中删除该文件的记录
		delete(w.fileMaxIdx, name)
	}
	return firstErr
}

// SaveSnapshot 将快照保存到独立的 .snap 文件中
func (w *WAL) SaveSnapshot(snapshot raftpb.Snapshot) error {
	//  生成文件名 (Term-Index.snap)
	filename := fmt.Sprintf("%016x-%016x.snap", snapshot.Metadata.Term, snapshot.Metadata.Index)
	snapPath := filepath.Join(w.snapDir, filename) // 保存到快照目录下

	// 序列化 Protobuf
	data, err := snapshot.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	// 写入临时文件 (防止写入中途崩溃导致原文件损坏)
	tmpPath := snapPath + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	// 确保出错时清理临时文件
	defer func() {
		tmpFile.Close()
		os.Remove(tmpPath)
	}()

	if _, err = tmpFile.Write(data); err != nil {
		return err
	}

	// 强制刷盘，确保数据落盘
	if err = tmpFile.Sync(); err != nil {
		return err
	}

	// 关闭文件并重命名 (原子操作)
	if err = tmpFile.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpPath, snapPath); err != nil {
		return err
	}

	// 同步目录，确保文件系统的元数据也落盘
	dirFile, err := os.Open(w.snapDir)
	if err != nil {
		return err
	}
	defer dirFile.Close()
	dirFile.Sync()

	// 清理旧快照文件，只保留最新的五个
	files, err := os.ReadDir(w.snapDir) // 读取目录下所有文件
	if err != nil {
		return err
	}

	var snapFiles []os.DirEntry
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".snap") { // 只保留 .snap 文件
			snapFiles = append(snapFiles, f)
		}
	}

	if len(snapFiles) > 5 {
		// 按文件名排序，文件名中包含 Term 和 Index，按字典序排序即可
		sort.Slice(snapFiles, func(i, j int) bool {
			return snapFiles[i].Name() < snapFiles[j].Name()
		})
		for _, f := range snapFiles[:len(snapFiles)-5] { // 删除最旧的快照文件
			if err := os.Remove(filepath.Join(w.snapDir, f.Name())); err != nil {
				// 删除失败不致命，打印警告即可
				log.Printf("Warning: failed to remove old snapshot file %s: %v", f.Name(), err)
			}
		}
	}

	return nil
}

// LoadSnapshot 读取最新的快照文件
func (w *WAL) LoadSnapshot() (*raftpb.Snapshot, error) {
	// 遍历目录查找所有 .snap 文件
	files, err := os.ReadDir(w.snapDir)
	if err != nil {
		return nil, err
	}

	var latestSnapFile string
	var maxIndex uint64 = 0

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".snap") {
			// 解析文件名中的 Index
			parts := strings.Split(f.Name(), "-") // 文件名格式: Term-Index.snap
			if len(parts) == 2 {
				indexStr := strings.TrimSuffix(parts[1], ".snap")
				index, _ := strconv.ParseUint(indexStr, 16, 64)
				if index > maxIndex { // 找到最新的快照文件
					maxIndex = index
					latestSnapFile = f.Name() // 记录最新的快照文件名
				}
			}
		}
	}

	if latestSnapFile == "" {
		return nil, nil // 没有快照
	}

	// 读取文件内容
	data, err := os.ReadFile(filepath.Join(w.snapDir, latestSnapFile))
	if err != nil {
		return nil, err
	}

	// 反序列化 Protobuf
	var snapshot raftpb.Snapshot
	if err := snapshot.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	return &snapshot, nil
}
