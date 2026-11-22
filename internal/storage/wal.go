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

// WALRecord 定义一个 WAL 记录，将 HardState 和 Entries 打包
type WALRecord struct {
	HardState raftpb.HardState
	Entries   []raftpb.Entry
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
	file                *os.File   // WAL 文件句柄
	snapDir             string     // 快照文件目录
}

// OpenWAL 打开 WAL 文件，如果文件不存在则创建一个新的文件，并返回一个 WAL 实例
func OpenWAL(walPath string, snapPath string) (*WAL, error) {
	// 打开文件流
	file, err := os.OpenFile(filepath.Join(walPath, "wal.log"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// 创建 WAL 实例
	w := &WAL{
		file:          file,
		MemoryStorage: raft.NewMemoryStorage(),
		snapDir:       snapPath,
	}

	// 先从独立 .snap 文件加载最新快照
	snap, err := w.LoadSnapshot()
	if err != nil {
		file.Close() // 关闭文件流
		return nil, err
	}
	if snap != nil {
		if err := w.MemoryStorage.ApplySnapshot(*snap); err != nil {
			file.Close() // 关闭文件流
			return nil, fmt.Errorf("failed to apply snapshot to memory storage: %w", err)
		}
	}

	// 从文件恢复 HardState 和 Entries
	hardStates, entriesList, err := w.ReadAll() // ReadAll 会把文件指针移动到最后一条合法记录的末尾，后面就不用再 Seek 了
	if err != nil {
		file.Close() // 关闭文件流
		return nil, err
	}

	// 日志连续性检查。
	// MemoryStorage.Append 要求新日志与当前末尾连续；遇到缺口会直接 panic
	// （missing log entry [last: X, append at: Y]）。接收过 Leader 快照的节点，
	// WAL 是从快照之后的索引开始写的，一旦快照文件丢失/损坏，重启时就会出现
	// “WAL 从 101 开始，但没有快照”的缺口。这里提前返回可操作的错误。
	if firstWAL := firstEntryIndex(entriesList); firstWAL > 0 {
		if snap == nil && firstWAL > 1 {
			// 如果 WAL 里有日志，但没有快照文件，说明快照目录丢失或损坏
			file.Close()
			return nil, fmt.Errorf("log starts at index %d but no snapshot file was found; "+
				"the snapshot directory is missing or corrupted", firstWAL)
		}
		if snap != nil && firstWAL > snap.Metadata.Index+1 {
			// 如果 WAL 里第一条日志的索引大于快照索引 + 1，说明快照文件丢失或损坏
			file.Close()
			return nil, fmt.Errorf("log gap: latest snapshot is at index %d, but WAL starts at index %d; "+
				"the snapshot file is missing or corrupted", snap.Metadata.Index, firstWAL)
		}
	}

	// 将读取到的数据恢复到内存存储中
	for _, entries := range entriesList {
		// 将日志条目追加到内存存储中
		if err := w.MemoryStorage.Append(entries); err != nil {
			file.Close() // 关闭文件流
			return nil, fmt.Errorf("failed to append entries to memory storage: %w", err)
		}
	}
	var hs raftpb.HardState
	if len(hardStates) > 0 {
		// 如果有 HardState，取最后一条作为最新的 HardState
		hs = hardStates[len(hardStates)-1]
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
		file.Close() // 关闭文件流
		return nil, fmt.Errorf("failed to set hard state in memory storage: %w", err)
	}

	// 校检快照和 WAL 文件的一致性
	// 注意：不能用 w.MemoryStorage.LastIndex() 来比较 —— ApplySnapshot 之后它至少等于快照索引，
	// 快照索引 > LastIndex 恒不成立，这个校验永远不会触发。这里改用 WAL 文件里真实的最后一条日志索引。
	if snap != nil {
		var walLastIndex uint64
		if n := len(entriesList); n > 0 {
			if last := entriesList[n-1]; len(last) > 0 {
				walLastIndex = last[len(last)-1].Index // WAL 最后一条日志的索引
			}
		}
		// 快照索引大于 WAL 最后一条日志是合法的：follower 落后太多时，leader 直接发快照帮它追赶，
		// 本地 WAL 里本来就不会有快照覆盖的那段日志，所以这里只提示，不当作错误。
		if walLastIndex > 0 && snap.Metadata.Index > walLastIndex {
			log.Printf("Warning: snapshot index %d is ahead of WAL last entry index %d (entries covered by the snapshot were never written to this WAL)", snap.Metadata.Index, walLastIndex)
		}
	}

	return w, nil
}

// Close 关闭 WAL 文件
func (w *WAL) Close() error {
	// 依旧要加锁保护并发写入，防止在关闭时还有 goroutine 在写入
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Close()
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
	// 2. 写入 4 字节的长度头 (大端序)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := w.file.Write(hdr[:]); err != nil {
		return err
	}
	// 3. 写入实际数据
	if _, err := w.file.Write(data); err != nil {
		return err
	}
	// 4. 强制刷盘
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}

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

	return nil
}

// ReadAll 重启时，按顺序读取出所有的记录
func (w *WAL) ReadAll() ([]raftpb.HardState, [][]raftpb.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 将文件指针归位到开头，准备读取
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, nil, err
	}

	var hardStates []raftpb.HardState
	var entriesList [][]raftpb.Entry
	var lastValidOffset int64 = 0 // 记录最后一条合法记录的结束位置

	for {
		// 1. 读取 4 字节长度头
		var hdr [4]byte
		if _, err := io.ReadFull(w.file, hdr[:]); err != nil {
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
		if _, err := io.ReadFull(w.file, payload); err != nil {
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
	fileInfo, err := w.file.Stat()
	if err != nil {
		// 如果连文件信息都获取不到，说明文件可能被删除了，直接返回错误
		log.Printf("Warning: failed to stat WAL file: %v. Returning error.", err)
		return nil, nil, err
	}
	if fileInfo.Size() > lastValidOffset {
		log.Printf("WAL file corrupted or incomplete. Truncating from %d to %d bytes.", fileInfo.Size(), lastValidOffset)
		if err := w.file.Truncate(lastValidOffset); err != nil {
			return nil, nil, fmt.Errorf("failed to truncate WAL: %w", err)
		}
	}

	// 5. 指针回到最后一条合法记录的末尾，准备追加写入
	if _, err := w.file.Seek(lastValidOffset, io.SeekStart); err != nil {
		return nil, nil, err
	}

	return hardStates, entriesList, nil
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
