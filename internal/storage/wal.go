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
			log.Printf("Warning: failed to apply snapshot file: %v. Ignoring.", err)
		}
	}

	// 从文件恢复 HardState 和 Entries
	hardStates, entriesList, err := w.ReadAll() // ReadAll 会把文件指针移动到最后一条合法记录的末尾，后面就不用再 Seek 了
	if err != nil {
		file.Close() // 关闭文件流
		return nil, err
	}
	// 将读取到的数据恢复到内存存储中
	for _, entries := range entriesList {
		// 将日志条目追加到内存存储中
		if err := w.MemoryStorage.Append(entries); err != nil {
			file.Close() // 关闭文件流
			return nil, fmt.Errorf("failed to append entries to memory storage: %w", err)
		}
	}
	if len(hardStates) > 0 {
		// 将最新的 HardState 设置到内存存储中
		if err := w.MemoryStorage.SetHardState(hardStates[len(hardStates)-1]); err != nil { // 只需要取最后一个 HardState，即最新状态
			file.Close() // 关闭文件流
			return nil, fmt.Errorf("failed to set hard state in memory storage: %w", err)
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
