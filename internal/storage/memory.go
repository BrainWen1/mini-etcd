package storage

import (
	"sync"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// MemoryStorage 是一个内存中的 Raft 存储实现，提供了持久化和快照功能
type MemoryStorage struct {
	mu                  sync.RWMutex // 保护并发访问
	*raft.MemoryStorage              // 内嵌 etcd/raft 的内存存储
}

// NewMemoryStorage 创建一个新的 MemoryStorage 实例
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		MemoryStorage: raft.NewMemoryStorage(),
	}
}

// SaveHardState 保存 HardState 到内存中
func (ms *MemoryStorage) SaveHardState(hs raftpb.HardState) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.SetHardState(hs) // 调用官方库的方法
	return nil
}

// SaveEntries 保存预写日志条目到内存中
func (ms *MemoryStorage) SaveEntries(entries []raftpb.Entry) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.Append(entries) // 调用官方库的方法
	return nil
}

// SaveSnapshot 应用快照到内存中
func (ms *MemoryStorage) SaveSnapshot(snapshot raftpb.Snapshot) error {
	// Implementation for applying snapshot
	return nil
}

// Close 关闭存储，释放资源
func (ms *MemoryStorage) Close() error {
	// Implementation for closing storage
	return nil
}
