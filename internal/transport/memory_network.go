package transport

import (
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// MemoryNetwork 是整个集群共享的内存总线
type MemoryNetwork struct {
	mu      sync.RWMutex
	inboxes map[uint64]chan raftpb.Message // 全局 Map: 节点ID -> 该节点的收件箱
}

// NewMemoryNetwork 创建一个新的 MemoryNetwork 实例
func NewMemoryNetwork() *MemoryNetwork {
	return &MemoryNetwork{
		inboxes: make(map[uint64]chan raftpb.Message),
	}
}

// Register 给节点分配一个收件箱
func (n *MemoryNetwork) Register(id uint64) <-chan raftpb.Message {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch := make(chan raftpb.Message, 1000) // 带缓冲，防止阻塞
	n.inboxes[id] = ch
	return ch
}

// Unregister 移除节点的收件箱
func (n *MemoryNetwork) Unregister(id uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 从全局 Map 中删除节点的收件箱
	// 不关闭通道，因为可能还有其他 goroutine 正在向该通道发送消息。
	delete(n.inboxes, id)
}

// Route 路由消息：把消息投递给目标节点
func (n *MemoryNetwork) Route(msgs []raftpb.Message) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, m := range msgs {
		if ch, ok := n.inboxes[m.To]; ok {
			// 使用 select 语句尝试发送消息到通道，如果通道已满，则丢弃消息以避免阻塞
			select {
			case ch <- m:
			default:
				// 如果通道已满，丢弃消息以避免阻塞
				// 可以在这里添加日志记录或其他处理逻辑
			}
		}
	}
}
