package transport

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type MemoryTransport struct {
	network *MemoryNetwork      // 全局网络总线的引用
	inbox   chan raftpb.Message // 每个节点的收件箱
}

// NewMemoryTransport 创建一个新的 MemoryTransport 实例
func NewMemoryTransport(id uint64, network *MemoryNetwork) *MemoryTransport {
	inbox := network.Register(id) // 给节点注册一个收件箱
	return &MemoryTransport{
		network: network,
		inbox:   inbox,
	}
}

// 实现 Send 接口：把消息扔给全局网络总线
func (t *MemoryTransport) Send(msgs []raftpb.Message) {
	t.network.Route(msgs)
}

// 实现 Recv 接口：返回自己的收件箱供循环读取
func (t *MemoryTransport) Recv() <-chan raftpb.Message {
	return t.inbox
}
