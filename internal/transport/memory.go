package transport

import (
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type MemoryTransport struct {
	id      uint64                // 节点 ID
	network *MemoryNetwork        // 全局网络总线的引用
	inbox   <-chan raftpb.Message // 每个节点的收件箱
}

// NewMemoryTransport 创建一个新的 MemoryTransport 实例
func NewMemoryTransport(id uint64, network *MemoryNetwork) *MemoryTransport {
	inbox := network.Register(id) // 给节点注册一个收件箱
	return &MemoryTransport{
		id:      id,
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

// Close 关闭传输通道
func (t *MemoryTransport) Close() {
	t.network.Unregister(t.id) // 注销收件箱
}

// AddPeer/RemovePeer 让 MemoryTransport 也满足 raftnode.Transport 接口。
// 内存网络按节点 ID 路由，没有地址概念，新节点只要 Register 就会被自动路由到，
// 因此这两个方法对内存实现是空操作。
func (t *MemoryTransport) AddPeer(id uint64, addr string) {}
func (t *MemoryTransport) RemovePeer(id uint64)           {}
