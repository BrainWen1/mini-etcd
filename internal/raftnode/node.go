package raftnode

import (
	"mini-etcd/internal/config"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Transport 定义了 Raft 节点之间的消息传输接口
type Transport interface {
	Send(msgs []raftpb.Message)  // 发送消息给其他节点
	Recv() <-chan raftpb.Message // 接收来自其他节点的消息
}

// RaftNode 封装了 etcd/raft 的 Node，并添加了存储和传输的引用
type RaftNode struct {
	raft.Node
	storage *raft.MemoryStorage
	trans   Transport
}

// NewRaftNode 创建一个新的 RaftNode 实例
func NewRaftNode(cfg config.Config, storage *raft.MemoryStorage, trans Transport) *RaftNode {
	var rpeers []raft.Peer
	for _, p := range cfg.Peers {
		rpeers = append(rpeers, raft.Peer{ID: p})
	}

	c := &raft.Config{
		ID:              cfg.ID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	return &RaftNode{
		Node:    raft.StartNode(c, rpeers),
		storage: storage,
		trans:   trans,
	}
}

// Step 处理来自其他节点的消息
// Propose 向 Raft 节点发起提议，data 是要提交的数据
// Tick 触发 Raft 节点的心跳和选举超时
// Ready 返回一个 channel，当 Raft 节点有 Ready 状态时会发送通知
// Advance 通知 Raft 节点已经处理完 Ready 状态
// Stop 停止 Raft 节点
