package raftnode

import (
	"log"
	"mini-etcd/internal/config"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Transport 定义了 Raft 节点之间的消息传输接口
type Transport interface {
	Send(msgs []raftpb.Message)  // 发送消息给其他节点
	Recv() <-chan raftpb.Message // 接收来自其他节点的消息
	Close()                      // 关闭传输通道
}

// Storage 定义了 Raft 节点的持久化存储接口
type Storage interface {
	raft.Storage
	Save(hardState raftpb.HardState, entries []raftpb.Entry) error
	SaveSnapshot(snapshot raftpb.Snapshot) error
	Close() error
}

// Applier 定义了应用已提交日志条目的接口
type Applier interface {
	Apply(entries []raftpb.Entry) error
	ApplySnapshot(snapshot raftpb.Snapshot) error
}

// RaftNode 封装了 etcd/raft 的 Node，并添加了存储和传输的引用
type RaftNode struct {
	raft.Node
	// RaftNode 持有存储、传输和应用的接口
	storage Storage
	trans   Transport
	applier Applier
}

// NewRaftNode 创建一个新的 RaftNode 实例
func NewRaftNode(cfg config.Config, storage Storage, trans Transport, applier Applier) *RaftNode {
	// 将 cfg.Peers 转换为 raft.Peer 列表
	var rpeers []raft.Peer
	for _, p := range cfg.Peers {
		rpeers = append(rpeers, raft.Peer{ID: p})
	}

	// 从已经持久化的 Storage 中获取 lastIndex 和 HardState，以便正确初始化 Raft 节点
	// 获取当前 Storage 中最后一条日志的索引
	lastIndex, err := storage.LastIndex() // 由 raft.Storage 接口提供
	if err != nil {
		log.Fatalf("Failed to get last index: %v", err)
	}
	// InitialState 获取当前 Storage 中的 HardState
	hardState, _, err := storage.InitialState() // 由 raft.Storage 接口提供
	if err != nil {
		log.Fatalf("Failed to get initial state: %v", err)
	}

	c := &raft.Config{
		ID:              cfg.ID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		// Applied:      lastIndex, // [注释掉] 纯内存状态机重启后数据为空，必须让 Raft 从 0 开始重放日志来重建数据
	}

	var node raft.Node
	if lastIndex == 0 && raft.IsEmptyHardState(hardState) {
		// 如果没有日志和 HardState，说明是全新节点，需要启动一个新的 Raft 节点
		log.Printf("[Node %d] 启动全新 Raft 节点 (Bootstrap)", cfg.ID)
		node = raft.StartNode(c, rpeers)

		// Bootstrap 后立刻打一个初始快照，把 ConfState 持久化
		// 快照的 index 传 1（MemoryStorage初始化时自带一个空快照），data 传 nil（状态机是空的）
		// 手动构造一个包含 ConfState 的快照对象，绕过 CreateSnapshot 的内存检查
		confState := raftpb.ConfState{Voters: cfg.Peers}
		snap := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index:     1, // 设为 1 即可，只要大于初始的 0
				Term:      1,
				ConfState: confState,
			},
		}
		// 直接调用 SaveSnapshot 落盘，当前进程不 Apply 这个快照，所以不会和 StartNode 冲突
		if err := storage.SaveSnapshot(snap); err != nil {
			log.Fatalf("Failed to save initial snapshot: %v", err)
		}
		log.Printf("[Node %d] 已生成初始快照文件 (index=1)，ConfState.Voters=%v", cfg.ID, cfg.Peers)
	} else {
		// 如果有日志或 HardState，说明是重启节点，需要从持久化存储中恢复
		log.Printf("[Node %d] 检测到 WAL，重新启动 Raft 节点 (Restart)", cfg.ID)
		node = raft.RestartNode(c)
	}

	return &RaftNode{
		Node:    node,
		storage: storage,
		trans:   trans,
		applier: applier,
	}
}

// Step 处理来自其他节点的消息
// Propose 向 Raft 节点发起提议，data 是要提交的数据
// Tick 触发 Raft 节点的心跳和选举超时
// Ready 返回一个 channel，当 Raft 节点有 Ready 状态时会发送通知
// Advance 通知 Raft 节点已经处理完 Ready 状态
// Stop 停止 Raft 节点
