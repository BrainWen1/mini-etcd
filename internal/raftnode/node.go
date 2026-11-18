package raftnode

import (
	"log"
	"mini-etcd/internal/config"
	"slices"

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
	ApplySnapshot(snapshot raftpb.Snapshot) error
	CreateSnapshot(index uint64, confState *raftpb.ConfState, data []byte) (raftpb.Snapshot, error)
	Compact(index uint64) error
	Close() error
}

// Applier 定义了应用已提交日志条目的接口
type Applier interface {
	Apply(entries []raftpb.Entry) error
	GetConsistentIndex() (uint64, error)
	ApplySnapshot(snapshot raftpb.Snapshot) error
	GetSnapshot() ([]byte, error)
}

// RaftNode 封装了 etcd/raft 的 Node，并添加了存储和传输的引用
type RaftNode struct {
	raft.Node
	// RaftNode 持有存储、传输和应用的接口
	storage Storage
	trans   Transport
	applier Applier

	lastSnapshotIndex uint64           // 记录上一次快照的索引
	confState         raftpb.ConfState // 记录当前集群的配置状态
}

// NewRaftNode 创建一个新的 RaftNode 实例
func NewRaftNode(cfg config.Config, storage Storage, trans Transport, applier Applier) *RaftNode {
	// 将 cfg.Peers 转换为 raft.Peer 列表。
	// 必须按节点 ID 升序构造：raft.StartNode 会按这个顺序生成初始的 ConfChange
	// 日志条目，所有节点必须以完全相同的顺序 Bootstrap。直接遍历 map 的顺序是
	// 随机的，会让各节点在相同日志下标上得到不同内容，破坏 Raft 的日志一致性前提。
	peersIDs := make([]uint64, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		peersIDs = append(peersIDs, id)
	}
	slices.Sort(peersIDs)

	rpeers := make([]raft.Peer, 0, len(peersIDs))
	for _, id := range peersIDs {
		rpeers = append(rpeers, raft.Peer{ID: id, Context: []byte(cfg.Peers[id])})
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

	// 从 bbolt 存储中获取当前已应用的最高日志索引（appliedIndex），用于初始化 Raft 节点的 Applied 字段
	appliedIndex, err := applier.GetConsistentIndex()
	if err != nil {
		log.Fatalf("Failed to get applied index: %v", err)
	}

	c := &raft.Config{
		ID:              cfg.ID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Applied:         appliedIndex,
	}

	var node raft.Node
	var confState raftpb.ConfState

	if lastIndex == 0 && raft.IsEmptyHardState(hardState) {
		// 如果没有日志和 HardState，说明是全新节点，需要启动一个新的 Raft 节点
		log.Printf("[Node %d] 启动全新 Raft 节点 (Bootstrap)", cfg.ID)
		node = raft.StartNode(c, rpeers)

		// Bootstrap 后立刻打一个初始快照，把 ConfState 持久化
		// 快照的 index 传 1（MemoryStorage初始化时自带一个空快照），data 传 nil（状态机是空的）
		// 手动构造一个包含 ConfState 的快照对象，绕过 CreateSnapshot 的内存检查
		confState = raftpb.ConfState{Voters: peersIDs}
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
		log.Printf("[Node %d] 已生成初始快照文件 (index=1)，ConfState.Voters=%v", cfg.ID, peersIDs)
	} else {
		// 如果有日志或 HardState，说明是重启节点，需要从持久化存储中恢复
		log.Printf("[Node %d] 检测到 WAL，重新启动 Raft 节点 (Restart)", cfg.ID)
		node = raft.RestartNode(c)

		// Restart 时，从 Storage 中读取 ConfState
		_, cs, err := storage.InitialState()
		if err != nil {
			log.Fatalf("Failed to get initial state: %v", err)
		}
		if len(cs.Voters) != 0 {
			// 如果有 Voters，说明是正常的集群配置，记录下来
			confState = cs
		} else {
			// 快照里没带 ConfState（例如旧版本写出的快照，或者快照目录被清空只剩 WAL），
			// 若让 confState 保持零值，后续本地快照会把空的 ConfState 写进磁盘，
			// 重启后 voters=() 会让集群配置永久丢失。这里回退到配置里的 peers 兜底。
			log.Printf("[Node %d] Warning: empty ConfState in storage, falling back to configured peers %v", cfg.ID, peersIDs)
			confState = raftpb.ConfState{Voters: peersIDs}
		}
	}

	// 返回 RaftNode 前获取一下当前快照索引
	snap, err := storage.Snapshot()
	if err != nil {
		log.Fatalf("Failed to get snapshot: %v", err)
	}
	if snap.Metadata.Index == 0 {
		log.Printf("[Node %d] 没有找到快照", cfg.ID)
	}

	return &RaftNode{
		Node:              node,
		storage:           storage,
		trans:             trans,
		applier:           applier,
		lastSnapshotIndex: snap.Metadata.Index,
		confState:         confState, // 使用从 Storage 中读取的 ConfState 初始化
	}
}

// Step 处理来自其他节点的消息
// Propose 向 Raft 节点发起提议，data 是要提交的数据
// Tick 触发 Raft 节点的心跳和选举超时
// Ready 返回一个 channel，当 Raft 节点有 Ready 状态时会发送通知
// Advance 通知 Raft 节点已经处理完 Ready 状态
// Stop 停止 Raft 节点
