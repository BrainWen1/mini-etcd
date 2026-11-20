package raftnode

import (
	"encoding/binary"
	"log"
	"mini-etcd/internal/config"
	"slices"
	"sync"

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

	leaderID uint64       // 当前节点认为的 Leader ID
	mu       sync.RWMutex // 保护 leaderID 的读写锁

	// readIndexChannels / readIndexWaiters 的 key 是 ReadIndex 的 RequestCtx（由 ReadIndexKey 生成），
	// 必须和传给 Node.ReadIndex 的字节完全一致，Raft 才能把 ReadState 对回本地 waiter。
	readIndexChannels map[string]chan struct{} // ReadIndex waiter 的通知 channel
	readIndexWaiters  map[string]uint64        // 尚未追平目标的 ReadIndex waiter -> 目标索引
	readIndexMu       sync.Mutex               // 保护 readIndexChannels 和 readIndexWaiters
	appliedIndex      uint64                   // 状态机当前已应用的最高日志索引
}

// readIndexKeyLen 是 ReadIndexKey 生成的 RequestCtx 长度：8 字节节点 ID + 8 字节进程内序号。
const readIndexKeyLen = 16

// ReadIndexKey 构造 ReadIndex 请求的 RequestCtx。
//
// etcd/raft 的 readOnly 用 RequestCtx 的字节串作为 pendingReadIndex 的 key 去重，
// 并把它原样回显到 MsgReadIndexResp。如果只用节点内自增序号，不同 follower 发往同一个
// Leader 的并发读请求会撞到同一个 key：后到的请求被静默丢弃，既不进队列也收不到响应，
// 对应的 GET 只能等到超时。把节点 ID 编进 key 才能保证集群范围内唯一。
func ReadIndexKey(nodeID, seq uint64) string {
	buf := make([]byte, readIndexKeyLen)
	binary.BigEndian.PutUint64(buf[:8], nodeID)
	binary.BigEndian.PutUint64(buf[8:], seq)
	return string(buf)
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
		leaderID:          0,         // 初始化为 0，表示尚未确定 Leader
		readIndexChannels: make(map[string]chan struct{}),
		readIndexWaiters:  make(map[string]uint64),
		// 重启后状态机已经停在 consistentIndex，这里必须同步初始化：
		// 否则 appliedIndex 从 0 开始，ReadIndex 会一直判定“未追平”，
		// 而重启后不会再有 CommittedEntries 来推进它，follower 上的读只能等到超时。
		appliedIndex: appliedIndex,
	}
}

// Step 处理来自其他节点的消息
// Propose 向 Raft 节点发起提议，data 是要提交的数据
// Tick 触发 Raft 节点的心跳和选举超时
// Ready 返回一个 channel，当 Raft 节点有 Ready 状态时会发送通知
// Advance 通知 Raft 节点已经处理完 Ready 状态
// Stop 停止 Raft 节点

// GetLeaderID 返回当前节点认为的 Leader ID
func (rn *RaftNode) GetLeaderID() uint64 {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.leaderID
}

// RegisterReadIndexWaiter 注册一个 ReadIndex waiter，返回一个 channel，当 Raft 追平 ReadIndex 时会关闭它。
// key 必须是传给 Node.ReadIndex 的 RequestCtx（由 ReadIndexKey 生成）。
// 必须先注册再调用 ReadIndex，否则 ReadState 可能先于注册到达，客户端只能等到超时。
func (rn *RaftNode) RegisterReadIndexWaiter(key string) <-chan struct{} {
	rn.readIndexMu.Lock()
	defer rn.readIndexMu.Unlock()
	ch := make(chan struct{})
	rn.readIndexChannels[key] = ch
	return ch
}

// RemoveReadIndexWaiter 移除一个 ReadIndex waiter，用于客户端超时或读取完成后清理。
func (rn *RaftNode) RemoveReadIndexWaiter(key string) {
	rn.readIndexMu.Lock()
	defer rn.readIndexMu.Unlock()
	delete(rn.readIndexChannels, key)
	delete(rn.readIndexWaiters, key)
}

// resolveReadIndexLocked 唤醒并清理一个 ReadIndex waiter。调用方必须持有 readIndexMu。
// 客户端可能已经超时并调用过 RemoveReadIndexWaiter，此时 channel 不存在，直接跳过。
func (rn *RaftNode) resolveReadIndexLocked(key string) {
	if ch, ok := rn.readIndexChannels[key]; ok {
		close(ch) // 关闭而不是发送：状态机已追平，读方可以安全读取
		delete(rn.readIndexChannels, key)
	}
	delete(rn.readIndexWaiters, key)
}
