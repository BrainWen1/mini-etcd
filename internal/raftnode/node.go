package raftnode

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"mini-etcd/internal/config"
	"mini-etcd/internal/types"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Transport 定义了 Raft 节点之间的消息传输接口
type Transport interface {
	Send(msgs []raftpb.Message)     // 发送消息给其他节点
	Recv() <-chan raftpb.Message    // 接收来自其他节点的消息
	Close()                         // 关闭传输通道
	AddPeer(id uint64, addr string) // 添加新的节点到传输通道
	RemovePeer(id uint64)           // 从传输通道移除节点
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
	Truncate(index uint64) error
}

// Applier 定义了应用已提交日志条目的接口
type Applier interface {
	Apply(entries []raftpb.Entry) error
	GetConsistentIndex() (uint64, error)
	AdvanceConsistentIndex(index uint64) error
	ApplySnapshot(snapshot raftpb.Snapshot) error
	GetSnapshot() ([]byte, error)

	// 成员表随状态机快照一起复制：ConfState 只有 ID，地址必须由状态机保存，
	// 否则落后节点通过快照追赶时无法恢复新成员的地址。
	UpsertMember(id uint64, raftAddr, grpcAddr string) (config.Member, error)
	RemoveMember(id uint64) error
	GetMembers() (map[uint64]config.Member, error)

	GetExpiredLeases() ([]int64, error) // 扫描状态机里过期的 LeaseID
}

// MembershipChangeHandler 在一条配置变更日志被本地应用后回调。
// added=true 表示新增/更新成员，added=false 表示移除成员。
// Server 用它同步 Leader 转发所需的 GRPCPeers，并持久化成员表 members（同时包含 Raft 和 gRPC 地址）。
type MembershipChangeHandler func(id uint64, raftAddr, grpcAddr string, added bool)

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

	// readBatches 的 key 是 ReadIndex 的 RequestCtx（由 ReadIndexKey 生成），
	// 必须和传给 Node.ReadIndex 的字节完全一致，Raft 才能把 ReadState 对回本地批次。
	readRequests chan *readIndexRequest     // GET 提交的读请求，由 readIndexLoop 合并成批次
	readBatches  map[string]*readIndexBatch // 已提交给 Raft、等待 ReadState 或追平的批次
	readIndexMu  sync.Mutex                 // 保护 readBatches
	readSeq      atomic.Uint64              // 生成 RequestCtx 中的进程内序号
	appliedIndex uint64                     // 状态机当前已应用的最高日志索引

	nodeID uint64        // 本节点 ID，用于构造集群范围内唯一的 RequestCtx
	stopc  chan struct{} // Run 退出时关闭，唤醒阻塞中的读请求

	// 动态成员变更相关
	membershipHandler MembershipChangeHandler // 配置变更应用后的回调，由 Server 在 Run 之前注入
	confChangeMu      sync.Mutex              // 保护 confChangeWaiters
	confChangeWaiters map[string][]chan error // key 为 "type-nodeID"，等待本地应用配置变更的提议者
	removed           chan struct{}           // 本节点被移出集群时关闭
	removedOnce       sync.Once               // 保证 removed 只关闭一次

	// members 是本地已知的成员表（含两种地址），与状态机里的成员表保持一致。
	// 应用快照后用它做 diff，把 Transport 和 Leader 转发表更新到快照对应的成员配置。
	membersMu sync.RWMutex
	members   map[uint64]config.Member
}

// readIndexKeyLen 是 ReadIndexKey 生成的 RequestCtx 长度：8 字节节点 ID + 8 字节进程内序号。
const readIndexKeyLen = 16

// readIndexTimeout 是一个 ReadIndex 批次从发起到收到 ReadState 的总预算。
// 与 gRPC 层单次请求的超时保持一致：底层已经放弃的读，上层也没必要再等。
const readIndexTimeout = 5 * time.Second

// readIndexAttemptTimeout 是单次 ReadIndex 尝试等待 ReadState 的时间。
// Raft 在节点还没有 Leader（刚重启、正在选举）时会静默丢弃 MsgReadIndex、
// 不回任何响应；只等一个短窗口就重试，可以让读请求在 Leader 稳定后立刻恢复，
// 而不是一直等到批次总超时。
const readIndexAttemptTimeout = 2 * time.Second

// readIndexRetryDelay 是两次 ReadIndex 尝试之间的间隔
const readIndexRetryDelay = 50 * time.Millisecond

// readIndexRequest 是一次 GET 的线性一致读请求。
type readIndexRequest struct {
	assigned chan *readIndexBatch // readIndexLoop 投递请求所属的批次，容量为 1
}

// readIndexBatch 把同一时间窗口内的并发读合并成一次 Raft ReadIndex 调用。
//
// etcd/raft 对每一次 ReadIndex 都会向所有 peer 广播一轮心跳（ReadOnlySafe），
// 逐请求调用会让消息量、以及单线程 Raft 循环的处理量随读 QPS 线性增长；
// 合并成批次后，一批读共享同一轮心跳。
type readIndexBatch struct {
	key         string
	responded   chan struct{} // Raft 循环收到 ReadState 后关闭，readIndexLoop 据此收集下一批
	done        chan struct{} // 本地 appliedIndex 追平目标索引后关闭，读请求据此放行
	target      uint64        // ReadState 给出的目标索引
	count       int           // 批内请求数，仅用于日志
	respondedOK bool          // responded 是否已关闭（只在 readIndexMu 下访问）
	resolved    bool          // 批次是否已经唤醒或已失败（只在 readIndexMu 下访问）
	err         error         // 失败原因，写入后立即关闭 done
}

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
		// Context 里同时带上 Raft 与 gRPC 地址：Bootstrap 产生的初始 ConfChange
		// 条目会被 Run 循环当作普通配置变更应用，所有节点都能据此填好成员表。
		rpeers = append(rpeers, raft.Peer{
			ID:      id,
			Context: config.EncodeMemberContext(cfg.Peers[id], cfg.GRPCPeers[id]),
		})
	}

	// 从已经持久化的 Storage 中获取 lastIndex、HardState 和快照 ConfState
	// 获取当前 Storage 中最后一条日志的索引
	lastIndex, err := storage.LastIndex() // 由 raft.Storage 接口提供
	if err != nil {
		log.Fatalf("Failed to get last index: %v", err)
	}
	// InitialState 获取当前 Storage 中的 HardState 和快照携带的 ConfState。
	// 注意 ConfState 只来自快照元数据，不来自 WAL 里的配置变更日志。
	hardState, snapshotConfState, err := storage.InitialState()
	if err != nil {
		log.Fatalf("Failed to get initial state: %v", err)
	}
	// 当前磁盘快照。必须在构造 raft 节点之前读取：
	// 它同时决定 raft 重启时的重放起点和 rn.lastSnapshotIndex 的初值。
	snap, err := storage.Snapshot()
	if err != nil {
		log.Fatalf("Failed to get snapshot: %v", err)
	}

	// 从 bbolt 存储中获取状态机已经应用到的日志索引（consistentIndex）
	appliedIndex, err := applier.GetConsistentIndex()
	if err != nil {
		log.Fatalf("Failed to get applied index: %v", err)
	}

	// 状态机里有数据、但 Raft 日志为空：raft.newRaft 会在 appliedTo() 直接 panic
	// （applied(N) is out of range [prevApplied(0), committed(0)]）。
	// 常见原因是复用了其他节点的 backend 目录、误删了 WAL 目录，或者以 -join 启动时
	// 没有清空旧数据。这里提前给出可操作的错误，而不是让进程在 raft 内部崩溃。
	if lastIndex == 0 && raft.IsEmptyHardState(hardState) && appliedIndex > 0 {
		log.Fatalf("[Node %d] 本地状态机已有数据 (consistentIndex=%d)，但 Raft 日志为空；请清空该节点数据目录（或确认没有复用其他节点的 backend）后重试",
			cfg.ID, appliedIndex)
	}

	// 全新集群（Bootstrap）：空 Storage、空 HardState，且没有 -join。
	isBootstrap := lastIndex == 0 && raft.IsEmptyHardState(hardState) && !cfg.Join

	// 交给 raft 的 Applied 不能直接用 consistentIndex。
	//
	// 快照里的 ConfState 只记录到 snapshot.Metadata.Index 为止的成员配置；
	// 而 (snapshotIndex, consistentIndex] 之间完全可能存在配置变更日志
	// （AddNode/RemoveNode，以及 Leader 当选时的空日志）。如果告诉 raft
	// “这些日志都已应用”，它既不会重放它们，ConfState 里也不会包含这些成员，
	// 重启后集群成员配置会永久回退：例如刚扩到 4 节点，重启后退回 3 节点的
	// 多数派判断，甚至完全不知道新节点的存在。
	//
	// 所以这里传快照索引，让 raft 从快照之后开始重放日志。状态机的幂等检查
	// （appliedIndex <= consistentIndex 时跳过）保证业务数据不会被重复写入，
	// 配置变更则会被重新应用，raft 内存中的成员配置随之恢复。
	raftApplied := uint64(0)
	if !isBootstrap && snap.Metadata.Index > 0 {
		raftApplied = snap.Metadata.Index
	}

	c := &raft.Config{
		ID:              cfg.ID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Applied:         raftApplied,
		// PreVote 对动态缩容很关键：被移除的节点可能还没收到“自己被移除”的日志
		// （Leader 在应用配置变更后就不再给它发消息了），它会继续发起选举。
		// 没有 PreVote 时，每次选举都会把集群的 term 抬高一任，逼现有 Leader 下台，
		// 期间写请求会返回 Unavailable；
		// 开启后，其他节点会拒绝它的预投票，集群 term 不会被打断，已移除节点也无法靠更大的 term 干扰集群。
		PreVote: true,
	}

	var node raft.Node
	var confState raftpb.ConfState

	if isBootstrap {
		// 如果没有日志和 HardState，说明是全新节点，需要启动一个新的 Raft 节点
		log.Printf("[Node %d] 启动全新 Raft 节点 (Bootstrap)", cfg.ID)
		node = raft.StartNode(c, rpeers)

		// Bootstrap 后立刻打一个初始快照，把 ConfState 持久化。
		// 快照的 index 传 1（MemoryStorage 初始化时自带一个空快照），data 传 nil（状态机是空的）。
		// 手动构造一个包含 ConfState 的快照对象，绕过 CreateSnapshot 的内存检查。
		confState = raftpb.ConfState{Voters: peersIDs}
		initialSnap := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index:     1, // 设为 1 即可，只要大于初始的 0
				Term:      1,
				ConfState: confState,
			},
		}
		// 直接调用 SaveSnapshot 落盘，当前进程不 Apply 这个快照，所以不会和 StartNode 冲突
		if err := storage.SaveSnapshot(initialSnap); err != nil {
			log.Fatalf("Failed to save initial snapshot: %v", err)
		}
		log.Printf("[Node %d] 已生成初始快照文件 (index=1)，ConfState.Voters=%v", cfg.ID, peersIDs)
	} else {
		if cfg.Join && lastIndex == 0 && raft.IsEmptyHardState(hardState) {
			// 加入已有集群的新节点：本地没有任何 Raft 状态，也不应该 Bootstrap
			// （Bootstrap 会让它自成一个集群）。用 RestartNode 以空状态启动，
			// 等待 Leader 把成员变更日志或快照复制过来。
			log.Printf("[Node %d] 以空状态启动，等待加入已有集群 (Join)", cfg.ID)
			node = raft.RestartNode(c)
			confState = raftpb.ConfState{}
		} else {
			// 如果有日志或 HardState，说明是重启节点，需要从持久化存储中恢复
			log.Printf("[Node %d] 检测到 WAL，重新启动 Raft 节点 (Restart)", cfg.ID)
			node = raft.RestartNode(c)

			// ConfState 只能从快照元数据恢复；快照之后发生的配置变更，
			// 由 raft 从快照索引开始重放 WAL 时通过 ApplyConfChange 重建。
			// 为空说明本地没有快照：新加入的节点在收到第一份快照之前重启，
			// 会退化为“WAL 里有从 1 开始的完整日志”，重放后同样能恢复配置；
			// 如果 WAL 本身也是空的，下面的空成员配置会让它无法参与选举。
			confState = snapshotConfState
			if len(confState.Voters) == 0 {
				log.Printf("[Node %d] 快照中没有 ConfState，将依赖 WAL 重放配置变更来重建成员配置", cfg.ID)
			}
		}
	}

	if snap.Metadata.Index == 0 {
		log.Printf("[Node %d] 没有找到快照", cfg.ID)
	}

	// 启动时的成员表。Transport 运行时会直接修改 cfg.Peers，所以这里构造一份
	// 独立的 Member 记录；之后每次应用 ConfChange / 快照都会覆盖它。
	members := make(map[uint64]config.Member, len(cfg.Peers))
	for id, raftAddr := range cfg.Peers {
		members[id] = config.Member{ID: id, RaftAddr: raftAddr, GRPCAddr: cfg.GRPCPeers[id]}
	}

	return &RaftNode{
		Node:              node,
		storage:           storage,
		trans:             trans,
		applier:           applier,
		lastSnapshotIndex: snap.Metadata.Index,
		confState:         confState, // 使用从 Storage 中读取的 ConfState 初始化
		leaderID:          0,         // 初始化为 0，表示尚未确定 Leader
		nodeID:            cfg.ID,
		readRequests:      make(chan *readIndexRequest, 64),
		readBatches:       make(map[string]*readIndexBatch),
		stopc:             make(chan struct{}),
		// 重启后状态机已经停在 consistentIndex，这里必须同步初始化：
		// 否则 appliedIndex 从 0 开始，ReadIndex 会一直判定“未追平”，
		// 而重启后不会再有 CommittedEntries 来推进它，follower 上的读只能等到超时。
		appliedIndex: appliedIndex,

		confChangeWaiters: make(map[string][]chan error),
		removed:           make(chan struct{}),
		members:           members,
	}
}

// SetMembershipChangeHandler 注入配置变更回调。必须在 Run 之前调用。
func (rn *RaftNode) SetMembershipChangeHandler(h MembershipChangeHandler) {
	rn.mu.Lock()
	rn.membershipHandler = h
	rn.mu.Unlock()
}

func (rn *RaftNode) getMembershipChangeHandler() MembershipChangeHandler {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.membershipHandler
}

// Removed 返回一个在本节点被移出集群时关闭的 channel。
// main 用它触发优雅退出，而不是在 Raft 循环里直接 os.Exit。
func (rn *RaftNode) Removed() <-chan struct{} {
	return rn.removed
}

func (rn *RaftNode) markRemoved() {
	rn.removedOnce.Do(func() { close(rn.removed) })
}

// applyMembershipChange 处理一条已经提交、并且已经调用过 ApplyConfChange 的配置变更。
//
// 处理顺序很重要：
//  1. 先写状态机 —— 成员表必须随快照一起复制，否则落后节点通过快照追赶时
//     只能拿到 ConfState 里的节点 ID，拿不到 Raft/gRPC 地址；
//  2. 再改内存里的 members 表；
//  3. 最后同步 Transport 与 Server 的 Leader 转发表和持久化成员表。
func (rn *RaftNode) applyMembershipChange(cc *raftpb.ConfChange, member config.Member) {
	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeUpdateNode:
		// 写状态机：UpsertMember 会返回实际写入的成员（可能是更新了地址），
		// 以便更新内存里的 members 表和 Transport。
		effective, err := rn.applier.UpsertMember(cc.NodeID, member.RaftAddr, member.GRPCAddr)
		if err != nil {
			// 状态机写失败意味着成员表无法进入后续快照，重启/新节点追赶时
			// 一定会再次出现“找不到新成员地址”的问题，继续运行只会扩大不一致。
			rn.storage.Close()
			log.Fatalf("[Node %d] 持久化成员 %d 失败: %v", rn.nodeID, cc.NodeID, err)
		}

		rn.membersMu.Lock()
		// 更新内存里的成员表
		rn.members[cc.NodeID] = effective
		rn.membersMu.Unlock()

		if effective.RaftAddr == "" {
			log.Printf("[Node %d] Warning: 成员 %d 没有 Raft 地址，Transport 无法向它发送消息", rn.nodeID, cc.NodeID)
		} else {
			// 更新 Transport 的 peer 表：如果是更新地址，AddPeer 会覆盖旧地址
			rn.trans.AddPeer(cc.NodeID, effective.RaftAddr)
		}
		log.Printf(">>> 集群成员新增/更新: Node %d, RaftAddr: %s, GRPCAddr: %s",
			cc.NodeID, effective.RaftAddr, effective.GRPCAddr)
		if h := rn.getMembershipChangeHandler(); h != nil {
			// 通知 Server 更新 Leader 转发表和持久化成员表 members（同时包含 Raft 和 gRPC 地址）
			h(cc.NodeID, effective.RaftAddr, effective.GRPCAddr, true)
		}

	case raftpb.ConfChangeRemoveNode:
		if err := rn.applier.RemoveMember(cc.NodeID); err != nil {
			rn.storage.Close()
			log.Fatalf("[Node %d] 删除成员 %d 失败: %v", rn.nodeID, cc.NodeID, err)
		}

		rn.membersMu.Lock()
		delete(rn.members, cc.NodeID)
		rn.membersMu.Unlock()

		log.Printf(">>> 集群成员移除: Node %d", cc.NodeID)
		rn.trans.RemovePeer(cc.NodeID)
		if h := rn.getMembershipChangeHandler(); h != nil {
			h(cc.NodeID, "", "", false)
		}
		// 如果移除的是自己，通知 main 优雅退出。
		// 不要在 Raft 循环里直接 os.Exit：那会跳过 WAL/传输层的正常关闭流程。
		if cc.NodeID == rn.nodeID {
			log.Printf(">>> 当前节点已被移除出集群，准备退出...")
			rn.markRemoved()
		}

	default:
		log.Printf("[Node %d] 未知的配置变更类型: %s", rn.nodeID, cc.Type)
	}
}

// syncMembershipFromSnapshot 在本地应用一份 Raft 快照后，把快照里的 ConfState 同步到内存里的成员表，并更新 Transport 与 Server 的转发表
//
// 这是“节点离线期间集群又发生变化”场景的关键修复。
// 例如节点 4 掉线期间节点 5 加入集群，之后节点 4 恢复：
//   - Leader 里关于节点 5 的 ConfChange 日志可能已经被压缩；
//   - 节点 4 只能收到快照，而快照的 ConfState 只有节点 ID，没有地址；
//   - 没有本方法时，节点 4 既不会给节点 5 发 Raft 消息（GET 的 ReadIndex 需要
//     发给 Leader），Leader 转发表里也没有节点 5 的 gRPC 地址（写请求会被拒绝（Unavailable））。
//
// 成员地址随状态机快照一起复制，所以 ApplySnapshot 之后从状态机读出来的
// 成员表就是快照那一刻的完整成员配置。
func (rn *RaftNode) syncMembershipFromSnapshot(confState raftpb.ConfState) {
	// 从状态机里读取成员表，和快照里的 ConfState 对比，更新内存里的 members 表。
	// 同时包含 Raft 和 gRPC 地址
	stored, err := rn.applier.GetMembers()
	if err != nil {
		rn.storage.Close()
		log.Fatalf("[Node %d] 读取状态机中的成员表失败: %v", rn.nodeID, err)
	}

	// ConfState 是快照时刻的权威成员集合（可能包含联合配置的 outgoing/learner）。
	// 状态机里理论上与它一致；以 ConfState 为准可以顺手清掉任何残留。
	wanted := make(map[uint64]struct{},
		len(confState.Voters)+len(confState.VotersOutgoing)+len(confState.Learners)+len(confState.LearnersNext))
	for _, ids := range [][]uint64{
		confState.Voters,
		confState.VotersOutgoing,
		confState.Learners,
		confState.LearnersNext,
	} {
		for _, id := range ids {
			wanted[id] = struct{}{} // 拿到快照里的所有成员 ID
		}
	}

	next := make(map[uint64]config.Member, len(wanted))
	for id := range wanted {
		// 根据从 confState 里拿到的 ID 去状态机里查地址，更新内存里的 members 表
		m, ok := stored[id]
		if !ok {
			// 本项目生成快照时，数据与成员表在同一个状态机快照里，二者必须一致。
			// 走到这里说明状态机数据损坏或被人为改写过，缺失的地址无法凭空恢复。
			log.Printf("[Node %d] Warning: 快照 ConfState 包含节点 %d，但状态机成员表里没有它的地址",
				rn.nodeID, id)
			m = config.Member{ID: id}
		}
		next[id] = m
	}

	rn.membersMu.Lock()
	// 更新内存里的成员表，供后续的配置变更提议和快照 diff 使用。
	prev := rn.members
	rn.members = next
	rn.membersMu.Unlock()

	// 新增或地址发生变化的成员：更新 Transport 与 Server 的转发表。
	for id, m := range next {
		if m.RaftAddr != "" {
			rn.trans.AddPeer(id, m.RaftAddr)
		}
		old, existed := prev[id]
		if !existed || old.RaftAddr != m.RaftAddr || old.GRPCAddr != m.GRPCAddr {
			// 成员地址发生变化，需要更新转发表
			log.Printf("[Node %d] 从快照同步成员: Node %d, RaftAddr: %s, GRPCAddr: %s",
				rn.nodeID, id, m.RaftAddr, m.GRPCAddr)
			if h := rn.getMembershipChangeHandler(); h != nil {
				h(id, m.RaftAddr, m.GRPCAddr, true)
			}
		}
	}
	// 快照里已经不存在的成员：断开连接并从转发表删除。
	for id := range prev {
		if _, ok := next[id]; !ok {
			rn.trans.RemovePeer(id)
			if h := rn.getMembershipChangeHandler(); h != nil {
				h(id, "", "", false)
			}
		}
	}
}

// Membership 返回当前成员表的副本（供诊断/测试使用）。
func (rn *RaftNode) Membership() map[uint64]config.Member {
	rn.membersMu.RLock()
	defer rn.membersMu.RUnlock()
	return maps.Clone(rn.members)
}

// confChangeKey 用于把提议和它最终被应用的那条日志对上。
// raftpb.ConfChange 没有请求 ID，但 Raft 同一时间只允许一个未完成的配置变更，
// 因此 type + nodeID 足以唯一标识一次提议。
func confChangeKey(cc *raftpb.ConfChange) string {
	return fmt.Sprintf("%d-%d", cc.Type, cc.NodeID)
}

// ProposeConfChangeAndWait 提交配置变更，并阻塞到该变更在本地被应用。
//
// 只调用 Node.ProposeConfChange 只能保证“已入队”，不能保证“已提交”。
// 如果 gRPC 层此时就返回成功，Leader 可能在变更丢失/超时后仍然告诉客户端成功。
func (rn *RaftNode) ProposeConfChangeAndWait(ctx context.Context, cc *raftpb.ConfChange) error {
	key := confChangeKey(cc) // cc.Type + cc.NodeID 唯一标识一次配置变更提议
	ch := make(chan error, 1)

	rn.confChangeMu.Lock()
	// 记录提议者的等待 channel，等到本地应用了这条配置变更后再唤醒它。
	rn.confChangeWaiters[key] = append(rn.confChangeWaiters[key], ch)
	rn.confChangeMu.Unlock()

	removeWaiter := func() {
		rn.confChangeMu.Lock()
		waiters := rn.confChangeWaiters[key]
		for i, w := range waiters {
			if w == ch {
				// [0, i) + [i+1, end) 拼接成新的 slice，去掉 ch
				// 不能直接用 copy，因为 ch 可能在别的 goroutine 被 close，导致 panic
				waiters = append(waiters[:i], waiters[i+1:]...)
				break
			}
		}
		if len(waiters) == 0 {
			// 如果没有其他提议者在等这条配置变更，直接删除这个 key
			delete(rn.confChangeWaiters, key)
		} else {
			// 还有其他提议者在等这条配置变更，更新 slice
			rn.confChangeWaiters[key] = waiters
		}
		rn.confChangeMu.Unlock()
	}

	// ProposeConfChange 只保证“已入队”，不能保证“已提交”，所以需要阻塞等待本地应用
	if err := rn.Node.ProposeConfChange(ctx, cc); err != nil {
		removeWaiter() // 如果入队失败，直接移除等待者，避免内存泄漏
		return err
	}

	select {
	case err := <-ch: // 等到本地应用了这条配置变更后，提议者会收到通知
		return err
	case <-ctx.Done():
		removeWaiter()
		return ctx.Err()
	case <-rn.stopc:
		removeWaiter()
		return raft.ErrStopped
	}
}

// resolveConfChangeWaiters 在本地应用一条配置变更后唤醒对应的提议者。
func (rn *RaftNode) resolveConfChangeWaiters(cc *raftpb.ConfChange) {
	// 拿到 waiters 后立即删除，避免在唤醒过程中又有新的提议者加入，导致重复唤醒。
	key := confChangeKey(cc)
	rn.confChangeMu.Lock()
	waiters := rn.confChangeWaiters[key]
	delete(rn.confChangeWaiters, key)
	rn.confChangeMu.Unlock()

	// 遍历所有等待者，发送 nil 表示成功应用配置变更。由于 channel 是带缓冲的，这里不会阻塞。
	for _, ch := range waiters {
		ch <- nil // 带缓冲，不会阻塞
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

// Batch merging and concurrent reading 批量合并并发读
// 以下是本人对这套典型的高性能网关设计模式的代码的理解，其中涉及到的高并发异步回调极其巧妙，也非常益于理解 Go 的并发编程模型和 Raft 的批处理机制。
//
// 1. loop 开头有这样一行拨开云雾见月明的代码：go rn.readIndexLoop()，这个循环协程在后台一直尝试从 readRequests 这个 channel 里读取 req，
// 阻塞等待第一个请求，然后又不阻塞收集一批次的请求，组装 readIndexBatch 之后，把 readIndexBatch 塞进这一批次里所有的 req 的 assign channel 里，
// 之后就步入 dispatchReadIndex 去调用 raft ReadIndex，ReadIndex 返回后就阻塞等待 responded channel 的消息。
//
// 2. 一个 GET 请求则是在 ReadIndex 里生成一份 readIndexRequest 作为自己的凭证，然后把这个 req 塞进 readRequests 让 readIndexLoop 去收集批次，
// 之后就阻塞等待 assigned channel 上等待消息，有消息就代表已经收集完一批次并且准备发起 ReadIndex 了，就接着阻塞在一批次所有请求共有的 done channel
// 上等待 ready loop 获取到 ReadState 来处理这一批次的ReadIndex。
//
// 3. ready loop 里处理 raft 吐出的 ReadState，通过 RequestCtx 去 readBatches 里获取这一批次的结构体 batch，
// 判断 respondedOK：如果是第一次处理这个ReadState就记录 rs.Index 并且通知 close(batch.responded)，readIndexLoop 收到 responded 的消息后就可以去收集下一批次了，
// 然后用 rn.appliedIndex 和 rs.Index 来判断这一批次的 GET 请求是否已经可以安全读了，如果可以的话就从 readBatches 里删除掉这一个 batch，
// 然后 close(batch.done) 通知这一批次所有的 GET 请求，GET 请求返回到 server.readBarrier，在 HandleGet 以及其他读请求句柄里根据返回的错误判断是否可以直接调用mvcc读数据。
// 而且在 ready loop 最后还会检查 readBatches 里不是第一次处理（前次步入 Ready 循环时出错或者 AppliedIndex 还未追平 ReadIndex）的批次的
// ReadIndex 是否被这次 Ready 的 AppliedIndex 追平了，是的话也会从 readBatches 里删除 batch 并 close done channel 来通知所有 GET 请求。
//
// 4. 至于 dispatchReadIndex 和 failReadIndexBatch 里的许多代码都是各种重试逻辑和边界处理，着实繁琐但是严谨。

// ReadIndex 执行一次线性一致读屏障：等到本地状态机应用了 Leader 在发起读取时
// 已经提交的全部日志后返回。返回 nil 后，从本地 MVCC 读到的是满足线性一致性的数据。
//
// 并发调用会由 readIndexLoop 合并成一次 Raft ReadIndex 调用（只用一轮心跳），
// 这里只负责排队和等待。
func (rn *RaftNode) ReadIndex(ctx context.Context) error {
	req := &readIndexRequest{assigned: make(chan *readIndexBatch, 1)}
	select {
	case rn.readRequests <- req: // 把请求投递给 readIndexLoop 处理
	case <-ctx.Done():
		return ctx.Err()
	case <-rn.stopc:
		return raft.ErrStopped
	}

	// 等批次分配：readIndexLoop 只有在合并完一批之后才会把批次交给请求
	var batch *readIndexBatch
	select {
	case batch = <-req.assigned: // 收到批次后，继续等待批次完成
	case <-ctx.Done():
		return ctx.Err()
	case <-rn.stopc:
		return raft.ErrStopped
	}

	select {
	case <-batch.done:
		// done 在批次的目标索引被本地应用后关闭，此时读本地状态机是安全的
		return batch.err
	case <-ctx.Done():
		return ctx.Err()
	case <-rn.stopc:
		return raft.ErrStopped
	}
}

// readIndexLoop 把并发读聚合成批次，每批只向 Raft 发起一次 ReadIndex。
//
// 它必须等本批的 ReadState 下发后才开始收集下一批：否则循环会立刻把陆续到达的
// 请求当成新的一批，批大小退化为 1，又回到“每个读一轮心跳广播”的老路上。
// 注意只等 ReadState，不等本地追平；追平由 Raft 循环按 appliedIndex 统一唤醒，
// 不会阻塞后续批次的收集。
func (rn *RaftNode) readIndexLoop() {
	for {
		var first *readIndexRequest
		select {
		case <-rn.stopc:
			return
		case first = <-rn.readRequests: // 阻塞等待第一个请求到来，作为本批的起点
		}

		// 把当前已经排队的请求都并入同一批。非阻塞收集：不为凑批推迟首个请求。
		reqs := []*readIndexRequest{first}
	drain: // drain 标签用于跳出内层循环，继续外层循环收集下一批请求
		for {
			select {
			case req := <-rn.readRequests:
				reqs = append(reqs, req)
			default:
				break drain
			}
		}

		// 打包批次：先创建一个 readIndexBatch，分配给所有请求，然后发起 ReadIndex
		batch := &readIndexBatch{
			responded: make(chan struct{}),
			done:      make(chan struct{}),
			count:     len(reqs),
		}

		// 先把批次交给所有请求：它们各自等 batch.done，互相之间不会阻塞
		for _, req := range reqs {
			req.assigned <- batch
		}

		// 发起 ReadIndex，并在没有响应时重试，直到批次总超时
		if err := rn.dispatchReadIndex(batch); err != nil {
			rn.failReadIndexBatch(batch, err)
		}
	}
}

// dispatchReadIndex 为批次发起 ReadIndex，并在没有响应时重试，直到批次总超时。
//
// 每次尝试使用新的 RequestCtx 并注册到 readBatches；失败后立即摘掉该 key，
// 迟到的 ReadState 会被 Raft 循环当作过期批次忽略。
func (rn *RaftNode) dispatchReadIndex(batch *readIndexBatch) error {
	deadline := time.Now().Add(readIndexTimeout)
	for {
		// 为本次批处理生成一个新的 RequestCtx，保证集群范围内唯一
		key := ReadIndexKey(rn.nodeID, rn.readSeq.Add(1))

		rn.readIndexMu.Lock()
		if batch.resolved { // 上一次尝试的 ReadState 恰好迟到并已经放行了批次
			rn.readIndexMu.Unlock()
			return nil
		}
		batch.key = key             // 记录本次尝试的 RequestCtx，方便 Raft 循环回包时找到批次
		rn.readBatches[key] = batch // 注册到 readBatches，等待 Raft 循环回包
		rn.readIndexMu.Unlock()

		attempt := readIndexAttemptTimeout
		if remaining := time.Until(deadline); remaining < attempt { // 如果剩余时间不足一次尝试的超时，缩短本次尝试的超时
			attempt = remaining
		}
		ctx, cancel := context.WithTimeout(context.Background(), attempt)
		// 发起 ReadIndex 请求，等待 Raft 循环回包
		err := rn.Node.ReadIndex(ctx, []byte(key))
		if err == nil {
			select {
			case <-batch.responded: // 阻塞等待 Raft 循环收到 ReadState 后关闭，readIndexLoop 可以收集下一批
				cancel()
				return nil
			case <-ctx.Done():
				err = ctx.Err()
			case <-rn.stopc:
				cancel()
				return raft.ErrStopped
			}
		}
		cancel()
		// 走到这，说明本次尝试失败了：可能是超时、节点还没 Leader、或者节点正在停止。
		// 先把本次尝试注册的 key 从 readBatches 摘掉，避免迟到的 ReadState 再唤醒本批次。

		rn.readIndexMu.Lock()
		if rn.readBatches[key] == batch {
			// 只有本次尝试注册的 key 还在 readBatches 中，才删除它
			// 否则说明 Raft 循环已经收到 ReadState 并唤醒了批次，不能再删除。
			delete(rn.readBatches, key)
		}
		resolved := batch.resolved
		rn.readIndexMu.Unlock()
		if resolved {
			// 批次已经被 Raft 循环唤醒，说明 ReadState 迟到了，直接返回成功
			return nil
		}

		// 如果批次总超时了，就返回错误，让 readIndexLoop 失败本批次，唤醒所有等待的请求。
		if !time.Now().Before(deadline) {
			return err
		}
		// 本次尝试失败了，但批次总超时还没到，继续下一轮尝试。
		// 稍等再重试：如果失败原因是本节点还不知道 Leader，立刻重试大概率仍会被丢弃
		select {
		case <-rn.stopc:
			return raft.ErrStopped
		case <-time.After(readIndexRetryDelay):
			// 继续下一轮尝试
		}
	}
}

// failReadIndexBatch 清理一个失败或超时的批次，并让等待它的请求立即收到错误。
func (rn *RaftNode) failReadIndexBatch(batch *readIndexBatch, err error) {
	rn.readIndexMu.Lock()
	// 批次可能已经在本循环取锁之前被 Raft 循环唤醒，此时不能再关闭 done，
	// 否则会重复 close。
	if batch.resolved {
		rn.readIndexMu.Unlock()
		return
	}
	batch.resolved = true
	delete(rn.readBatches, batch.key)
	batch.err = err
	close(batch.done)
	rn.readIndexMu.Unlock()

	log.Printf(">>> ReadIndex 批次失败 (请求数 %d): %v", batch.count, err)
}

// resolveReadIndexLocked 唤醒并清理一个已追平的批次。调用方必须持有 readIndexMu。
func (rn *RaftNode) resolveReadIndexLocked(key string) {
	batch, ok := rn.readBatches[key]
	if !ok {
		return
	}
	delete(rn.readBatches, key)
	if batch.resolved {
		return
	}
	batch.resolved = true
	close(batch.done) // 关闭而不是发送：状态机已追平，等 done 的读方可以安全读取
}

// leaseExpiryCheckInterval 是 leaseExpiryLoop 扫描过期 LeaseID 的间隔。
const leaseExpiryCheckInterval = 2 * time.Second

// leaseExpiryLoop 定期扫描状态机里过期的 LeaseID，并向 Raft 提交 Revoke 命令。
func (rn *RaftNode) leaseExpiryLoop(stopc <-chan struct{}) {
	ticker := time.NewTicker(leaseExpiryCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopc:
			return
		case <-ticker.C:
			// 1. 只有 Leader 才有资格发起检查
			rn.mu.RLock()
			isLeader := rn.leaderID == rn.nodeID
			rn.mu.RUnlock()
			if !isLeader {
				// 如果不是 Leader，跳过本次检查
				// 这些跳过的轮次不会影响过期 LeaseID 的最终回收，因为 Leader 会在自己的轮次里扫描到
				// 过期的 LeaseID 并提交 Revoke 命令，Follower 会在收到 Revoke 日志后应用它们。
				// 另外，Leader 可能在本轮检查前就已经提交了 Revoke 命令，Follower 会在应用日志时回收 LeaseID。
				// 至于 Follower 也要一直循环尝试扫描过期 LeaseID，是因为当前 Leader 是有可能下台的，Follower 也可能在下一轮成为 Leader，继续扫描过期 LeaseID 并提交 Revoke 命令。
				continue
			}

			// 2. 从 MVCC 扫描出所有过期的 LeaseID
			expiredIDs, err := rn.applier.GetExpiredLeases()
			if err != nil {
				continue
			}

			// 3. 将过期的 LeaseID 打包成 Revoke 命令，提交给 Raft
			for _, leaseID := range expiredIDs {
				cmd := types.LeaseCommand{
					RequestID: fmt.Sprintf("revoke-%d-%d", leaseID, time.Now().UnixNano()),
					Op:        types.OpLeaseRevoke,
					LeaseID:   leaseID,
				}
				data, _ := json.Marshal(cmd)
				// 异步 Propose，不阻塞 ticker
				go rn.Node.Propose(context.Background(), data)
			}
		}
	}
}
