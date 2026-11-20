package raftnode

import (
	"context"
	"encoding/binary"
	"log"
	"mini-etcd/internal/config"
	"slices"
	"sync"
	"sync/atomic"
	"time"

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

	// readBatches 的 key 是 ReadIndex 的 RequestCtx（由 ReadIndexKey 生成），
	// 必须和传给 Node.ReadIndex 的字节完全一致，Raft 才能把 ReadState 对回本地批次。
	readRequests chan *readIndexRequest     // GET 提交的读请求，由 readIndexLoop 合并成批次
	readBatches  map[string]*readIndexBatch // 已提交给 Raft、等待 ReadState 或追平的批次
	readIndexMu  sync.Mutex                 // 保护 readBatches
	readSeq      atomic.Uint64              // 生成 RequestCtx 中的进程内序号
	appliedIndex uint64                     // 状态机当前已应用的最高日志索引

	nodeID uint64        // 本节点 ID，用于构造集群范围内唯一的 RequestCtx
	stopc  chan struct{} // Run 退出时关闭，唤醒阻塞中的读请求
}

// readIndexKeyLen 是 ReadIndexKey 生成的 RequestCtx 长度：8 字节节点 ID + 8 字节进程内序号。
const readIndexKeyLen = 16

// readIndexTimeout 是一个 ReadIndex 批次从发起到收到 ReadState 的总预算。
// 与 HTTP 层单次请求的超时保持一致：底层已经放弃的读，上层也没必要再等。
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
		nodeID:            cfg.ID,
		readRequests:      make(chan *readIndexRequest, 64),
		readBatches:       make(map[string]*readIndexBatch),
		stopc:             make(chan struct{}),
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
