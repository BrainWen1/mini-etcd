package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"mini-etcd/internal/apply"
	"mini-etcd/internal/config"
	"mini-etcd/internal/mvcc"
	"mini-etcd/internal/raftnode"
	"mini-etcd/internal/storage"
	"mini-etcd/internal/transport"
	"mini-etcd/internal/types"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bwmarrin/snowflake"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// 成员管理的可判别错误。gRPC 层用 errors.Is 把它们映射成
// AlreadyExists / NotFound / FailedPrecondition，而不是一律返回 Internal。
var (
	ErrMemberExists   = errors.New("member already exists")
	ErrMemberNotFound = errors.New("member not found")
	ErrLastMember     = errors.New("refusing to remove the last member")
)

// Server 代表一个 Raft 节点的服务实例
type Server struct {
	Cfg config.Config
	Rn  *raftnode.RaftNode // server 持有其对应的 Raft 节点的引用

	trans   *transport.TCPTransport
	Applier *apply.Applier
	storage *storage.WAL
	mvcc    *mvcc.MVCC

	// reqSeq 为每个写请求生成进程内唯一 ID。
	// 不能用 time.Now().UnixNano()：高并发下可能取到相同值，导致 waiter 的 key 冲突，
	// 轻则某个请求一直等到超时，重则客户端收到另一个请求的执行结果。
	reqSeq atomic.Uint64

	// snowflakeNode 用于生成全局唯一的 LeaseID。
	// 必须整个进程复用一个实例：NewNode 每次都从 step=0 开始，同一毫秒内
	// 新建两个节点会生成完全相同的 ID，两个租约互相覆盖。
	snowflakeNode *snowflake.Node

	// 动态成员信息。
	// grpcPeers 是 Leader 转发用的地址表：必须和 Raft 成员变更同步更新，
	// 否则新节点当选 Leader 后，老节点会转发到空地址（gRPC Unavailable）。
	// members 同时保存 Raft/gRPC 地址，用于重复添加校验和成员查询。
	membersMu sync.RWMutex
	grpcPeers map[uint64]string
	members   map[uint64]config.Member

	stopc    chan struct{}
	stopOnce sync.Once

	// key -> []chan string，按 key 注册的监听器列表。每条变更日志应用后会通知所有监听器。
	watchMu             sync.RWMutex                 // 保护 watchChannels 的并发访问
	exactWatchChannels  map[string][]chan mvcc.Event // 精确匹配的 key 监听器
	prefixWatchChannels map[string][]chan mvcc.Event // 前缀匹配的 key 监听器
}

// NewServer 创建一个新的 Server 实例
func NewServer(cfg config.Config, storage *storage.WAL, trans *transport.TCPTransport, applier *apply.Applier, kvstore *mvcc.MVCC) *Server {
	// 创建 RaftNode 实例
	rn := raftnode.NewRaftNode(cfg, storage, trans, applier)

	// 复制一份 gRPC 地址表，避免和外部共享的 map 产生并发读写。
	grpcPeers := maps.Clone(cfg.GRPCPeers)
	if grpcPeers == nil {
		grpcPeers = make(map[uint64]string)
	}
	members := make(map[uint64]config.Member, len(cfg.Peers))
	for id, raftAddr := range cfg.Peers {
		members[id] = config.Member{ID: id, RaftAddr: raftAddr, GRPCAddr: cfg.GRPCPeers[id]}
	}

	// snowflake 节点 ID 只支持 0-1023；配置的节点 ID 超出范围时无法生成 LeaseID，
	// 这是部署配置错误，启动阶段直接失败比运行期静默返回 0 更好排查。
	snowflakeNode, err := snowflake.NewNode(int64(cfg.ID))
	if err != nil {
		log.Fatalf("创建 snowflake 节点失败 (node id %d): %v", cfg.ID, err)
	}

	s := &Server{
		Cfg:                 cfg,
		Rn:                  rn,
		trans:               trans,
		storage:             storage,
		Applier:             applier,
		mvcc:                kvstore,
		snowflakeNode:       snowflakeNode,
		grpcPeers:           grpcPeers,
		members:             members,
		stopc:               make(chan struct{}),
		exactWatchChannels:  make(map[string][]chan mvcc.Event),
		prefixWatchChannels: make(map[string][]chan mvcc.Event),
	}

	// 每条配置变更日志被应用后，所有节点都会同步更新成员表并落盘。
	rn.SetMembershipChangeHandler(s.applyMembershipChange)

	return s
}

// applyMembershipChange 由一个已提交的 ConfChange 触发（在 Raft 循环 goroutine 中执行）。
// 所有节点都会执行同一串变更，因此成员表在集群范围内收敛。
// 更新 server 的成员表和 Leader 转发表。
func (s *Server) applyMembershipChange(id uint64, raftAddr, grpcAddr string, added bool) {
	s.membersMu.Lock()
	if added { // added=true 表示新增或更新成员
		s.members[id] = config.Member{ID: id, RaftAddr: raftAddr, GRPCAddr: grpcAddr}
		s.grpcPeers[id] = grpcAddr
	} else {
		delete(s.members, id)
		delete(s.grpcPeers, id)
	}
	s.membersMu.Unlock()
}

func (s *Server) Start() {
	// 启动 Raft 节点的核心循环
	go s.Rn.Run(s.stopc, s.Cfg.TickInterval)
}

func (s *Server) Stop() {
	// 幂等：被移出集群的节点会从 Removed 通道退出，用户也可能同时按 Ctrl-C，
	// 两条路径都调用 Stop 时不能重复 close(stopc)（会 panic）。
	s.stopOnce.Do(func() {
		// 通知 stopc 关闭，RaftNode 的 Run 循环会退出
		close(s.stopc)
		// 关闭监听器和所有 TCP 连接，避免进程退出前端口仍被占用、
		// 以及收发 goroutine 继续在已停止的 Raft 循环上堆积消息
		s.trans.Close()

		if s.mvcc != nil {
			s.mvcc.Close() // 优雅关闭 MVCC，停止后台 goroutine
		}
	})
}

// Removed 返回一个在本节点被移出集群时关闭的 channel。
func (s *Server) Removed() <-chan struct{} {
	return s.Rn.Removed()
}

// nextRequestID 生成进程内唯一的请求 ID：节点 ID + 自增序号。
// 不能用 time.Now().UnixNano()：高并发下可能取到相同值，导致 waiter 的 key 冲突。
func (s *Server) nextRequestID() string {
	return fmt.Sprintf("%d-%d", s.Cfg.ID, s.reqSeq.Add(1))
}

// proposeAndWait 把命令提交给 Raft，并阻塞等待状态机应用完成。
// 调用方必须先用 nextRequestID 生成 reqID，并用同一个 reqID 编码命令。
func (s *Server) proposeAndWait(reqID string, cmd []byte) types.Result {
	// 1. 先注册 waiter，再提交；顺序反过来的话 Apply 可能先于注册完成，客户端会一直等到超时
	waitCh := s.Applier.RegisterWaiter(reqID)

	// 2. 将请求提交给 Raft
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 设置超时时间，防止阻塞
	err := s.Rn.Node.Propose(ctx, cmd)
	cancel() // 释放资源
	if err != nil {
		// 如果提议失败（比如超时或节点不是 Leader），要把 waiter 清理掉
		s.Applier.RemoveWaiter(reqID)
		return types.Result{Succeeded: false, Err: err}
	}

	// 3. 阻塞等待 Raft 应用结果
	// 这里也要设置超时，防止 Raft 迟迟不 Apply
	select {
	case res := <-waitCh:
		return res // 返回应用结果给客户端
	case <-time.After(5 * time.Second): // 超时兜底
		s.Applier.RemoveWaiter(reqID)
		return types.Result{Succeeded: false, Err: errors.New("apply timeout")}
	}
}

// validateLeases 确认请求引用的租约仍然存在。
//
// 必须在提议前校验：如果一条 PUT 带着已经不存在的 LeaseID 进入 Raft 日志，
// 各节点在 Apply 时无法为它建立绑定，key 会永远不随租约过期而被回收。
// 先过 readBarrier 再读本地状态，避免刚当选的 Leader 状态机尚未追平，
// 把仍然存在的租约误判为不存在。
func (s *Server) validateLeases(leaseIDs []int64) error {
	if len(leaseIDs) == 0 {
		return nil
	}
	// 先追平本地状态机，避免 Follower 还没应用完 Leader 的日志就读本地状态。
	if err := s.readBarrier(); err != nil {
		return err
	}
	for _, leaseID := range leaseIDs {
		ok, err := s.mvcc.LeaseExists(leaseID)
		if err != nil {
			return err
		}
		if !ok {
			// 包装 mvcc.ErrLeaseNotFound，让上层（gRPC）能映射成 NotFound。
			return fmt.Errorf("lease %d: %w", leaseID, mvcc.ErrLeaseNotFound)
		}
	}
	return nil
}

// HandlePut 处理客户端的 PUT 请求，提交给 Raft 提议
func (s *Server) HandlePut(key, value string, leaseID int64) types.Result {
	if leaseID > 0 {
		if err := s.validateLeases([]int64{leaseID}); err != nil {
			return types.Result{Succeeded: false, Err: err}
		}
	}
	reqID := s.nextRequestID()
	cmd, err := types.EncodePutCommand(reqID, key, value, leaseID)
	if err != nil {
		return types.Result{Succeeded: false, Err: fmt.Errorf("encode command failed: %w", err)}
	}
	return s.proposeAndWait(reqID, cmd)
}

// readBarrier 执行一次线性一致读屏障：等到本地状态机应用了 Leader 在发起读取时
// 已经提交的全部日志。所有读路径（最新值、历史 revision、前缀查询）都必须先经过它，
// 否则 Follower 可能返回还没追平的旧数据。
// 并发调用由 RaftNode 合并成批次，一批读只触发一轮心跳，不会随 QPS 放大消息量。
func (s *Server) readBarrier() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 设置超时时间，防止阻塞
	defer cancel()

	if err := s.Rn.ReadIndex(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return errors.New("read index timeout")
		}
		return fmt.Errorf("read index failed: %w", err)
	}
	return nil
}

// HandleGet 处理客户端的 GET 请求，读取最新数据
func (s *Server) HandleGet(key string) (string, error) {
	if err := s.readBarrier(); err != nil {
		return "", err
	}
	return s.mvcc.Get(key)
}

// HandleGetAtRevision 处理客户端的 GET 请求，读取指定 revision 的数据
func (s *Server) HandleGetAtRevision(key string, rev uint64) (string, error) {
	// 历史版本读同样要追平：落后节点上可能还没应用 rev 之后的日志，
	// 直接读本地 MVCC 会返回 rev 之前就已经过期的值。
	if err := s.readBarrier(); err != nil {
		return "", err
	}
	return s.mvcc.GetAtRevision(key, rev)
}

// HandleDelete 处理客户端的 DELETE 请求，提交给 Raft 提议
func (s *Server) HandleDelete(key string) types.Result {
	reqID := s.nextRequestID()
	cmd, err := types.EncodeDeleteCommand(reqID, key)
	if err != nil {
		return types.Result{Succeeded: false, Err: fmt.Errorf("encode command failed: %w", err)}
	}
	return s.proposeAndWait(reqID, cmd)
}

// HandleTxn 处理客户端的 TXN 请求：提交事务命令，由状态机在 Apply 时
// 在同一 bbolt 事务中评估 compares 并执行 success/failure 分支。
func (s *Server) HandleTxn(compares []types.Compare, success, failure []types.Operation) types.Result {
	txnCmd := types.TxnCommand{
		RequestID: s.nextRequestID(),
		Compares:  compares,
		Success:   success,
		Failure:   failure,
	}
	// 写进 Raft 日志前先校验：非法命令一旦进入日志，每次重放都会失败
	if err := types.ValidateTxnCommand(txnCmd); err != nil {
		return types.Result{Succeeded: false, Err: fmt.Errorf("invalid txn: %w", err)}
	}
	// 同样在提议前确认事务引用的租约都存在
	var leaseIDs []int64
	for _, ops := range [][]types.Operation{success, failure} {
		for _, op := range ops {
			if op.LeaseID > 0 {
				leaseIDs = append(leaseIDs, op.LeaseID)
			}
		}
	}
	if err := s.validateLeases(leaseIDs); err != nil {
		return types.Result{Succeeded: false, Err: err}
	}
	cmd, err := types.EncodeTxnCommand(txnCmd)
	if err != nil {
		return types.Result{Succeeded: false, Err: fmt.Errorf("encode command failed: %w", err)}
	}
	return s.proposeAndWait(txnCmd.RequestID, cmd)
}

// HandleRange 处理客户端的 RANGE 请求，读取指定范围的数据
func (s *Server) HandleRange(prefix string, limit int) ([]mvcc.KVPair, error) {
	// 与 GET 一样，前缀查询也必须先追平 ReadIndex，避免 Follower 返回旧结果
	if err := s.readBarrier(); err != nil {
		return nil, err
	}
	return s.mvcc.Range(prefix, limit)
}

// IsLeader 返回当前节点是否是 Leader
func (s *Server) IsLeader() bool {
	leaderID := s.Rn.GetLeaderID()
	return leaderID == s.Cfg.ID
}

// GetLeaderGRPCAddr 返回当前 Leader 的地址，如果没有 Leader 则返回空字符串
func (s *Server) GetLeaderGRPCAddr() string {
	leaderID := s.Rn.GetLeaderID()
	if leaderID == 0 {
		return ""
	}
	// 成员变更会并发修改这张表，必须加读锁。
	// 直接读 Cfg.GRPCPeers 会漏掉动态加入的节点，也可能触发 concurrent map read/write。
	s.membersMu.RLock()
	defer s.membersMu.RUnlock()
	return s.grpcPeers[leaderID] // 返回 Leader 的 gRPC 地址
}

// Member 返回指定成员的信息（用于外部查询/测试）。
func (s *Server) Member(id uint64) (config.Member, bool) {
	s.membersMu.RLock()
	defer s.membersMu.RUnlock()
	m, ok := s.members[id]
	return m, ok
}

// HandleAddMember 处理客户端的 AddMember 请求：提交配置变更并等待本地应用完成。
//
// raftAddr 是新节点的 Raft 通信地址（Transport 拨号用）；
// grpcAddr 是新节点的 gRPC API 地址（Leader 转发用）。两者都必须提供，
// 只记 Raft 地址会导致新节点当选 Leader 后整个集群的写请求无法转发。
func (s *Server) HandleAddMember(id uint64, raftAddr, grpcAddr string) error {
	if id == 0 {
		return errors.New("member id must be non-zero")
	}
	if raftAddr == "" {
		return errors.New("raft address must not be empty")
	}
	if grpcAddr == "" {
		return errors.New("grpc address must not be empty")
	}

	s.membersMu.RLock()
	existing, exists := s.members[id]
	s.membersMu.RUnlock()
	if exists {
		// 幂等：重复提交完全相同的变更直接视为成功。
		// 地址不同则拒绝：Raft 的 AddNode 对已存在节点是空操作，
		// 但本地 Transport 会被改成新地址，导致各节点成员表不一致。
		if existing.RaftAddr == raftAddr && existing.GRPCAddr == grpcAddr {
			return nil
		}
		return fmt.Errorf("%w: member %d as raft=%s grpc=%s; remove it first",
			ErrMemberExists, id, existing.RaftAddr, existing.GRPCAddr)
	}

	// members 里没有这个 id，提交配置变更给 Raft 并等待本地应用完成。
	cc := &raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  id,
		Context: config.EncodeMemberContext(raftAddr, grpcAddr),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Rn.ProposeConfChangeAndWait(ctx, cc); err != nil {
		return fmt.Errorf("propose conf change failed: %w", err)
	}
	return nil
}

// HandleRemoveMember 处理客户端的 RemoveMember 请求：提交配置变更并等待本地应用完成。
func (s *Server) HandleRemoveMember(id uint64) error {
	if id == 0 {
		return errors.New("member id must be non-zero")
	}

	s.membersMu.RLock()
	_, exists := s.members[id]
	remaining := len(s.members)
	s.membersMu.RUnlock()
	if !exists {
		return fmt.Errorf("%w: member %d", ErrMemberNotFound, id)
	}
	// 移除最后一个成员会让集群永久失去法定人数，直接拒绝。
	if remaining <= 1 {
		return ErrLastMember
	}

	cc := &raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Rn.ProposeConfChangeAndWait(ctx, cc); err != nil {
		return fmt.Errorf("propose conf change failed: %w", err)
	}
	return nil
}

// Watch 注册一个监听器，返回一个 channel，客户端可以从中接收指定 key 的变更事件。
// 监听器是按 key 注册的，客户端可以注册多个 key 的监听器。
// 返回的 channel 是带缓冲的，防止客户端处理太慢阻塞 Apply。
func (s *Server) Watch(key string, isPrefix bool) chan mvcc.Event {
	// 创建 channel
	ch := make(chan mvcc.Event, 100)

	s.watchMu.Lock()
	defer s.watchMu.Unlock()

	if isPrefix {
		// 如果是前缀匹配，存储在 prefixWatchChannels 中
		s.prefixWatchChannels[key] = append(s.prefixWatchChannels[key], ch)
	} else {
		// 如果是精确匹配，存储在 exactWatchChannels 中
		s.exactWatchChannels[key] = append(s.exactWatchChannels[key], ch)
	}

	// 返回 channel，用于接受事件通知和注销监听器
	return ch
}

// Unwatch 注销一个监听器，停止接收指定 key 的变更事件。
func (s *Server) Unwatch(key string, ch chan mvcc.Event, isPrefix bool) {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()

	// 根据 isPrefix 决定从哪个 map 中注销 channel
	targetMap := s.exactWatchChannels
	if isPrefix {
		targetMap = s.prefixWatchChannels
	}

	chans, ok := targetMap[key]
	if !ok {
		return // 没有注册过这个 key，直接返回
	}
	for i, c := range chans {
		if c == ch {
			// 找到要注销的 channel，移除它
			targetMap[key] = append(chans[:i], chans[i+1:]...)
			close(ch) // 关闭 channel，通知客户端停止接收事件
			break
		}
	}

	if len(targetMap[key]) == 0 {
		// 如果没有剩余的 channel，删除这个 key 的 []chan
		delete(targetMap, key)
	}
}

// NotifyWatcher 通知所有匹配的 Watch 监听器（由 apply 层通过 WatchNotifier 接口调用）
func (s *Server) NotifyWatcher(key string, event mvcc.Event) {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()

	// 1. 精确匹配
	if chans, ok := s.exactWatchChannels[key]; ok {
		s.broadcast(chans, event)
	}

	// 2. 前缀匹配：遍历所有的 prefix 规则，判断 key 是否以它开头
	// 只适应前缀订阅不多的情况。如果前缀订阅非常多，这里会成为性能瓶颈
	// 可以考虑用 Trie 树或前缀哈希表来优化匹配效率。
	for prefix, chans := range s.prefixWatchChannels {
		if strings.HasPrefix(key, prefix) {
			s.broadcast(chans, event)
		}
	}
}

// broadcast 向所有监听器发送事件，如果 channel 满了就丢弃事件，避免阻塞
func (s *Server) broadcast(chans []chan mvcc.Event, event mvcc.Event) {
	for _, ch := range chans {
		select {
		case ch <- event:
		default:
			// 客户端处理太慢，丢弃事件
			log.Printf("[Node %d] watch channel full, dropping event for key %s", s.Cfg.ID, event.Key)
		}
	}
}

// GetHistory 返回指定 key 的历史变更事件列表，只包含 revision > startRev 的事件。
// 如果 isPrefix 为 true，则返回所有以 key 为前缀的历史事件。
func (s *Server) GetHistory(key string, isPrefix bool, startRev uint64) ([]mvcc.Event, error) {
	return s.mvcc.GetHistory(key, isPrefix, startRev) // 直接调用 MVCC 的 GetHistory 方法
}

// HandleLeaseGrant 处理客户端的 LeaseGrant 请求，提交给 Raft 提议
func (s *Server) HandleLeaseGrant(ttl int64) (int64, types.Result) {
	reqID := s.nextRequestID()
	leaseID := s.snowflakeNode.Generate().Int64() // 使用雪花算法生成唯一的 LeaseID
	if leaseID <= 0 {
		return 0, types.Result{Succeeded: false, Err: errors.New("failed to generate lease id")}
	}
	cmd, err := types.EncodeLeaseCommand(types.LeaseCommand{
		RequestID: reqID,
		Op:        types.OpLeaseGrant,
		LeaseID:   leaseID,
		TTL:       ttl,
		// 开始时间由 Leader 在提议前打戳：所有节点都用 LeaseStart+TTL 计算过期时间，
		// 保证同一条日志在各节点得到相同结果（不能在各节点 Apply 时调用 time.Now()）。
		LeaseStart: time.Now().Unix(),
	})
	if err != nil {
		return 0, types.Result{Succeeded: false, Err: fmt.Errorf("encode command failed: %w", err)}
	}
	return leaseID, s.proposeAndWait(reqID, cmd)
}

// HandleLeaseRevoke 处理客户端的 LeaseRevoke 请求，提交给 Raft 提议
func (s *Server) HandleLeaseRevoke(leaseID int64) types.Result {
	reqID := s.nextRequestID()
	cmd, err := types.EncodeLeaseCommand(types.LeaseCommand{
		RequestID: reqID,
		Op:        types.OpLeaseRevoke,
		LeaseID:   leaseID,
	})

	if err != nil {
		return types.Result{Succeeded: false, Err: fmt.Errorf("encode command failed: %w", err)}
	}
	return s.proposeAndWait(reqID, cmd)
}

// LeaseTTL 返回租约的剩余有效期（秒），租约不存在时返回 mvcc.ErrLeaseNotFound。
// 只用于 KeepAlive 的响应，读的是本地已应用的状态：续租本身已经过 Raft，
// 此刻本地状态至少包含这条续租日志。
func (s *Server) LeaseTTL(leaseID int64) (int64, error) {
	return s.mvcc.LeaseTTL(leaseID)
}

// HandleLeaseKeepAlive 处理客户端的 LeaseKeepAlive 请求，提交给 Raft 提议
func (s *Server) HandleLeaseKeepAlive(leaseID int64) types.Result {
	reqID := s.nextRequestID()
	cmd, err := types.EncodeLeaseCommand(types.LeaseCommand{
		RequestID: reqID,
		Op:        types.OpLeaseKeepAlive,
		LeaseID:   leaseID,
		// 续租只更新时间戳；TTL 由状态机提供，过期时间 = LeaseStart + TTL。
		LeaseStart: time.Now().Unix(),
	})
	if err != nil {
		return types.Result{Succeeded: false, Err: fmt.Errorf("encode command failed: %w", err)}
	}
	return s.proposeAndWait(reqID, cmd)
}
