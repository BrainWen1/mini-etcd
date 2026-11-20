package server

import (
	"context"
	"errors"
	"fmt"
	"mini-etcd/internal/apply"
	"mini-etcd/internal/config"
	"mini-etcd/internal/mvcc"
	"mini-etcd/internal/raftnode"
	"mini-etcd/internal/storage"
	"mini-etcd/internal/transport"
	"mini-etcd/internal/types"
	"sync/atomic"
	"time"
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

	stopc chan struct{}
}

// NewServer 创建一个新的 Server 实例
func NewServer(cfg config.Config, storage *storage.WAL, trans *transport.TCPTransport, applier *apply.Applier, mvcc *mvcc.MVCC) *Server {
	// 创建 RaftNode 实例
	rn := raftnode.NewRaftNode(cfg, storage, trans, applier)

	return &Server{
		Cfg:     cfg,
		Rn:      rn,
		trans:   trans,
		storage: storage,
		Applier: applier,
		mvcc:    mvcc,
		stopc:   make(chan struct{}),
	}
}

func (s *Server) Start() {
	// 启动 Raft 节点的核心循环
	go s.Rn.Run(s.stopc, s.Cfg.TickInterval)
}

func (s *Server) Stop() {
	// 通知 stopc 关闭，RaftNode 的 Run 循环会退出
	close(s.stopc)
	// 关闭监听器和所有 TCP 连接，避免进程退出前端口仍被占用、
	// 以及收发 goroutine 继续在已停止的 Raft 循环上堆积消息
	s.trans.Close()
}

// nextRequestID 生成进程内唯一的请求 ID：节点 ID + 自增序号。
// 不能用 time.Now().UnixNano()：高并发下可能取到相同值，导致 waiter 的 key 冲突。
func (s *Server) nextRequestID() string {
	return fmt.Sprintf("%d-%d", s.Cfg.ID, s.reqSeq.Add(1))
}

// proposeAndWait 把命令提交给 Raft，并阻塞等待状态机应用完成。
// 调用方必须先用 nextRequestID 生成 reqID，并用同一个 reqID 编码命令。
func (s *Server) proposeAndWait(reqID string, cmd []byte) error {
	// 1. 先注册 waiter，再提交；顺序反过来的话 Apply 可能先于注册完成，客户端会一直等到超时
	waitCh := s.Applier.RegisterWaiter(reqID)

	// 2. 将请求提交给 Raft
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 设置超时时间，防止阻塞
	err := s.Rn.Node.Propose(ctx, cmd)
	cancel() // 释放资源
	if err != nil {
		// 如果提议失败（比如超时或节点不是 Leader），要把 waiter 清理掉
		s.Applier.RemoveWaiter(reqID)
		return fmt.Errorf("propose failed: %w", err)
	}

	// 3. 阻塞等待 Raft 应用结果
	// 这里也要设置超时，防止 Raft 迟迟不 Apply
	select {
	case applyErr := <-waitCh:
		return applyErr // 返回应用结果，可能是 nil 或者具体的错误
	case <-time.After(5 * time.Second): // 超时兜底
		s.Applier.RemoveWaiter(reqID)
		return errors.New("timeout waiting for apply")
	}
}

// HandlePut 处理客户端的 PUT 请求，提交给 Raft 提议
func (s *Server) HandlePut(key, value string) error {
	reqID := s.nextRequestID()
	cmd, err := types.EncodePutCommand(reqID, key, value)
	if err != nil {
		return fmt.Errorf("encode command failed: %w", err)
	}
	return s.proposeAndWait(reqID, cmd)
}

// HandleGet 处理客户端的 GET 请求，读取最新数据
func (s *Server) HandleGet(key string) (string, error) {
	// 直接去 MVCC 层读最新数据
	return s.mvcc.Get(key)
}

// HandleGetAtRevision 处理客户端的 GET 请求，读取指定 revision 的数据
func (s *Server) HandleGetAtRevision(key string, rev uint64) (string, error) {
	// 直接去 MVCC 层读历史数据
	return s.mvcc.GetAtRevision(key, rev)
}

// HandleDelete 处理客户端的 DELETE 请求，提交给 Raft 提议
func (s *Server) HandleDelete(key string) error {
	reqID := s.nextRequestID()
	cmd, err := types.EncodeDeleteCommand(reqID, key)
	if err != nil {
		return fmt.Errorf("encode command failed: %w", err)
	}
	return s.proposeAndWait(reqID, cmd)
}

// HandleTxn 处理客户端的 TXN 请求：提交事务命令，由状态机在 Apply 时
// 在同一 bbolt 事务中评估 compares 并执行 success/failure 分支。
func (s *Server) HandleTxn(compares []types.Compare, success, failure []types.Operation) error {
	txnCmd := types.TxnCommand{
		RequestID: s.nextRequestID(),
		Compares:  compares,
		Success:   success,
		Failure:   failure,
	}
	// 写进 Raft 日志前先校验：非法命令一旦进入日志，每次重放都会失败
	if err := types.ValidateTxnCommand(txnCmd); err != nil {
		return fmt.Errorf("invalid txn: %w", err)
	}
	cmd, err := types.EncodeTxnCommand(txnCmd)
	if err != nil {
		return fmt.Errorf("encode command failed: %w", err)
	}
	return s.proposeAndWait(txnCmd.RequestID, cmd)
}

// HandleRange 处理客户端的 RANGE 请求，读取指定范围的数据
func (s *Server) HandleRange(prefix string, limit int) ([]mvcc.KVPair, error) {
	return s.mvcc.Range(prefix, limit)
}
