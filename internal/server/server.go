package server

import (
	"mini-etcd/internal/config"
	"mini-etcd/internal/raftnode"
	"mini-etcd/internal/transport"

	"go.etcd.io/etcd/raft/v3"
)

// Server 代表一个 Raft 节点的服务实例
type Server struct {
	cfg     config.Config
	rn      *raftnode.RaftNode
	trans   *transport.MemoryTransport
	stopc   chan struct{}
	storage *raft.MemoryStorage
}

// NewServer 创建一个新的 Server 实例
func NewServer(cfg config.Config, trans *transport.MemoryTransport) *Server {
	storage := raft.NewMemoryStorage()
	rn := raftnode.NewRaftNode(cfg, storage, trans)

	return &Server{
		cfg:     cfg,
		rn:      rn,
		trans:   trans,
		storage: storage,
		stopc:   make(chan struct{}),
	}
}

func (s *Server) Start() {
	// 启动 Raft 节点的核心循环
	go s.rn.Run(s.stopc, s.cfg.TickInterval)
}

func (s *Server) Stop() {
	// 先这样简单处理
	close(s.stopc)
}
