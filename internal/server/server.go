package server

import (
	"mini-etcd/internal/apply"
	"mini-etcd/internal/config"
	"mini-etcd/internal/raftnode"
	"mini-etcd/internal/storage"
	"mini-etcd/internal/transport"
)

// Server 代表一个 Raft 节点的服务实例
type Server struct {
	Cfg config.Config
	Rn  *raftnode.RaftNode // server 持有其对应的 Raft 节点的引用

	trans   *transport.MemoryTransport
	Applier *apply.Applier
	storage *storage.MemoryStorage

	stopc chan struct{}
}

// NewServer 创建一个新的 Server 实例
func NewServer(cfg config.Config, storage *storage.MemoryStorage, trans *transport.MemoryTransport, applier *apply.Applier) *Server {
	// 创建 RaftNode 实例
	rn := raftnode.NewRaftNode(cfg, storage, trans, applier)

	return &Server{
		Cfg:     cfg,
		Rn:      rn,
		trans:   trans,
		storage: storage,
		Applier: applier,
		stopc:   make(chan struct{}),
	}
}

func (s *Server) Start() {
	// 启动 Raft 节点的核心循环
	go s.Rn.Run(s.stopc, s.Cfg.TickInterval)
}

func (s *Server) Stop() {
	// 先这样简单处理
	close(s.stopc)
}
