package main

import (
	"mini-etcd/internal/config"
	"mini-etcd/internal/server"
	"mini-etcd/internal/transport"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	peerIDs := []uint64{1, 2, 3}

	// 创建全局唯一的网络总线
	network := transport.NewMemoryNetwork()

	var servers []*server.Server

	// 启动 3 个节点
	for _, id := range peerIDs {
		cfg := config.Default(id, peerIDs)

		// 给节点创建专属网卡，并接入全局网络
		trans := transport.NewMemoryTransport(id, network)

		// 组装 Server
		srv := server.NewServer(cfg, trans)
		servers = append(servers, srv)

		go srv.Start()
	}

	// 优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 停止所有节点
	for _, srv := range servers {
		srv.Stop()
	}
}
