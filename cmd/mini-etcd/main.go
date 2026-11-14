package main

import (
	"context"
	"fmt"
	"log"
	"mini-etcd/internal/apply"
	"mini-etcd/internal/config"
	"mini-etcd/internal/server"
	"mini-etcd/internal/storage"
	"mini-etcd/internal/transport"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	peerIDs := []uint64{1, 2, 3}

	// 创建全局唯一的网络总线
	network := transport.NewMemoryNetwork()

	var servers []*server.Server

	// 启动 3 个节点
	for _, id := range peerIDs {
		// 创建节点默认配置
		cfg := config.Default(id, peerIDs)

		// 初始化各个组件
		storage := storage.NewMemoryStorage()
		trans := transport.NewMemoryTransport(id, network)
		applier := apply.NewApplier()

		// 组装 Server
		srv := server.NewServer(cfg, storage, trans, applier)
		servers = append(servers, srv)
		go srv.Start()
	}

	// 等待一段时间让节点启动并选出 Leader
	time.Sleep(2 * time.Second)

	// 向所有节点尝试 Propose
	log.Println(">>> 准备发起写请求：PUT name mini-etcd")
	requestID := fmt.Sprintf("%d", time.Now().UnixNano()) // 用唯一的请求 ID 来标识这次请求，模拟重复请求
	for _, srv := range servers {
		entry := fmt.Appendf([]byte{}, "%s PUT name mini-etcd%d", requestID, srv.Cfg.ID)

		err := srv.Rn.Propose(context.Background(), entry) // 通过 RaftNode 发起提议
		if err != nil {
			log.Printf("节点 %d 提议失败: %v\n", srv.Cfg.ID, err)
		} else {
			log.Printf("节点 %d 提议成功\n", srv.Cfg.ID)
		}
	}

	// 等待日志复制并应用
	time.Sleep(2 * time.Second)

	// 模拟查询
	for _, srv := range servers {
		val, ok := srv.Applier.Get("name")
		log.Printf("读取数据: name = %s, 存在=%v\n", val, ok)
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
