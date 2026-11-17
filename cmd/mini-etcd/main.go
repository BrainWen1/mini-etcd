package main

import (
	"log"
	"mini-etcd/internal/apply"
	"mini-etcd/internal/backend"
	"mini-etcd/internal/config"
	"mini-etcd/internal/httpapi"
	"mini-etcd/internal/mvcc"
	"mini-etcd/internal/server"
	"mini-etcd/internal/storage"
	"mini-etcd/internal/transport"
	"os"
	"os/signal"
	"strconv"
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

		// 创建节点数据目录
		if err := os.MkdirAll(cfg.WALPath, 0755); err != nil {
			log.Fatalf("Failed to create WAL directory for node %d: %v", id, err)
		}
		if err := os.MkdirAll(cfg.SnapshotPath, 0755); err != nil {
			log.Fatalf("Failed to create snapshot directory for node %d: %v", id, err)
		}
		if err := os.MkdirAll(cfg.BackendPath, 0755); err != nil {
			log.Fatalf("Failed to create backend directory for node %d: %v", id, err)
		}

		// 初始化各个组件
		storage, err := storage.OpenWAL(cfg.WALPath, cfg.SnapshotPath) // 使用 WAL 作为持久化存储
		if err != nil {
			log.Fatalf("Failed to open WAL for node %d: %v", id, err)
		}

		be, err := backend.NewBackend(cfg.BackendPath)
		if err != nil {
			log.Fatalf("Failed to open backend for node %d: %v", id, err)
		}
		defer be.Close()
		mvccStorage := mvcc.NewMVCC(be)

		applier := apply.NewApplier(mvccStorage) // 初始化状态机应用器

		snap, err := storage.LoadSnapshot() // 从磁盘加载最新快照
		if err != nil {
			log.Fatalf("Failed to load snapshot: %v", err)
		}
		// bbolt 本身是持久化的，里面的数据可能比磁盘上的 Raft 快照更新
		// （快照每 10 条日志才打一次）。如果无条件用旧快照覆盖状态机，
		// 就会把快照之后已经落盘的写入回滚掉；只有快照比后端数据新时才需要恢复。
		backendIndex, err := mvccStorage.GetConsistentIndex()
		if err != nil {
			log.Fatalf("Failed to get consistent index for node %d: %v", id, err)
		}
		if snap != nil && len(snap.Data) > 0 && snap.Metadata.Index > backendIndex {
			log.Printf("[Node %d] 从磁盘加载快照，恢复状态机数据 (index=%d)", id, snap.Metadata.Index)
			if err := applier.ApplySnapshot(*snap); err != nil { // 将快照应用到状态机
				log.Fatalf("Failed to apply snapshot to applier: %v", err)
			}
		}

		trans := transport.NewMemoryTransport(id, network)

		// 组装 Server
		srv := server.NewServer(cfg, storage, trans, applier, mvccStorage)
		servers = append(servers, srv)
		go srv.Start()
	}

	time.Sleep(2 * time.Second) // 先等待节点集群选出 Leader
	// 启动 HTTP 服务
	// 给每个节点都启动一个 HTTP 服务，监听不同的端口
	for idx, srv := range servers {
		hServer := httpapi.NewHTTPServer(srv)
		port := 10000 + idx // 每个节点的 HTTP 服务端口不同
		// 注意，port 不能在 goroutine 里直接使用 idx，否则异步环境下可能会捕获到错误的 idx 值，导致监听端口混乱
		go func(port int) {
			if err := hServer.Start(strconv.Itoa(port)); err != nil {
				log.Fatalf("HTTP server failed: %v", err)
			}
		}(port)
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
