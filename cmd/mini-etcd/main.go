package main

import (
	"flag"
	"fmt"
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
	"strings"
	"syscall"
	"time"
)

func main() {
	// 解析命令行参数，获取节点 ID
	idFlag := flag.Uint64("id", 0, "Node ID")
	flag.Parse()
	id := *idFlag

	// 假设硬编码集群配置
	peersAddrMap := map[uint64]string{ // 用于 Raft 节点之间的通信，需要知道完整的 ip 和 port
		1: "127.0.0.1:8001",
		2: "127.0.0.1:8002",
		3: "127.0.0.1:8003",
	}
	httpAddrMap := map[uint64]string{ // 用于启动 HTTP 服务，提供客户端访问接口，只需要知道端口号即可
		1: "127.0.0.1:10001",
		2: "127.0.0.1:10002",
		3: "127.0.0.1:10003",
	}

	// 校验节点 ID：如果 ID 不在集群配置里，ListenAddr 会是空字符串，
	// 节点会监听在随机端口上，看起来启动成功，实际既无法参与 Raft 通信，
	// 也无法提供 HTTP 服务。这里直接失败退出，避免出现"假活"节点。
	listenAddr, ok := peersAddrMap[id]
	if !ok {
		log.Fatalf("invalid -id %d: not found in cluster config (valid IDs: 1, 2, 3)", id)
	}
	httpPort, err := parseHTTPPort(httpAddrMap[id])
	if err != nil {
		log.Fatalf("no HTTP port configured for node %d: %v", id, err)
	}

	// 配置当前节点参数
	cfg := config.Default(id, peersAddrMap, httpAddrMap)
	cfg.ListenAddr = listenAddr
	cfg.HTTPPort = httpPort

	// 启动节点
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

	// 创建 TCPTransport 实例（同步完成端口监听，失败时直接退出）
	trans, err := transport.NewTCPTransport(cfg)
	if err != nil {
		log.Fatalf("Failed to start TCP transport for node %d: %v", id, err)
	}

	// 组装 Server
	srv := server.NewServer(cfg, storage, trans, applier, mvccStorage)
	go srv.Start()

	time.Sleep(2 * time.Second) // 先等待节点集群选出 Leader
	// 启动 HTTP 服务
	hServer := httpapi.NewHTTPServer(srv)
	go func(port string) {
		if err := hServer.Start(port); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}(cfg.HTTPPort)

	// 优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 停止服务
	srv.Stop()
}

// parseHTTPPort 解析 HTTP 端口字符串，确保格式正确
func parseHTTPPort(portStr string) (string, error) {
	parts := strings.Split(portStr, ":") // 确保格式为 "host:port"
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid HTTP port format: %s", portStr)
	}

	return parts[1], nil
}
