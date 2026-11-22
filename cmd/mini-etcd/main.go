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
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	// 解析命令行参数
	idFlag := flag.Uint64("id", 0, "Node ID")
	joinFlag := flag.Bool("join", false, "Join an existing cluster instead of bootstrapping a new one")
	// 动态扩容时新节点不在下面的默认拓扑里，必须显式给出自己的地址，
	// 以及已有集群成员的 Raft 地址（用于回包/转发）。
	// example:
	// $go run ./cmd/mini-etcd -id 4 -join \
	// -raft-addr "127.0.0.1:8004" \
	// -http-addr "127.0.0.1:10004" \
	// -peers "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003,4=127.0.0.1:8004" \
	// -http-peers "1=127.0.0.1:10001,2=127.0.0.1:10002,3=127.0.0.1:10003,4=127.0.0.1:10004"
	// $curl -X POST http://127.0.0.1:10001/v1/cluster/members/add \
	//   -d '{"id":4, "raft_addr":"127.0.0.1:8004", "http_addr":"127.0.0.1:10004"}'
	raftAddrFlag := flag.String("raft-addr", "", "Raft listen address of this node, e.g. 127.0.0.1:8004")
	httpAddrFlag := flag.String("http-addr", "", "HTTP API address of this node, e.g. 127.0.0.1:10004")
	peersFlag := flag.String("peers", "", "Known cluster members' raft addresses: 1=127.0.0.1:8001,2=127.0.0.1:8002")
	httpPeersFlag := flag.String("http-peers", "", "Known cluster members' HTTP addresses: 1=127.0.0.1:10001,2=127.0.0.1:10002")
	flag.Parse()

	log.Printf("启动 mini-etcd 节点，参数: id=%d, join=%v, raft-addr=%s, http-addr=%s, peers=%s, http-peers=%s", *idFlag, *joinFlag, *raftAddrFlag, *httpAddrFlag, *peersFlag, *httpPeersFlag)

	id := *idFlag
	if id == 0 {
		log.Fatal("必须指定节点 ID，例如 -id=1")
	}
	join := *joinFlag

	// 默认拓扑：本地三节点集群。动态扩容时应通过 -peers/-http-peers
	// 传入完整拓扑，或者使用默认地址并仅指定新节点自己的地址。
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

	var err error
	if *peersFlag != "" {
		peersAddrMap, err = parsePeerMap(*peersFlag)
		if err != nil {
			log.Fatalf("invalid -peers: %v", err)
		}
	}
	if *httpPeersFlag != "" {
		httpAddrMap, err = parsePeerMap(*httpPeersFlag)
		if err != nil {
			log.Fatalf("invalid -http-peers: %v", err)
		}
	}
	// 自己的地址必须进表：Transport 依赖这张表建立出站连接，
	// Server 依赖 HTTPPeers 决定是否能把请求代理给别人。
	if *raftAddrFlag != "" {
		peersAddrMap[id] = *raftAddrFlag
	}
	if *httpAddrFlag != "" {
		httpAddrMap[id] = *httpAddrFlag
	}

	// 配置当前节点参数
	cfg := config.Default(id, peersAddrMap, httpAddrMap)
	cfg.Join = join

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
	storage, err := storage.OpenWAL(cfg.WALPath, cfg.SnapshotPath)
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

	// 状态机里的成员表随快照一起复制，是重启后最权威的地址来源：
	// 动态成员的名字和地址只存在于配置变更日志里，日志被压缩后，
	// 只能从“数据 + 成员表”这份自洽的状态机快照里恢复。
	// 必须在 ApplySnapshot 之后读取，否则读到的是快照之前的旧成员表。
	if members, err := mvccStorage.GetMembers(); err != nil {
		log.Printf("[Node %d] Warning: failed to load membership from state machine: %v", id, err)
	} else {
		cfg.MergeMembers(members)
	}

	// 校验节点 ID：如果 ID 不在集群配置里，ListenAddr 会是空字符串，
	// 节点会监听在随机端口上，看起来启动成功，实际既无法参与 Raft 通信，
	// 也无法提供 HTTP 服务。这里直接失败退出，避免出现"假活"节点。
	// 校验必须放在成员表合并之后：动态加入的节点重启后，自己的地址
	// 可能只存在于状态机快照里，命令行不一定给出。
	listenAddr, ok := cfg.Peers[id]
	if !ok {
		log.Fatalf("no raft address for node %d: pass -raft-addr and -peers when joining an existing cluster", id)
	}
	httpAddr, ok := cfg.HTTPPeers[id]
	if !ok {
		log.Fatalf("no HTTP address for node %d: pass -http-addr", id)
	}
	httpPort, err := parseHTTPPort(httpAddr)
	if err != nil {
		log.Fatalf("invalid HTTP address for node %d: %v", id, err)
	}
	cfg.ListenAddr = listenAddr
	cfg.HTTPPort = httpPort

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

	// 优雅退出：收到信号，或者本节点已被移出集群（配置变更日志被应用）
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh: // 收到终止信号，优雅退出
		log.Printf("[Node %d] 收到退出信号", id)
	case <-srv.Removed(): // 如果本节点被移出集群，srv.Removed() 返回的 removed channel 会被关闭
		log.Printf("[Node %d] 已被移出集群，优雅退出", id)
		// 给 HTTP 层一点时间把“移除成功”的响应写回客户端。
		// 移除请求可能正是打到本节点（Leader 自删）的，立刻退出会让客户端看到连接被重置。
		time.Sleep(500 * time.Millisecond)
	}

	// 停止服务
	srv.Stop()
}

// parseHTTPPort 解析 HTTP 地址字符串，返回端口部分。
// 用 net.SplitHostPort 而不是按 ":" 切分，兼容 IPv6 地址（[::1]:10001）。
func parseHTTPPort(addr string) (string, error) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		return "", fmt.Errorf("invalid address format: %s", addr)
	}
	return port, nil
}

// parsePeerMap 解析 "id=addr,id=addr" 形式的成员地址表。
func parsePeerMap(s string) (map[uint64]string, error) {
	peers := make(map[uint64]string)
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid entry %q, want id=addr", item)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("invalid node id in %q", item)
		}
		addr := strings.TrimSpace(parts[1])
		if addr == "" {
			return nil, fmt.Errorf("empty address in %q", item)
		}
		peers[id] = addr
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("no members specified")
	}
	return peers, nil
}
