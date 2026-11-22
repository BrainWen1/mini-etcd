package config

import (
	"fmt"
	"time"
)

type Config struct {
	ID            uint64            // 当前节点的 ID
	Peers         map[uint64]string // 所有节点的 ID -> 网络地址映射
	HTTPPeers     map[uint64]string // 所有节点的 ID -> HTTP 端口映射
	TickInterval  time.Duration     // Raft 的 Tick 间隔 (每隔这个时间就触发一次 Tick)
	ElectionTick  int               // 选举超时的 Tick 数量 (如果超过这个数量还没收到心跳，就发起选举)
	HeartbeatTick int               // 心跳间隔的 Tick 数量 (每隔这个数量就发一次心跳)

	WALPath      string // WAL 文件路径
	SnapshotPath string // 快照文件路径
	BackendPath  string // 后端存储路径(bbolt 数据库文件路径)

	ListenAddr string // Raft 节点监听的地址
	HTTPPort   string // HTTP API 监听的端口

	Join bool
}

func Default(id uint64, peers map[uint64]string, httpPeers map[uint64]string) Config {
	return Config{
		ID:            id,
		Peers:         peers,
		HTTPPeers:     httpPeers,
		TickInterval:  100 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,

		WALPath:      fmt.Sprintf("./data/wal/%d/", id),
		SnapshotPath: fmt.Sprintf("./data/snap/%d/", id),
		BackendPath:  fmt.Sprintf("./data/backend/%d/", id),
	}
}
