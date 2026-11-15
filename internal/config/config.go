package config

import (
	"fmt"
	"time"
)

type Config struct {
	ID            uint64        // 当前节点的 ID
	Peers         []uint64      // 所有节点的 ID 列表 (先不用管网络地址)
	TickInterval  time.Duration // Raft 的 Tick 间隔 (每隔这个时间就触发一次 Tick)
	ElectionTick  int           // 选举超时的 Tick 数量 (如果超过这个数量还没收到心跳，就发起选举)
	HeartbeatTick int           // 心跳间隔的 Tick 数量 (每隔这个数量就发一次心跳)

	WALPath      string // WAL 文件路径
	SnapshotPath string // 快照文件路径
}

func Default(id uint64, peers []uint64) Config {
	return Config{
		ID:            id,
		Peers:         peers,
		TickInterval:  100 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,
		WALPath:       fmt.Sprintf("./data/wal/%d/", id),
		SnapshotPath:  fmt.Sprintf("./data/snap/%d/", id),
	}
}
