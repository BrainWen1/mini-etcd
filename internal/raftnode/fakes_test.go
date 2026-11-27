package raftnode

// 本文件提供 raftnode 测试共用的 Transport / Applier 空实现。
// nop = no operation：只满足接口，不产生任何副作用。

import (
	"mini-etcd/internal/config"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// nopTransport 是一个什么都不做的 Transport 空实现。
// NewRaftNode 只把 Transport 存下来，真正收发的逻辑在 Run 里；
// 只测试构造/配置变更时不需要真实的网络。
type nopTransport struct{}

// Send 假装发送消息，实际丢弃。
func (nopTransport) Send([]raftpb.Message) {}

// Recv 返回一个永远不会有数据的 channel。
func (nopTransport) Recv() <-chan raftpb.Message { return make(chan raftpb.Message) }

// Close 什么都不用关。
func (nopTransport) Close() {}

// AddPeer 和 RemovePeer 也什么都不做。
func (nopTransport) AddPeer(id uint64, addr string) {}
func (nopTransport) RemovePeer(id uint64)           {}

// nopApplier 是 Applier 的空实现，只关心 Raft 层的测试用它。
type nopApplier struct{}

// Apply 假装应用日志，实际什么都不做。
func (nopApplier) Apply([]raftpb.Entry) error { return nil }

// ApplySnapshot 假装从快照恢复状态机，实际什么都不做。
func (nopApplier) ApplySnapshot(raftpb.Snapshot) error { return nil }

// GetSnapshot 假装导出状态机数据，返回空数据。
func (nopApplier) GetSnapshot() ([]byte, error) { return nil, nil }

// GetConsistentIndex 假装返回已应用的最高日志索引，返回 0。
func (nopApplier) GetConsistentIndex() (uint64, error) { return 0, nil }

// AdvanceConsistentIndex 假装推进已应用索引，实际什么都不做。
func (nopApplier) AdvanceConsistentIndex(uint64) error { return nil }

// UpsertMember 假装保存成员，直接回显调用方传入的地址。
func (nopApplier) UpsertMember(id uint64, raftAddr, grpcAddr string) (config.Member, error) {
	return config.Member{ID: id, RaftAddr: raftAddr, GRPCAddr: grpcAddr}, nil
}

// RemoveMember 假装删除成员，实际什么都不做。
func (nopApplier) RemoveMember(uint64) error { return nil }

// GetMembers 假装状态机里没有成员表。
func (nopApplier) GetMembers() (map[uint64]config.Member, error) {
	return map[uint64]config.Member{}, nil
}

// GetExpiredLeases 假装状态机里没有过期的 LeaseID。
func (nopApplier) GetExpiredLeases() ([]int64, error) { return nil, nil }
