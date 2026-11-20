package raftnode

// 本文件是 NewRaftNode 重启分支的回归测试，只覆盖一件事：
// 磁盘上的快照没有携带 ConfState（集群成员列表）时，不能让 confState 保持空值。
//
// ---------------------------------------------------------------------------
// 一、背景
// ---------------------------------------------------------------------------
//
// ConfState 是 raftpb 里的一个结构，最重要的字段是 Voters：当前集群的所有投票成员 ID。
// 它决定了"一次选举需要几票才算赢"（多数派），也决定了 raft 在重启时认不认得自己属于哪个集群。
//
// 它存在哪儿？它不在 HardState 里，而是**挂在快照的元数据上**：
//
//   Snapshot.Metadata.ConfState.Voters = [1, 2, 3]
//
// 所以 raft 每次重启，都需要从"最新快照"里读出 ConfState 才知道集群成员。
//
// ---------------------------------------------------------------------------
// 二、没有它会发生什么
// ---------------------------------------------------------------------------
//
// 本项目的 Node 结构体自己缓存了一份 confState（node.go 里的 RaftNode.confState），
// 打本地快照时用的是这一份（loop.go: CreateSnapshot(lastAppliedIndex, &rn.confState, ...)）。
//
// 如果重启分支写成：
//
//   if len(cs.Voters) != 0 { confState = cs }
//   // 没有 else —— 快照里没 Voters 时 confState 就是零值 raftpb.ConfState{}
//
// 那么接下来第一次触发本地快照时，就会把 Voters=[] 写进 .snap 文件。
// 之后每次重启都从这份空快照里读 ConfState，集群成员信息永久丢失，
// 节点会认为"集群里没有任何投票者"，选举/多数派判断全部失效，且无法自动恢复。
//
// 修复方式：快照里没 Voters 时，回退到启动配置里的 cfg.Peers（例如 main.go 里写死的 [1,2,3]），
// 并打一条 Warning 提醒运维"你的快照数据不完整"。
//
// 什么时候会出现"快照没有 ConfState"？
//   - 旧版本代码写出的快照格式不对；
//   - 快照目录被人工清空，只剩下 WAL（这种情况 OpenWAL 读不到快照，ConfState 自然为空）；
//   - 未来做快照格式升级时漏字段。
//
// 正常路径（快照里有 Voters）不会被这个测试覆盖，由真实启动冒烟测试覆盖。

import (
	"testing"

	"mini-etcd/internal/config"
	"mini-etcd/internal/storage"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// nopTransport 是一个什么都不做的 Transport 空实现（nop = no operation）。
// NewRaftNode 内部并不会调用 Send/Recv（它只是把接口存下来，真正收发的逻辑在 Run 里），
// 但参数类型要求必须传一个 Transport，所以这里塞一个空的。
type nopTransport struct{}

// Send 假装发送消息，实际丢弃。
func (nopTransport) Send([]raftpb.Message) {}

// Recv 返回一个永远不会有数据的 channel。注意每个测试只调用一次，所以每次新建 channel 没问题。
func (nopTransport) Recv() <-chan raftpb.Message { return make(chan raftpb.Message) }

// Close 什么都不用关。
func (nopTransport) Close() {}

// nopApplier 是 Applier 的空实现，本测试不关心状态机。
type nopApplier struct{}

// Apply 假装应用日志，实际什么都不做。
func (nopApplier) Apply([]raftpb.Entry) error { return nil }

// ApplySnapshot 假装从快照恢复状态机，实际什么都不做。
func (nopApplier) ApplySnapshot(raftpb.Snapshot) error { return nil }

// GetSnapshot 假装导出状态机数据，返回空数据。
func (nopApplier) GetSnapshot() ([]byte, error) { return nil, nil }

// GetConsistentIndex 假装返回已应用的最高日志索引，返回 0。
func (nopApplier) GetConsistentIndex() (uint64, error) { return 0, nil }

// TestConfStateFallback 验证：快照里没有 ConfState 时，NewRaftNode 会回退到 cfg.Peers。
//
// 测试步骤：
//  1. 造一个磁盘快照，Index=10、Term=2，但**故意不填 ConfState**；
//  2. 重新 OpenWAL —— 这时磁盘上已经有快照了，NewRaftNode 会走 Restart 分支而不是 Bootstrap；
//  3. 调用 NewRaftNode，断言它缓存下来的 confState.Voters 等于配置里的 [1 2 3]。
func TestConfStateFallback(t *testing.T) {
	// 两个独立的临时目录，测试结束自动删除，不会碰你仓库里的 ./data。
	walDir := t.TempDir()
	snapDir := t.TempDir()

	// 第一个 WAL 句柄只用来往磁盘上写快照文件（模拟"上次运行留下的数据"）。
	w0, err := storage.OpenWAL(walDir, snapDir)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// 关键：这里只有 Index 和 Term，ConfState 是零值（Voters 为 nil）。
	// 现状下 SaveSnapshot 不做校验，会老老实实把这份"残缺快照"写到 snapDir 下。
	if err := w0.SaveSnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
		Index: 10,
		Term:  2,
	}}); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}
	if err := w0.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 第二个 WAL 句柄模拟"重启"：OpenWAL 会把刚才那份快照 ApplySnapshot 进 MemoryStorage，
	// 于是 LastIndex() 变成 10（非 0），NewRaftNode 就会走 Restart 分支。
	w, err := storage.OpenWAL(walDir, snapDir)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	defer w.Close()

	// 启动配置里，集群成员是 1、2、3。这是兜底数据的来源。
	cfg := config.Default(1, map[uint64]string{
		1: "127.0.0.1:8001",
		2: "127.0.0.1:8002",
		3: "127.0.0.1:8003",
	},
		map[uint64]string{
			1: "127.0.0.1:10001",
			2: "127.0.0.1:10002",
			3: "127.0.0.1:10003",
		})

	// 真正调用被测函数。修复前：rn.confState.Voters 为空（bug）。
	// 修复后：打一条 Warning，并把 Voters 填成 [1 2 3]。
	rn := NewRaftNode(cfg, w, nopTransport{}, nopApplier{})

	// NewRaftNode 内部调用 raft.RestartNode 启动了后台 goroutine，必须 Stop，否则测试结束时 goroutine 还活着。
	defer rn.Node.Stop()

	// 断言兜底生效。
	if len(rn.confState.Voters) != 3 {
		t.Fatalf("confState = %+v, want voters [1 2 3]（空 ConfState 必须回退到 cfg.Peers）", rn.confState)
	}

	t.Logf("confState fallback ok: %v", rn.confState.Voters)
}
