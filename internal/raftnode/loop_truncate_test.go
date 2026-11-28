package raftnode

// 本文件验证 loop.go 里两个快照触发点之后的 WAL Truncate：
//
//  1. 收到 Leader 发来的快照（rd.Snapshot 非空）→ ApplySnapshot → Truncate(snapshot.Index)；
//  2. 本地累计应用超过阈值、生成快照 → SaveSnapshot → Truncate(lastAppliedIndex)。
//
// 为了不依赖真实网络和选举，这里用 scriptedNode 直接给 Run 喂预设的 Ready。
// scriptedNode 内嵌了 nil 的 raft.Node，只实现 Run 会调用的方法；
// 如果 Run 意外调用了别的方法，会直接 panic，相当于顺带做了断言。

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"mini-etcd/internal/config"
	"mini-etcd/internal/storage"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// scriptedNode 按脚本返回 Ready，用于精确控制 Run 的输入。
type scriptedNode struct {
	raft.Node // nil：未实现的方法一旦被调用就会 panic，帮助发现意外的交互
	ready     chan raft.Ready
	advanced  chan struct{}
}

func newScriptedNode() *scriptedNode {
	return &scriptedNode{
		ready:    make(chan raft.Ready, 1),
		advanced: make(chan struct{}, 8),
	}
}

func (n *scriptedNode) Ready() <-chan raft.Ready { return n.ready }
func (n *scriptedNode) Tick()                    {}
func (n *scriptedNode) Step(context.Context, raftpb.Message) error {
	return nil
}
func (n *scriptedNode) Advance()            { n.advanced <- struct{}{} }
func (n *scriptedNode) Status() raft.Status { return raft.Status{} }
func (n *scriptedNode) ApplyConfChange(raftpb.ConfChangeI) *raftpb.ConfState {
	return nil
}
func (n *scriptedNode) Stop()                                   {}
func (n *scriptedNode) ReadIndex(context.Context, []byte) error { return nil }
func (n *scriptedNode) Propose(context.Context, []byte) error   { return nil }

// waitAdvanced 等待 Run 完整处理完一个 Ready（Advance 是 Ready 分支的最后一步）。
func (n *scriptedNode) waitAdvanced(t *testing.T) {
	t.Helper()
	select {
	case <-n.advanced:
	case <-time.After(5 * time.Second):
		t.Fatal("Run 未在 5s 内处理完注入的 Ready")
	}
}

// loopTestApplier 是一个记录 consistentIndex / 快照调用的最小 Applier。
type loopTestApplier struct {
	nopApplier
	consistent       uint64
	appliedSnapshots []uint64
}

func (a *loopTestApplier) Apply(entries []raftpb.Entry) error {
	if n := len(entries); n > 0 && entries[n-1].Index > a.consistent {
		a.consistent = entries[n-1].Index
	}
	return nil
}

func (a *loopTestApplier) GetConsistentIndex() (uint64, error) { return a.consistent, nil }

func (a *loopTestApplier) AdvanceConsistentIndex(index uint64) error {
	if index > a.consistent {
		a.consistent = index
	}
	return nil
}

func (a *loopTestApplier) GetSnapshot() ([]byte, error) { return []byte("state-machine-snapshot"), nil }

func (a *loopTestApplier) ApplySnapshot(snap raftpb.Snapshot) error {
	a.appliedSnapshots = append(a.appliedSnapshots, snap.Metadata.Index)
	return nil
}

func (a *loopTestApplier) GetMembers() (map[uint64]config.Member, error) {
	return map[uint64]config.Member{1: {ID: 1, RaftAddr: "127.0.0.1:8001"}}, nil
}

// newLoopTestNode 直接构造 Run 需要的字段，绕过 NewRaftNode 的真实 raft 启动。
func newLoopTestNode(t *testing.T, w *storage.WAL, applier Applier) (*RaftNode, *scriptedNode) {
	t.Helper()
	sn := newScriptedNode()
	return &RaftNode{
		Node:              sn,
		storage:           w,
		trans:             nopTransport{},
		applier:           applier,
		nodeID:            1,
		readRequests:      make(chan *readIndexRequest, 8),
		readBatches:       make(map[string]*readIndexBatch),
		stopc:             make(chan struct{}),
		confChangeWaiters: make(map[string][]chan error),
		removed:           make(chan struct{}),
		members:           map[uint64]config.Member{1: {ID: 1, RaftAddr: "127.0.0.1:8001"}},
	}, sn
}

// loopTestEntries 生成连续日志，数据非空，确保 Run 会交给 Applier.Apply。
func loopTestEntries(from, to uint64) []raftpb.Entry {
	entries := make([]raftpb.Entry, 0, to-from+1)
	for i := from; i <= to; i++ {
		entries = append(entries, raftpb.Entry{
			Index: i,
			Term:  1,
			Type:  raftpb.EntryNormal,
			Data:  []byte{byte('a' + i%26)},
		})
	}
	return entries
}

func loopWALFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".wal" {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	return files
}

// assertDeletedPrefix 断言被删掉的都是文件名最小的那批分段。
// after 里允许出现 before 中不存在的新文件：触发快照的那次 Save 也可能让活跃分段轮转。
func assertDeletedPrefix(t *testing.T, before, after []string) {
	t.Helper()
	if len(after) == 0 {
		t.Fatalf("活跃分段被误删：删除前 %v", before)
	}

	exists := make(map[string]bool, len(after))
	for _, name := range after {
		exists[name] = true
	}
	var kept []string
	for _, name := range before {
		if exists[name] {
			kept = append(kept, name)
		}
	}
	if len(kept) == len(before) {
		t.Fatalf("没有任何旧分段被删除：before=%v after=%v", before, after)
	}
	// 保留下来的旧分段必须是 before 的一个后缀。
	suffix := before[len(before)-len(kept):]
	for i := range kept {
		if kept[i] != suffix[i] {
			t.Fatalf("保留的旧分段 %v 不是 before=%v 的后缀", kept, before)
		}
	}
}

// prefilledWAL 打开一个小分段的 WAL，并预先写入 1..total 条日志。
func prefilledWAL(t *testing.T, walDir, snapDir string, total uint64) *storage.WAL {
	t.Helper()
	w, err := storage.OpenWALWithSegmentSize(walDir, snapDir, 1024)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	for _, e := range loopTestEntries(1, total) {
		// Term 非空保证每条都落盘；Commit 留空，避免和后面 Ready 的 Commit 冲突
		if err := w.Save(raftpb.HardState{Term: 1}, []raftpb.Entry{e}); err != nil {
			t.Fatalf("Save(index=%d): %v", e.Index, err)
		}
	}
	return w
}

// TestRunTruncatesWALAfterLocalSnapshot 覆盖第二个触发点：
// 本地应用日志达到 snapshotThreshold 后生成快照，并 Truncate(lastAppliedIndex)。
func TestRunTruncatesWALAfterLocalSnapshot(t *testing.T) {
	const total = 40
	const snapshotAt = 30 // 1..30 已应用，31..40 尚未提交

	walDir, snapDir := t.TempDir(), t.TempDir()
	w := prefilledWAL(t, walDir, snapDir, total)
	before := loopWALFiles(t, walDir)
	if len(before) < 4 {
		t.Fatalf("前置条件不成立：只生成了 %d 个分段，无法验证 Truncate", len(before))
	}

	applier := &loopTestApplier{}
	rn, sn := newLoopTestNode(t, w, applier)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		rn.Run(stop, time.Hour) // ticker 设得很长，避免干扰脚本化 Ready
		close(done)
	}()

	// 应用 1..30：lastSnapshotIndex=0，30-0 >= snapshotThreshold(10)，必然触发快照。
	sn.ready <- raft.Ready{
		HardState:        raftpb.HardState{Term: 1, Commit: snapshotAt},
		CommittedEntries: loopTestEntries(1, snapshotAt),
	}
	sn.waitAdvanced(t)

	after := loopWALFiles(t, walDir)
	assertDeletedPrefix(t, before, after)

	close(stop)
	<-done
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 重启：快照索引 30 已落盘，30 之前的 WAL 分段被删除，31..40 必须还能恢复。
	w2, err := storage.OpenWALWithSegmentSize(walDir, snapDir, 1024)
	if err != nil {
		t.Fatalf("重启 OpenWAL: %v", err)
	}
	defer w2.Close()

	snap, err := w2.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Metadata.Index != snapshotAt {
		t.Fatalf("快照索引 = %d，期望 %d", snap.Metadata.Index, snapshotAt)
	}
	last, err := w2.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if last != total {
		t.Fatalf("重启后 LastIndex = %d，期望 %d（快照之后的日志不能被误删）", last, total)
	}
	got, err := w2.Entries(snapshotAt+1, total+1, math.MaxUint64)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(got) != total-snapshotAt {
		t.Fatalf("重启后快照之后的日志条数 = %d，期望 %d", len(got), total-snapshotAt)
	}
}

// TestRunTruncatesWALAfterReceivedSnapshot 覆盖第一个触发点：
// 收到 Leader 的快照后 ApplySnapshot，并 Truncate(snapshot.Index)。
func TestRunTruncatesWALAfterReceivedSnapshot(t *testing.T) {
	const total = 40
	const snapshotAt = 30

	walDir, snapDir := t.TempDir(), t.TempDir()
	w := prefilledWAL(t, walDir, snapDir, total)
	before := loopWALFiles(t, walDir)
	if len(before) < 4 {
		t.Fatalf("前置条件不成立：只生成了 %d 个分段，无法验证 Truncate", len(before))
	}

	applier := &loopTestApplier{}
	rn, sn := newLoopTestNode(t, w, applier)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		rn.Run(stop, time.Hour)
		close(done)
	}()

	// 模拟 Leader 直接发快照帮落后的 follower 追赶。
	// Data 必须非空：loop.go 会拒绝空数据快照并终止进程。
	sn.ready <- raft.Ready{
		Snapshot: raftpb.Snapshot{
			Data: []byte("leader-state-machine-snapshot"),
			Metadata: raftpb.SnapshotMetadata{
				Index:     snapshotAt,
				Term:      1,
				ConfState: raftpb.ConfState{Voters: []uint64{1}},
			},
		},
	}
	sn.waitAdvanced(t)

	after := loopWALFiles(t, walDir)
	assertDeletedPrefix(t, before, after)

	if len(applier.appliedSnapshots) != 1 || applier.appliedSnapshots[0] != snapshotAt {
		t.Fatalf("状态机收到的快照 = %v，期望 [%d]", applier.appliedSnapshots, snapshotAt)
	}

	close(stop)
	<-done
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 重启验证：快照文件 + 保留下来的 WAL 分段能正常拼出完整日志。
	w2, err := storage.OpenWALWithSegmentSize(walDir, snapDir, 1024)
	if err != nil {
		t.Fatalf("重启 OpenWAL: %v", err)
	}
	defer w2.Close()

	snap, err := w2.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Metadata.Index != snapshotAt {
		t.Fatalf("重启后快照索引 = %d，期望 %d", snap.Metadata.Index, snapshotAt)
	}
	last, err := w2.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if last != total {
		t.Fatalf("重启后 LastIndex = %d，期望 %d", last, total)
	}
	got, err := w2.Entries(snapshotAt+1, total+1, math.MaxUint64)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(got) != total-snapshotAt {
		t.Fatalf("重启后快照之后的日志条数 = %d，期望 %d", len(got), total-snapshotAt)
	}
}
