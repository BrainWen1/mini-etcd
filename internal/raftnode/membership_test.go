package raftnode

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"mini-etcd/internal/apply"
	"mini-etcd/internal/backend"
	"mini-etcd/internal/config"
	"mini-etcd/internal/mvcc"
	"mini-etcd/internal/storage"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// waitForLeader 等待节点成为 Leader（单节点集群会很快选出自己）。
func waitForLeader(t *testing.T, rn *RaftNode, id uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for rn.GetLeaderID() != id {
		if time.Now().After(deadline) {
			t.Fatalf("节点 %d 未能在 3s 内成为 Leader", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestProposeConfChangeAndWait 在单节点集群上验证完整的配置变更链路：
//  1. ProposeConfChangeAndWait 必须等到变更被本地应用后才返回；
//  2. Run 循环必须把新增成员从 ConfChange.Context 解析出来，并回调成员处理器。
func TestProposeConfChangeAndWait(t *testing.T) {
	walDir, snapDir := t.TempDir(), t.TempDir()
	w, err := storage.OpenWAL(walDir, snapDir)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	cfg := config.Default(1,
		map[uint64]string{1: "127.0.0.1:8001"},
		map[uint64]string{1: "127.0.0.1:10001"})

	rn := NewRaftNode(cfg, w, nopTransport{}, nopApplier{})

	// 配置变更被应用时会把事件投进这个带缓冲的 channel。
	// 先注册再启动 Run，避免漏掉 Bootstrap 产生的初始配置变更。
	type event struct {
		id       uint64
		raftAddr string
		httpAddr string
		added    bool
	}
	events := make(chan event, 16)
	rn.SetMembershipChangeHandler(func(id uint64, raftAddr, httpAddr string, added bool) {
		events <- event{id: id, raftAddr: raftAddr, httpAddr: httpAddr, added: added}
	})

	stop := make(chan struct{})
	go rn.Run(stop, 10*time.Millisecond)
	defer func() {
		close(stop)
		// 给 Run 循环一点时间退出，避免测试结束时还有 goroutine 在写 WAL。
		time.Sleep(50 * time.Millisecond)
	}()

	// 等单节点集群选出自己。
	waitForLeader(t, rn, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cc := &raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  4,
		Context: config.EncodeMemberContext("127.0.0.1:8004", "127.0.0.1:10004"),
	}
	if err := rn.ProposeConfChangeAndWait(ctx, cc); err != nil {
		t.Fatalf("ProposeConfChangeAndWait(add) = %v", err)
	}

	// 在事件流里找到 node 4 的新增事件，确认两种地址都被带出来了。
	for {
		select {
		case ev := <-events:
			if ev.id != 4 {
				continue // Bootstrap 产生的初始成员事件
			}
			if !ev.added || ev.raftAddr != "127.0.0.1:8004" || ev.httpAddr != "127.0.0.1:10004" {
				t.Fatalf("unexpected membership event: %+v", ev)
			}
			return
		case <-time.After(3 * time.Second):
			t.Fatal("等待 node 4 的成员变更事件超时")
		}
	}
}

// recordingTransport 记录 Raft 循环对 Transport 的增删操作。
type recordingTransport struct {
	nopTransport
	added   map[uint64]string
	removed map[uint64]bool
}

func newRecordingTransport() *recordingTransport {
	return &recordingTransport{
		added:   make(map[uint64]string),
		removed: make(map[uint64]bool),
	}
}

func (t *recordingTransport) AddPeer(id uint64, addr string) { t.added[id] = addr }
func (t *recordingTransport) RemovePeer(id uint64)           { t.removed[id] = true }

// staticApplier 是一个成员表固定的 Applier，用于直接测试快照同步逻辑。
type staticApplier struct {
	nopApplier
	members map[uint64]config.Member
}

func (a *staticApplier) GetMembers() (map[uint64]config.Member, error) {
	return a.members, nil
}

// TestSyncMembershipFromSnapshot 是“节点离线期间集群新增成员”的直接回归测试。
//
// 场景：节点 4 掉线，期间节点 5 加入；节点 4 恢复后只收到一份快照。
// 快照的 ConfState 只有 ID（1,2,3,4,5），地址必须从状态机里恢复；
// 否则节点 4 不知道 Leader 5 的 Raft 地址（GET 的 ReadIndex 发不出去），
// 代理表里也没有 5 的 HTTP 地址（写请求 503）。
func TestSyncMembershipFromSnapshot(t *testing.T) {
	trans := newRecordingTransport()
	applier := &staticApplier{members: map[uint64]config.Member{
		1: {ID: 1, RaftAddr: "127.0.0.1:8001", HTTPAddr: "127.0.0.1:10001"},
		2: {ID: 2, RaftAddr: "127.0.0.1:8002", HTTPAddr: "127.0.0.1:10002"},
		3: {ID: 3, RaftAddr: "127.0.0.1:8003", HTTPAddr: "127.0.0.1:10003"},
		4: {ID: 4, RaftAddr: "127.0.0.1:8004", HTTPAddr: "127.0.0.1:10004"},
		// 节点 5 的地址只存在于快照携带的状态机成员表里。
		5: {ID: 5, RaftAddr: "127.0.0.1:8005", HTTPAddr: "127.0.0.1:10005"},
	}}

	type event struct {
		id       uint64
		raftAddr string
		httpAddr string
		added    bool
	}
	events := make(chan event, 16)

	rn := &RaftNode{
		nodeID:  4,
		trans:   trans,
		applier: applier,
		// 恢复前节点 4 的本地成员表：只有 1..4，且 6 是离线期间被移除的陈旧成员。
		members: map[uint64]config.Member{
			1: {ID: 1, RaftAddr: "127.0.0.1:8001", HTTPAddr: "127.0.0.1:10001"},
			6: {ID: 6, RaftAddr: "127.0.0.1:8006", HTTPAddr: "127.0.0.1:10006"},
		},
	}
	rn.SetMembershipChangeHandler(func(id uint64, raftAddr, httpAddr string, added bool) {
		events <- event{id: id, raftAddr: raftAddr, httpAddr: httpAddr, added: added}
	})

	rn.syncMembershipFromSnapshot(raftpb.ConfState{Voters: []uint64{1, 2, 3, 4, 5}})

	// 5 的 Raft 地址必须进入 Transport，否则它永远读不到 Leader 的 ReadIndex 响应。
	if got := trans.added[5]; got != "127.0.0.1:8005" {
		t.Fatalf("transport address for node 5 = %q, want 127.0.0.1:8005", got)
	}
	// 快照里不存在的旧成员 6 必须被移除。
	if !trans.removed[6] {
		t.Fatal("stale member 6 was not removed from transport")
	}

	var sawAdd5, sawRemove6 bool
	for len(events) > 0 {
		ev := <-events
		switch {
		case ev.id == 5 && ev.added:
			sawAdd5 = true
			if ev.raftAddr != "127.0.0.1:8005" || ev.httpAddr != "127.0.0.1:10005" {
				t.Fatalf("node 5 event = %+v, want both addresses", ev)
			}
		case ev.id == 6 && !ev.added:
			sawRemove6 = true
		}
	}
	if !sawAdd5 || !sawRemove6 {
		t.Fatalf("membership events = add5:%v remove6:%v, want both", sawAdd5, sawRemove6)
	}

	// 成员表已更新：5 在，6 不在。
	members := rn.Membership()
	if _, ok := members[5]; !ok {
		t.Fatalf("members = %+v, want node 5", members)
	}
	if _, ok := members[6]; ok {
		t.Fatalf("members = %+v, stale node 6 should be removed", members)
	}
}

// TestConfChangesReplayAfterRestart 是“重启丢成员配置”的回归测试。
//
// 背景：快照的 ConfState 只记录到 snapshot.Metadata.Index 为止的成员配置，
// 而 (snapshotIndex, committed] 之间可能还有 AddNode/RemoveNode 日志。
// 如果 raft.Config.Applied 直接取状态机的 consistentIndex，raft 就不会重放
// 这段日志，也不会应用其中的配置变更；重启后 raft 内存里的成员配置会回退，
// 例如 4 节点集群重启后退回 3 节点的多数派判断。
//
// 本测试：单节点集群先 AddNode(4)，此时快照还停在 bootstrap 的 index=1，
// 然后重启 RaftNode，断言 AddNode(4) 被重新应用（成员事件再次触发）。
func TestConfChangesReplayAfterRestart(t *testing.T) {
	walDir, snapDir := t.TempDir(), t.TempDir()

	be, err := backend.NewBackend(t.TempDir())
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}
	m := mvcc.NewMVCC(be)
	t.Cleanup(func() {
		_ = m.Close()
		_ = be.Close()
	})
	applier := apply.NewApplier(m)

	cfg := config.Default(1,
		map[uint64]string{1: "127.0.0.1:8001"},
		map[uint64]string{1: "127.0.0.1:10001"})

	// ---------- 第一次启动：添加 node 4 ----------
	w1, err := storage.OpenWAL(walDir, snapDir)
	if err != nil {
		t.Fatalf("OpenWAL(first): %v", err)
	}
	rn1 := NewRaftNode(cfg, w1, nopTransport{}, applier)
	stop1 := make(chan struct{})
	go rn1.Run(stop1, 10*time.Millisecond)
	waitForLeader(t, rn1, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cc := &raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  4,
		Context: config.EncodeMemberContext("127.0.0.1:8004", "127.0.0.1:10004"),
	}
	if err := rn1.ProposeConfChangeAndWait(ctx, cc); err != nil {
		t.Fatalf("ProposeConfChangeAndWait(add): %v", err)
	}

	// 停掉第一个节点并关闭 WAL。
	close(stop1)
	time.Sleep(100 * time.Millisecond)
	if err := w1.Close(); err != nil {
		t.Fatalf("close WAL(first): %v", err)
	}

	// 删掉所有快照文件，模拟“配置变更已经应用到状态机/索引，但新快照还没来得及
	// 落盘就崩溃”的窗口。此时重启只能靠 WAL 重放：WAL 从 index=1 开始，
	// 没有缺口，全部配置变更都能被重新应用。
	snaps, err := filepath.Glob(filepath.Join(snapDir, "*.snap"))
	if err != nil {
		t.Fatalf("glob snapshots: %v", err)
	}
	for _, p := range snaps {
		if err := os.Remove(p); err != nil {
			t.Fatalf("remove snapshot %s: %v", p, err)
		}
	}

	// ---------- 第二次启动：模拟重启 ----------
	w2, err := storage.OpenWAL(walDir, snapDir)
	if err != nil {
		t.Fatalf("OpenWAL(second): %v", err)
	}
	rn2 := NewRaftNode(cfg, w2, nopTransport{}, applier)

	added := make(chan config.Member, 4)
	rn2.SetMembershipChangeHandler(func(id uint64, raftAddr, httpAddr string, isAdd bool) {
		if isAdd && id == 4 {
			added <- config.Member{ID: id, RaftAddr: raftAddr, HTTPAddr: httpAddr}
		}
	})

	stop2 := make(chan struct{})
	go rn2.Run(stop2, 10*time.Millisecond)
	t.Cleanup(func() {
		close(stop2)
		time.Sleep(50 * time.Millisecond)
		_ = w2.Close()
	})

	select {
	case member := <-added:
		if member.RaftAddr != "127.0.0.1:8004" || member.HTTPAddr != "127.0.0.1:10004" {
			t.Fatalf("replayed member = %+v, want both addresses", member)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("重启后没有重放 AddNode(4)：raft 会丢失快照之后的成员配置")
	}

	// 最关键的断言：raft 内存里的成员配置必须包含 4。
	// 只看成员事件/成员表不够——它们由应用层维护；raft.Status 才代表
	// 选举多数派和消息发送真正使用的配置。
	deadline := time.Now().Add(3 * time.Second)
	for {
		voters := rn2.Status().Config.Voters.IDs()
		if _, ok := voters[4]; ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("raft voters = %v, want node 4（重启只恢复快照 ConfState 会丢掉快照后的变更）", voters)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 成员表也必须能从状态机里读回来（随重启持久化）。
	members, err := applier.GetMembers()
	if err != nil {
		t.Fatalf("GetMembers: %v", err)
	}
	if members[4].RaftAddr != "127.0.0.1:8004" {
		t.Fatalf("persisted members = %+v, want member 4", members)
	}
}

// TestConfChangeAdvancesConsistentIndex 是“重启 panic”的回归测试。
//
// 配置变更（以及 Leader 当选写入的空日志）没有业务数据，如果 consistentIndex
// 不跟着推进，快照索引会大于状态机索引；重启时 raft 的 appliedTo 会因为
// Applied < snapshotIndex 直接 panic。这里用真实的 MVCC/Applier 验证
// 配置变更之后 consistentIndex 确实前进了。
func TestConfChangeAdvancesConsistentIndex(t *testing.T) {
	walDir, snapDir := t.TempDir(), t.TempDir()
	w, err := storage.OpenWAL(walDir, snapDir)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	be, err := backend.NewBackend(t.TempDir())
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}
	defer be.Close()
	m := mvcc.NewMVCC(be)
	applier := apply.NewApplier(m)

	cfg := config.Default(1,
		map[uint64]string{1: "127.0.0.1:8001"},
		map[uint64]string{1: "127.0.0.1:10001"})
	rn := NewRaftNode(cfg, w, nopTransport{}, applier)

	stop := make(chan struct{})
	go rn.Run(stop, 10*time.Millisecond)
	defer func() {
		close(stop)
		time.Sleep(50 * time.Millisecond)
	}()

	waitForLeader(t, rn, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cc := &raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  4,
		Context: config.EncodeMemberContext("127.0.0.1:8004", "127.0.0.1:10004"),
	}
	if err := rn.ProposeConfChangeAndWait(ctx, cc); err != nil {
		t.Fatalf("ProposeConfChangeAndWait(add) = %v", err)
	}

	idx, err := m.GetConsistentIndex()
	if err != nil {
		t.Fatalf("GetConsistentIndex: %v", err)
	}
	// 单节点启动至少会应用 index=1 的初始 ConfChange 和随后的 no-op/配置变更。
	if idx == 0 {
		t.Fatal("consistentIndex 仍是 0：配置变更/空日志没有推进状态机索引，重启时快照会越界")
	}
	t.Logf("consistentIndex after conf change = %d", idx)
}
