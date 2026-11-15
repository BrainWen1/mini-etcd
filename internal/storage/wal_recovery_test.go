package storage

// 本文件是 OpenWAL 的回归测试，只覆盖一件事：崩溃重启时，磁盘上的快照和 WAL 可能不一致，
// OpenWAL 必须能正确处理。
//
// ---------------------------------------------------------------------------
// 一、先把名词解释清楚
// ---------------------------------------------------------------------------
//
// HardState（硬状态）：Raft 必须持久化的"当前状态"，只有三个字段：
//   - Term   ：当前任期。可以理解成"第几届领导"，每发生一次选举就 +1。
//   - Vote   ：这一任期里我把票投给了哪个节点。0 等于 raft.None，表示"这届我还没投票"。
//   - Commit ：已提交（committed）的最大日志索引。Commit=N 的意思是"index<=N 的日志都已经
//              被集群多数派确认，可以安全交给状态机执行了"。
//
// Entry（日志条目）：Raft 存的一条操作。Index 是它的位置，从 1 开始严格 +1 递增；
//   Term 是它被写入时的任期。
//
// Snapshot（快照）：把"截至 index=N 的日志全部执行完之后，状态机长什么样"序列化存成一个文件。
//   好处是可以扔掉 index<=N 的旧日志，重启时直接反序列化恢复，不用从 1 开始重放。
//   本项目把快照单独存成 snapDir 下的 xxx.snap 文件，不存进 wal.log。
//
// MemoryStorage：etcd/raft 提供的内存版 raft.Storage。Raft 算法运行时会高频调用
//   Entries()/Term() 等接口，每次都读磁盘会慢死，所以内存里放一份缓存。
//   进程重启后内存清空，所以 OpenWAL 必须把磁盘上的快照 + WAL 重新灌回 MemoryStorage。
//
// ---------------------------------------------------------------------------
// 二、我们要防的 bug 到底是怎么产生的
// ---------------------------------------------------------------------------
//
// 处理一个 Raft Ready 时，本项目 loop.go 的顺序是：
//
//   1. SaveSnapshot(rd.Snapshot)   把快照写入 .snap 文件
//   2. ApplySnapshot(rd.Snapshot)  更新内存
//   3. Save(rd.HardState, rd.Entries) 把 HardState 和日志追加进 wal.log
//
// 注意 1 和 3 是两次独立的磁盘写入，中间没有事务。如果进程在第 1 步之后、第 3 步之前被 kill
// （或者机器断电），磁盘上就会出现：
//
//   - .snap 文件：index = 100, term = 5   ← 新的
//   - wal.log  ：HardState.Commit = 2      ← 旧的，还停留在崩溃前
//
// 重启时 OpenWAL 会先 ApplySnapshot(index=100)，此时 MemoryStorage 里 firstIndex=101、
// committed=100。然后它把 WAL 里读到的旧 HardState（Commit=2）SetHardState 进去，
// 结果 committed 从 100 被"倒退"回 2。
// 随后 raft.RestartNode → newRaft → loadState 发现 Commit 落在快照索引之下，直接 panic：
//
//   panic: 1 state.commit 2 is out of range [100, 100]
//
// 修复方式：快照本身就是"index=100 已经提交"的持久化证据（快照只可能基于已提交的日志生成），
// 所以 OpenWAL 看到 HardState.Commit < 快照 Index 时，以快照为准把 Commit 抬上去；
// Term 也落后时一并抬高，并且必须把 Vote 清空（见 TestOpenWALRepairsStaleHardState 的注释）。
//
// 结论：这个崩溃窗口在"快照和 WAL 分两个文件存"的设计下无法彻底消除，只能让重启路径容忍它。

import (
	"testing"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// TestOpenWALRepairsStaleHardState 覆盖上面说的崩溃窗口：
// "快照先落盘成功，HardState 还没来得及写进 wal.log，进程就挂了"。
//
// 测试步骤（一步步对应真实时间线）：
//  1. 造一个 follower 的本地状态：WAL 里只有 index 1~2 两条日志，HardState.Commit=2；
//  2. leader 发来一个 index=100 的快照，follower 把 .snap 文件写到磁盘；
//  3. 此时模拟崩溃：直接 Close 掉 WAL，不写新的 HardState；
//  4. 重新 OpenWAL，检查它有没有把 Commit 修正成 100；
//  5. 最关键的一步：真的调用 raft.RestartNode。修复前这里会 panic，测试会挂掉。
func TestOpenWALRepairsStaleHardState(t *testing.T) {
	// t.TempDir() 会为本次测试创建两个独立的临时目录，测试结束后由 go test 自动删除，
	// 所以这个测试不会碰到你仓库里的 ./data 目录，可以放心反复跑。
	walDir := t.TempDir()
	snapDir := t.TempDir()

	// ---- 第 1 步：构造崩溃前的磁盘状态 ----
	// w 是"第一次运行"的进程持有的 WAL 句柄。
	w, err := OpenWAL(walDir, snapDir)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Save 会把「HardState + 一批日志」打包成一条记录，先 fsync 到 wal.log，再更新 MemoryStorage。
	// 这里模拟 follower 落后的状态：
	//   - 只有 index=1(term=1) 和 index=2(term=2) 两条日志；
	//   - Term=2 说明它至少经历过一次任期变更；
	//   - Commit=2 说明这两条都已经提交；
	//   - Vote 留空（0 = raft.None），表示这个任期还没投票，对本测试无影响。
	if err := w.Save(raftpb.HardState{Term: 2, Commit: 2}, []raftpb.Entry{
		{Index: 1, Term: 1, Type: raftpb.EntryNormal},
		{Index: 2, Term: 2, Type: raftpb.EntryNormal},
	}); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// 模拟 leader 因为 follower 落后太多，直接发了一个快照过来：
	//   - Index=100：快照覆盖了 index<=100 的所有日志；
	//   - Term=5   ：快照生成时的任期；
	//   - ConfState.Voters：这个集群的成员是 1、2、3 三个节点。
	// SaveSnapshot 只写 .snap 文件，不会动 MemoryStorage，这正是"快照文件已落盘"的那个瞬间。
	snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
		Index:     100,
		Term:      5,
		ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
	}}
	if err := w.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	// 打印一下崩溃瞬间的内存状态用于观察：LastIndex 仍然只有 2，
	// 因为 ApplySnapshot 还没被调用（loop.go 里它在 SaveSnapshot 之后）。
	if last, err := w.LastIndex(); err == nil {
		t.Logf("崩溃前: MemoryStorage.LastIndex=%d, WAL 里的日志只到 2", last)
	}

	// ---- 第 2 步：模拟崩溃 ----
	// 这里只关闭文件，不写任何新的 HardState。磁盘上留下的就是
	// 「snap 文件 index=100」+「wal.log 里 HardState.Commit=2」的不一致组合。
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// ---- 第 3 步：重启，走 OpenWAL 的恢复路径 ----
	w2, err := OpenWAL(walDir, snapDir)
	if err != nil {
		// 修复后这里不应该报错。修复前也不报错，而是更晚一步在 RestartNode 里 panic，
		// 所以这个断言只是顺手检查 OpenWAL 自己没崩。
		t.Fatalf("OpenWAL failed: %v", err)
	}
	defer w2.Close()

	// InitialState 返回 MemoryStorage 当前的 HardState 和 ConfState。
	// 修复逻辑就在 OpenWAL 内部：读到 WAL 里的旧 HardState 后，发现 Commit(2) < 快照 Index(100)，
	// 于是把 Commit 抬到 100，并把 Term 从 2 抬到 5，同时把 Vote 清成 raft.None。
	hs, cs, err := w2.InitialState()
	if err != nil {
		t.Fatalf("InitialState failed: %v", err)
	}
	last, _ := w2.LastIndex()
	t.Logf("重启后: lastIndex=%d hardState=%+v voters=%v", last, hs, cs.Voters)

	// ---- 第 4 步：检查修复结果 ----
	if hs.Commit != 100 {
		t.Fatalf("hardState.Commit = %d, want 100 (应该以快照索引为准修正)", hs.Commit)
	}
	// Term 也必须跟着抬上去，否则会在"旧任期"里恢复出一个"新任期"的快照，后续投票会乱。
	if hs.Term != 5 {
		t.Fatalf("hardState.Term = %d, want 5", hs.Term)
	}
	// Vote 必须被清空：原来可能记着"Term=2 时把票投给了节点 X"，
	// 但我们现在把 Term 抬到了 5，Term=2 的选票在 Term=5 里完全无效。
	// 如果不清空，相当于在 Term=5 里凭空声称投过票，可能破坏选举安全性（同一任期选出两个 leader）。
	if hs.Vote != raft.None {
		t.Fatalf("hardState.Vote = %d, want raft.None(0) (抬 Term 时必须清空旧选票)", hs.Vote)
	}

	// ---- 第 5 步：真正跑一次 RestartNode，验证不会 panic ----
	// 这是本测试最核心的断言：修复前，newRaft → loadState 会因为
	// "commit 2 is out of range [100, 100]" 直接 panic，导致整个测试进程崩溃。
	//
	// 注意 Config 里几个必填项（不填这里会 panic "max inflight messages must be greater than 0"）：
	//   - MaxSizePerMsg   ：单条消息最多带多少字节日志，must > 0；
	//   - MaxInflightMsgs ：对单个 follower 同时最多有多少条未确认消息，must > 0；
	//   - ElectionTick / HeartbeatTick：选举和心跳的 tick 数，这里不真的跑 tick，填个合理值即可。
	node := raft.RestartNode(&raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Storage:         w2, // w2 既是 raft.Storage，也是本项目的 WAL
	})
	// RestartNode 会启动一个后台 goroutine，必须 Stop 掉，否则测试结束时报 goroutine 泄漏/数据竞争。
	node.Stop()

	t.Log("raft.RestartNode 正常启动，未 panic（修复前这里会 panic）")
}

// TestOpenWALKeenpsFreshHardState 是上面那个修复的"反向"测试，防止修过头。
//
// 正常流程下，HardState 和快照是一起推进的，重启后 HardState 本来就可能比快照新
// （例如快照在 index=10，但之后又提交到 index=12，WAL 里记着 Commit=12）。
// 这种情况下 OpenWAL 绝对不能自作聪明去改 HardState —— 一旦把 Commit 从 12 改回 10，
// 已经提交的日志会被要求重新投票/重新提交，严重影响正确性。
func TestOpenWALKeenpsFreshHardState(t *testing.T) {
	walDir := t.TempDir()
	snapDir := t.TempDir()

	w, err := OpenWAL(walDir, snapDir)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// 构造 12 条连续日志：index 1~10 属于 term=2，index 11~12 属于 term=3。
	// 必须从 1 开始严格连续地写，因为 MemoryStorage.Append 要求新日志和已有日志首尾相接，
	// 中间缺一条会 panic("missing log entry")。
	var ents []raftpb.Entry
	for i := uint64(1); i <= 12; i++ {
		term := uint64(2)
		if i >= 11 {
			term = 3
		}
		ents = append(ents, raftpb.Entry{Index: i, Term: term, Type: raftpb.EntryNormal})
	}

	// 关键点：HardState{Term:3, Vote:2, Commit:12} 比下面 index=10 的快照"新"。
	// Vote=2 也故意设成非零，用来确认修复逻辑没有顺手把它清掉。
	if err := w.Save(raftpb.HardState{Term: 3, Vote: 2, Commit: 12}, ents); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// 快照基于 index=10 生成（在真实流程里，此时 index 11、12 已经提交但还没进快照）。
	snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
		Index:     10,
		Term:      2,
		ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
	}}
	if err := w.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 重启。
	w2, err := OpenWAL(walDir, snapDir)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	defer w2.Close()

	hs, _, err := w2.InitialState()
	if err != nil {
		t.Fatalf("InitialState failed: %v", err)
	}

	// 三个字段都必须原封不动：
	//   - 修复条件是 Commit < 快照 Index（这里 12 >= 10，不满足），所以完全不会进修复分支；
	//   - Term 和 Vote 更不能被碰，否则会破坏"同一任期只投一票"的约束。
	if hs.Term != 3 || hs.Vote != 2 || hs.Commit != 12 {
		t.Fatalf("正常的 HardState 被误改了: got %+v, want {Term:3 Vote:2 Commit:12}", hs)
	}
}

// TestOpenWALSynthesizesHardStateFromSnapshot 覆盖第三种不一致：**有快照，但 WAL 是空的**。
//
// 什么时候会这样？
//   - Bootstrap 时 node.go 会先写一个 index=1 的初始快照，如果进程在第一次 Ready 之前就挂了，
//     磁盘上就只有 .snap 文件，wal.log 里一条记录都没有；
//   - 或者快照文件被单独恢复回来，而 WAL 是新建的空文件。
//
// 这种情况下 OpenWAL 读不到任何 HardState。如果不处理，raft 会以 Term=0 启动：
// 磁盘上的快照明明已经证明「Term 到过 2、index 到过 10」，节点却从第 0 届重新选举，
// 属于状态倒退（虽然不会 panic，但安全性上有瑕疵）。
//
// 修复方式：快照本身就是 Term/Index 的持久化证据，直接合成
// HardState{Term: snap.Term, Commit: snap.Index, Vote: raft.None}。
func TestOpenWALSynthesizesHardStateFromSnapshot(t *testing.T) {
	walDir := t.TempDir()
	snapDir := t.TempDir()

	w, err := OpenWAL(walDir, snapDir)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// 只写快照文件，故意一次都不调用 w.Save —— 模拟"WAL 里没有任何记录"。
	snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
		Index:     10,
		Term:      2,
		ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
	}}
	if err := w.SaveSnapshot(snap); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	w2, err := OpenWAL(walDir, snapDir)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	defer w2.Close()

	hs, cs, err := w2.InitialState()
	if err != nil {
		t.Fatalf("InitialState failed: %v", err)
	}
	t.Logf("重启后: hardState=%+v voters=%v", hs, cs.Voters)

	// Term 和 Commit 都必须来自快照，而不是零值。
	if hs.Term != 2 {
		t.Fatalf("hardState.Term = %d, want 2 (应使用快照任期，否则节点从 Term=0 重新选举)", hs.Term)
	}
	if hs.Commit != 10 {
		t.Fatalf("hardState.Commit = %d, want 10 (快照索引本身就是已提交的证据)", hs.Commit)
	}
	// 合成出来的 HardState 不能凭空声称投过票。
	if hs.Vote != raft.None {
		t.Fatalf("hardState.Vote = %d, want raft.None(0)", hs.Vote)
	}
	// ConfState 来自快照元数据，顺带确认没被丢掉。
	if len(cs.Voters) != 3 {
		t.Fatalf("confState.Voters = %v, want [1 2 3]", cs.Voters)
	}

	// 再真正启动一次 raft，确认合成出来的 HardState 是自洽的
	// （loadState 要求 Commit 落在 [快照索引, 最后一条日志索引] 区间内，超出会 panic）。
	node := raft.RestartNode(&raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Storage:         w2,
	})
	node.Stop()

	t.Log("WAL 为空时按快照合成 HardState 成功，raft 以 term=2 正常启动")
}
