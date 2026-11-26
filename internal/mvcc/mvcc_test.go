package mvcc

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"mini-etcd/internal/backend"
	"mini-etcd/internal/types"
)

func newTestMVCC(t *testing.T) *MVCC {
	t.Helper()

	be, err := backend.NewBackend(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open backend: %v", err)
	}
	t.Cleanup(func() {
		if err := be.Close(); err != nil {
			t.Errorf("failed to close backend: %v", err)
		}
	})
	return NewMVCC(be)
}

// TestPutSkipsStaleEntry 验证重放旧日志（索引 <= consistentIndex）时不会覆盖新值。
func TestPutSkipsStaleEntry(t *testing.T) {
	m := newTestMVCC(t)

	if err := m.Put("k", "v1", 5); err != nil {
		t.Fatalf("Put(v1, index=5): %v", err)
	}
	if err := m.Put("k", "v2", 8); err != nil {
		t.Fatalf("Put(v2, index=8): %v", err)
	}
	// 模拟重启后重放旧日志
	if err := m.Put("k", "v1", 5); err != nil {
		t.Fatalf("replay Put(v1, index=5): %v", err)
	}

	got, err := m.Get("k")
	if err != nil {
		t.Fatalf("Get(k): %v", err)
	}
	if got != "v2" {
		t.Fatalf("Get(k) = %q, want %q (旧日志覆盖了新值)", got, "v2")
	}

	idx, err := m.GetConsistentIndex()
	if err != nil {
		t.Fatalf("GetConsistentIndex: %v", err)
	}
	if idx != 8 {
		t.Fatalf("GetConsistentIndex() = %d, want 8", idx)
	}
}

// TestMembershipSurvivesSnapshot 验证成员表随 MVCC 快照一起复制。
//
// 这是动态扩容的关键不变量：ConfState 只有节点 ID，成员地址只能从状态机里恢复。
// 如果成员表不进入快照，离线期间错过 ConfChange 日志的节点在通过快照追赶后，
// 就永远不知道新成员的 Raft/HTTP 地址。
func TestMembershipSurvivesSnapshot(t *testing.T) {
	src := newTestMVCC(t)
	if _, err := src.UpsertMember(4, "127.0.0.1:8004", "127.0.0.1:10004"); err != nil {
		t.Fatalf("UpsertMember(4): %v", err)
	}
	if _, err := src.UpsertMember(5, "127.0.0.1:8005", "127.0.0.1:10005"); err != nil {
		t.Fatalf("UpsertMember(5): %v", err)
	}

	snap, err := src.GetSnapshot()
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}

	dst := newTestMVCC(t)
	if err := dst.ApplySnapshot(snap); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	members, err := dst.GetMembers()
	if err != nil {
		t.Fatalf("GetMembers: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("members = %+v, want 2 members", members)
	}
	if m := members[4]; m.RaftAddr != "127.0.0.1:8004" || m.HTTPAddr != "127.0.0.1:10004" {
		t.Fatalf("member 4 = %+v, want both addresses restored", m)
	}

	// 移除必须同样进入快照：否则恢复后会把已移除的节点重新加回 Transport。
	if err := dst.RemoveMember(5); err != nil {
		t.Fatalf("RemoveMember(5): %v", err)
	}
	snap2, err := dst.GetSnapshot()
	if err != nil {
		t.Fatalf("GetSnapshot after remove: %v", err)
	}
	dst2 := newTestMVCC(t)
	if err := dst2.ApplySnapshot(snap2); err != nil {
		t.Fatalf("ApplySnapshot after remove: %v", err)
	}
	members, err = dst2.GetMembers()
	if err != nil {
		t.Fatalf("GetMembers after remove: %v", err)
	}
	if _, ok := members[5]; ok {
		t.Fatalf("member 5 should be gone after removal: %+v", members)
	}
}

// TestGetMissingKey 验证 key 不存在时返回 ErrKeyNotFound。
func TestGetMissingKey(t *testing.T) {
	m := newTestMVCC(t)

	if _, err := m.Get("not-exist"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(not-exist) error = %v, want ErrKeyNotFound", err)
	}
}

// TestSnapshotRoundTrip 验证快照导出/恢复能完整还原数据与 consistentIndex。
func TestSnapshotRoundTrip(t *testing.T) {
	src := newTestMVCC(t)
	if err := src.Put("a", "1", 3); err != nil {
		t.Fatalf("Put(a): %v", err)
	}
	if err := src.Put("b", "2", 4); err != nil {
		t.Fatalf("Put(b): %v", err)
	}

	snap, err := src.GetSnapshot()
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}

	dst := newTestMVCC(t)
	if err := dst.ApplySnapshot(snap); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	for key, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := dst.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if got != want {
			t.Fatalf("Get(%s) = %q, want %q", key, got, want)
		}
	}

	idx, err := dst.GetConsistentIndex()
	if err != nil {
		t.Fatalf("GetConsistentIndex: %v", err)
	}
	if idx != 4 {
		t.Fatalf("GetConsistentIndex() = %d, want 4", idx)
	}
}

// TestSnapshotRoundTripBinaryRevision 验证 revision >= 128 时快照仍然能完整恢复。
//
// Data 桶的 key 里直接拼接了 8 字节大端序 revision，revision=128 时最后一位是 0x80，
// 不是合法 UTF-8。早期用 encoding/json 序列化快照会把这个字节替换成 U+FFFD，
// 恢复后 key 被改写，Get 只能返回 ErrKeyNotFound。
func TestSnapshotRoundTripBinaryRevision(t *testing.T) {
	src := newTestMVCC(t)
	for i := 1; i <= 200; i++ {
		if err := src.Put("k", fmt.Sprintf("v%d", i), uint64(i)); err != nil {
			t.Fatalf("Put(#%d): %v", i, err)
		}
	}

	snap, err := src.GetSnapshot()
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}

	dst := newTestMVCC(t)
	if err := dst.ApplySnapshot(snap); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	got, err := dst.Get("k")
	if err != nil {
		t.Fatalf("Get(k) after snapshot: %v", err)
	}
	if got != "v200" {
		t.Fatalf("Get(k) = %q, want %q", got, "v200")
	}

	// 历史版本同样要能读，确认 revision 没有被破坏
	got, err = dst.GetAtRevision("k", 150)
	if err != nil {
		t.Fatalf("GetAtRevision(k,150): %v", err)
	}
	if got != "v150" {
		t.Fatalf("GetAtRevision(k,150) = %q, want %q", got, "v150")
	}
}

// TestGetAtRevisionIgnoresOtherKeysWithNUL 验证用户 key 中包含 \x00 时，
// GetAtRevision 不会把形如 "key\x00other" 的其它 key 的版本误当成自己的值返回。
// TestTxnBranches 验证条件成立走 success、条件不成立走 failure。
func TestTxnBranches(t *testing.T) {
	m := newTestMVCC(t)
	if err := m.Put("balance", "100", 1); err != nil {
		t.Fatalf("Put(balance): %v", err)
	}

	txn := types.TxnCommand{
		Op:       "TXN",
		Compares: []types.Compare{{Key: "balance", Op: "==", Value: "100"}},
		Success: []types.Operation{
			{Op: "PUT", Key: "balance", Value: "90"},
			{Op: "PUT", Key: "log", Value: "debit"},
		},
		Failure: []types.Operation{{Op: "PUT", Key: "log", Value: "skip"}},
	}
	if err := m.Txn(txn, 2); err != nil {
		t.Fatalf("Txn(success branch): %v", err)
	}
	if got, _ := m.Get("balance"); got != "90" {
		t.Fatalf("balance = %q, want 90", got)
	}
	if got, _ := m.Get("log"); got != "debit" {
		t.Fatalf("log = %q, want debit", got)
	}

	// 条件不再成立（balance 已是 90），应走 failure 分支
	txn.Compares[0].Value = "100"
	if err := m.Txn(txn, 3); err != nil {
		t.Fatalf("Txn(failure branch): %v", err)
	}
	if got, _ := m.Get("log"); got != "skip" {
		t.Fatalf("log = %q, want skip", got)
	}
	if got, _ := m.Get("balance"); got != "90" {
		t.Fatalf("balance = %q, want 90 (failure 分支不应修改 balance)", got)
	}
}

// TestTxnCompareOps 验证支持的比较运算符，以及缺失 key 按空串比较的语义。
func TestTxnCompareOps(t *testing.T) {
	cases := []struct {
		name   string
		cmp    types.Compare
		expect bool // 条件是否成立
	}{
		{"eq", types.Compare{Key: "k", Op: "==", Value: "5"}, true},
		{"eq-default-op", types.Compare{Key: "k", Value: "5"}, true},
		{"ne", types.Compare{Key: "k", Op: "!=", Value: "6"}, true},
		{"gt", types.Compare{Key: "k", Op: ">", Value: "4"}, true},
		{"ge", types.Compare{Key: "k", Op: ">=", Value: "5"}, true},
		{"lt", types.Compare{Key: "k", Op: "<", Value: "6"}, true},
		{"le", types.Compare{Key: "k", Op: "<=", Value: "5"}, true},
		{"gt-false", types.Compare{Key: "k", Op: ">", Value: "5"}, false},
		{"missing-eq-empty", types.Compare{Key: "missing", Op: "==", Value: ""}, true},
		{"missing-ne", types.Compare{Key: "missing", Op: "!=", Value: "x"}, true},
		{"missing-eq-nonempty", types.Compare{Key: "missing", Op: "==", Value: "x"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestMVCC(t)
			if err := m.Put("k", "5", 1); err != nil {
				t.Fatalf("Put(k): %v", err)
			}
			if err := m.Txn(types.TxnCommand{
				Op:       "TXN",
				Compares: []types.Compare{tc.cmp},
				Success:  []types.Operation{{Op: "PUT", Key: "hit", Value: "1"}},
				Failure:  []types.Operation{{Op: "PUT", Key: "miss", Value: "1"}},
			}, 2); err != nil {
				t.Fatalf("Txn: %v", err)
			}

			_, hitErr := m.Get("hit")
			_, missErr := m.Get("miss")
			if tc.expect {
				if hitErr != nil || !errors.Is(missErr, ErrKeyNotFound) {
					t.Fatalf("条件成立：hit err=%v, miss err=%v, want hit 存在且 miss 不存在", hitErr, missErr)
				}
			} else {
				if !errors.Is(hitErr, ErrKeyNotFound) || missErr != nil {
					t.Fatalf("条件不成立：hit err=%v, miss err=%v, want hit 不存在且 miss 存在", hitErr, missErr)
				}
			}
		})
	}
}

// TestTxnIdempotentReplay 验证重放旧事务日志时整条事务被跳过。
func TestTxnIdempotentReplay(t *testing.T) {
	m := newTestMVCC(t)
	if err := m.Put("k", "0", 1); err != nil {
		t.Fatalf("Put(k): %v", err)
	}

	txn := types.TxnCommand{
		Op:       "TXN",
		Compares: []types.Compare{{Key: "k", Op: "==", Value: "0"}},
		Success:  []types.Operation{{Op: "PUT", Key: "k", Value: "1"}},
	}
	if err := m.Txn(txn, 2); err != nil {
		t.Fatalf("Txn(index=2): %v", err)
	}
	// 模拟重启重放：同一条事务再次应用，条件此时已不成立，
	// 如果没有幂等检查就会误走 failure 分支。这里让它带一个 failure 写入来验证。
	txn.Failure = []types.Operation{{Op: "PUT", Key: "replayed", Value: "1"}}
	if err := m.Txn(txn, 2); err != nil {
		t.Fatalf("replay Txn(index=2): %v", err)
	}

	if got, _ := m.Get("k"); got != "1" {
		t.Fatalf("k = %q, want 1", got)
	}
	if _, err := m.Get("replayed"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("重放的旧事务不应执行 failure 分支, Get(replayed) err = %v", err)
	}
	idx, err := m.GetConsistentIndex()
	if err != nil {
		t.Fatalf("GetConsistentIndex: %v", err)
	}
	if idx != 2 {
		t.Fatalf("GetConsistentIndex() = %d, want 2", idx)
	}
}

// TestTxnRejectsInvalidOperation 验证分支内的非法操作会让整个事务回滚，
// 不会留下写了一半的数据，也不会推进 consistentIndex 掩盖这条日志。
func TestTxnRejectsInvalidOperation(t *testing.T) {
	m := newTestMVCC(t)
	if err := m.Put("k", "0", 1); err != nil {
		t.Fatalf("Put(k): %v", err)
	}

	err := m.Txn(types.TxnCommand{
		Op: "TXN",
		Success: []types.Operation{
			{Op: "PUT", Key: "a", Value: "1"},
			{Op: "BOGUS", Key: "b"},
		},
	}, 2)
	if err == nil {
		t.Fatal("Txn with unsupported operation returned nil, want error")
	}

	if _, err := m.Get("a"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("事务失败后 a 不应存在, Get(a) err = %v", err)
	}
	idx, err := m.GetConsistentIndex()
	if err != nil {
		t.Fatalf("GetConsistentIndex: %v", err)
	}
	if idx != 1 {
		t.Fatalf("GetConsistentIndex() = %d, want 1 (失败的事务不应推进索引)", idx)
	}
}

// TestTxnDelete 验证事务中的 DELETE 写入墓碑，Get 返回 ErrKeyNotFound。
func TestTxnDelete(t *testing.T) {
	m := newTestMVCC(t)
	if err := m.Put("k", "v", 1); err != nil {
		t.Fatalf("Put(k): %v", err)
	}

	if err := m.Txn(types.TxnCommand{
		Op:      "TXN",
		Success: []types.Operation{{Op: "DELETE", Key: "k"}},
	}, 2); err != nil {
		t.Fatalf("Txn(DELETE): %v", err)
	}
	if _, err := m.Get("k"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(k) after txn delete err = %v, want ErrKeyNotFound", err)
	}
}

func TestGetAtRevisionIgnoresOtherKeysWithNUL(t *testing.T) {
	m := newTestMVCC(t)

	if err := m.Put("a", "mine", 1); err != nil {
		t.Fatalf("Put(a): %v", err)
	}
	if err := m.Put("a\x00b", "other", 2); err != nil {
		t.Fatalf("Put(a\\x00b): %v", err)
	}

	got, err := m.GetAtRevision("a", math.MaxUint64)
	if err != nil {
		t.Fatalf("GetAtRevision(a): %v", err)
	}
	if got != "mine" {
		t.Fatalf(`GetAtRevision(a) = %q, want %q (读到了 "a\x00b" 的值)`, got, "mine")
	}
}

// putWithLease 通过 ApplyBatch 写入一个绑定租约的 key（MVCC.Put 不接受 LeaseID）。
func putWithLease(t *testing.T, m *MVCC, key, value string, leaseID int64, index uint64) {
	t.Helper()
	if _, _, err := m.ApplyBatch([]Entry{{Index: index, Op: "PUT", Key: key, Value: value, LeaseID: leaseID}}); err != nil {
		t.Fatalf("ApplyBatch(PUT %s, lease=%d, index=%d): %v", key, leaseID, index, err)
	}
}

// TestLeaseRevokeDeletesAllBoundKeys 验证撤销租约时会删除它绑定的**所有** key。
//
// 早期实现里每个 key 都走 applyPut 的幂等检查：第一个 key 写完墓碑后
// consistentIndex 就等于 Revoke 日志的索引，后续 key 会被误判为“已应用”
// 而全部跳过，导致只有第一个 key 被删除。
func TestLeaseRevokeDeletesAllBoundKeys(t *testing.T) {
	m := newTestMVCC(t)

	if err := m.GrantLease(100, 60, time.Now().Unix(), 1); err != nil {
		t.Fatalf("GrantLease: %v", err)
	}
	putWithLease(t, m, "a", "1", 100, 2)
	putWithLease(t, m, "b", "2", 100, 3)

	if _, err := m.RevokeLease(100, 4); err != nil {
		t.Fatalf("RevokeLease: %v", err)
	}

	for _, key := range []string{"a", "b"} {
		if _, err := m.Get(key); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("Get(%s) error = %v, want ErrKeyNotFound（租约撤销后 key 应被删除）", key, err)
		}
	}
	if ok, err := m.LeaseExists(100); err != nil || ok {
		t.Fatalf("LeaseExists(100) = %v, %v; want false, nil", ok, err)
	}
}

// TestLeaseRebindDoesNotDeleteKeyWhenOldLeaseExpires 验证 key 改绑到新租约后，
// 旧租约过期/撤销不会把它一并删除。
func TestLeaseRebindDoesNotDeleteKeyWhenOldLeaseExpires(t *testing.T) {
	m := newTestMVCC(t)

	if err := m.GrantLease(100, 60, time.Now().Unix(), 1); err != nil {
		t.Fatalf("GrantLease(100): %v", err)
	}
	if err := m.GrantLease(200, 60, time.Now().Unix(), 2); err != nil {
		t.Fatalf("GrantLease(200): %v", err)
	}
	putWithLease(t, m, "k", "old", 100, 3)
	putWithLease(t, m, "k", "new", 200, 4)

	if _, err := m.RevokeLease(100, 5); err != nil {
		t.Fatalf("RevokeLease(100): %v", err)
	}
	got, err := m.Get("k")
	if err != nil {
		t.Fatalf("Get(k) after revoke old lease: %v", err)
	}
	if got != "new" {
		t.Fatalf("Get(k) = %q, want %q", got, "new")
	}

	if _, err := m.RevokeLease(200, 6); err != nil {
		t.Fatalf("RevokeLease(200): %v", err)
	}
	if _, err := m.Get("k"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(k) error = %v, want ErrKeyNotFound", err)
	}
}

// TestLeaseRevokeReplayIsNoop 验证重放旧的 Revoke 日志不会删除撤销之后新写入的数据，
// 也不会让 consistentIndex 回退。
func TestLeaseRevokeReplayIsNoop(t *testing.T) {
	m := newTestMVCC(t)

	if err := m.GrantLease(100, 60, time.Now().Unix(), 1); err != nil {
		t.Fatalf("GrantLease: %v", err)
	}
	putWithLease(t, m, "k", "old", 100, 2)
	if _, err := m.RevokeLease(100, 3); err != nil {
		t.Fatalf("RevokeLease: %v", err)
	}
	if err := m.Put("k", "new", 4); err != nil {
		t.Fatalf("Put(k, index=4): %v", err)
	}

	// 模拟重启后重放索引 3 的 Revoke
	if _, err := m.RevokeLease(100, 3); err != nil {
		t.Fatalf("replay RevokeLease: %v", err)
	}
	got, err := m.Get("k")
	if err != nil {
		t.Fatalf("Get(k) after replay: %v", err)
	}
	if got != "new" {
		t.Fatalf("Get(k) = %q, want %q（重放旧 Revoke 不应覆盖新值）", got, "new")
	}
	idx, err := m.GetConsistentIndex()
	if err != nil {
		t.Fatalf("GetConsistentIndex: %v", err)
	}
	if idx != 4 {
		t.Fatalf("GetConsistentIndex() = %d, want 4（重放不应回退索引）", idx)
	}
}

// TestLeaseSnapshotPreservesBindings 验证租约与 key 绑定会随快照一起复制：
// 从快照恢复的节点在应用后续 Revoke 日志时必须删除同样的 key，否则节点间
// 状态机会永久分歧。
func TestLeaseSnapshotPreservesBindings(t *testing.T) {
	src := newTestMVCC(t)
	if err := src.GrantLease(100, 60, time.Now().Unix(), 1); err != nil {
		t.Fatalf("GrantLease: %v", err)
	}
	putWithLease(t, src, "a", "1", 100, 2)
	putWithLease(t, src, "b", "2", 100, 3)

	snap, err := src.GetSnapshot()
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}

	dst := newTestMVCC(t)
	if err := dst.ApplySnapshot(snap); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	if ok, err := dst.LeaseExists(100); err != nil || !ok {
		t.Fatalf("LeaseExists(100) after snapshot = %v, %v; want true, nil", ok, err)
	}

	// 模拟 Leader 提交的 Revoke 日志在恢复节点上应用
	if _, err := dst.RevokeLease(100, 4); err != nil {
		t.Fatalf("RevokeLease after snapshot: %v", err)
	}
	for _, key := range []string{"a", "b"} {
		if _, err := dst.Get(key); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("Get(%s) after snapshot+revoke error = %v, want ErrKeyNotFound", key, err)
		}
	}
}

// TestLeaseExpiryDerivedFromLeaseStart 验证过期时间只由日志里的 LeaseStart 决定，
// 而不是应用日志时的本地时钟。否则落后节点重放同一条 Grant 会用更晚的时钟
// 算出更晚的过期时间，与 Leader 分歧。
func TestLeaseExpiryDerivedFromLeaseStart(t *testing.T) {
	m := newTestMVCC(t)

	now := time.Now().Unix()
	// 开始于 2 分钟前、TTL 60 秒：刚应用就已经过期
	if err := m.GrantLease(300, 60, now-120, 1); err != nil {
		t.Fatalf("GrantLease(300): %v", err)
	}
	// 刚刚开始：不应过期
	if err := m.GrantLease(400, 60, now, 2); err != nil {
		t.Fatalf("GrantLease(400): %v", err)
	}

	expired, err := m.GetExpiredLeases()
	if err != nil {
		t.Fatalf("GetExpiredLeases: %v", err)
	}
	if len(expired) != 1 || expired[0] != 300 {
		t.Fatalf("GetExpiredLeases() = %v, want [300]", expired)
	}

	// 续租把开始时间刷新到 now，过期时间变成 now+TTL，不再过期
	if err := m.KeepAliveLease(300, now, 3); err != nil {
		t.Fatalf("KeepAliveLease(300): %v", err)
	}
	expired, err = m.GetExpiredLeases()
	if err != nil {
		t.Fatalf("GetExpiredLeases after keepalive: %v", err)
	}
	if len(expired) != 0 {
		t.Fatalf("GetExpiredLeases() = %v, want empty", expired)
	}
}
