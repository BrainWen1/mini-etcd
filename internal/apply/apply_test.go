package apply

import (
	"mini-etcd/internal/backend"
	"mini-etcd/internal/mvcc"
	"mini-etcd/internal/types"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func newTestApplier(t *testing.T) *Applier {
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
	return NewApplier(mvcc.NewMVCC(be))
}

// TestCommandRoundTripWithSpaces 验证 key/value 中包含空格、换行时仍能正确编解码。
//
// 旧实现使用空格分隔命令（"reqID PUT key value"），内含空格的 value 会让
// strings.Split 得到超过 4 段，整条命令被静默丢弃，客户端只能等到超时。
func TestCommandRoundTripWithSpaces(t *testing.T) {
	cases := []struct {
		key   string
		value string
	}{
		{key: "hello world", value: "mini etcd"},
		{key: "k", value: "line1\nline2"},
		{key: " json ", value: `{"a":"b c"}`},
	}

	for _, tc := range cases {
		data, err := types.EncodePutCommand("1-1", tc.key, tc.value)
		if err != nil {
			t.Fatalf("EncodePutCommand(%q, %q): %v", tc.key, tc.value, err)
		}
		cmd, ok := types.DecodeCommand(data)
		if !ok {
			t.Fatalf("decodeCommand(%q) failed", string(data))
		}
		if cmd.RequestID != "1-1" || cmd.Op != "PUT" || cmd.Key != tc.key || cmd.Value != tc.value {
			t.Fatalf("decodeCommand round trip = %+v, want key=%q value=%q", cmd, tc.key, tc.value)
		}
	}
}

// TestApplyTxn 验证 TXN 日志会被状态机执行，并唤醒等待的客户端。
func TestApplyTxn(t *testing.T) {
	a := newTestApplier(t)

	seed, err := types.EncodePutCommand("seed", "balance", "100")
	if err != nil {
		t.Fatalf("EncodePutCommand: %v", err)
	}
	if err := a.Apply([]raftpb.Entry{{Index: 1, Type: raftpb.EntryNormal, Data: seed}}); err != nil {
		t.Fatalf("Apply(seed): %v", err)
	}

	txnData, err := types.EncodeTxnCommand(types.TxnCommand{
		RequestID: "txn-1",
		Compares:  []types.Compare{{Key: "balance", Op: "==", Value: "100"}},
		Success:   []types.Operation{{Op: "PUT", Key: "balance", Value: "90"}},
		Failure:   []types.Operation{{Op: "PUT", Key: "balance", Value: "0"}},
	})
	if err != nil {
		t.Fatalf("EncodeTxnCommand: %v", err)
	}

	waitCh := a.RegisterWaiter("txn-1")
	if err := a.Apply([]raftpb.Entry{{Index: 2, Type: raftpb.EntryNormal, Data: txnData}}); err != nil {
		t.Fatalf("Apply(txn): %v", err)
	}
	select {
	case err := <-waitCh:
		if err != nil {
			t.Fatalf("Txn apply error = %v, want nil", err)
		}
	default:
		t.Fatal("TXN 应用后没有通知等待者")
	}

	if got, _ := a.Get("balance"); got != "90" {
		t.Fatalf("balance = %q, want 90", got)
	}
}

// TestApplyUnparseableTxnNotifiesWaiter 验证无法解析为 TxnCommand 的 TXN 日志
// 会被跳过（不阻断后续日志），同时唤醒等待者，避免客户端一直等到超时。
func TestApplyUnparseableTxnNotifiesWaiter(t *testing.T) {
	a := newTestApplier(t)

	// 信封能解析出 request_id 和 op=TXN，但事务体本身不是合法结构。
	data := []byte(`{"request_id":"bad-1","op":"TXN","compares":"not-an-array"}`)
	waitCh := a.RegisterWaiter("bad-1")

	if err := a.Apply([]raftpb.Entry{{Index: 1, Type: raftpb.EntryNormal, Data: data}}); err != nil {
		t.Fatalf("Apply(unparseable txn) = %v, want nil（跳过该日志）", err)
	}
	select {
	case err := <-waitCh:
		if err == nil {
			t.Fatal("无法解析的事务应通知等待者错误")
		}
	default:
		t.Fatal("无法解析的事务没有通知等待者")
	}
}

// TestApplyInvalidTxnSkipped 验证非法事务只被跳过并唤醒等待者，
// 不会写入数据，也不会阻断同一批次里的其它正常日志。
//
// 一批日志现在共用一个 bbolt 事务（见 MVCC.ApplyBatch）。如果把非法事务交给
// 存储层执行，失败会让整批回滚，连带丢掉同一批里已经成功的写；所以在进入批次
// 之前先校验，非法事务只跳过自己。
func TestApplyInvalidTxnSkipped(t *testing.T) {
	a := newTestApplier(t)

	// DecodeTxnCommand 能解析，但比较运算符不受支持。
	data := []byte(`{"request_id":"bad-2","op":"TXN","compares":[{"key":"k","op":"~=","value":"v"}],"success":[{"op":"PUT","key":"a","value":"1"}]}`)
	put, err := types.EncodePutCommand("next", "b", "2")
	if err != nil {
		t.Fatalf("EncodePutCommand: %v", err)
	}
	waitCh := a.RegisterWaiter("bad-2")

	// 非法事务和一条正常 PUT 在同一个 Ready 里
	entries := []raftpb.Entry{
		{Index: 1, Type: raftpb.EntryNormal, Data: data},
		{Index: 2, Type: raftpb.EntryNormal, Data: put},
	}
	if err := a.Apply(entries); err != nil {
		t.Fatalf("Apply(batch with invalid txn) = %v, want nil（跳过该条日志）", err)
	}
	select {
	case err := <-waitCh:
		if err == nil {
			t.Fatal("非法事务应通知等待者错误")
		}
	default:
		t.Fatal("非法事务没有通知等待者")
	}
	if _, ok := a.Get("a"); ok {
		t.Fatal("非法事务不应写入数据")
	}

	// 同一批次里的正常日志不受影响
	if got, _ := a.Get("b"); got != "2" {
		t.Fatalf("b = %q, want 2", got)
	}
	idx, err := a.GetConsistentIndex()
	if err != nil {
		t.Fatalf("GetConsistentIndex: %v", err)
	}
	if idx != 2 {
		t.Fatalf("GetConsistentIndex() = %d, want 2", idx)
	}
}

// TestApplyBatchNotifiesOnlyAfterCommit 验证一批日志里的等待者都会在整批提交后
// 收到通知，并且 PUT / DELETE / TXN 混合的一批日志按顺序全部生效。
func TestApplyBatchNotifiesOnlyAfterCommit(t *testing.T) {
	a := newTestApplier(t)

	put1, _ := types.EncodePutCommand("put-1", "a", "1")
	put2, _ := types.EncodePutCommand("put-2", "b", "2")
	del, _ := types.EncodeDeleteCommand("del-1", "a")
	txnData, err := types.EncodeTxnCommand(types.TxnCommand{
		RequestID: "txn-1",
		Compares:  []types.Compare{{Key: "b", Op: "==", Value: "2"}},
		Success:   []types.Operation{{Op: "PUT", Key: "c", Value: "3"}},
	})
	if err != nil {
		t.Fatalf("EncodeTxnCommand: %v", err)
	}

	waiters := map[string]chan error{
		"put-1": a.RegisterWaiter("put-1"),
		"put-2": a.RegisterWaiter("put-2"),
		"del-1": a.RegisterWaiter("del-1"),
		"txn-1": a.RegisterWaiter("txn-1"),
	}
	entries := []raftpb.Entry{
		{Index: 1, Type: raftpb.EntryNormal, Data: put1},
		{Index: 2, Type: raftpb.EntryNormal, Data: put2},
		{Index: 3, Type: raftpb.EntryNormal, Data: del},
		{Index: 4, Type: raftpb.EntryNormal, Data: txnData},
	}
	if err := a.Apply(entries); err != nil {
		t.Fatalf("Apply(batch): %v", err)
	}

	for id, ch := range waiters {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("waiter %s got error %v, want nil", id, err)
			}
		default:
			t.Fatalf("waiter %s 没有被通知", id)
		}
	}

	if _, ok := a.Get("a"); ok {
		t.Fatal("a 应已被删除")
	}
	if got, _ := a.Get("b"); got != "2" {
		t.Fatalf("b = %q, want 2", got)
	}
	if got, _ := a.Get("c"); got != "3" {
		t.Fatalf("c = %q, want 3", got)
	}
	idx, err := a.GetConsistentIndex()
	if err != nil {
		t.Fatalf("GetConsistentIndex: %v", err)
	}
	if idx != 4 {
		t.Fatalf("GetConsistentIndex() = %d, want 4", idx)
	}
}
