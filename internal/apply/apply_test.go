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

	// 旧格式（空格分隔）解析出的 Op 恰好是 TXN，但内容不是合法的事务 JSON。
	data := []byte("bad-1 TXN k v")
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

// TestApplyInvalidTxnRollsBack 验证事务执行失败时整条事务回滚，
// 并唤醒等待者；后续日志仍然可以正常应用。
func TestApplyInvalidTxnRollsBack(t *testing.T) {
	a := newTestApplier(t)

	// DecodeTxnCommand 能解析，但比较运算符不受支持，MVCC.Txn 会返回错误。
	data := []byte(`{"request_id":"bad-2","op":"TXN","compares":[{"key":"k","op":"~=","value":"v"}],"success":[{"op":"PUT","key":"a","value":"1"}]}`)
	waitCh := a.RegisterWaiter("bad-2")

	if err := a.Apply([]raftpb.Entry{{Index: 1, Type: raftpb.EntryNormal, Data: data}}); err == nil {
		t.Fatal("Apply(invalid txn) = nil, want error")
	}
	select {
	case err := <-waitCh:
		if err == nil {
			t.Fatal("执行失败的事务应通知等待者错误")
		}
	default:
		t.Fatal("执行失败的事务没有通知等待者")
	}
	if _, ok := a.Get("a"); ok {
		t.Fatal("失败的事务不应写入数据")
	}

	// 失败日志没有推进 consistentIndex，但不应阻塞后续正常日志
	put, err := types.EncodePutCommand("next", "b", "2")
	if err != nil {
		t.Fatalf("EncodePutCommand: %v", err)
	}
	if err := a.Apply([]raftpb.Entry{{Index: 2, Type: raftpb.EntryNormal, Data: put}}); err != nil {
		t.Fatalf("Apply(next): %v", err)
	}
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

// TestApplyLegacyPutCommand 验证先解析信封、再按类型 Decode 之后，
// 旧的空格分隔 PUT 日志仍能被应用（否则升级后重放旧 WAL 会丢数据）。
func TestApplyLegacyPutCommand(t *testing.T) {
	a := newTestApplier(t)

	if err := a.Apply([]raftpb.Entry{{Index: 1, Type: raftpb.EntryNormal, Data: []byte("123 PUT name mini-etcd")}}); err != nil {
		t.Fatalf("Apply(legacy put): %v", err)
	}
	if got, ok := a.Get("name"); !ok || got != "mini-etcd" {
		t.Fatalf("name = %q, ok = %v; want mini-etcd", got, ok)
	}
}

// TestDecodeLegacyCommand 验证旧的空格分隔日志仍能解析，避免升级后无法重放旧 WAL。
func TestDecodeLegacyCommand(t *testing.T) {
	cmd, ok := types.DecodeCommand([]byte("123 PUT name mini-etcd"))
	if !ok {
		t.Fatal("decodeCommand(legacy) failed")
	}
	if cmd.RequestID != "123" || cmd.Op != "PUT" || cmd.Key != "name" || cmd.Value != "mini-etcd" {
		t.Fatalf("decodeCommand(legacy) = %+v", cmd)
	}
}
