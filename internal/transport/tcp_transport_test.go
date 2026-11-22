package transport

import (
	"testing"
	"time"

	"mini-etcd/internal/config"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func newTestTransport(t *testing.T, id uint64, peers map[uint64]string) *TCPTransport {
	t.Helper()
	tr, err := NewTCPTransport(config.Config{
		ID:         id,
		Peers:      peers,
		ListenAddr: "127.0.0.1:0", // 由内核分配空闲端口，避免测试之间抢端口
	})
	if err != nil {
		t.Fatalf("NewTCPTransport(%d) failed: %v", id, err)
	}
	t.Cleanup(tr.Close)
	return tr
}

// TestTCPTransportSendRecv 验证一条消息能被正确分帧、发送、接收和反序列化。
func TestTCPTransportSendRecv(t *testing.T) {
	peers := map[uint64]string{}
	t2 := newTestTransport(t, 2, peers)
	peers[2] = t2.listener.Addr().String()
	t1 := newTestTransport(t, 1, peers)

	want := raftpb.Message{From: 1, To: 2, Type: raftpb.MsgHeartbeat, Term: 7}
	t1.Send([]raftpb.Message{want})

	select {
	case got := <-t2.Recv():
		if got.From != want.From || got.To != want.To ||
			got.Type != want.Type || got.Term != want.Term {
			t.Fatalf("got message %+v, want %+v", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

// TestTCPTransportSendOrder 验证异步发送仍然保持同一节点内的消息顺序。
func TestTCPTransportSendOrder(t *testing.T) {
	peers := map[uint64]string{}
	t2 := newTestTransport(t, 2, peers)
	peers[2] = t2.listener.Addr().String()
	t1 := newTestTransport(t, 1, peers)

	const n = 200 // 小于 sendC 的缓冲区大小，避免触发"队列满丢弃"
	for i := 1; i <= n; i++ {
		t1.Send([]raftpb.Message{{
			From: 1, To: 2, Type: raftpb.MsgApp, Term: 1, Index: uint64(i),
		}})
	}

	for i := 1; i <= n; i++ {
		select {
		case got := <-t2.Recv():
			if got.Index != uint64(i) {
				t.Fatalf("message %d: got index %d, want %d", i, got.Index, i)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}
}

// TestTCPTransportAddRemovePeer 验证动态成员变更能即时反映到传输层：
// AddPeer 后可以拨号送达；RemovePeer 后消息不再送达，且重复移除不会 panic。
func TestTCPTransportAddRemovePeer(t *testing.T) {
	peers := map[uint64]string{}
	t2 := newTestTransport(t, 2, peers)
	t1 := newTestTransport(t, 1, peers)

	// 运行时加入节点 2。
	t1.AddPeer(2, t2.listener.Addr().String())
	t1.Send([]raftpb.Message{{From: 1, To: 2, Type: raftpb.MsgApp, Term: 1, Index: 1}})

	select {
	case got := <-t2.Recv():
		if got.Index != 1 {
			t.Fatalf("got index %d, want 1", got.Index)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message after AddPeer")
	}

	// 运行时移除节点 2。重复调用必须幂等（stopC 只能关闭一次）。
	t1.RemovePeer(2)
	t1.RemovePeer(2)

	t1.Send([]raftpb.Message{{From: 1, To: 2, Type: raftpb.MsgApp, Term: 1, Index: 2}})
	select {
	case got := <-t2.Recv():
		t.Fatalf("received message after RemovePeer: %+v", got)
	case <-time.After(500 * time.Millisecond):
		// 预期：地址已删除，消息被丢弃
	}
}

// TestTCPTransportReconnectAfterWriteError 验证写失败后逻辑连接不会被丢弃：
// 同一个 writeLoop 应在冷却期结束后重新建连并继续发送，否则每失败一次
// 就会泄漏一个 goroutine 和一条 fd，且同一节点会出现多条并发连接。
func TestTCPTransportReconnectAfterWriteError(t *testing.T) {
	peers := map[uint64]string{}
	t2 := newTestTransport(t, 2, peers)
	peers[2] = t2.listener.Addr().String()
	t1 := newTestTransport(t, 1, peers)

	recvIndex := func(want uint64) {
		t.Helper()
		select {
		case got := <-t2.Recv():
			if got.Index != want {
				t.Fatalf("got index %d, want %d", got.Index, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for index %d", want)
		}
	}

	t1.Send([]raftpb.Message{{From: 1, To: 2, Type: raftpb.MsgApp, Term: 1, Index: 1}})
	recvIndex(1)

	t1.mu.RLock()
	pc := t1.conns[2]
	t1.mu.RUnlock()
	if pc == nil {
		t.Fatal("no logical connection for node 2")
	}

	// 模拟底层 TCP 连接被网络/对端断开。
	pc.connMu.Lock()
	conn := pc.netConn
	pc.connMu.Unlock()
	if conn == nil {
		t.Fatal("physical connection was not established")
	}
	conn.Close()

	// 这条消息会写失败并触发重连退避。
	t1.Send([]raftpb.Message{{From: 1, To: 2, Type: raftpb.MsgApp, Term: 1, Index: 2}})
	time.Sleep(retryInterval + 300*time.Millisecond)

	// 冷却期过后，应由同一个逻辑连接重新建连并成功送达。
	t1.Send([]raftpb.Message{{From: 1, To: 2, Type: raftpb.MsgApp, Term: 1, Index: 3}})
	recvIndex(3)

	t1.mu.RLock()
	pcAfter := t1.conns[2]
	t1.mu.RUnlock()
	if pcAfter != pc {
		t.Fatal("logical connection was replaced after write error; old writeLoop would leak")
	}
}
