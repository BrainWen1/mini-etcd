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
