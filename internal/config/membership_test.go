package config

import "testing"

// TestMemberContextRoundTrip 验证 Raft/HTTP 两种地址都能通过 ConfChange.Context 传递。
func TestMemberContextRoundTrip(t *testing.T) {
	m := DecodeMemberContext(EncodeMemberContext("127.0.0.1:8004", "127.0.0.1:10004"))
	if m.RaftAddr != "127.0.0.1:8004" || m.HTTPAddr != "127.0.0.1:10004" {
		t.Fatalf("decoded member = %+v, want raft=127.0.0.1:8004 http=127.0.0.1:10004", m)
	}
}
