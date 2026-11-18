package apply

import (
	"testing"
)

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
		data, err := EncodePutCommand("1-1", tc.key, tc.value)
		if err != nil {
			t.Fatalf("EncodePutCommand(%q, %q): %v", tc.key, tc.value, err)
		}
		cmd, ok := decodeCommand(data)
		if !ok {
			t.Fatalf("decodeCommand(%q) failed", string(data))
		}
		if cmd.RequestID != "1-1" || cmd.Op != "PUT" || cmd.Key != tc.key || cmd.Value != tc.value {
			t.Fatalf("decodeCommand round trip = %+v, want key=%q value=%q", cmd, tc.key, tc.value)
		}
	}
}

// TestDecodeLegacyCommand 验证旧的空格分隔日志仍能解析，避免升级后无法重放旧 WAL。
func TestDecodeLegacyCommand(t *testing.T) {
	cmd, ok := decodeCommand([]byte("123 PUT name mini-etcd"))
	if !ok {
		t.Fatal("decodeCommand(legacy) failed")
	}
	if cmd.RequestID != "123" || cmd.Op != "PUT" || cmd.Key != "name" || cmd.Value != "mini-etcd" {
		t.Fatalf("decodeCommand(legacy) = %+v", cmd)
	}
}
