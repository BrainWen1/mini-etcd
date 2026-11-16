package mvcc

import (
	"errors"
	"testing"

	"mini-etcd/internal/backend"
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
