package storage

// 本文件覆盖分段 WAL 的两个核心能力：
//  1. 分段存储：文件按大小轮转，重启后按文件名顺序读回全部日志；
//  2. 空间释放：快照之后 Truncate 能删掉已经被快照完全覆盖的分段，
//     同时保留活跃分段和最新 HardState 所在分段。

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// testSegmentSize 故意设得很小（默认是 64MB），让少量日志就能触发轮转。
// 一条 WAL 记录的 gob 编码约 200 字节，1024 字节能装 4 条左右。
const testSegmentSize = 1024

// numberedEntries 生成 [from, to] 的连续日志，每条带固定长度的业务数据。
func numberedEntries(from, to uint64) []raftpb.Entry {
	entries := make([]raftpb.Entry, 0, to-from+1)
	for i := from; i <= to; i++ {
		entries = append(entries, raftpb.Entry{
			Index: i,
			Term:  1,
			Type:  raftpb.EntryNormal,
			Data:  fmt.Appendf([]byte{}, "entry-%03d-payload", i),
		})
	}
	return entries
}

// walSegmentFiles 返回目录下所有 .wal 分段文件名（已排序）。
func walSegmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == walFileExt {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	return files
}

func fileExists(t *testing.T, dir, name string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, name))
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	t.Fatalf("Stat(%s): %v", name, err)
	return false
}

// TestWALSegmentedSaveAndRecover 验证：
//   - 写入量足够时会产生多个分段文件；
//   - 文件名是 16 位十六进制、按字典序就是写入顺序；
//   - fileMaxIdx 的 key 是文件名而不是完整路径（否则 Truncate 找不到文件）；
//   - 重启后能从所有分段中恢复出完整、连续的日志。
func TestWALSegmentedSaveAndRecover(t *testing.T) {
	walDir, snapDir := t.TempDir(), t.TempDir()

	w, err := OpenWALWithSegmentSize(walDir, snapDir, testSegmentSize)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	entries := numberedEntries(1, 40)
	for _, e := range entries {
		// HardState.Term 非空，确保每条记录都会写入 WAL
		if err := w.Save(raftpb.HardState{Term: 1}, []raftpb.Entry{e}); err != nil {
			t.Fatalf("Save(index=%d): %v", e.Index, err)
		}
	}

	files := walSegmentFiles(t, walDir)
	if len(files) < 3 {
		t.Fatalf("分段数 = %d，期望至少 3（segmentSize=%d）", len(files), testSegmentSize)
	}

	for i, name := range files {
		seq, err := strconv.ParseUint(strings.TrimSuffix(name, walFileExt), 16, 64)
		if err != nil {
			t.Fatalf("分段文件名 %q 不是合法的十六进制序号: %v", name, err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("第 %d 个分段文件名 = %q（序号 %d），期望序号 %d", i, name, seq, i+1)
		}
		// Truncate 依赖 fileMaxIdx 的 key 与真实文件名一致。
		if _, ok := w.fileMaxIdx[name]; !ok {
			t.Fatalf("fileMaxIdx 缺少分段 %q，现有 key: %v", name, mapKeys(w.fileMaxIdx))
		}
		info, err := os.Stat(filepath.Join(walDir, name))
		if err != nil {
			t.Fatalf("Stat(%s): %v", name, err)
		}
		if info.Size() > testSegmentSize {
			t.Fatalf("分段 %s 大小 %d 超过阈值 %d", name, info.Size(), testSegmentSize)
		}
	}
	for name := range w.fileMaxIdx {
		if name != filepath.Base(name) {
			t.Fatalf("fileMaxIdx 的 key %q 必须是文件名，不能是完整路径", name)
		}
	}

	active := filepath.Base(w.activeFile.Name())
	if got := w.fileMaxIdx[active]; got != 40 {
		t.Fatalf("活跃分段 %s 的最大索引 = %d，期望 40", active, got)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := OpenWALWithSegmentSize(walDir, snapDir, testSegmentSize)
	if err != nil {
		t.Fatalf("重启 OpenWAL: %v", err)
	}
	defer w2.Close()

	last, err := w2.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if last != 40 {
		t.Fatalf("重启后 LastIndex = %d，期望 40", last)
	}
	got, err := w2.Entries(1, 41, math.MaxUint64)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(got) != 40 {
		t.Fatalf("重启后恢复的日志条数 = %d，期望 40", len(got))
	}
	for i, e := range got {
		if e.Index != uint64(i+1) {
			t.Fatalf("第 %d 条日志 Index = %d，期望 %d（分段读取顺序错误）", i, e.Index, i+1)
		}
	}
}

// TestWALRotationAcrossHexSequence 回归文件名序号用十六进制解析的问题。
// 旧实现用 strconv.ParseUint(name, 10, 64) 解析，序号到 10（十六进制 a）时解析失败，
// 随后又创建 1 号文件并追加，造成文件覆盖与重启后的日志乱序/丢失。
func TestWALRotationAcrossHexSequence(t *testing.T) {
	walDir, snapDir := t.TempDir(), t.TempDir()

	w, err := OpenWALWithSegmentSize(walDir, snapDir, testSegmentSize)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	const total = 60 // 约 15 个分段，足以越过 0xa
	for _, e := range numberedEntries(1, total) {
		if err := w.Save(raftpb.HardState{Term: 1}, []raftpb.Entry{e}); err != nil {
			t.Fatalf("Save(index=%d): %v", e.Index, err)
		}
	}

	files := walSegmentFiles(t, walDir)
	if len(files) < 12 {
		t.Fatalf("分段数 = %d，期望至少 12（序号必须能越过 0xa）", len(files))
	}
	seen := make(map[string]bool, len(files))
	for _, name := range files {
		if seen[name] {
			t.Fatalf("分段文件名重复: %s", name)
		}
		seen[name] = true
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := OpenWALWithSegmentSize(walDir, snapDir, testSegmentSize)
	if err != nil {
		t.Fatalf("重启 OpenWAL: %v", err)
	}
	defer w2.Close()

	last, err := w2.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if last != total {
		t.Fatalf("重启后 LastIndex = %d，期望 %d", last, total)
	}
	got, err := w2.Entries(1, total+1, math.MaxUint64)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(got) != total {
		t.Fatalf("重启后恢复的日志条数 = %d，期望 %d", len(got), total)
	}
	for i, e := range got {
		if e.Index != uint64(i+1) {
			t.Fatalf("第 %d 条日志 Index = %d，期望 %d（序号解析错误导致分段乱序）", i, e.Index, i+1)
		}
	}
}

// TestWALTruncateDeletesOnlyCoveredSegments 验证 Truncate 的删除边界：
//   - 最大索引 < 快照索引的非活跃分段被删除；
//   - 活跃分段永远保留；
//   - 最大索引 >= 快照索引的分段保留；
//   - 删除后重启（配合快照）能正常恢复快照之后的日志。
func TestWALTruncateDeletesOnlyCoveredSegments(t *testing.T) {
	walDir, snapDir := t.TempDir(), t.TempDir()

	w, err := OpenWALWithSegmentSize(walDir, snapDir, testSegmentSize)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	for _, e := range numberedEntries(1, 40) {
		// HardState 传空，避免 lastHardStateFile 干扰本测试对删除范围的判断
		if err := w.Save(raftpb.HardState{}, []raftpb.Entry{e}); err != nil {
			t.Fatalf("Save(index=%d): %v", e.Index, err)
		}
	}

	const snapshotIndex = 20
	// 真实调用点一定先写快照再 Truncate；这里补上快照，保证重启后日志没有缺口。
	if err := w.SaveSnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
		Index:     snapshotIndex,
		Term:      1,
		ConfState: raftpb.ConfState{Voters: []uint64{1}},
	}}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	before := make(map[string]uint64, len(w.fileMaxIdx))
	for name, idx := range w.fileMaxIdx {
		before[name] = idx
	}
	beforeFiles := walSegmentFiles(t, walDir)
	active := filepath.Base(w.activeFile.Name())

	if err := w.Truncate(snapshotIndex); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	afterFiles := walSegmentFiles(t, walDir)
	if len(afterFiles) >= len(beforeFiles) {
		t.Fatalf("Truncate 后分段数 = %d，期望少于删除前的 %d", len(afterFiles), len(beforeFiles))
	}
	if len(afterFiles) == 0 {
		t.Fatalf("Truncate 删掉了所有分段，活跃分段必须保留")
	}

	for name, maxIdx := range before {
		wantGone := name != active && maxIdx > 0 && maxIdx < snapshotIndex
		if gotGone := !fileExists(t, walDir, name); gotGone != wantGone {
			t.Fatalf("分段 %s (maxIdx=%d) 删除状态 = %v，期望 %v (snapshotIndex=%d, active=%s)",
				name, maxIdx, gotGone, wantGone, snapshotIndex, active)
		}
		if wantGone {
			if _, ok := w.fileMaxIdx[name]; ok {
				t.Fatalf("已删除的分段 %s 仍残留在 fileMaxIdx 中", name)
			}
		}
	}
	if !fileExists(t, walDir, active) {
		t.Fatalf("活跃分段 %s 被 Truncate 误删", active)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := OpenWALWithSegmentSize(walDir, snapDir, testSegmentSize)
	if err != nil {
		t.Fatalf("重启 OpenWAL: %v", err)
	}
	defer w2.Close()

	snap, err := w2.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Metadata.Index != snapshotIndex {
		t.Fatalf("重启后快照索引 = %d，期望 %d", snap.Metadata.Index, snapshotIndex)
	}
	last, err := w2.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	if last != 40 {
		t.Fatalf("重启后 LastIndex = %d，期望 40（快照之后的日志不能丢）", last)
	}
	got, err := w2.Entries(snapshotIndex+1, 41, math.MaxUint64)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(got) != 20 {
		t.Fatalf("重启后快照之后的日志条数 = %d，期望 20", len(got))
	}
}

// TestWALTruncateKeepsLatestHardStateSegment 验证 Truncate 不会删掉最新 HardState 所在的分段。
// 快照只保存 Term/Commit，Vote 只能靠 WAL 恢复；删掉它会让重启后的节点在当前任期里
// “忘记”自己投过票，可能被说服投出第二票，破坏 Raft 的选举安全性。
func TestWALTruncateKeepsLatestHardStateSegment(t *testing.T) {
	walDir, snapDir := t.TempDir(), t.TempDir()

	w, err := OpenWALWithSegmentSize(walDir, snapDir, 512)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	// 前两条日志和一个带 Vote 的 HardState 写入第一个分段。
	if err := w.Save(raftpb.HardState{Term: 7, Vote: 3, Commit: 2}, numberedEntries(1, 2)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	hsFile := w.lastHardStateFile
	if hsFile == "" {
		t.Fatalf("lastHardStateFile 为空，非空 HardState 写入后必须被记录")
	}

	// 后续日志用空 HardState，确保最新 HardState 停留在第一个分段。
	for _, e := range numberedEntries(3, 40) {
		if err := w.Save(raftpb.HardState{}, []raftpb.Entry{e}); err != nil {
			t.Fatalf("Save(index=%d): %v", e.Index, err)
		}
	}

	active := filepath.Base(w.activeFile.Name())
	if hsFile == active {
		t.Fatalf("测试前置条件不成立：HardState 所在分段 %s 仍是活跃分段", hsFile)
	}

	// 先落一个覆盖全部日志的快照，再让 Truncate 尝试删掉所有非活跃分段。
	if err := w.SaveSnapshot(raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{
		Index:     40,
		Term:      7,
		ConfState: raftpb.ConfState{Voters: []uint64{1}},
	}}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := w.Truncate(1000); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	if !fileExists(t, walDir, hsFile) {
		t.Fatalf("最新 HardState 所在分段 %s 被 Truncate 误删", hsFile)
	}
	if !fileExists(t, walDir, active) {
		t.Fatalf("活跃分段 %s 被 Truncate 误删", active)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := OpenWALWithSegmentSize(walDir, snapDir, 512)
	if err != nil {
		t.Fatalf("重启 OpenWAL: %v", err)
	}
	defer w2.Close()

	hs, _, err := w2.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	if hs.Term != 7 || hs.Vote != 3 {
		t.Fatalf("重启后 HardState = %+v，期望 Term=7 Vote=3（Vote 必须从保留的分段恢复）", hs)
	}
	if hs.Commit != 40 {
		t.Fatalf("重启后 Commit = %d，期望 40（应修复到快照索引）", hs.Commit)
	}
}

func mapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
