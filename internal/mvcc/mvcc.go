package mvcc

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"mini-etcd/internal/backend"
	"mini-etcd/internal/config"
	"mini-etcd/internal/types"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ErrKeyNotFound 表示请求的 key 在状态机中不存在
var ErrKeyNotFound = errors.New("mvcc: key not found")

// ErrLeaseNotFound 表示租约不存在（已过期/已撤销），或续租的目标已经不存在。
var ErrLeaseNotFound = errors.New("mvcc: lease not found")

// TombstoneValue 是一个特殊的值，用于标记某个 key 已被删除。
var TombstoneValue = []byte{0xFF}

const (
	// 垃圾回收时间间隔
	gcInterval = 5 * time.Minute
	// 保留的历史版本数量
	retainCount = 1000
)

type MVCC struct {
	be    *backend.Backend
	stopC chan struct{} // 用于通知 MVCC 停止后台 goroutine 的 channel
	doneC chan struct{} // 用于通知 MVCC 后台 goroutine 已经退出的 channel
}

func NewMVCC(be *backend.Backend) *MVCC {
	m := &MVCC{
		be:    be,
		stopC: make(chan struct{}),
		doneC: make(chan struct{}),
	}

	go m.gcLoop() // 启动后台垃圾回收循环

	return m
}

// Close 优雅关闭 MVCC，停止后台 goroutine
func (m *MVCC) Close() error {
	close(m.stopC) // 通知后台 goroutine 停止
	<-m.doneC      // 阻塞等待后台 goroutine 完全退出
	// Server 调用 mvcc.Close() 的时候，可能 gcLoop 协程正在执行 Compact()
	// 所以 Close() 还要等待 doneC 完全关闭才能退出
	return nil
}

// gcLoop 定时触发垃圾回收
func (m *MVCC) gcLoop() {
	defer close(m.doneC)                 // 退出时关闭 doneC，通知外部 goroutine 已经退出
	ticker := time.NewTicker(gcInterval) // 定时器，每隔 gcInterval 触发一次
	defer ticker.Stop()

	for {
		select {
		case <-m.stopC: // 收到停止信号，退出循环
			return
		case <-ticker.C: // 定时器触发，执行垃圾回收
			if err := m.Compact(); err != nil {
				log.Printf("[GC] compaction failed: %v", err)
			}
		}
	}
}

// Compact 执行压缩，保留最近的 retainCount 个 revision
// 核心原则：不仅要看全局 revision，还必须绝对保护每个 Key 的 latestRevision。
// 因为很多 Key 可能很久没更新了，但是又不能全部删掉，所以需要保留每个 Key 的最新版本。
// 而在 remainCount 个 revision 之内的历史版本，则根据时间局部性原理，全量保留即可。
func (m *MVCC) Compact() error {
	return m.be.Update(func(tx backend.WriteTx) error {
		// 1. 获取当前全局最新 revision
		revBytes := tx.Get(backend.MetaBucket, []byte(backend.RevisionKey))
		if len(revBytes) != 8 {
			return nil // 还没有数据，无需 GC
		}
		currentRev := binary.BigEndian.Uint64(revBytes)

		// 如果当前版本还没达到保留阈值，直接跳过
		if currentRev <= retainCount {
			return nil
		}

		// 计算安全水位线，小于等于这个版本的旧数据是潜在的删除对象
		targetRev := currentRev - retainCount
		log.Printf("[GC] 开始垃圾回收, 当前 revision: %d, 压缩水位线: %d", currentRev, targetRev)

		c := tx.GetCursor(backend.DataBucket) // 获取 DataBucket 的游标
		if c == nil {
			return nil
		}

		var keysToDelete [][]byte // 收集要删除的 key\x00rev
		deletedCount := 0         // 统计删除的历史版本数量

		// 2. 遍历整个 DataBucket（key\x00rev）
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			key, rev := parseKeyAndRevision(k)
			if rev == 0 {
				continue // 异常脏数据，跳过
			}

			// 只有在安全水位线之下的数据才考虑删除
			if rev <= targetRev {
				// 必须要保护该 Key 的最新版本
				// 如果这个 Key 很久没更新了，它的最新版本可能远小于 targetRev。
				latestRevBytes := tx.Get(backend.DataRevisionBucket, []byte(key))
				if len(latestRevBytes) == 8 {
					latestRev := binary.BigEndian.Uint64(latestRevBytes)
					if rev == latestRev {
						continue // 这是最新版本，强行保留
					}
				}

				// 收集要删除的 key\x00rev
				// 注意：由于 bbolt 的游标机制，不能在遍历时直接 Delete，所以先收集起来
				kCopy := make([]byte, len(k))
				copy(kCopy, k)
				keysToDelete = append(keysToDelete, kCopy)
			}
		}

		// 3. 批量删除旧版本
		for _, k := range keysToDelete {
			if err := tx.DeleteKeyInBucket(backend.DataBucket, k); err != nil {
				return fmt.Errorf("failed to delete old version key %s: %w", k, err)
			}
			deletedCount++
		}

		if deletedCount > 0 {
			log.Printf("[GC] 垃圾回收完成, 共清理了 %d 个历史版本", deletedCount)
		} else {
			log.Printf("[GC] 垃圾回收完成, 没有需要清理的数据")
		}
		return nil
	})
}

// Put 写入数据和 appliedIndex（同一事务）
func (m *MVCC) Put(key, value string, appliedIndex uint64) error {
	return m.be.Update(func(tx backend.WriteTx) error {
		_, err := applyPut(tx, key, value, appliedIndex, 0) // 不绑定租约
		return err
	})
}

// Entry 是一条已完成解码、待应用到状态机的日志。
// Op 取值为 "PUT"、"DELETE" 或 "TXN"：PUT/DELETE 使用 Key/Value，TXN 使用 Txn。
type Entry struct {
	Index   uint64
	Op      string
	Key     string
	Value   string
	Txn     types.TxnCommand // 可选字段：仅当 Op="TXN" 时使用，表示事务命令
	LeaseID int64            // 可选字段：如果是租约相关的事件，记录租约 ID
}

// ApplyBatch 在同一个 bbolt 事务里按顺序应用一批已提交日志。
//
// 每条日志单独开启一次写事务意味着一批日志要付出同样多次 fsync，批量应用把整批
// 日志的落盘成本压缩成一次。整批要么全部生效、要么全部回滚，因此调用方必须等
// 本方法成功返回之后才能通知客户端写入成功。
func (m *MVCC) ApplyBatch(entries []Entry) ([]Event, map[uint64]bool, error) {
	if len(entries) == 0 {
		return nil, nil, nil
	}
	var allEvents []Event               // 本批应用成功后要通知的事件
	txnResults := make(map[uint64]bool) // 记录每条 TXN 日志的执行结果，key 是日志索引，value 是是否走了 Success 分支

	err := m.be.Update(func(tx backend.WriteTx) error { // 在同一个 bbolt 事务里应用整批日志
		for _, e := range entries {
			var err error
			var events []Event
			var succeeded bool
			switch e.Op {
			case "PUT":
				events, err = applyPut(tx, e.Key, e.Value, e.Index, e.LeaseID)
			case "DELETE":
				events, err = applyPut(tx, e.Key, string(TombstoneValue), e.Index, e.LeaseID)
			case "TXN":
				succeeded, events, err = applyTxn(tx, e.Txn, e.Index)
			default:
				err = fmt.Errorf("mvcc: unsupported entry op %q", e.Op)
			}
			if err != nil {
				return err
			}

			// 收集事件，供 Watch 监听器使用
			allEvents = append(allEvents, events...)

			// 记录 TXN 日志的执行结果，供调用方返回给客户端
			txnResults[e.Index] = succeeded
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return allEvents, txnResults, nil
}

// Event 是状态机变更事件，供 Watch 监听器使用。
type Event struct {
	Type     string // "PUT" 或 "DELETE"
	Key      string
	Value    string
	Revision uint64 // 变更发生时的全局 revision，用于 Watch 监听器判断事件顺序
	LeaseID  int64  // 可选字段：如果是租约相关的事件，记录租约 ID
}

func (e Event) JSON() string {
	b, err := json.Marshal(e)
	if err != nil {
		return ""
	}
	return string(b)
}

// applyPut 在给定事务内写入一条 KV，并推进 consistentIndex
func applyPut(tx backend.WriteTx, key, value string, appliedIndex uint64, leaseID int64) ([]Event, error) {
	// 1. 先做幂等检查：appliedIndex 小于等于当前 consistentIndex，说明这条日志
	//    之前已经应用过（重启重放、快照回放等），整条日志都必须跳过。
	//    注意顺序：一定不能先写业务数据再判断，否则重放旧日志会把新值覆盖成旧值。
	var currentIdx uint64 = 0 // 默认值为 0，表示还没有应用过任何日志
	currentIdxBytes := tx.Get(backend.MetaBucket, []byte(backend.ConsistentIndexKey))
	if len(currentIdxBytes) == 8 { // len(nil slice) == 0，这里既判断了 nil 也判断了长度不够的情况
		// 只有能从 metaBucket 中读到 currentIdx 才能把 Get 到的值赋给 currentIdx，否则 currentIdx 仍然是 0
		currentIdx = binary.BigEndian.Uint64(currentIdxBytes)
	}
	// 不管 currentIdx 是默认值 0 还是从 metaBucket 中读到的值，都要和 appliedIndex 做比较，保证幂等性
	if appliedIndex <= currentIdx {
		log.Printf("[mvcc] Put: appliedIndex %d <= current consistentIndex %d, skipping duplicate entry\n", appliedIndex, currentIdx)
		return nil, nil
	}

	return applyPutValue(tx, key, value, appliedIndex, leaseID)
}

// applyPutValue 写入一条 KV、推进 revision/consistentIndex，并维护租约绑定。
//
// 调用方必须已经完成幂等检查。单独拆出来是因为一条 LEASE_REVOKE 日志可能在
// 同一个事务里为多个 key 写墓碑：如果每个 key 都走 applyPut 的幂等检查，
// 第一个 key 写入后 consistentIndex 就等于本条日志的索引，后续 key 会被误判
// 为“已应用”而全部跳过。
func applyPutValue(tx backend.WriteTx, key, value string, appliedIndex uint64, leaseID int64) ([]Event, error) {
	// 2. 更新全局 revision
	var currentRev uint64 = 0 // 默认值为 0，表示还没有写入过任何数据
	currentRevBytes := tx.Get(backend.MetaBucket, []byte(backend.RevisionKey))
	if len(currentRevBytes) == 8 { // 这里同样也同时覆盖了 nil 和长度不够的情况
		currentRev = binary.BigEndian.Uint64(currentRevBytes) // 存在的话赋值给 currentRev
	}
	// 同样不管 currentRev 是默认值 0 还是从 metaBucket 中读到的值，都要自增
	currentRev++ // 每次写入数据时，revision 自增
	revBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(revBytes, currentRev)
	if err := tx.Put(backend.MetaBucket, []byte(backend.RevisionKey), revBytes); err != nil { // 把自增后的 revision 写回 metaBucket
		log.Printf("Failed to update revision: %v", err)
		return nil, err
	}

	// 3. 写入 Data 桶，key + revision 作为唯一标识，保证同一个 key 的不同版本可以共存
	keyBytes := backend.GetKeyWithRevision(key, currentRev)
	if err := tx.Put(backend.DataBucket, keyBytes, []byte(value)); err != nil {
		log.Printf("Failed to put key %s: %v", key, err)
		return nil, err
	}

	// 4. 更新 DataRevision 桶，记录每个 key 的最新 revision，方便 Get 时获取最新值
	if err := tx.Put(backend.DataRevisionBucket, []byte(key), revBytes); err != nil {
		log.Printf("Failed to update DataRevision for key %s: %v", key, err)
		return nil, err
	}

	// 5. 把 uint64 的 appliedIndex 转成 8 字节的 []byte，和数据在同一事务里更新，
	//    保证 [数据 + 索引] 原子落盘，不会出现数据写了但索引没写的情况
	idxBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idxBytes, appliedIndex)
	if err := tx.Put(backend.MetaBucket, []byte(backend.ConsistentIndexKey), idxBytes); err != nil {
		log.Printf("Failed to update consistent index: %v", err)
		return nil, err
	}

	// 6. 维护 key 与租约的绑定关系。
	//    PUT 绑定到新租约时，必须先从旧租约的 Key 列表里摘掉自己，否则旧租约
	//    过期时会把这个已经改绑的 key 一并删除（数据丢失）；
	//    DELETE 或不带租约的 PUT 都要解除绑定。
	isDelete := bytes.Equal([]byte(value), TombstoneValue)
	if err := bindKeyToLease(tx, key, leaseID, isDelete); err != nil {
		return nil, err
	}

	// 7. 构造事件，供 Watch 监听器使用
	eventType := "PUT"
	if isDelete {
		eventType = "DELETE"
	}
	return []Event{
		{
			Type:     eventType,
			Key:      key,
			Value:    value,
			Revision: currentRev,
		},
	}, nil
}

// Get 获取指定 key 的最新值，如果 key 不存在则返回 ErrKeyNotFound
func (m *MVCC) Get(key string) (string, error) {
	var result string
	err := m.be.View(func(tx backend.ReadTx) error {
		// 1. 先从 DataRevision 桶获取指定 key 的最新 revision
		revBytes := tx.Get(backend.DataRevisionBucket, []byte(key))
		if revBytes == nil || len(revBytes) != 8 {
			return ErrKeyNotFound // key 不存在，直接返回错误
		}
		rev := binary.BigEndian.Uint64(revBytes) // 把 8 字节的 []byte 转成 uint64

		// 2. 再从 Data 桶获取对应 revision 的值
		keyWithRev := backend.GetKeyWithRevision(key, rev)
		valBytes := tx.Get(backend.DataBucket, keyWithRev)
		if valBytes == nil || bytes.Equal(valBytes, TombstoneValue) { // 键真的不存在或者被标记为已删除
			return ErrKeyNotFound // key 对应的值不存在，返回错误
		}
		result = string(valBytes)
		return nil
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

// GetAtRevision 获取指定 key 在指定的 revision 上限下的最新值，如果 key 不存在则返回 ErrKeyNotFound
func (m *MVCC) GetAtRevision(key string, maxRev uint64) (string, error) {
	prefix := fmt.Appendf([]byte{}, "%s\x00", key) // 构造前缀 key\x00
	var result string
	var found bool = false // 标记是否找到符合条件的版本

	err := m.be.View(func(tx backend.ReadTx) error {
		c := tx.GetCursor(backend.DataBucket) // 获取 DataBucket 的游标
		if c == nil {
			return nil
		}

		// 遍历所有以 prefix 开头的 key，找到 revision 小于等于 maxRev 的最大版本
		k, v := c.Seek(prefix)                       // Seek 会返回第一个大于等于 prefix 的 key-value 对
		for k != nil && bytes.HasPrefix(k, prefix) { // 边界判断
			// 合法版本的 key 长度固定为 len(prefix) + 8（8 字节大端序 revision）。
			// 用户 key 本身可能包含 \x00，此时形如 "key\x00other" 的其它 key 的版本
			// 也会落进 prefix 区间；用长度做精确判断，避免把别的 key 的 value 当成结果返回。
			if len(k) != len(prefix)+8 {
				k, v = c.Next()
				continue
			}
			rev := binary.BigEndian.Uint64(k[len(prefix):]) // 直接从固定偏移解析 rev
			if rev > maxRev {                               // 超出目标版本，停止，因为 bbolt 底层的 B+ tree 是按 key 升序排列的，后续的 rev 只会更大
				break
			}

			if !bytes.Equal(v, TombstoneValue) { // 如果不是 TombstoneValue，说明找到了合法的值
				result = string(v)
				found = true
			} else { // 如果是 TombstoneValue，说明这个版本被删除了，继续往前找
				found = false // 标记为未找到合法值
				result = ""   // 清空结果
			}
			k, v = c.Next() // 移动到下一个符合前缀的 key
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "", ErrKeyNotFound // 没有找到符合条件的版本，返回错误
	}
	return result, nil
}

// parseKeyAndRevision 从 key\x00rev 中解析出 key 和 rev
func parseKeyAndRevision(key []byte) (string, uint64) {
	parts := bytes.SplitN(key, []byte("\x00"), 2)
	if len(parts) != 2 || len(parts[1]) != 8 { // 长度不对说明不是本格式的 key，返回 rev=0 由调用方忽略
		return string(key), 0
	}
	rev := binary.BigEndian.Uint64(parts[1])
	return string(parts[0]), rev
}

// AdvanceConsistentIndex 把状态机的已应用索引推进到 index。
//
// 空日志（Leader 当选时写入的 no-op）和配置变更不修改业务数据，但它们同样消耗
// 一个 Raft 日志下标。快照的 metadata.Index 记的是“最后一条已应用日志”，
// 如果 consistentIndex 停在这些日志之前，快照索引就会大于状态机索引：
// 重启时 NewRaftNode 把 Applied 设为 consistentIndex，而 raft 内部认为
// 快照之前的日志都已应用（raftLog.applied = snapshotIndex），
// newRaft 的 appliedTo() 会直接 panic：
//
//	applied(N) is out of range [prevApplied(M), committed(...)]
//
// 所以只要最后一条日志不是“真正写入数据”的普通日志，就必须显式推进索引。
func (m *MVCC) AdvanceConsistentIndex(index uint64) error {
	return m.be.Update(func(tx backend.WriteTx) error {
		var current uint64
		if b := tx.Get(backend.MetaBucket, []byte(backend.ConsistentIndexKey)); len(b) == 8 {
			current = binary.BigEndian.Uint64(b)
		}
		if index <= current {
			return nil
		}
		idxBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idxBytes, index)
		return tx.Put(backend.MetaBucket, []byte(backend.ConsistentIndexKey), idxBytes)
	})
}

// GetConsistentIndex 读取当前已应用的最高日志索引（供 main 启动时恢复 Raft 状态用）
func (m *MVCC) GetConsistentIndex() (uint64, error) {
	var idx uint64 = 0 // 默认值为 0，表示还没有应用过任何日志
	err := m.be.View(func(tx backend.ReadTx) error {
		val := tx.Get(backend.MetaBucket, []byte(backend.ConsistentIndexKey))
		if val != nil {
			idx = binary.BigEndian.Uint64(val)
		}
		return nil
	})
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		log.Printf("Failed to get consistent index: %v", err)
	}
	return idx, err
}

// ---------------------------------------------------------------------------
// 集群成员表
// ---------------------------------------------------------------------------
//
// 为什么成员表要放在状态机里，而不是只放在内存/独立 JSON 文件？
//
// Raft 的 ConfState 只记录成员 ID，不记录地址。地址原本只存在于
// ConfChange.Context 里，也就是只存在于“某一条日志”里。一旦这条日志被快照
// 压缩掉，落后太多的节点通过快照追赶时就只能拿到 ID，拿不到地址：
//   - Transport 没有新成员的 Raft 地址，发不出 MsgApp/MsgReadIndex；
//   - Leader 代理表没有新成员的 HTTP 地址，写请求 503。
//
// 把成员表写进 bbolt 的 MetaBucket 后，它会随 MVCC 快照一起被复制和恢复，
// 于是“快照 = 数据 + 成员表”成为一份自洽的状态，新节点恢复快照后能立即
// 补齐 Transport 和代理地址表。成员表不参与 consistentIndex 的幂等判断，
// 因此不需要和业务日志共用事务，也不会把索引推快。
const MembershipMetaPrefix = "member/"

// memberMetaKey 返回成员在 MetaBucket 中的键，例如 "member/4"。
func memberMetaKey(id uint64) []byte {
	return []byte(MembershipMetaPrefix + strconv.FormatUint(id, 10))
}

// UpsertMember 写入/更新一个成员的两种地址，返回写入的记录。
//
// ConfChange.Context 永远同时携带 Raft 与 HTTP 地址，所以这里直接以本次
// 变更的地址为准，不做“空字段保留旧值”的合并。返回最终记录是为了让调用方
// （Raft 循环）用同一份地址去更新 Transport 与 Leader 代理表。
func (m *MVCC) UpsertMember(id uint64, raftAddr, httpAddr string) (config.Member, error) {
	var result config.Member
	err := m.be.Update(func(tx backend.WriteTx) error {
		key := memberMetaKey(id)
		result = config.Member{ID: id, RaftAddr: raftAddr, HTTPAddr: httpAddr}
		data, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("encode member %d: %w", id, err)
		}
		return tx.Put(backend.MetaBucket, key, data)
	})
	return result, err
}

// RemoveMember 从状态机中删除一个成员。成员被移出集群后，
// 后续快照里不能再带上它的地址，否则重启后会把已移除节点重新加回 Transport。
func (m *MVCC) RemoveMember(id uint64) error {
	return m.be.Update(func(tx backend.WriteTx) error {
		return tx.DeleteKeyInBucket(backend.MetaBucket, memberMetaKey(id))
	})
}

// GetMembers 读取状态机中的全量成员表。
//
// 快照恢复后（ApplySnapshot）成员表随 MetaBucket 一起被替换，
// Raft 循环会用本方法的结果重建 Transport 的 peer 地址表和 Leader 代理表。
func (m *MVCC) GetMembers() (map[uint64]config.Member, error) {
	members := make(map[uint64]config.Member)
	err := m.be.View(func(tx backend.ReadTx) error {
		all, err := tx.GetAllFromBucket(backend.MetaBucket)
		if err != nil {
			return err
		}
		for k, v := range all {
			if !strings.HasPrefix(k, MembershipMetaPrefix) {
				continue
			}
			var member config.Member
			if err := json.Unmarshal(v, &member); err != nil {
				// 单条记录损坏说明状态机数据已不可信。这里仍然跳过并告警，
				// 避免因为一个成员记录让整个节点无法启动；缺失的地址会在
				// Transport 的告警日志里暴露出来。
				log.Printf("[mvcc] 忽略损坏的成员记录 %q: %v", k, err)
				continue
			}
			members[member.ID] = member
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return members, nil
}

// 定义快照的数据结构。
//
// Lease/LeaseKey 必须随快照一起复制：租约以及“key -> 租约”的绑定关系也是
// 状态机的一部分。如果快照里没有它们，落后的 follower 从快照恢复后既不知道
// 哪些 key 绑定了租约，也没有租约记录，之后应用同一条 LEASE_REVOKE 日志时
// 无法删除这些 key，与 Leader 的状态机产生永久分歧。
// 反向索引（lease_key_rev）可以由 LeaseKey 重建，不必进快照。
type MVCCSnapshot struct {
	Data     map[string][]byte `json:"data"`
	Meta     map[string][]byte `json:"meta"`
	Lease    map[string][]byte `json:"lease"`     // leaseID -> (ttl、expireTime)
	LeaseKey map[string][]byte `json:"lease_key"` // leaseID -> keys
}

// snapshotMagic 标识快照格式为 gob 编码。
//
// 不能用 encoding/json 序列化 MVCCSnapshot：Data 桶的 key 是
// "用户 key + \x00 + 8 字节二进制 revision"，其中 revision 的字节很可能不是
// 合法 UTF-8（例如 revision=128 时最后一位是 0x80）。encoding/json 在序列化
// 字符串时会把非法 UTF-8 字节替换成 U+FFFD（EF BF BD），于是快照恢复后 key
// 被改写，数据永久丢失。gob 是二进制安全的，可以原样保留任意字节。
var snapshotMagic = []byte("MVC2")

// decodeSnapshot 解析快照数据。没有 magic 前缀说明数据不是本项目的快照
// （损坏或被其他数据覆盖），直接报错而不是猜测格式。
func decodeSnapshot(data []byte) (*MVCCSnapshot, error) {
	if !bytes.HasPrefix(data, snapshotMagic) {
		return nil, errors.New("invalid snapshot: missing MVC2 magic header")
	}
	var snap MVCCSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data[len(snapshotMagic):])).Decode(&snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// GetSnapshot 获取当前状态机的全量快照数据
func (m *MVCC) GetSnapshot() ([]byte, error) {
	var snap MVCCSnapshot
	err := m.be.View(func(tx backend.ReadTx) error {
		var err error
		snap.Data, err = tx.GetAllFromBucket(backend.DataBucket) // 获取全量普通数据
		if err != nil {
			log.Printf("Failed to get all data from Data bucket: %v", err)
			return err
		}
		snap.Meta, err = tx.GetAllFromBucket(backend.MetaBucket) // 获取全量元数据
		if err != nil {
			log.Printf("Failed to get all data from Meta bucket: %v", err)
			return err
		}
		snap.Lease, err = tx.GetAllFromBucket(backend.LeaseBucket) // 获取全量租约
		if err != nil {
			log.Printf("Failed to get all data from Lease bucket: %v", err)
			return err
		}
		snap.LeaseKey, err = tx.GetAllFromBucket(backend.LeaseKeyBucket) // 获取租约的 key 绑定
		if err != nil {
			log.Printf("Failed to get all data from LeaseKey bucket: %v", err)
			return err
		}
		return nil
	})
	if err != nil {
		log.Printf("Failed to get snapshot: %v", err)
		return nil, err
	}

	// 使用 gob 而不是 JSON：Data 桶的 key 中含有二进制 revision，JSON 会破坏非法 UTF-8 字节
	var buf bytes.Buffer
	buf.Write(snapshotMagic)
	if err := gob.NewEncoder(&buf).Encode(&snap); err != nil {
		log.Printf("Failed to encode snapshot: %v", err)
		return nil, err
	}
	return buf.Bytes(), nil
}

// ApplySnapshot 从快照数据恢复状态机
func (m *MVCC) ApplySnapshot(data []byte) error {
	snap, err := decodeSnapshot(data)
	if err != nil {
		log.Printf("Failed to unmarshal snapshot data: %v", err)
		return err
	}

	// 开启底层写事务，清空旧数据，写入新数据
	return m.be.Update(func(tx backend.WriteTx) error {
		// 1. 清空现有的桶（DataRevision 也必须清空，否则会残留指向已不存在版本的旧索引）
		if err := tx.ClearBucket(backend.DataBucket); err != nil {
			log.Printf("Failed to clear Data bucket: %v", err)
			return err
		}
		if err := tx.ClearBucket(backend.MetaBucket); err != nil {
			log.Printf("Failed to clear Meta bucket: %v", err)
			return err
		}
		if err := tx.ClearBucket(backend.DataRevisionBucket); err != nil {
			log.Printf("Failed to clear DataRevision bucket: %v", err)
			return err
		}
		// 租约相关的桶也必须清空：否则本地残留的旧租约/绑定会在快照恢复后
		// 与新状态机混在一起，各节点对同一条后续日志产生不同结果。
		if err := tx.ClearBucket(backend.LeaseBucket); err != nil {
			log.Printf("Failed to clear Lease bucket: %v", err)
			return err
		}
		if err := tx.ClearBucket(backend.LeaseKeyBucket); err != nil {
			log.Printf("Failed to clear LeaseKey bucket: %v", err)
			return err
		}
		if err := tx.ClearBucket(backend.LeaseKeyRevBucket); err != nil {
			log.Printf("Failed to clear LeaseKeyRev bucket: %v", err)
			return err
		}

		// 2. 恢复 Data 桶
		for k, v := range snap.Data {
			if err := tx.Put(backend.DataBucket, []byte(k), v); err != nil {
				log.Printf("Failed to restore Data bucket key %s: %v", k, err)
				return err
			}
		}

		// 3. 恢复 Meta 桶
		for k, v := range snap.Meta {
			if err := tx.Put(backend.MetaBucket, []byte(k), v); err != nil {
				log.Printf("Failed to restore Meta bucket key %s: %v", k, err)
				return err
			}
		}

		// 4. 恢复租约与 key 绑定
		for k, v := range snap.Lease {
			if err := tx.Put(backend.LeaseBucket, []byte(k), v); err != nil {
				log.Printf("Failed to restore Lease bucket key %s: %v", k, err)
				return err
			}
		}
		for k, v := range snap.LeaseKey {
			if err := tx.Put(backend.LeaseKeyBucket, []byte(k), v); err != nil {
				log.Printf("Failed to restore LeaseKey bucket key %s: %v", k, err)
				return err
			}
		}

		// 5. 根据 Data 桶重建 DataRevision 桶。
		//    DataRevision 没有存进快照，如果这里不重建，新节点/重启节点恢复快照后
		//    Get(key) 会因为查不到最新 revision 而全部返回 ErrKeyNotFound。
		if err := rebuildDataRevision(tx); err != nil {
			return err
		}

		// 6. 根据 LeaseKey 重建反向索引（同样没有存进快照）
		return rebuildLeaseKeyRev(tx)
	})
}

// rebuildDataRevision 扫描 Data 桶，重新计算每个 key 的最新 revision 并写回 DataRevision 桶。
func rebuildDataRevision(tx backend.WriteTx) error {
	c := tx.GetCursor(backend.DataBucket)
	if c == nil {
		return nil
	}

	// 先收集每个 key 的最大 revision；不要在遍历 Data 桶的游标期间直接写其它桶，
	// 避免游标失效（虽然写的是不同桶，但收集后再写更稳妥）。
	latest := make(map[string]uint64)
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		key, rev := parseKeyAndRevision(k)
		if rev == 0 { // 非法格式的历史脏数据，跳过
			continue
		}
		if rev > latest[key] {
			latest[key] = rev
		}
	}

	for key, rev := range latest {
		revBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(revBytes, rev)
		if err := tx.Put(backend.DataRevisionBucket, []byte(key), revBytes); err != nil {
			log.Printf("Failed to rebuild DataRevision for key %s: %v", key, err)
			return err
		}
	}
	return nil
}

// rebuildLeaseKeyRev 根据 LeaseKeyBucket 重建 “key -> LeaseID” 的反向索引。
// 快照里只保存 LeaseKey，恢复后必须重建反向索引，否则后续 PUT/DELETE
// 无法把 key 从旧租约的列表里摘掉，租约到期时会误删已改绑的 key。
func rebuildLeaseKeyRev(tx backend.WriteTx) error {
	// 定义一个临时结构体来存储 key 和 leaseID 的绑定关系
	type binding struct {
		key     string
		leaseID int64
	}
	var bindings []binding

	c := tx.GetCursor(backend.LeaseKeyBucket)
	if c != nil {
		// 遍历 LeaseKeyBucket，收集所有 key -> leaseID 的绑定关系
		for k, v := c.First(); k != nil; k, v = c.Next() {
			leaseID, err := strconv.ParseInt(string(k), 10, 64)
			if err != nil {
				continue
			}
			var keys []string
			if err := json.Unmarshal(v, &keys); err != nil {
				continue
			}
			for _, key := range keys {
				// 收集绑定关系，后续统一写入 LeaseKeyRevBucket
				bindings = append(bindings, binding{key: key, leaseID: leaseID})
			}
		}
	}

	// 写入 LeaseKeyRevBucket，建立 key -> leaseID 的反向索引
	for _, b := range bindings {
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, uint64(b.leaseID))
		if err := tx.Put(backend.LeaseKeyRevBucket, []byte(b.key), idBytes); err != nil {
			return err
		}
	}
	return nil
}

// Delete 插入 TombstoneValue 标记 key 已被删除
func (m *MVCC) Delete(key string, appliedIndex uint64) error {
	return m.Put(key, string(TombstoneValue), appliedIndex)
}

type KVPair struct {
	Key   string
	Value string
}

// Range 查询所有以 prefix 为前缀的键值对（最新版本）
func (m *MVCC) Range(prefix string, limit int) ([]KVPair, error) {
	var results []KVPair

	// 如果 limit <= 0，默认不限制数量
	if limit <= 0 {
		limit = -1
	}

	err := m.be.View(func(tx backend.ReadTx) error {
		// 1. 从 DataRevisionBucket 里扫描前缀（拿到每个 key 的最新 revision）
		// 因为 DataRevisionBucket 里的 key 就是纯粹的 key，没有 \x00 后缀
		c := tx.GetCursor(backend.DataRevisionBucket)
		if c == nil {
			return nil
		}

		prefixBytes := []byte(prefix)
		// Seek 定位到第一个以 prefix 开头的 key
		k, revBytes := c.Seek(prefixBytes)
		for k != nil && bytes.HasPrefix(k, prefixBytes) {
			// 合法版本的 revBytes 长度必须是 8 字节
			if len(revBytes) != 8 {
				k, revBytes = c.Next()
				continue
			}
			// 如果 limit > 0 且已经收集到足够的结果，就停止遍历
			if limit > 0 && len(results) >= limit {
				break
			}

			rev := binary.BigEndian.Uint64(revBytes)
			keyStr := string(k)

			// 2. 根据 key 和 rev，去 DataBucket 拿具体的数据
			keyWithRev := backend.GetKeyWithRevision(keyStr, rev)
			valBytes := tx.Get(backend.DataBucket, keyWithRev)

			if valBytes != nil {
				if bytes.Equal(valBytes, TombstoneValue) {
					// 如果 valBytes 是墓碑标记，跳过这个 key
					k, revBytes = c.Next()
					continue
				}
				// 收集结果
				results = append(results, KVPair{
					Key:   keyStr,
					Value: string(valBytes),
				})
			}

			// 移动到下一个 key
			k, revBytes = c.Next()
		}
		return nil
	})

	return results, err
}

// Txn 在单个 bbolt 事务中评估 If 条件，并执行 Then/Else 分支。
// 条件判断和分支内的写入必须原子完成：否则并发读可能看到"条件已满足"却还没写入的中间状态。
func (m *MVCC) Txn(txnCmd types.TxnCommand, appliedIndex uint64) error {
	return m.be.Update(func(tx backend.WriteTx) error {
		_, _, err := applyTxn(tx, txnCmd, appliedIndex)
		return err
	})
}

// applyTxn 在给定事务内执行一条事务日志
func applyTxn(tx backend.WriteTx, txnCmd types.TxnCommand, appliedIndex uint64) (bool, []Event, error) {
	// 1. 幂等检查（与 Put 相同）：重放旧日志时整条事务都必须跳过
	var currentIdx uint64 = 0
	currentIdxBytes := tx.Get(backend.MetaBucket, []byte(backend.ConsistentIndexKey))
	if len(currentIdxBytes) == 8 {
		currentIdx = binary.BigEndian.Uint64(currentIdxBytes)
	}
	if appliedIndex <= currentIdx {
		return false, nil, nil // 重复应用，直接跳过
	}

	// 2. 评估所有 If 条件（多个条件之间是 AND）。
	//    key 不存在时按空串比较，与 etcd 语义一致。
	allMatched := true
	for _, cmp := range txnCmd.Compares {
		actualVal, _ := getTxValue(tx, cmp.Key)
		matched, err := cmp.Match(actualVal)
		if err != nil {
			return false, nil, fmt.Errorf("txn compare %q: %w", cmp.Key, err)
		}
		if !matched {
			allMatched = false
			break
		}
	}

	// 3. 决定执行哪个分支
	ops := txnCmd.Failure
	if allMatched {
		ops = txnCmd.Success
	}

	// 4. 执行分支里的每一个操作
	var currentRev uint64 = 0
	revBytes := tx.Get(backend.MetaBucket, []byte(backend.RevisionKey))
	if len(revBytes) == 8 {
		currentRev = binary.BigEndian.Uint64(revBytes)
	}

	var events []Event

	for _, op := range ops {
		var valToWrite []byte
		switch op.Op {
		case "PUT":
			valToWrite = []byte(op.Value)
		case "DELETE":
			valToWrite = TombstoneValue
		default:
			// 正常路径下 Server 已经校验过；这里兜底，避免把非法操作当成空值写入。
			return false, nil, fmt.Errorf("txn: unsupported operation %q", op.Op)
		}

		currentRev++ // 事务内的每个写操作，Revision 都加 1

		// 写入 DataBucket
		keyWithRev := backend.GetKeyWithRevision(op.Key, currentRev)
		if err := tx.Put(backend.DataBucket, keyWithRev, valToWrite); err != nil {
			return false, nil, err
		}

		// 更新 DataRevisionBucket
		opRevBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(opRevBytes, currentRev)
		if err := tx.Put(backend.DataRevisionBucket, []byte(op.Key), opRevBytes); err != nil {
			return false, nil, err
		}

		// 维护 key 与租约的绑定关系（DELETE 会解除绑定）
		if err := bindKeyToLease(tx, op.Key, op.LeaseID, op.Op == "DELETE"); err != nil {
			return false, nil, err
		}

		// 构造事件，供 Watch 监听器使用
		eventType := op.Op
		events = append(events, Event{
			Type:     eventType,
			Key:      op.Key,
			Value:    string(valToWrite),
			Revision: currentRev,
		})
	}

	// 5. 更新全局 Revision 和 AppliedIndex（同一事务，保证原子落盘）
	if err := putMetaUint64(tx, backend.RevisionKey, currentRev); err != nil {
		return false, nil, err
	}
	if err := putMetaUint64(tx, backend.ConsistentIndexKey, appliedIndex); err != nil {
		return false, nil, err
	}
	// allMatched 表示 If 条件是否全部满足，供调用方判断执行了哪个分支
	// 比如用于分布式锁利用事务的原子性抢锁后，调用方可以根据 allMatched 判断自己是否抢到锁
	return allMatched, events, nil
}

// getTxValue 在同一个 bbolt 事务内读取 key 的最新值。
// 第二个返回值表示 key 是否存在；不存在（或已被删除）时返回 ("", false)。
func getTxValue(tx backend.WriteTx, key string) (string, bool) {
	revBytes := tx.Get(backend.DataRevisionBucket, []byte(key))
	if len(revBytes) != 8 {
		return "", false
	}
	rev := binary.BigEndian.Uint64(revBytes)
	keyWithRev := backend.GetKeyWithRevision(key, rev)
	val := tx.Get(backend.DataBucket, keyWithRev)
	if val == nil || bytes.Equal(val, TombstoneValue) {
		return "", false
	}
	return string(val), true
}

// putMetaUint64 把 uint64 以 8 字节大端序写入 MetaBucket
func putMetaUint64(tx backend.WriteTx, key string, val uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return tx.Put(backend.MetaBucket, []byte(key), buf)
}

// GetHistory 查询指定 revision 之后的历史事件（供 Watch 断线重连补发）
func (m *MVCC) GetHistory(key string, isPrefix bool, startRev uint64) ([]Event, error) {
	var events []Event

	// 开启只读事务，遍历 Data 桶，收集指定 key 的历史事件
	err := m.be.View(func(tx backend.ReadTx) error {
		// 获取 Data 桶的游标
		c := tx.GetCursor(backend.DataBucket)
		if c == nil {
			return nil
		}

		// 如果是精确匹配，直接用 key 前缀去 Seek，提高效率
		// 如果是前缀匹配，就用用户传的 prefix 去 Seek
		seekPrefix := key
		if !isPrefix {
			// 精确匹配时，key 后面加上 \x00，避免把其它 key 的历史版本也匹配到
			seekPrefix = key + "\x00"
		}
		prefixBytes := []byte(seekPrefix)

		k, v := c.Seek(prefixBytes)
		for k != nil && bytes.HasPrefix(k, prefixBytes) {
			// 解析出真正的 key 和 rev
			origKey, rev := parseKeyAndRevision(k)

			// 注意：如果是前缀匹配，parseKeyAndRevision 可能把 "/user/1\x00rev" 解析为 key="/user/1", rev=rev
			if rev > startRev {
				isDelete := bytes.Equal(v, TombstoneValue)
				evType := "PUT"
				valStr := string(v)
				if isDelete {
					evType = "DELETE"
					valStr = ""
				}
				events = append(events, Event{
					Type:     evType,
					Key:      origKey,
					Value:    valStr,
					Revision: rev,
				})
			}
			k, v = c.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 因为 bbolt 是按 key 排序的，事件可能是乱序的，必须按 Revision 升序排列
	sort.Slice(events, func(i, j int) bool {
		return events[i].Revision < events[j].Revision
	})

	return events, nil
}

// ---------------------------------------------------------------------------
// 租约（Lease）
// ---------------------------------------------------------------------------

// leaseIDKey 把 LeaseID 转成 LaunchBucket/LeaseKeyBucket 中的 key。
func leaseIDKey(leaseID int64) []byte {
	return []byte(strconv.FormatInt(leaseID, 10))
}

// encodeLease 把 TTL 和过期时间序列化成 16 字节：前 8 字节 TTL，后 8 字节 ExpireTime。
func encodeLease(ttl, expireTime int64) []byte {
	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val[:8], uint64(ttl))
	binary.BigEndian.PutUint64(val[8:], uint64(expireTime))
	return val
}

// decodeLease 解析 encodeLease 的输出；长度不对说明数据损坏。
func decodeLease(val []byte) (ttl, expireTime int64, ok bool) {
	if len(val) != 16 {
		return 0, 0, false
	}
	return int64(binary.BigEndian.Uint64(val[:8])), int64(binary.BigEndian.Uint64(val[8:])), true
}

// readLeaseKeys 读取租约绑定的 key 列表。
func readLeaseKeys(tx backend.WriteTx, leaseID int64) []string {
	keysBytes := tx.Get(backend.LeaseKeyBucket, leaseIDKey(leaseID))
	if keysBytes == nil {
		return nil
	}
	var keys []string
	if err := json.Unmarshal(keysBytes, &keys); err != nil {
		log.Printf("[mvcc] 解析租约 %d 的 key 列表失败: %v", leaseID, err)
		return nil
	}
	return keys
}

// writeLeaseKeys 写回租约的 key 列表；列表为空时删除整条记录，避免留下空数组。
func writeLeaseKeys(tx backend.WriteTx, leaseID int64, keys []string) error {
	if len(keys) == 0 {
		return tx.DeleteKeyInBucket(backend.LeaseKeyBucket, leaseIDKey(leaseID))
	}
	data, err := json.Marshal(keys)
	if err != nil {
		return err
	}
	return tx.Put(backend.LeaseKeyBucket, leaseIDKey(leaseID), data)
}

// leaseIDForKey 通过反向索引查出 key 当前绑定的租约，没有绑定返回 0。
func leaseIDForKey(tx backend.WriteTx, key string) int64 {
	val := tx.Get(backend.LeaseKeyRevBucket, []byte(key))
	if len(val) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(val))
}

// bindKeyToLease 维护 key 与租约的绑定关系。
//
// leaseID <= 0 或 isDelete=true 表示解除绑定。改绑时必须先从旧租约的列表里
// 摘掉这个 key，否则旧租约到期时会删掉一个已经属于新租约的 key。
// 目标租约不存在时只写数据、不建立绑定：这样 key 不会绑定到一个永远不会被
// 回收的租约上（正常路径下 Server 已经在提议前校验过租约存在）。
func bindKeyToLease(tx backend.WriteTx, key string, leaseID int64, isDelete bool) error {
	if isDelete {
		leaseID = 0
	}
	// 如果 key 当前绑定的租约就是目标租约，说明绑定关系没有变化，直接返回。
	oldID := leaseIDForKey(tx, key)
	if oldID == leaseID {
		return nil
	}

	// 1. 从旧租约的 key 列表里移除
	if oldID > 0 {
		// 读取旧租约的 key 列表
		oldKeys := readLeaseKeys(tx, oldID)
		kept := oldKeys[:0]
		for _, k := range oldKeys {
			// 如果当前 key 不是目标 key，则保留
			// 也就是移除目标 key
			if k != key {
				kept = append(kept, k)
			}
		}
		// 将移除了目标 key 的新列表写回旧租约的记录
		if err := writeLeaseKeys(tx, oldID, kept); err != nil {
			return err
		}
	}

	// 2. 解除绑定：删除反向索引即可
	if leaseID <= 0 {
		return tx.DeleteKeyInBucket(backend.LeaseKeyRevBucket, []byte(key))
	}

	// 3. 目标租约不存在：不建立绑定
	if tx.Get(backend.LeaseBucket, leaseIDKey(leaseID)) == nil {
		return tx.DeleteKeyInBucket(backend.LeaseKeyRevBucket, []byte(key))
	}

	// 4. 加入新租约的 key 列表
	keys := readLeaseKeys(tx, leaseID)
	if !slices.Contains(keys, key) {
		keys = append(keys, key)
		if err := writeLeaseKeys(tx, leaseID, keys); err != nil {
			return err
		}
	}

	// 5. 写入反向索引
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, uint64(leaseID))
	return tx.Put(backend.LeaseKeyRevBucket, []byte(key), idBytes)
}

// appliedAlready 判断 appliedIndex 是否已经应用过（重启重放、快照回放等）。
func appliedAlready(tx backend.WriteTx, appliedIndex uint64) bool {
	var current uint64
	if b := tx.Get(backend.MetaBucket, []byte(backend.ConsistentIndexKey)); len(b) == 8 {
		current = binary.BigEndian.Uint64(b)
	}
	return appliedIndex <= current
}

// GrantLease 授予租约，写入租约 ID、TTL 和过期时间。
//
// 过期时间 = 日志里的 leaseStart + TTL，不使用本节点的 time.Now()：
// 状态机必须只由日志内容决定，否则落后节点重放同一条 Grant 时会用更晚的
// 本地时钟重新计算，得到一个比 Leader 更晚的过期时间，节点之间产生分歧。
func (m *MVCC) GrantLease(leaseID, ttl, leaseStart int64, appliedIndex uint64) error {
	return m.be.Update(func(tx backend.WriteTx) error {
		if appliedAlready(tx, appliedIndex) {
			log.Printf("[mvcc] LeaseGrant: appliedIndex %d already applied, skipping duplicate entry\n", appliedIndex)
			return nil
		}

		expireTime := leaseStart + ttl
		if err := tx.Put(backend.LeaseBucket, leaseIDKey(leaseID), encodeLease(ttl, expireTime)); err != nil {
			return err
		}
		return putMetaUint64(tx, backend.ConsistentIndexKey, appliedIndex)
	})
}

// RevokeLease 撤销租约，删除租约记录，并为该租约绑定的所有 Key 写入 TombstoneValue。
// 返回值是需要推给 Watch 的 DELETE 事件。
func (m *MVCC) RevokeLease(leaseID int64, appliedIndex uint64) ([]Event, error) {
	var events []Event
	err := m.be.Update(func(tx backend.WriteTx) error {
		// 幂等检查：重放旧日志时不能再删一次 key（否则会把后来写入的新值覆盖成墓碑），
		// 也不能把 consistentIndex 回退到本条日志的索引。
		if appliedAlready(tx, appliedIndex) {
			log.Printf("[mvcc] LeaseRevoke: appliedIndex %d already applied, skipping duplicate entry\n", appliedIndex)
			return nil
		}

		// 1. 取出该租约绑定的 key（副本；写入过程中会同步修改 LeaseKeyBucket）
		keys := readLeaseKeys(tx, leaseID)

		// 2. 为每个 key 写墓碑。
		//    这里必须调用 applyPutValue：applyPut 的幂等检查基于 consistentIndex，
		//    第一个 key 写完后 consistentIndex 就等于本条日志索引，后续 key 会被
		//    误判为“已应用”而全部跳过，导致只删掉第一个 key。
		for _, key := range keys {
			ev, err := applyPutValue(tx, key, string(TombstoneValue), appliedIndex, leaseID)
			if err != nil {
				return err
			}
			events = append(events, ev...)
		}

		// 3. 删除租约记录和 key 列表（反向索引已由 applyPutValue 里调用的解绑流程清理）
		if err := tx.DeleteKeyInBucket(backend.LeaseBucket, leaseIDKey(leaseID)); err != nil {
			return err
		}
		if err := tx.DeleteKeyInBucket(backend.LeaseKeyBucket, leaseIDKey(leaseID)); err != nil {
			return err
		}

		// 4. 更新 consistentIndex
		return putMetaUint64(tx, backend.ConsistentIndexKey, appliedIndex)
	})
	return events, err
}

// KeepAliveLease 续租，用日志里的 leaseStart 重新计算过期时间。
//
// TTL 在 Grant 时写入状态机，之后不再变化，所以续租只需要 Leader 打的时间戳：
// 过期时间同样是 leaseStart + TTL，所有节点重放同一条日志会得到相同结果。
// 租约不存在时返回 ErrLeaseNotFound：这是正常的客户端错误（租约已过期/撤销），
// 调用方应只把它返回给客户端，而不是当成状态机故障。
func (m *MVCC) KeepAliveLease(leaseID, leaseStart int64, appliedIndex uint64) error {
	return m.be.Update(func(tx backend.WriteTx) error {
		if appliedAlready(tx, appliedIndex) {
			return nil
		}

		ttl, _, ok := decodeLease(tx.Get(backend.LeaseBucket, leaseIDKey(leaseID)))
		if !ok {
			return ErrLeaseNotFound
		}

		expireTime := leaseStart + ttl
		if err := tx.Put(backend.LeaseBucket, leaseIDKey(leaseID), encodeLease(ttl, expireTime)); err != nil {
			return err
		}
		return putMetaUint64(tx, backend.ConsistentIndexKey, appliedIndex)
	})
}

// LeaseExists 判断租约是否仍然存在。
func (m *MVCC) LeaseExists(leaseID int64) (bool, error) {
	var exists bool
	err := m.be.View(func(tx backend.ReadTx) error {
		exists = tx.Get(backend.LeaseBucket, leaseIDKey(leaseID)) != nil
		return nil
	})
	return exists, err
}

// GetExpiredLeases 获取所有已过期的租约 ID
func (m *MVCC) GetExpiredLeases() ([]int64, error) {
	var expiredLeases []int64
	now := time.Now().Unix() // 在 View 事务外获取当前时间，避免事务内的时间漂移导致误判

	err := m.be.View(func(tx backend.ReadTx) error {
		c := tx.GetCursor(backend.LeaseBucket)
		if c == nil {
			return nil
		}

		for k, v := c.First(); k != nil; k, v = c.Next() {
			_, expireTime, ok := decodeLease(v)
			if !ok {
				continue // 数据损坏，跳过
			}
			if expireTime <= now {
				leaseID, err := strconv.ParseInt(string(k), 10, 64)
				if err != nil {
					continue // 数据损坏，跳过
				}
				expiredLeases = append(expiredLeases, leaseID)
			}
		}
		return nil
	})
	return expiredLeases, err
}
