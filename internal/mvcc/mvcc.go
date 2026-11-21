package mvcc

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"mini-etcd/internal/backend"
	"mini-etcd/internal/types"
	"time"

	"log"
)

// ErrKeyNotFound 表示请求的 key 在状态机中不存在
var ErrKeyNotFound = errors.New("mvcc: key not found")

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
		return applyPut(tx, key, value, appliedIndex)
	})
}

// Entry 是一条已完成解码、待应用到状态机的日志。
// Op 取值为 "PUT"、"DELETE" 或 "TXN"：PUT/DELETE 使用 Key/Value，TXN 使用 Txn。
type Entry struct {
	Index uint64
	Op    string
	Key   string
	Value string
	Txn   types.TxnCommand // 可选字段：仅当 Op="TXN" 时使用，表示事务命令
}

// ApplyBatch 在同一个 bbolt 事务里按顺序应用一批已提交日志。
//
// 每条日志单独开启一次写事务意味着一批日志要付出同样多次 fsync，批量应用把整批
// 日志的落盘成本压缩成一次。整批要么全部生效、要么全部回滚，因此调用方必须等
// 本方法成功返回之后才能通知客户端写入成功。
func (m *MVCC) ApplyBatch(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	return m.be.Update(func(tx backend.WriteTx) error { // 在同一个 bbolt 事务里应用整批日志
		for _, e := range entries {
			var err error
			switch e.Op {
			case "PUT":
				err = applyPut(tx, e.Key, e.Value, e.Index)
			case "DELETE":
				err = applyPut(tx, e.Key, string(TombstoneValue), e.Index)
			case "TXN":
				err = applyTxn(tx, e.Txn, e.Index)
			default:
				err = fmt.Errorf("mvcc: unsupported entry op %q", e.Op)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// applyPut 在给定事务内写入一条 KV，并推进 consistentIndex
func applyPut(tx backend.WriteTx, key, value string, appliedIndex uint64) error {
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
		return nil
	}

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
		return err
	}

	// 3. 写入 Data 桶，key + revision 作为唯一标识，保证同一个 key 的不同版本可以共存
	keyBytes := backend.GetKeyWithRevision(key, currentRev)
	if err := tx.Put(backend.DataBucket, keyBytes, []byte(value)); err != nil {
		log.Printf("Failed to put key %s: %v", key, err)
		return err
	}

	// 4. 更新 DataRevision 桶，记录每个 key 的最新 revision，方便 Get 时获取最新值
	if err := tx.Put(backend.DataRevisionBucket, []byte(key), revBytes); err != nil {
		log.Printf("Failed to update DataRevision for key %s: %v", key, err)
		return err
	}

	// 5. 把 uint64 的 appliedIndex 转成 8 字节的 []byte，和数据在同一事务里更新，
	//    保证 [数据 + 索引] 原子落盘，不会出现数据写了但索引没写的情况
	idxBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idxBytes, appliedIndex)
	if err := tx.Put(backend.MetaBucket, []byte(backend.ConsistentIndexKey), idxBytes); err != nil {
		log.Printf("Failed to update consistent index: %v", err)
		return err
	}

	return nil
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

// 定义快照的数据结构
type MVCCSnapshot struct {
	Data map[string][]byte `json:"data"`
	Meta map[string][]byte `json:"meta"`
}

// snapshotMagic 标识当前快照使用 gob 编码。
//
// 早期版本直接用 encoding/json 序列化 MVCCSnapshot，但 Data 桶的 key 是
// "用户 key + \x00 + 8 字节二进制 revision"，其中 revision 的字节很可能不是
// 合法 UTF-8（例如 revision=128 时最后一位是 0x80）。encoding/json 在序列化
// 字符串时会把非法 UTF-8 字节替换成 U+FFFD（EF BF BD），于是快照恢复后 key
// 被改写，数据永久丢失。gob 是二进制安全的，可以原样保留任意字节。
var snapshotMagic = []byte("MVC2")

// decodeSnapshot 解析快照数据，兼容旧版本的 JSON 格式。
func decodeSnapshot(data []byte) (*MVCCSnapshot, error) {
	var snap MVCCSnapshot
	if bytes.HasPrefix(data, snapshotMagic) {
		if err := gob.NewDecoder(bytes.NewReader(data[len(snapshotMagic):])).Decode(&snap); err != nil {
			return nil, err
		}
		return &snap, nil
	}
	// 旧版快照是裸 JSON（没有 magic 前缀）
	if err := json.Unmarshal(data, &snap); err != nil {
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

		// 4. 根据 Data 桶重建 DataRevision 桶。
		//    DataRevision 没有存进快照，如果这里不重建，新节点/重启节点恢复快照后
		//    Get(key) 会因为查不到最新 revision 而全部返回 ErrKeyNotFound。
		return rebuildDataRevision(tx)
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
		return applyTxn(tx, txnCmd, appliedIndex)
	})
}

// applyTxn 在给定事务内执行一条事务日志
func applyTxn(tx backend.WriteTx, txnCmd types.TxnCommand, appliedIndex uint64) error {
	// 1. 幂等检查（与 Put 相同）：重放旧日志时整条事务都必须跳过
	var currentIdx uint64 = 0
	currentIdxBytes := tx.Get(backend.MetaBucket, []byte(backend.ConsistentIndexKey))
	if len(currentIdxBytes) == 8 {
		currentIdx = binary.BigEndian.Uint64(currentIdxBytes)
	}
	if appliedIndex <= currentIdx {
		return nil // 重复应用，直接跳过
	}

	// 2. 评估所有 If 条件（多个条件之间是 AND）。
	//    key 不存在时按空串比较，与 etcd 语义一致。
	allMatched := true
	for _, cmp := range txnCmd.Compares {
		actualVal, _ := getTxValue(tx, cmp.Key)
		matched, err := cmp.Match(actualVal)
		if err != nil {
			return fmt.Errorf("txn compare %q: %w", cmp.Key, err)
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

	for _, op := range ops {
		var valToWrite []byte
		switch op.Op {
		case "PUT":
			valToWrite = []byte(op.Value)
		case "DELETE":
			valToWrite = TombstoneValue
		default:
			// 正常路径下 Server 已经校验过；这里兜底，避免把非法操作当成空值写入。
			return fmt.Errorf("txn: unsupported operation %q", op.Op)
		}

		currentRev++ // 事务内的每个写操作，Revision 都加 1

		// 写入 DataBucket
		keyWithRev := backend.GetKeyWithRevision(op.Key, currentRev)
		if err := tx.Put(backend.DataBucket, keyWithRev, valToWrite); err != nil {
			return err
		}

		// 更新 DataRevisionBucket
		opRevBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(opRevBytes, currentRev)
		if err := tx.Put(backend.DataRevisionBucket, []byte(op.Key), opRevBytes); err != nil {
			return err
		}
	}

	// 5. 更新全局 Revision 和 AppliedIndex（同一事务，保证原子落盘）
	if err := putMetaUint64(tx, backend.RevisionKey, currentRev); err != nil {
		return err
	}
	return putMetaUint64(tx, backend.ConsistentIndexKey, appliedIndex)
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
