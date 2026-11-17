package mvcc

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"mini-etcd/internal/backend"

	"log"
)

// ErrKeyNotFound 表示请求的 key 在状态机中不存在
var ErrKeyNotFound = errors.New("mvcc: key not found")

type MVCC struct {
	be *backend.Backend
}

func NewMVCC(be *backend.Backend) *MVCC {
	return &MVCC{be: be}
}

// Put 写入数据和 appliedIndex（同一事务）
func (m *MVCC) Put(key, value string, appliedIndex uint64) error {
	return m.be.Update(func(tx backend.WriteTx) error {
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
	})
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
		if valBytes == nil {
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
			if rev > maxRev { // 超出目标版本，停止，因为 bbolt 底层的 B+ tree 是按 key 升序排列的，后续的 rev 只会更大
				break
			}
			result = string(v) // 持续覆盖，最后留下的就是 <= maxRev 的最大版本
			found = true       // 标记找到了符合条件的版本
			k, v = c.Next()    // 移动到下一个符合前缀的 key
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
