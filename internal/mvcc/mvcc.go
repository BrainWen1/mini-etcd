package mvcc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
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
		currentIdxBytes := tx.Get(backend.MetaBucket, []byte(backend.ConsistentIndexKey))
		if len(currentIdxBytes) >= 8 { // 校验长度，避免 panic
			currentIdx := binary.BigEndian.Uint64(currentIdxBytes)
			if appliedIndex <= currentIdx {
				log.Printf("[mvcc] Put: appliedIndex %d <= current consistentIndex %d, skipping duplicate entry\n", appliedIndex, currentIdx)
				return nil
			}
		}

		// 2. 写业务数据
		if err := tx.Put(backend.DataBucket, []byte(key), []byte(value)); err != nil {
			log.Printf("Failed to put key %s: %v", key, err)
			return err
		}

		// 3. 把 uint64 的 appliedIndex 转成 8 字节的 []byte，和数据在同一事务里更新，
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

// Get 读取普通 KV 数据，key 不存在时返回 ErrKeyNotFound
func (m *MVCC) Get(key string) (string, error) {
	var (
		result string
		found  bool
	)
	err := m.be.View(func(tx backend.ReadTx) error {
		val := tx.Get(backend.DataBucket, []byte(key))
		if val != nil {
			result = string(val)
			found = true
		}
		return nil
	})
	if err != nil {
		log.Printf("Failed to get key %s: %v", key, err)
		return "", err
	}
	if !found {
		return "", ErrKeyNotFound
	}
	return result, nil
}

// GetConsistentIndex 读取当前已应用的最高日志索引（供 main 启动时恢复 Raft 状态用）
func (m *MVCC) GetConsistentIndex() (uint64, error) {
	var idx uint64
	err := m.be.View(func(tx backend.ReadTx) error {
		val := tx.Get(backend.MetaBucket, []byte(backend.ConsistentIndexKey))
		if val != nil {
			idx = binary.BigEndian.Uint64(val)
		}
		return nil
	})
	if err != nil {
		log.Printf("Failed to get consistent index: %v", err)
	}
	return idx, err
}

// 定义快照的数据结构
type MVCCSnapshot struct {
	Data map[string][]byte `json:"data"`
	Meta map[string][]byte `json:"meta"`
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
	return json.Marshal(snap) // 这里为了简单，直接用 JSON 序列化，后续可以考虑更高效的序列化方式，比如 Protobuf
}

// ApplySnapshot 从快照数据恢复状态机
func (m *MVCC) ApplySnapshot(data []byte) error {
	var snap MVCCSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		log.Printf("Failed to unmarshal snapshot data: %v", err)
		return err
	}

	// 开启底层写事务，清空旧数据，写入新数据
	return m.be.Update(func(tx backend.WriteTx) error {
		// 1. 清空现有的桶
		if err := tx.ClearBucket(backend.DataBucket); err != nil {
			log.Printf("Failed to clear Data bucket: %v", err)
			return err
		}
		if err := tx.ClearBucket(backend.MetaBucket); err != nil {
			log.Printf("Failed to clear Meta bucket: %v", err)
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
		return nil
	})
}
