package backend

import (
	"fmt"
	"log"
	"path/filepath"

	"go.etcd.io/bbolt"
)

// Bucket 名称
var (
	MetaBucket         = []byte("meta")          // 存 consistentIndex, revision 等元数据 ("RevisionKey" -> "67")
	DataBucket         = []byte("data")          // 存带 revision 的 KV 数据 ("key_RevisionKey" -> "mini-etcd")
	DataRevisionBucket = []byte("data_revision") // 存每个 key 的 revision ("key" -> "67")
)

const (
	ConsistentIndexKey = "consistentIndex" // 存储一致性索引的键名
	RevisionKey        = "revision"        // 存储全局 revision 的键名
)

// GetKeyWithRevision 生成带 revision 的 key，用于存储在 DataBucket 中
func GetKeyWithRevision(key string, revision uint64) string {
	// 使用不可见字符 \x00 作为分隔符，避免 key 和 revision 拼接后产生歧义
	return fmt.Sprintf("%s\x00%d", key, revision)
}

// Backend 封装底层的 bbolt
type Backend struct {
	db *bbolt.DB
}

// NewBackend 打开或创建数据库，并初始化 Bucket
func NewBackend(path string) (*Backend, error) {
	db, err := bbolt.Open(filepath.Join(path, "bolt.db"), 0600, nil) // 只允许当前用户读写
	if err != nil {
		return nil, err
	}

	// 初始化：确保三个 Bucket 存在
	err = db.Update(func(tx *bbolt.Tx) error {
		// 如果 Bucket 不存在就创建，存在就返回已存在的 Bucket
		if _, err := tx.CreateBucketIfNotExists(MetaBucket); err != nil {
			log.Printf("Failed to create Meta bucket: %v", err)
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(DataBucket); err != nil {
			log.Printf("Failed to create Data bucket: %v", err)
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(DataRevisionBucket); err != nil {
			log.Printf("Failed to create DataRevision bucket: %v", err)
			return err
		}
		return nil
	})
	if err != nil {
		db.Close() // 释放资源
		return nil, err
	}

	return &Backend{db: db}, nil
}

// Close 关闭数据库
func (b *Backend) Close() error {
	return b.db.Close()
}

// ReadTx 定义只读事务的方法（供 MVCC 读使用）
type ReadTx interface {
	Get(bucket, key []byte) []byte
	GetCursor(bucket []byte) *bbolt.Cursor
	GetAllFromBucket(bucket []byte) (map[string][]byte, error)
}

// WriteTx 定义读写事务的方法（供 MVCC 写使用）
type WriteTx interface {
	ReadTx // 读写事务通常也需要读
	Put(bucket, key, value []byte) error
	DeleteKeyInBucket(bucket, key []byte) error
	ClearBucket(bucket []byte) error
}

// View 开启一个只读事务
func (b *Backend) View(fn func(tx ReadTx) error) error {
	return b.db.View(func(btx *bbolt.Tx) error {
		return fn(&txWrapper{btx})
	})
}

// Update 开启一个读写事务
func (b *Backend) Update(fn func(tx WriteTx) error) error {
	return b.db.Update(func(btx *bbolt.Tx) error {
		return fn(&txWrapper{btx})
	})
}

// 内部封装，把 bbolt.Tx 包装成接口，提供 Bucket 操作
type txWrapper struct {
	tx *bbolt.Tx
}

// Get 获取指定 bucket 下的 key 的值
func (t *txWrapper) Get(bucket, key []byte) []byte {
	b := t.tx.Bucket(bucket)
	if b == nil {
		return nil
	}
	val := b.Get(key)
	if val == nil {
		return nil
	}
	// bbolt 的返回的内存只在这个事务内有效，必须拷贝一份出去
	res := make([]byte, len(val))
	copy(res, val)
	return res
}

// GetCursor 获取指定 bucket 的游标
func (t *txWrapper) GetCursor(bucket []byte) *bbolt.Cursor {
	b := t.tx.Bucket(bucket)
	if b == nil {
		return nil
	}
	return b.Cursor()
}

// GetAllFromBucket 获取指定 bucket 下的所有键值对
func (t *txWrapper) GetAllFromBucket(bucket []byte) (map[string][]byte, error) {
	b := t.tx.Bucket(bucket)
	if b == nil {
		return nil, fmt.Errorf("bucket %s not found", bucket)
	}
	result := make(map[string][]byte)
	c := b.Cursor()                                    // 获取游标
	for k, v := c.First(); k != nil; k, v = c.Next() { // 遍历所有键值对
		// 深拷贝
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		result[string(keyCopy)] = valCopy
	}
	return result, nil
}

// Put 设置指定 bucket 下的 key 的值
func (t *txWrapper) Put(bucket, key, value []byte) error {
	b := t.tx.Bucket(bucket)
	if b == nil {
		return fmt.Errorf("bucket %s not found", bucket)
	}
	return b.Put(key, value)
}

// DeleteKeyInBucket 删除指定 bucket 下的 key
func (t *txWrapper) DeleteKeyInBucket(bucket, key []byte) error {
	b := t.tx.Bucket(bucket)
	if b == nil {
		return fmt.Errorf("bucket %s not found", bucket)
	}
	return b.Delete(key)
}

// ClearBucket 清空指定 bucket 下的所有键值对
func (t *txWrapper) ClearBucket(bucket []byte) error {
	// 检查 bucket 是否存在
	b := t.tx.Bucket(bucket)
	if b == nil {
		return fmt.Errorf("bucket %s not found", bucket)
	}
	// bbolt 没有直接清空 bucket 的方法，只能删除再创建
	if err := t.tx.DeleteBucket(bucket); err != nil {
		return err
	}
	_, err := t.tx.CreateBucket(bucket)
	return err
}
