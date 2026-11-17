package apply

import (
	"errors"
	"log"
	"mini-etcd/internal/mvcc"
	"strings"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Applier 定义了应用已提交日志条目的接口
type Applier struct {
	mvccStorage *mvcc.MVCC // 状态机的持久化存储
	waiters     sync.Map   // 用于等待日志应用完成的同步机制，key 是 requestID，value 是 chan error
}

func NewApplier(mvccStorage *mvcc.MVCC) *Applier {
	return &Applier{
		mvccStorage: mvccStorage,
	}
}

// RegisterWaiter 注册等待者并返回 channel，供 Server 调用
func (a *Applier) RegisterWaiter(requestID string) chan error {
	ch := make(chan error, 1)
	a.waiters.Store(requestID, ch)
	return ch
}

// RemoveWaiter 清理超时的等待者，防止内存泄漏
func (a *Applier) RemoveWaiter(requestID string) {
	a.waiters.Delete(requestID)
}

// Apply 应用已提交的日志条目到状态机
func (a *Applier) Apply(entries []raftpb.Entry) error {
	for _, entry := range entries {
		// 忽略空日志和配置变更日志，只处理普通业务日志
		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}

		// 解析数据 (简单粗暴地用空格分割)
		cmd := string(entry.Data)
		log.Printf("[apply] 日志索引: %d, 命令: %s\n", entry.Index, cmd)
		parts := strings.Split(cmd, " ")

		if len(parts) == 4 {
			requestID := parts[0]

			switch parts[1] {
			case "PUT": // 处理 PUT 命令
				key, val := parts[2], parts[3]
				// 写状态机失败不能吞掉：否则可能日志里显示成功应用，但数据实际没落盘
				err := a.mvccStorage.Put(key, val, entry.Index)
				// 无论成功还是失败，都要通知等待的客户端
				if ch, ok := a.waiters.Load(requestID); ok {
					ch.(chan error) <- err      // 发送结果
					a.waiters.Delete(requestID) // 通知完就删掉
				}
				if err != nil {
					log.Printf("[apply] 应用数据失败: %s -> %s, request ID: %s, 错误: %v\n", key, val, requestID, err)
					return err
				}
				log.Printf("[apply] 成功应用数据: %s -> %s, request ID: %s\n", key, val, requestID)
			default:
				log.Printf("[apply] 未知命令: %s\n", cmd)
			}
		}
	}
	return nil
}

// GetConsistentIndex 获取当前已应用的最高日志索引
func (a *Applier) GetConsistentIndex() (uint64, error) {
	return a.mvccStorage.GetConsistentIndex()
}

// ApplySnapshot 应用快照到状态机
func (a *Applier) ApplySnapshot(snapshot raftpb.Snapshot) error {
	return a.mvccStorage.ApplySnapshot(snapshot.Data)
}

// GetSnapshot 获取状态机的快照数据
func (a *Applier) GetSnapshot() ([]byte, error) {
	return a.mvccStorage.GetSnapshot()
}

// Get 获取状态机中指定 key 的值，第二个返回值表示 key 是否存在
func (a *Applier) Get(key string) (string, bool) {
	val, err := a.mvccStorage.Get(key)
	if err != nil {
		// key 不存在是正常情况，不当作错误打印
		if !errors.Is(err, mvcc.ErrKeyNotFound) {
			log.Printf("Error getting key %s: %v", key, err)
		}
		return "", false
	}
	return val, true
}
