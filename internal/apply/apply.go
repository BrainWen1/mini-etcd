package apply

import (
	"errors"
	"fmt"
	"log"
	"mini-etcd/internal/mvcc"
	"mini-etcd/internal/types"
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

// notifyWaiter 唤醒等待该请求的客户端（如果客户端还没超时离开）
func (a *Applier) notifyWaiter(requestID string, err error) {
	if requestID == "" {
		return
	}
	// LoadAndDelete 是原子操作，避免和 RemoveWaiter 竞争时重复通知
	if ch, ok := a.waiters.LoadAndDelete(requestID); ok {
		ch.(chan error) <- err // channel 带 1 个缓冲，不会阻塞
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

		// 先解析命令信封（request_id + op），再在 switch 里调用各自具体的 Decode：
		// PUT/DELETE 与 TXN 的命令结构不同，不能用同一个 Decode 解析。
		cmdType, ok := types.DecodeCommandType(entry.Data)
		if !ok {
			log.Printf("[apply] 无法解析的日志索引: %d, 数据: %q\n", entry.Index, string(entry.Data))
			continue
		}
		log.Printf("[apply] 日志索引: %d, 操作: %s\n", entry.Index, cmdType.Op)

		switch cmdType.Op {
		case "PUT": // 处理 PUT 命令
			cmd, ok := types.DecodeCommand(entry.Data)
			if !ok {
				log.Printf("[apply] 无法解析的 PUT 命令, 索引: %d, 数据: %q\n", entry.Index, string(entry.Data))
				a.notifyWaiter(cmdType.RequestID, fmt.Errorf("无法解析的 PUT 命令"))
				continue
			}
			// 写状态机失败不能吞掉：否则可能日志里显示成功应用，但数据实际没落盘
			err := a.mvccStorage.Put(cmd.Key, cmd.Value, entry.Index)
			// 无论成功还是失败，都要通知等待的客户端
			a.notifyWaiter(cmd.RequestID, err)
			if err != nil {
				log.Printf("[apply] 应用数据失败: %s -> %s, request ID: %s, 错误: %v\n", cmd.Key, cmd.Value, cmd.RequestID, err)
				return err
			}
			log.Printf("[apply] 成功应用数据: %s -> %s, request ID: %s\n", cmd.Key, cmd.Value, cmd.RequestID)
		case "DELETE": // 处理 DELETE 命令
			cmd, ok := types.DecodeCommand(entry.Data)
			if !ok {
				log.Printf("[apply] 无法解析的 DELETE 命令, 索引: %d, 数据: %q\n", entry.Index, string(entry.Data))
				a.notifyWaiter(cmdType.RequestID, fmt.Errorf("无法解析的 DELETE 命令"))
				continue
			}
			err := a.mvccStorage.Delete(cmd.Key, entry.Index)
			a.notifyWaiter(cmd.RequestID, err)
			if err != nil {
				log.Printf("[apply] 删除数据失败: %s, request ID: %s, 错误: %v\n", cmd.Key, cmd.RequestID, err)
				return err
			}
			log.Printf("[apply] 成功删除数据: %s, request ID: %s\n", cmd.Key, cmd.RequestID)
		case "TXN": // 处理事务命令
			txnCmd, err := types.DecodeTxnCommand(entry.Data)
			if err != nil {
				// 命令已经进了日志，无法回滚；至少要唤醒等待者，避免客户端干等到超时。
				// 不返回错误：与未知命令一致，跳过这条日志，后续写入会推进 consistentIndex。
				// 注意必须用信封里的 RequestID：解析失败时 txnCmd 是零值，RequestID 为空，
				// notifyWaiter 会直接返回，客户端只能等到 5 秒超时。
				log.Printf("[apply] 解析事务命令失败, 索引: %d, request ID: %s, 错误: %v\n", entry.Index, cmdType.RequestID, err)
				a.notifyWaiter(cmdType.RequestID, err)
				continue
			}

			err = a.mvccStorage.Txn(txnCmd, entry.Index)
			a.notifyWaiter(txnCmd.RequestID, err)
			if err != nil {
				log.Printf("[apply] 应用事务失败, 索引: %d, request ID: %s, 错误: %v\n", entry.Index, txnCmd.RequestID, err)
				return err
			}
			log.Printf("[apply] 成功应用事务, 索引: %d, request ID: %s, compares: %d, success: %d, failure: %d\n",
				entry.Index, txnCmd.RequestID, len(txnCmd.Compares), len(txnCmd.Success), len(txnCmd.Failure))
		default:
			// 未知命令也要唤醒客户端，否则客户端只能等到 5 秒超时
			err := fmt.Errorf("unknown command: %s", cmdType.Op)
			a.notifyWaiter(cmdType.RequestID, err)
			log.Printf("[apply] 未知命令: %s, request ID: %s\n", cmdType.Op, cmdType.RequestID)
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
