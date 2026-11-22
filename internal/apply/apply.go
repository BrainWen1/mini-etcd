package apply

import (
	"errors"
	"fmt"
	"log"
	"mini-etcd/internal/config"
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

// Apply 应用已提交的日志条目到状态机。
//
// 本批所有可应用的日志会合并进同一个 bbolt 事务（见 MVCC.ApplyBatch），
// 一批日志只落盘一次，而不是每条日志一次 fsync。
// 副作用是通知客户端必须推迟到整批提交成功之后：否则提交失败时客户端已经收到了成功。
func (a *Applier) Apply(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	var (
		batch   []mvcc.Entry // 本批待应用的日志
		waiters []string     // 本批提交成功后才通知的 requestID
	)

	for _, entry := range entries {
		// 先解析命令信封（request_id + op），再在 switch 里调用各自具体的 Decode：
		// PUT/DELETE 与 TXN 的命令结构不同，不能用同一个 Decode 解析。
		cmdType, ok := types.DecodeCommandType(entry.Data)
		if !ok {
			log.Printf("[apply] 无法解析的日志索引: %d, 数据: %q\n", entry.Index, string(entry.Data))
			continue
		}

		switch cmdType.Op {
		case "PUT": // 处理 PUT 命令
			cmd, ok := types.DecodeCommand(entry.Data)
			if !ok {
				log.Printf("[apply] 无法解析的 PUT 命令, 索引: %d, 数据: %q\n", entry.Index, string(entry.Data))
				a.notifyWaiter(cmdType.RequestID, fmt.Errorf("无法解析的 PUT 命令"))
				continue
			}
			batch = append(batch, mvcc.Entry{Index: entry.Index, Op: "PUT", Key: cmd.Key, Value: cmd.Value})
			waiters = append(waiters, cmd.RequestID)
		case "DELETE": // 处理 DELETE 命令
			cmd, ok := types.DecodeCommand(entry.Data)
			if !ok {
				log.Printf("[apply] 无法解析的 DELETE 命令, 索引: %d, 数据: %q\n", entry.Index, string(entry.Data))
				a.notifyWaiter(cmdType.RequestID, fmt.Errorf("无法解析的 DELETE 命令"))
				continue
			}
			batch = append(batch, mvcc.Entry{Index: entry.Index, Op: "DELETE", Key: cmd.Key})
			waiters = append(waiters, cmd.RequestID)
		case "TXN": // 处理事务命令
			txnCmd, err := types.DecodeTxnCommand(entry.Data)
			if err != nil {
				// 命令已经进了日志，无法回滚；至少要唤醒等待者，避免客户端干等到超时。
				// 与未知命令一致：跳过这条日志，后续写入会推进 consistentIndex。
				// 注意必须用信封里的 RequestID：解析失败时 txnCmd 是零值，RequestID 为空，
				// notifyWaiter 会直接返回，客户端只能等到 5 秒超时。
				log.Printf("[apply] 解析事务命令失败, 索引: %d, request ID: %s, 错误: %v\n", entry.Index, cmdType.RequestID, err)
				a.notifyWaiter(cmdType.RequestID, err)
				continue
			}
			// 在进入批次之前校验：非法事务只跳过这一条，而不是让整批回滚。
			// 校验结果是确定性的，所有节点都会跳过同一条日志，不会造成状态机分歧；
			// 正常路径下 Server 已经校验过，这里是对损坏日志的防御。
			if err := types.ValidateTxnCommand(txnCmd); err != nil {
				log.Printf("[apply] 非法事务, 索引: %d, request ID: %s, 错误: %v\n", entry.Index, txnCmd.RequestID, err)
				a.notifyWaiter(txnCmd.RequestID, err)
				continue
			}
			batch = append(batch, mvcc.Entry{Index: entry.Index, Op: "TXN", Txn: txnCmd})
			waiters = append(waiters, txnCmd.RequestID)
		default:
			// 未知命令也要唤醒客户端，否则客户端只能等到 5 秒超时
			err := fmt.Errorf("unknown command: %s", cmdType.Op)
			a.notifyWaiter(cmdType.RequestID, err)
			log.Printf("[apply] 未知命令: %s, request ID: %s\n", cmdType.Op, cmdType.RequestID)
		}
	}

	if len(batch) > 0 {
		if err := a.mvccStorage.ApplyBatch(batch); err != nil {
			// 整批已经回滚，这一批里的客户端一个都不能收到成功
			for _, id := range waiters {
				a.notifyWaiter(id, err)
			}
			log.Printf("[apply] 应用日志失败, 索引: %d-%d, 错误: %v\n", batch[0].Index, batch[len(batch)-1].Index, err)
			return err
		}
		log.Printf("[apply] 已应用 %d 条日志, 索引: %d-%d\n", len(batch), batch[0].Index, batch[len(batch)-1].Index)
	}

	// 整批提交成功之后才通知客户端
	for _, id := range waiters {
		a.notifyWaiter(id, nil)
	}
	return nil
}

// GetConsistentIndex 获取当前已应用的最高日志索引
func (a *Applier) GetConsistentIndex() (uint64, error) {
	return a.mvccStorage.GetConsistentIndex()
}

// AdvanceConsistentIndex 把状态机的已应用索引推进到 index。
// 空日志和配置变更没有业务数据，但必须计入索引，否则快照索引会超过状态机索引，
// 重启时 raft 的 appliedTo 会 panic（详见 MVCC.AdvanceConsistentIndex）。
func (a *Applier) AdvanceConsistentIndex(index uint64) error {
	return a.mvccStorage.AdvanceConsistentIndex(index)
}

// UpsertMember 把成员地址写入状态机。
// 成员表放在状态机里是为了随快照一起复制：ConfState 只有节点 ID，
// 落后节点只靠快照追赶时无法从快照元数据里恢复 Raft/HTTP 地址。
func (a *Applier) UpsertMember(id uint64, raftAddr, httpAddr string) (config.Member, error) {
	return a.mvccStorage.UpsertMember(id, raftAddr, httpAddr)
}

// RemoveMember 从状态机中删除成员。
func (a *Applier) RemoveMember(id uint64) error {
	return a.mvccStorage.RemoveMember(id)
}

// GetMembers 读取状态机中的全量成员表（快照恢复后用它重建地址表）。
func (a *Applier) GetMembers() (map[uint64]config.Member, error) {
	return a.mvccStorage.GetMembers()
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
