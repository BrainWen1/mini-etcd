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

type WatchNotifier interface {
	NotifyWatcher(key string, event mvcc.Event)
}

// Applier 定义了应用已提交日志条目的接口
type Applier struct {
	mvccStorage *mvcc.MVCC    // 状态机的持久化存储
	waiters     sync.Map      // 用于等待日志应用完成的同步机制，key 是 requestID，value 是 chan error
	notifier    WatchNotifier // 用于通知 Server 层的 Hub，触发 watch channel 的事件分发
}

func NewApplier(mvccStorage *mvcc.MVCC) *Applier {
	return &Applier{
		mvccStorage: mvccStorage,
	}
}

// SetNotifier 设置通知器
func (a *Applier) SetNotifier(notifier WatchNotifier) {
	a.notifier = notifier
}

// notifyWaiter 唤醒等待该请求的客户端（如果客户端还没超时离开）
func (a *Applier) notifyWaiter(requestID string, res types.Result) {
	if requestID == "" {
		return
	}
	// LoadAndDelete 是原子操作，避免和 RemoveWaiter 竞争时重复通知
	if ch, ok := a.waiters.LoadAndDelete(requestID); ok {
		ch.(chan types.Result) <- res // channel 带 1 个缓冲，不会阻塞
	}
}

// RegisterWaiter 注册等待者并返回 channel，供 Server 调用
func (a *Applier) RegisterWaiter(requestID string) chan types.Result {
	ch := make(chan types.Result, 1)
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
		batch            []mvcc.Entry              // 本批待应用的日志
		indexToRequestID = make(map[uint64]string) // 本批日志索引 -> requestID，提交成功/失败后据此通知客户端
	)

	// flushBatch 把当前累积的 KV 日志合并成一批提交，并在整批落盘成功后才通知
	// 客户端与 Watch 监听器。租约命令会直接修改状态机，所以它到来之前必须先
	// 把前面的 KV 日志刷出去，保证日志的先后顺序。
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		events, txnResults, err := a.mvccStorage.ApplyBatch(batch)
		if err != nil {
			// 整批已经回滚，这一批里的客户端一个都不能收到成功
			for _, reqID := range indexToRequestID {
				a.notifyWaiter(reqID, types.Result{Err: err})
			}
			log.Printf("[apply] 应用日志失败, 索引: %d-%d, 错误: %v\n", batch[0].Index, batch[len(batch)-1].Index, err)
			return err
		}

		// 整批落盘成功之后再通知客户端。txnResults 记录每条日志实际执行的分支：
		// TXN 命中所有 compares 时为 true（分布式锁靠它判断是否抢到锁），
		// 走 failure 分支时为 false；PUT/DELETE 恒为 false。
		for idx, reqID := range indexToRequestID {
			a.notifyWaiter(reqID, types.Result{Succeeded: txnResults[idx]})
		}

		// 成功应用一批日志后，通知所有 watch channel
		for _, event := range events {
			if a.notifier != nil {
				a.notifier.NotifyWatcher(event.Key, event)
			}
		}
		log.Printf("[apply] 已应用 %d 条日志, 索引: %d-%d\n", len(batch), batch[0].Index, batch[len(batch)-1].Index)

		batch = nil
		// 索引到 requestID 的映射只属于这一批：下一次 flush 是另一批日志，
		// 残留旧条目会让已经通知过的请求被重复处理。
		indexToRequestID = make(map[uint64]string)
		return nil
	}

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
				a.notifyWaiter(cmdType.RequestID, types.Result{Err: fmt.Errorf("无法解析的 PUT 命令")})
				continue
			}
			batch = append(batch, mvcc.Entry{Index: entry.Index, Op: "PUT", Key: cmd.Key, Value: cmd.Value, LeaseID: cmd.LeaseID})
			indexToRequestID[entry.Index] = cmd.RequestID
		case "DELETE": // 处理 DELETE 命令
			cmd, ok := types.DecodeCommand(entry.Data)
			if !ok {
				log.Printf("[apply] 无法解析的 DELETE 命令, 索引: %d, 数据: %q\n", entry.Index, string(entry.Data))
				a.notifyWaiter(cmdType.RequestID, types.Result{Err: fmt.Errorf("无法解析的 DELETE 命令")})
				continue
			}
			batch = append(batch, mvcc.Entry{Index: entry.Index, Op: "DELETE", Key: cmd.Key, LeaseID: cmd.LeaseID})
			indexToRequestID[entry.Index] = cmd.RequestID
		case "TXN": // 处理事务命令
			txnCmd, err := types.DecodeTxnCommand(entry.Data)
			if err != nil {
				// 命令已经进了日志，无法回滚；至少要唤醒等待者，避免客户端干等到超时。
				// 与未知命令一致：跳过这条日志，后续写入会推进 consistentIndex。
				// 注意必须用信封里的 RequestID：解析失败时 txnCmd 是零值，RequestID 为空，
				// notifyWaiter 会直接返回，客户端只能等到 5 秒超时。
				log.Printf("[apply] 解析事务命令失败, 索引: %d, request ID: %s, 错误: %v\n", entry.Index, cmdType.RequestID, err)
				a.notifyWaiter(cmdType.RequestID, types.Result{Err: fmt.Errorf("解析事务命令失败: %w", err)})
				continue
			}
			// 在进入批次之前校验：非法事务只跳过这一条，而不是让整批回滚。
			// 校验结果是确定性的，所有节点都会跳过同一条日志，不会造成状态机分歧；
			// 正常路径下 Server 已经校验过，这里是对损坏日志的防御。
			if err := types.ValidateTxnCommand(txnCmd); err != nil {
				log.Printf("[apply] 非法事务, 索引: %d, request ID: %s, 错误: %v\n", entry.Index, txnCmd.RequestID, err)
				a.notifyWaiter(txnCmd.RequestID, types.Result{Err: err})
				continue
			}
			batch = append(batch, mvcc.Entry{Index: entry.Index, Op: "TXN", Txn: txnCmd})
			indexToRequestID[entry.Index] = txnCmd.RequestID
		// 处理租约命令
		case types.OpLeaseGrant, types.OpLeaseRevoke, types.OpLeaseKeepAlive:
			// 租约命令立即修改状态机，而 PUT/DELETE/TXN 是攒成一批最后统一提交的。
			// 这里必须先刷出已累积的 KV 日志，否则同一批 Ready 里的
			// [PUT k lease=1, LEASE_REVOKE 1] 会先执行 Revoke 再执行 Put，
			// 结果 key 绑定到一个已经不存在的租约上，永远不会被回收。
			if err := flushBatch(); err != nil {
				return err
			}

			var leaseCmd types.LeaseCommand
			leaseCmd, err := types.DecodeLeaseCommand(entry.Data)
			if err != nil {
				log.Printf("[apply] 解析租约命令失败, 索引: %d, 错误: %v\n", entry.Index, err)
				a.notifyWaiter(cmdType.RequestID, types.Result{Err: fmt.Errorf("解析租约命令失败: %w", err)})
				continue
			}
			// 语义校验放在应用之前：结果是确定性的（所有节点看同一条日志），
			// 非法命令只跳过这一条，而不是让状态机写出错误的过期时间。
			if err := types.ValidateLeaseCommand(leaseCmd); err != nil {
				log.Printf("[apply] 非法租约命令, 索引: %d, request ID: %s, 错误: %v\n", entry.Index, leaseCmd.RequestID, err)
				a.notifyWaiter(leaseCmd.RequestID, types.Result{Err: err})
				continue
			}

			switch leaseCmd.Op {
			case types.OpLeaseGrant:
				err := a.mvccStorage.GrantLease(leaseCmd.LeaseID, leaseCmd.TTL, leaseCmd.LeaseStart, entry.Index)
				a.notifyWaiter(leaseCmd.RequestID, types.Result{Err: err})
				if err != nil {
					// Grant 失败直接返回，后续的日志也不应用了。因为 LeaseID 是全局唯一的，如果 Grant 失败，
					// 后续的日志里可能有绑定这个 LeaseID 的 PUT/DELETE，应用这些日志会导致状态机分歧。
					return err
				}
				// 租约命令各自开启独立事务并立即拿到结果，已经在上面的 notifyWaiter 通知过了，
				// 不需要再进入 indexToRequestID 等待批次提交。

			case types.OpLeaseRevoke:
				eventsLease, err := a.mvccStorage.RevokeLease(leaseCmd.LeaseID, entry.Index)
				a.notifyWaiter(leaseCmd.RequestID, types.Result{Err: err})
				if err != nil {
					// Revoke 失败也直接返回
					return err
				}

				// 租约过期会删除绑定的 Key，需要把这些 DELETE 事件推给 Watch
				if a.notifier != nil {
					for _, ev := range eventsLease {
						a.notifier.NotifyWatcher(ev.Key, ev)
					}
				}
			case types.OpLeaseKeepAlive:
				err := a.mvccStorage.KeepAliveLease(leaseCmd.LeaseID, leaseCmd.LeaseStart, entry.Index)
				a.notifyWaiter(leaseCmd.RequestID, types.Result{Err: err})
				// 租约不存在是正常的客户端错误（已过期/已撤销），只返回给客户端。
				// 不能把整个 Apply 判为失败：Raft 循环会在 Apply 返回错误时
				// log.Fatalf 退出进程，一个过期的 keepalive 就能打挂整个节点。
				if err != nil && !errors.Is(err, mvcc.ErrLeaseNotFound) {
					return err
				}
			}
		default:
			// 未知命令也要唤醒客户端，否则客户端只能等到 5 秒超时
			err := fmt.Errorf("unknown command: %s", cmdType.Op)
			a.notifyWaiter(cmdType.RequestID, types.Result{Err: err})
			log.Printf("[apply] 未知命令: %s, request ID: %s\n", cmdType.Op, cmdType.RequestID)
		}
	}

	// 批量提交剩余的 KV 日志
	return flushBatch()
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
// 落后节点只靠快照追赶时无法从快照元数据里恢复 Raft/gRPC 地址。
func (a *Applier) UpsertMember(id uint64, raftAddr, grpcAddr string) (config.Member, error) {
	return a.mvccStorage.UpsertMember(id, raftAddr, grpcAddr)
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

// GetExpiredLeases 扫描状态机里过期的 LeaseID
func (a *Applier) GetExpiredLeases() ([]int64, error) {
	return a.mvccStorage.GetExpiredLeases()
}
