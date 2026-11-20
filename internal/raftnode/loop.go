package raftnode

import (
	"context"
	"encoding/binary"
	"errors"
	"log"
	"time"

	"go.etcd.io/etcd/raft/v3"
)

// Run 启动核心循环
func (rn *RaftNode) Run(stopc <-chan struct{}, tickerInterval time.Duration) { // stopc 为只读 channel，接收停止信号
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()

	for {
		select {
		// stopc 的优先级最高，防止 rn.trans 的 inbox 关闭后疯狂从中读取到零值，导致服务阻塞
		case <-stopc: // 收到停止信号，退出循环
			rn.Node.Stop()
			return
		case <-ticker.C: // 触发一次 Tick，处理心跳和选举超时
			rn.Node.Tick()
		case m := <-rn.trans.Recv(): // 收到来自其他节点的消息，调用 Step 处理
			err := rn.Node.Step(context.Background(), m)
			if err != nil {
				log.Printf("Error stepping raft node: %v", err)
			}
		case rd := <-rn.Node.Ready(): // Raft 节点有 Ready 状态，处理 Ready
			// 更新 leaderID 字段
			if rd.SoftState != nil {
				rn.mu.Lock()
				rn.leaderID = rd.SoftState.Lead
				rn.mu.Unlock()
			}

			// 1. 优先保存快照
			if !raft.IsEmptySnap(rd.Snapshot) {
				// 存到独立的 .snap 文件
				if err := rn.storage.SaveSnapshot(rd.Snapshot); err != nil {
					log.Fatalf("save snapshot failed: %v", err)
				}
				// 让 MemoryStorage 应用快照，更新内存状态
				if err := rn.storage.ApplySnapshot(rd.Snapshot); err != nil {
					log.Fatalf("apply snapshot failed: %v", err)
				}
				// 让状态机（MVCC）从快照恢复数据
				if err := rn.applier.ApplySnapshot(rd.Snapshot); err != nil {
					log.Fatalf("apply snapshot to state machine failed: %v", err)
				}

				// 更新 RaftNode 的 lastSnapshotIndex 和 confState 字段
				rn.lastSnapshotIndex = rd.Snapshot.Metadata.Index
				rn.confState = rd.Snapshot.Metadata.ConfState
				// 状态机已经被整体替换到快照索引，appliedIndex 必须同步推进。
				// 否则落后节点收到快照后 appliedIndex 还停在旧值，而后续不会再有
				// 快照之前的 CommittedEntries，ReadIndex 就永远判定为“未追平”。
				rn.appliedIndex = max(rn.appliedIndex, rd.Snapshot.Metadata.Index)
			}

			// 2. 持久化
			err := rn.storage.Save(rd.HardState, rd.Entries)
			if err != nil {
				// WAL 持久化失败是致命错误，直接退出
				rn.storage.Close() // 先关闭 WAL 文件，释放资源
				log.Fatalf("save hard state and entries failed: %v", err)
			}

			// 3. 发送消息
			rn.trans.Send(rd.Messages)

			// 4. 应用
			if len(rd.CommittedEntries) > 0 {
				err := rn.applier.Apply(rd.CommittedEntries)
				if err != nil {
					log.Printf("Error applying committed entries: %v", err)
				}

				// 判断是否需要触发快照
				// 获取最后一条已应用日志的索引
				// 由于这里的 Raeady 中的 CommittedEntries 对于每一个节点来说数量可能不同
				// 所以可能出现多个节点之间的 lastAppliedIndex 不一致，也就是落盘的快照索引不一致的情况
				// 但是这是很正常的，而且 Raft 共识会通过日志追赶机制来保证最终一致性，所以不需要担心
				lastAppliedIndex := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
				rn.appliedIndex = lastAppliedIndex // 更新 RaftNode 的 appliedIndex 字段

				// 如果距离上一次快照的索引超过阈值，就触发快照
				if lastAppliedIndex-rn.lastSnapshotIndex >= 10 { // 先设置为 10 方便测试，实际应用中可以设置为 10000 或更大
					log.Printf(">>> 触发本地快照, applied index: %d", lastAppliedIndex)

					// a. 从状态机获取当前全量数据的字节流
					// 注意：你需要在 apply.go 里实现 GetSnapshot() 方法，返回当前状态机的全量数据的字节流
					snapData, err := rn.applier.GetSnapshot()
					if err != nil {
						log.Printf("failed to get snapshot data from applier: %v", err)
					} else {
						// b. 让 MemoryStorage 根据当前已应用日志索引、ConfState 和状态机数据生成快照对象
						// confState 使用 raftNode 的维护的 confState 字段，确保快照里记录的是最新的集群配置
						snap, err := rn.storage.CreateSnapshot(lastAppliedIndex, &rn.confState, snapData)
						if err != nil {
							log.Printf("failed to create raft snapshot: %v", err)
						} else {
							// c. 将快照序列化并写入独立的 .snap 文件
							if err := rn.storage.SaveSnapshot(snap); err != nil {
								log.Printf("failed to save raft snapshot to disk: %v", err)
							} else {
								log.Printf(">>> 快照落盘成功, index: %d", lastAppliedIndex)
								rn.lastSnapshotIndex = lastAppliedIndex

								// d. 通知 MemoryStorage 清理旧的日志，防止内存泄漏
								// 为了安全，保留最近 100 条，不要全删，防止 Raft 追赶日志时越界
								compactIndex := uint64(1) // 默认从 1 开始压缩，防止下溢
								if lastAppliedIndex > 100 {
									compactIndex = lastAppliedIndex - 100
								}

								// 忽略已经压缩过的错误
								if err := rn.storage.Compact(compactIndex); err != nil && !errors.Is(err, raft.ErrCompacted) {
									log.Printf("failed to compact logs: %v", err)
								}
							}
						}
					}
				}
			}

			// 5. 处理 ReadIndex 请求
			// 必须在本轮 CommittedEntries 应用完成之后进行：ReadState.Index 是发起读取时 Leader
			// 的提交索引，只有本地状态机追平它，此时读到的数据才满足线性一致性。
			// ReadState 只会属于本节点自己发起的读请求（其它节点的读由 Leader 直接回包）。
			rn.readIndexMu.Lock()
			for _, rs := range rd.ReadStates {
				// RequestCtx 由 ReadIndexKey 生成，长度固定；做个防御性校验，避免脏数据 panic
				if len(rs.RequestCtx) != readIndexKeyLen {
					log.Printf(">>> 忽略非法 ReadIndex RequestCtx, 长度: %d", len(rs.RequestCtx))
					continue
				}
				key := string(rs.RequestCtx)
				seq := binary.BigEndian.Uint64(rs.RequestCtx[8:])

				if rn.appliedIndex >= rs.Index {
					// 已经追平，唤醒等待方，通知上层可以安全读取最新数据
					log.Printf(">>> ReadIndex 追平, reqID: %d, applied index: %d, read index: %d", seq, rn.appliedIndex, rs.Index)
					rn.resolveReadIndexLocked(key)
				} else {
					// 还没追平，记下目标索引，等后续 Ready 里状态机追上再唤醒
					log.Printf(">>> ReadIndex 未追平, reqID: %d, applied index: %d, read index: %d", seq, rn.appliedIndex, rs.Index)
					rn.readIndexWaiters[key] = rs.Index
				}
			}
			// 复查此前挂起的 waiter：本轮应用可能刚好把它们的目标索引追平
			for key, targetIndex := range rn.readIndexWaiters {
				if rn.appliedIndex >= targetIndex {
					rn.resolveReadIndexLocked(key)
				}
			}
			rn.readIndexMu.Unlock()

			// 6. 推进
			rn.Node.Advance()
		}
	}
}
