package raftnode

import (
	"context"
	"errors"
	"log"
	"mini-etcd/internal/config"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	// 触发快照的阈值：距离上一次快照超过 snapshotThreshold 条日志就触发快照
	snapshotThreshold = 10
)

// Run 启动核心循环
func (rn *RaftNode) Run(stopc <-chan struct{}, tickerInterval time.Duration) { // stopc 为只读 channel，接收停止信号
	defer close(rn.stopc) // 退出时唤醒所有还在等待 ReadIndex 的请求

	// 读请求聚合循环：把并发 GET 合并成一次 Raft ReadIndex，避免每个读都触发一轮心跳广播
	go rn.readIndexLoop()

	// 过期 Lease 扫描循环：定期扫描状态机里过期的 LeaseID，并向 Raft 提交 Revoke 命令
	go rn.leaseExpiryLoop(stopc)

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
				// 快照是状态机的完整替代品。空 Data 只可能来自 bootstrap 时手写的
				// 占位快照（它的 Index=1、状态机本来就是空的），正常情况下不会被发送；
				// 如果真的收到，说明 Leader 端快照数据损坏。此时继续执行会用空数据
				// 覆盖本地状态机，宁可直接终止并保留现场。
				if len(rd.Snapshot.Data) == 0 {
					rn.storage.Close()
					log.Fatalf("[Node %d] 收到空数据快照 (index=%d)，拒绝覆盖本地状态机",
						rn.nodeID, rd.Snapshot.Metadata.Index)
				}

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
				// 快照里只有节点 ID（ConfState），成员地址在状态机的成员表里。
				// 必须在 ApplySnapshot 之后立刻同步：落后节点可能错过了新成员的
				// ConfChange 日志（已被 Leader 压缩），只能靠这一步补齐 Transport
				// 的 peer 地址和 Leader 转发表。否则它对 Leader 的 ReadIndex
				// 发不出去、写请求也转发不到新节点。
				rn.syncMembershipFromSnapshot(rd.Snapshot.Metadata.ConfState)

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
				var normalEntries []raftpb.Entry
				// 本批是否应用了配置变更。配置变更会改变 ConfState，必须让内存快照
				// 尽快包含新的成员配置：新节点加入时可能一步都追不上日志，
				// Leader 只能给它发快照；如果快照的 ConfState 里没有这个新节点，
				// raft.restore 会直接拒绝（"it is not in the ConfState"），
				// 新节点将永远停在同步阶段。
				membershipChanged := false

				for _, entry := range rd.CommittedEntries {
					switch entry.Type {
					case raftpb.EntryConfChange:
						// a. 解析变更指令
						var cc raftpb.ConfChange
						if err := cc.Unmarshal(entry.Data); err != nil {
							// 解析失败的条目不能交给 ApplyConfChange：零值 cc 会被当成
							// “向 NodeID=0 添加节点”，污染本地 Progress 表。
							log.Printf("[Node %d] 解析配置变更失败, index: %d, err: %v", rn.nodeID, entry.Index, err)
							continue
						}

						// b. 通知 Raft 核心应用
						// ApplyConfChange 将配置变更（此前已通过 ProposeConfChange 提交）应用到节点。
						// 每当在 Ready.CommittedEntries 中观察到配置变更时，都必须调用此方法；
						// 除非应用决定拒绝该配置变更（即将其视为空操作），此时不得调用。
						// 返回一个非空的、不透明的 ConfState protobuf 对象，该对象必须在快照中进行记录。
						newConfState := rn.Node.ApplyConfChange(cc)
						if newConfState != nil {
							rn.mu.Lock()
							// 更新 RaftNode 的 confState 字段
							// 后面的快照会用这个最新的 confState 生成快照元数据，确保快照里记录的是最新的集群配置
							rn.confState = *newConfState
							rn.mu.Unlock()
						}

						// c. 应用到状态机、网络层与本地状态。
						// Context 里同时编码了 Raft 地址和 gRPC 地址：
						//   - Raft 地址给 Transport 建连；
						//   - gRPC 地址给 Leader 转发，缺少它时新节点当选 Leader 后
						//     其他节点会把写请求转发到空地址。
						member := config.DecodeMemberContext(cc.Context)
						rn.applyMembershipChange(&cc, member)
						membershipChanged = true

						// d. 唤醒正在等待这条配置变更落地的提议者
						rn.resolveConfChangeWaiters(&cc)

					case raftpb.EntryNormal:
						if len(entry.Data) > 0 {
							// 收集普通业务日志交给 Applier
							normalEntries = append(normalEntries, entry)
						}
					}
				}

				// d. 正常执行业务日志
				// Apply 在一批日志全部落盘成功后才返回 nil；失败时整批回滚，状态机仍停在
				// 上一批的最后一条。此时绝不能推进 appliedIndex：否则 ReadIndex 会把尚未
				// 落盘的数据当成已应用返回给客户端。继续运行也无法自愈（这批 Ready 已被
				// 消费，Raft 不会重发），只能按致命错误终止，等重启后从 consistentIndex 重放。
				if len(normalEntries) > 0 {
					err := rn.applier.Apply(normalEntries)
					if err != nil {
						rn.storage.Close()
						log.Fatalf("apply committed entries failed: %v", err)
					}
				}

				// 判断是否需要触发快照
				// 获取最后一条已应用日志的索引
				// 由于这里的 Raeady 中的 CommittedEntries 对于每一个节点来说数量可能不同
				// 所以可能出现多个节点之间的 lastAppliedIndex 不一致，也就是落盘的快照索引不一致的情况
				// 但是这是很正常的，而且 Raft 共识会通过日志追赶机制来保证最终一致性，所以不需要担心
				lastAppliedIndex := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index

				// 配置变更和空日志（Leader 当选的 no-op）不写业务数据，Apply 不会推进
				// 状态机的 consistentIndex。但快照 metadata.Index 记录的是最后一条
				// 已应用日志；如果二者不对齐，重启时 raft.RestartNode 会以
				// Applied=consistentIndex 启动，而 raftLog.applied 已经是快照索引，
				// appliedTo() 直接 panic: applied(N) is out of range [prevApplied(M), ...]。
				// 所以这里显式把索引补齐到最后一条已应用日志。
				if idx, err := rn.applier.GetConsistentIndex(); err != nil {
					rn.storage.Close()
					log.Fatalf("get consistent index failed: %v", err)
				} else if idx < lastAppliedIndex {
					if err := rn.applier.AdvanceConsistentIndex(lastAppliedIndex); err != nil {
						rn.storage.Close()
						log.Fatalf("advance consistent index failed: %v", err)
					}
				}

				// 只增不减：重启后会从快照索引开始重放日志，此时状态机的
				// consistentIndex 可能已经领先本批日志的 lastAppliedIndex，
				// 直接把 appliedIndex 回退会让 ReadIndex 误判“未追平”。
				if lastAppliedIndex > rn.appliedIndex {
					rn.appliedIndex = lastAppliedIndex
				}

				// 触发快照的两种条件：
				//  1. 常规阈值：距离上一次快照超过 snapshotThreshold 条；
				//  2. 本批应用了配置变更：必须立刻让内存快照带上新的 ConfState。
				//
				// 第 2 点不是优化而是正确性要求。新成员刚加入时，Leader 给它发日志会
				// 被拒绝（它本地是空的），最终退化为发快照。若此时内存里的快照还是
				// 加入之前生成的，它的 ConfState 不包含新节点，raft.restore 会直接
				// 拒绝（"attempted to restore snapshot but it is not in the ConfState"），
				// 并且 Leader 会进入 StateSnapshot，不再重试，新节点永远追不上。
				//
				// 快照必须在这里（而不是在逐条处理配置变更时）生成：
				// 此时同一批 Ready 里的普通日志已经应用，consistentIndex 已推进到
				// lastAppliedIndex，快照的数据和索引是一致的。在逐条处理配置变更时
				// 打快照会用到“更旧的数据 + 更新的索引”，发给 follower 会丢日志。
				if lastAppliedIndex > rn.lastSnapshotIndex &&
					(membershipChanged || lastAppliedIndex-rn.lastSnapshotIndex >= snapshotThreshold) {
					if membershipChanged {
						log.Printf(">>> 配置变更后立即刷新快照, applied index: %d", lastAppliedIndex)
					} else {
						log.Printf(">>> 触发本地快照, applied index: %d", lastAppliedIndex)
					}

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
				batch, ok := rn.readBatches[string(rs.RequestCtx)] // 通过 RequestCtx 找到对应的批次
				if !ok {
					// 批次已经因为超时被清理，迟到的 ReadState 直接丢弃
					continue
				}
				if !batch.respondedOK { // 只处理第一次收到的 ReadState，避免重复处理
					batch.respondedOK = true // 标记本批次已经收到 ReadState，避免重复处理
					batch.target = rs.Index  // 记录本次 ReadIndex 返回的索引
					close(batch.responded)   // 通知 readIndexLoop 可以收集下一批了
				}
				if rn.appliedIndex >= batch.target {
					// 已经追平，放行批次里的所有读请求
					rn.resolveReadIndexLocked(string(rs.RequestCtx))
				} else {
					log.Printf(">>> ReadIndex 未追平, 请求数: %d, applied index: %d, read index: %d",
						batch.count, rn.appliedIndex, batch.target)
				}
			}
			// 本轮应用可能刚好让之前挂起的批次追平
			for key, batch := range rn.readBatches {
				if batch.respondedOK && rn.appliedIndex >= batch.target {
					rn.resolveReadIndexLocked(key)
				}
			}
			rn.readIndexMu.Unlock()

			// 6. 推进
			rn.Node.Advance()
		}
	}
}
