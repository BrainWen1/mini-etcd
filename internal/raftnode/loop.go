package raftnode

import (
	"context"
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
			// 1. 持久化
			if !raft.IsEmptyHardState(rd.HardState) { // Ready 的 HardState 经常是空的，只有在节点状态发生变化时才会有值
				rn.storage.SaveHardState(rd.HardState)
			}

			rn.storage.SaveEntries(rd.Entries)

			// 2. 发送消息
			rn.trans.Send(rd.Messages)

			// 3. 应用
			if len(rd.CommittedEntries) > 0 {
				err := rn.applier.Apply(rd.CommittedEntries)
				if err != nil {
					log.Printf("Error applying committed entries: %v", err)
				}
			}

			// 4. 推进
			rn.Node.Advance()
		}
	}
}
