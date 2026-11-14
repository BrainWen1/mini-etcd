package raftnode

import (
	"context"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Run 启动核心循环
func (rn *RaftNode) Run(stopc <-chan struct{}, tickerInterval time.Duration) { // stopc 为只读 channel，接收停止信号
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopc: // 收到停止信号，退出循环
			rn.Node.Stop()
			return
		case <-ticker.C: // 触发一次 Tick，处理心跳和选举超时
			rn.Node.Tick()
		case m := <-rn.trans.Recv(): // 收到来自其他节点的消息，调用 Step 处理
			rn.Node.Step(context.Background(), m)
		case rd := <-rn.Node.Ready(): // Raft 节点有 Ready 状态，处理 Ready
			// 1. 持久化
			rn.storage.SetHardState(rd.HardState)
			rn.storage.Append(rd.Entries)

			// 2. 发送消息
			rn.trans.Send(rd.Messages)

			// 3. 应用
			for _, entry := range rd.CommittedEntries {
				if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
					println("Apply Data:", string(entry.Data))
				}
			}

			// 4. 推进
			rn.Node.Advance()
		}
	}
}
