package apply

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"mini-etcd/internal/mvcc"
	"strings"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Command 是写请求在 Raft 日志中的编码格式。
//
// 旧实现用空格分隔（"reqID PUT key value"），key 或 value 只要包含空格，
// strings.Split 就会得到超过 4 段，整条命令被静默丢弃，客户端只能等到超时。
// 改为 JSON 后 key/value 可以是任意字符串（包括空格、换行等）。
type Command struct {
	RequestID string `json:"request_id"`
	Op        string `json:"op"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// EncodePutCommand 把 PUT 请求编码成可以写进 Raft 日志的字节流
func EncodePutCommand(requestID, key, value string) ([]byte, error) {
	return json.Marshal(Command{RequestID: requestID, Op: "PUT", Key: key, Value: value})
}

// EncodeDeleteCommand 把 DELETE 请求编码成可以写进 Raft 日志的字节流
func EncodeDeleteCommand(requestID, key string) ([]byte, error) {
	return json.Marshal(Command{RequestID: requestID, Op: "DELETE", Key: key})
}

// decodeCommand 解析日志数据，并兼容旧版本的空格分隔格式
func decodeCommand(data []byte) (Command, bool) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err == nil && cmd.Op != "" {
		return cmd, true
	}

	// 兼容旧日志："reqID PUT key value"（只可能是 value 不含空格的历史数据）
	parts := strings.Split(string(data), " ")
	if len(parts) == 4 {
		return Command{RequestID: parts[0], Op: parts[1], Key: parts[2], Value: parts[3]}, true
	}
	return Command{}, false
}

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

		// 解析数据
		cmd, ok := decodeCommand(entry.Data)
		if !ok {
			log.Printf("[apply] 无法解析的日志索引: %d, 数据: %q\n", entry.Index, string(entry.Data))
			continue
		}
		log.Printf("[apply] 日志索引: %d, 操作: %s, key: %s\n", entry.Index, cmd.Op, cmd.Key)

		switch cmd.Op {
		case "PUT": // 处理 PUT 命令
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
			err := a.mvccStorage.Delete(cmd.Key, entry.Index)
			a.notifyWaiter(cmd.RequestID, err)
			if err != nil {
				log.Printf("[apply] 删除数据失败: %s, request ID: %s, 错误: %v\n", cmd.Key, cmd.RequestID, err)
				return err
			}
			log.Printf("[apply] 成功删除数据: %s, request ID: %s\n", cmd.Key, cmd.RequestID)
		default:
			// 未知命令也要唤醒客户端，否则客户端只能等到 5 秒超时
			err := fmt.Errorf("unknown command: %s", cmd.Op)
			a.notifyWaiter(cmd.RequestID, err)
			log.Printf("[apply] 未知命令: %s, request ID: %s\n", cmd.Op, cmd.RequestID)
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
