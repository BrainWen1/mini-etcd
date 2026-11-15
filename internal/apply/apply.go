package apply

import (
	"errors"
	"log"
	"strings"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Applier 定义了应用已提交日志条目的接口
type Applier struct {
	mu         sync.RWMutex        // 保护并发访问
	data       map[string]string   // 存储状态机的数据
	appliedIDs map[string]struct{} // 记录已应用的日志条目 ID，做幂等处理
}

func NewApplier() *Applier {
	return &Applier{
		data:       make(map[string]string),
		appliedIDs: make(map[string]struct{}),
	}
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
			// 检查是否已经应用过该请求，避免重复应用
			a.mu.Lock()
			if _, exists := a.appliedIDs[requestID]; exists {
				a.mu.Unlock()
				log.Printf("[apply] 拦截到重复请求: %s\n", requestID)
				continue
			}
			// 到这说明是第一次应用该请求，记录下来
			a.appliedIDs[requestID] = struct{}{}
			a.mu.Unlock()

			switch parts[1] {
			case "PUT": // 处理 PUT 命令
				key, val := parts[2], parts[3]
				a.mu.Lock()
				a.data[key] = val
				a.mu.Unlock()
				log.Printf("[apply] 成功应用数据: %s -> %s\n", key, val)
			default:
				log.Printf("[apply] 未知命令: %s\n", cmd)
			}
		}
	}
	return nil
}

// Get 获取状态机中的数据
func (a *Applier) Get(key string) (string, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	val, ok := a.data[key]
	return val, ok
}

// ApplySnapshot 应用快照
func (a *Applier) ApplySnapshot(snapshot raftpb.Snapshot) error {
	// Implementation for applying snapshot
	return errors.New("ApplySnapshot not implemented yet")
}
