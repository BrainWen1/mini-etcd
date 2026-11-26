package types

import (
	"encoding/json"
	"errors"
	"fmt"
)

// CommandType 仅用于提取操作类型的信封结构体
type CommandType struct {
	RequestID string `json:"request_id"`
	Op        string `json:"op"`
}

// Command 是写请求在 Raft 日志中的编码格式。
//
// 用 JSON 而不是空格分隔：key/value 可以包含空格、换行等任意字符，
// 不会因为分隔符出现在数据里而把命令解析错。
type Command struct {
	RequestID string `json:"request_id"`
	Op        string `json:"op"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	LeaseID   int64  `json:"lease_id,omitempty"` // 可选字段，绑定的 LeaseID
}

// DecodeCommandType 只解析日志条目的信封（request_id 与操作类型）。
// PUT/DELETE 和 TXN 的命令结构不同，必须先判断类型，再调用各自的 Decode。
func DecodeCommandType(data []byte) (CommandType, bool) {
	var ct CommandType
	if err := json.Unmarshal(data, &ct); err != nil || ct.Op == "" {
		return CommandType{}, false
	}
	return ct, true
}

// EncodePutCommand 把 PUT 请求编码成可以写进 Raft 日志的字节流
func EncodePutCommand(requestID, key, value string, leaseID int64) ([]byte, error) {
	return json.Marshal(Command{RequestID: requestID, Op: "PUT", Key: key, Value: value, LeaseID: leaseID})
}

// EncodeDeleteCommand 把 DELETE 请求编码成可以写进 Raft 日志的字节流
func EncodeDeleteCommand(requestID, key string) ([]byte, error) {
	return json.Marshal(Command{RequestID: requestID, Op: "DELETE", Key: key})
}

// DecodeCommand 解析日志中的 PUT/DELETE 命令。
func DecodeCommand(data []byte) (Command, bool) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil || cmd.Op == "" {
		return Command{}, false
	}
	return cmd, true
}

// Compare 定义事务中的一个比较条件。
// 注意 key 不存在（或已被删除）时按空串参与比较，与 etcd 的语义一致。
type Compare struct {
	Key   string `json:"key"`
	Op    string `json:"op"` // "==", "!=", ">", ">=", "<", "<="，缺省视为 "=="
	Value string `json:"value"`
}

// ValidateCompareOp 检查比较运算符是否受支持
func ValidateCompareOp(op string) error {
	switch op {
	case "", "==", "=", "!=", ">", ">=", "<", "<=":
		return nil
	default:
		return fmt.Errorf("unsupported compare op %q", op)
	}
}

// Match 判断实际值是否满足比较条件
func (c Compare) Match(actual string) (bool, error) {
	if err := ValidateCompareOp(c.Op); err != nil {
		return false, err
	}
	switch c.Op {
	case "", "==", "=":
		return actual == c.Value, nil
	case "!=":
		return actual != c.Value, nil
	case ">":
		return actual > c.Value, nil
	case ">=":
		return actual >= c.Value, nil
	case "<":
		return actual < c.Value, nil
	case "<=":
		return actual <= c.Value, nil
	}
	return false, fmt.Errorf("unsupported compare op %q", c.Op) // 不可达，防御性返回
}

// Operation 定义事务分支中的一个写操作（PUT 或 DELETE）
type Operation struct {
	Op      string `json:"op"` // "PUT", "DELETE"
	Key     string `json:"key"`
	Value   string `json:"value"`              // DELETE 时可为空
	LeaseID int64  `json:"lease_id,omitempty"` // 可选字段，绑定的 LeaseID
}

// TxnCommand 定义完整的事务命令
type TxnCommand struct {
	RequestID string      `json:"request_id"`
	Op        string      `json:"op"`       // 固定为 "TXN"
	Compares  []Compare   `json:"compares"` // If 条件（多个条件之间是 AND）
	Success   []Operation `json:"success"`  // Then 分支
	Failure   []Operation `json:"failure"`  // Else 分支
}

// EncodeTxnCommand 把事务请求编码成可以写进 Raft 日志的字节流
func EncodeTxnCommand(cmd TxnCommand) ([]byte, error) {
	cmd.Op = "TXN"
	return json.Marshal(cmd)
}

// DecodeTxnCommand 解析日志中的事务命令，并校验 op 字段
func DecodeTxnCommand(data []byte) (TxnCommand, error) {
	var cmd TxnCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return cmd, fmt.Errorf("decode txn command: %w", err)
	}
	if cmd.Op != "TXN" {
		return cmd, fmt.Errorf("decode txn command: unexpected op %q", cmd.Op)
	}
	return cmd, nil
}

// ValidateTxnCommand 校验事务内的比较条件和写操作。
// 在把命令写进 Raft 日志之前调用，避免一条非法命令让 Apply 反复失败。
func ValidateTxnCommand(cmd TxnCommand) error {
	for i, cmp := range cmd.Compares {
		if cmp.Key == "" {
			return fmt.Errorf("compares[%d]: key must not be empty", i)
		}
		if err := ValidateCompareOp(cmp.Op); err != nil {
			return fmt.Errorf("compares[%d]: %w", i, err)
		}
	}
	for i, op := range cmd.Success {
		if err := validateOperation(op); err != nil {
			return fmt.Errorf("success[%d]: %w", i, err)
		}
	}
	for i, op := range cmd.Failure {
		if err := validateOperation(op); err != nil {
			return fmt.Errorf("failure[%d]: %w", i, err)
		}
	}
	return nil
}

func validateOperation(op Operation) error {
	if op.Key == "" {
		return errors.New("key must not be empty")
	}
	switch op.Op {
	case "PUT", "DELETE":
		return nil
	default:
		return fmt.Errorf("unsupported operation %q", op.Op)
	}
}

// LeaseCommand.Op 的取值
const (
	OpLeaseGrant     = "LEASE_GRANT"
	OpLeaseRevoke    = "LEASE_REVOKE"
	OpLeaseKeepAlive = "LEASE_KEEPALIVE"
)

// LeaseCommand 定义租约操作的命令结构
type LeaseCommand struct {
	RequestID string `json:"request_id"`
	Op        string `json:"op"`       // "LEASE_GRANT", "LEASE_REVOKE", "LEASE_KEEPALIVE"
	LeaseID   int64  `json:"lease_id"` // 租约 ID
	TTL       int64  `json:"ttl"`      // GRANT 时指定租约的 TTL（秒），其他操作忽略
	// LeaseStart 是 Leader 在提议时打上的“本次有效期开始时间”（Unix 秒）。
	// 所有节点都用 LeaseStart+TTL 计算过期时间，而不是各自在 Apply 时调用 time.Now()：
	// 状态机的输入必须只来自日志，否则落后节点重放同一条 Grant 会算出不同的过期时间，
	// 节点之间产生分歧。
	//
	// 这里存“开始时间”而不是“绝对过期时间”，是因为 KeepAlive 需要重置有效期，
	// 而 TTL 只存在于状态机里（Grant 时写入、之后不再变化）。携带开始时间时，
	// 续租只需要 Leader 打一个时间戳，不必先读一次状态机拿 TTL。
	LeaseStart int64 `json:"lease_start,omitempty"`
}

// ValidateLeaseCommand 校验租约命令的语义字段。
// 与 TXN 的校验一样，结果只依赖日志内容：非法命令在所有节点上都会被同样跳过，
// 不会造成状态机分歧，也不会让 Apply 返回错误导致节点退出。
func ValidateLeaseCommand(cmd LeaseCommand) error {
	if cmd.LeaseID <= 0 {
		return fmt.Errorf("invalid lease id %d", cmd.LeaseID)
	}
	if cmd.Op == OpLeaseGrant && cmd.TTL <= 0 {
		return fmt.Errorf("invalid lease ttl %d", cmd.TTL)
	}
	// GRANT/KEEPALIVE 的过期时间 = LeaseStart + TTL，两个字段缺一不可；
	// REVOKE 不涉及时间。
	if cmd.Op != OpLeaseRevoke && cmd.LeaseStart <= 0 {
		return fmt.Errorf("invalid lease start %d", cmd.LeaseStart)
	}
	return nil
}

// EncodeLeaseCommand 把租约请求编码成可以写进 Raft 日志的字节流
func EncodeLeaseCommand(cmd LeaseCommand) ([]byte, error) {
	return json.Marshal(cmd)
}

// DecodeLeaseCommand 解析日志中的租约命令，并校验 op 字段
func DecodeLeaseCommand(data []byte) (LeaseCommand, error) {
	var cmd LeaseCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return cmd, fmt.Errorf("decode lease command: %w", err)
	}
	switch cmd.Op {
	case OpLeaseGrant, OpLeaseRevoke, OpLeaseKeepAlive:
		return cmd, nil
	default:
		return cmd, fmt.Errorf("decode lease command: unexpected op %q", cmd.Op)
	}
}
