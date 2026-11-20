package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// CommandType 仅用于提取操作类型的信封结构体
type CommandType struct {
	RequestID string `json:"request_id"`
	Op        string `json:"op"`
}

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

// DecodeCommandType 只解析日志条目的信封（request_id 与操作类型）。
// PUT/DELETE 和 TXN 的命令结构不同，必须先判断类型，再调用各自的 Decode。
// 这里同样兼容旧版本的空格分隔格式（"reqID PUT key value"）。
func DecodeCommandType(data []byte) (CommandType, bool) {
	var ct CommandType
	if err := json.Unmarshal(data, &ct); err == nil && ct.Op != "" {
		return ct, true
	}

	parts := strings.Split(string(data), " ")
	if len(parts) >= 2 && parts[1] != "" {
		return CommandType{RequestID: parts[0], Op: parts[1]}, true
	}
	return CommandType{}, false
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
func DecodeCommand(data []byte) (Command, bool) {
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
	Op    string `json:"op"` // "PUT", "DELETE"
	Key   string `json:"key"`
	Value string `json:"value"` // DELETE 时可为空
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
