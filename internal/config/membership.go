package config

import "encoding/json"

// Member 描述一个集群成员的两种地址：
//   - RaftAddr 用于节点之间的 Raft 消息（TCPTransport 拨号用）
//   - GRPCAddr 用于客户端请求的 Leader 转发（Follower 把写请求转给 Leader 用）
//
// 两者必须一起保存：只记住 Raft 地址的话，新节点当选 Leader 后，
// 老节点会把写请求转发到一个不存在的 gRPC 地址。
type Member struct {
	ID       uint64 `json:"id"`
	RaftAddr string `json:"raft_addr"`
	GRPCAddr string `json:"grpc_addr"`
}

// memberContext 是写入 raftpb.ConfChange.Context 的结构。
// Context 会随日志条目复制到所有节点，因此集群里每个节点都能从
// 同一条 ConfChange 学到新成员的两种地址。
type memberContext struct {
	RaftAddr string `json:"raft_addr"`
	GRPCAddr string `json:"grpc_addr"`
}

// EncodeMemberContext 把成员地址编码进 ConfChange.Context。
func EncodeMemberContext(raftAddr, grpcAddr string) []byte {
	// 结构体只有两个 string 字段，json.Marshal 不会失败。
	data, _ := json.Marshal(memberContext{RaftAddr: raftAddr, GRPCAddr: grpcAddr})
	return data
}

// DecodeMemberContext 解析 ConfChange.Context。
//
// Context 是确认提交的配置变更日志自带的元数据，只可能是本项目的
// EncodeMemberContext 写出的 JSON；解析失败说明日志损坏，返回空 Member，
// 由调用方决定如何处理。
func DecodeMemberContext(data []byte) Member {
	var mc memberContext
	if err := json.Unmarshal(data, &mc); err != nil {
		return Member{}
	}
	return Member{RaftAddr: mc.RaftAddr, GRPCAddr: mc.GRPCAddr}
}

// MergeMembers 把成员表合并进配置的 Peers / GRPCPeers。
//
// 唯一的调用方是 main：启动时从状态机（MetaBucket）读出成员表，
// 合并进来自命令行/默认拓扑的启动配置。状态机里的成员表随快照复制，
// 是重启后最权威的地址来源。
//
// 空地址不覆盖已有地址：成员记录里两个字段本应都有值，这里只是防止
// 一条残缺记录把启动参数里可用的地址抹掉。
func (c *Config) MergeMembers(members map[uint64]Member) {
	for id, m := range members {
		if m.RaftAddr != "" {
			c.Peers[id] = m.RaftAddr
		}
		if m.GRPCAddr != "" {
			c.GRPCPeers[id] = m.GRPCAddr
		}
	}
}
