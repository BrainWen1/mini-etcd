// Package metrics 提供了用于监控的 Prometheus 指标定义和注册
//   - RaftState: 当前节点的 Raft 状态（Follower=0, Candidate=1, Leader=2, Shutdown=3）
//   - RaftTerm: 当前节点的 Raft 任期
//   - RaftCommitIndex: 当前节点已提交的 Raft 日志索引
//   - RaftAppliedIndex: 当前节点已应用的 Raft 日志索引
//   - APIRequestsTotal: API 请求总数，按方法和状态码分类统计
//
// promauto.NewGauge 和 promauto.NewCounterVec 会自动注册指标到 Prometheus 默认注册器，无需手动注册。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RaftState 表示节点角色：0=Follower, 1=Candidate, 2=Leader, 3=Shutdown
	RaftState = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "minietcd_raft_state",
		Help: "Current Raft state of this node (0=Follower, 1=Candidate, 2=Leader)",
	})

	// RaftTerm 当前任期
	RaftTerm = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "minietcd_raft_term",
		Help: "Current Raft term",
	})

	// RaftCommitIndex 已提交的日志索引
	RaftCommitIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "minietcd_raft_commit_index",
		Help: "The last committed Raft log index",
	})

	// RaftAppliedIndex 已应用的日志索引
	RaftAppliedIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "minietcd_raft_applied_index",
		Help: "The last applied Raft log index",
	})

	// APIRequestsTotal 统计 API 请求总数
	APIRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "minietcd_api_requests_total",
		Help: "Total number of API requests by method and status",
	}, []string{"method", "status"})
)
