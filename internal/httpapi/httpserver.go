package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"mini-etcd/internal/mvcc"
	"mini-etcd/internal/server"
	"mini-etcd/internal/types"
)

type HTTPServer struct {
	srv *server.Server // 组合底层的 Server
}

func NewHTTPServer(srv *server.Server) *HTTPServer {
	return &HTTPServer{srv: srv}
}

// Start 启动 HTTP 服务，监听指定端口
func (h *HTTPServer) Start(port string) error {
	// 使用 http.NewServeMux 创建一个新的路由器
	mux := http.NewServeMux()

	// 注册路由
	mux.HandleFunc("POST /v1/kv", h.handlePut)
	mux.HandleFunc("GET /v1/kv/{key}", h.handleGet)
	mux.HandleFunc("DELETE /v1/kv/{key}", h.handleDelete)
	mux.HandleFunc("GET /v1/kv/range", h.handleRange)
	mux.HandleFunc("POST /v1/kv/txn", h.handleTxn)

	mux.HandleFunc("GET /v1/kv/watch", h.handleWatch)
	mux.HandleFunc("POST /v1/lease/grant", h.handleLeaseGrant)
	mux.HandleFunc("POST /v1/lease/revoke", h.handleLeaseRevoke)
	mux.HandleFunc("POST /v1/lease/keepalive", h.handleLeaseKeepAlive)

	// 动态成员管理：写请求同样需要代理到 Leader（配置变更必须由 Leader 提议）
	mux.HandleFunc("POST /v1/cluster/members/add", h.handleAddMember)
	mux.HandleFunc("POST /v1/cluster/members/remove", h.handleRemoveMember)

	log.Printf("HTTP API Server 启动，监听 %s", port)
	return http.ListenAndServe(":"+port, mux)
}

// 定义请求和响应的 JSON 结构体
type PutRequest struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	LeaseID int64  `json:"lease_id,omitempty"` // 可选字段，绑定的 LeaseID
}

type GetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type RangeKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type RangeResponse struct {
	KVs []RangeKV `json:"kvs"`
}

// TxnRequest 是事务请求体。compares 之间是 AND 关系；
// 条件全部成立时执行 success，否则执行 failure。
type TxnRequest struct {
	Compares []types.Compare   `json:"compares"`
	Success  []types.Operation `json:"success"`
	Failure  []types.Operation `json:"failure"`
}

// TxnResponse 是事务响应体。
// succeeded 表示事务是否走了 success 分支：分布式锁用 compares 判断锁是否空闲，
// 客户端必须看这个字段（而不是 HTTP 200）才知道自己有没有抢到锁。
type TxnResponse struct {
	Status    string `json:"status"`
	Succeeded bool   `json:"succeeded"`
}

// validateTxnKeys 对事务中所有 key 做与 PUT/DELETE 相同的校验：
// \x00 会破坏存储层 "key\x00revision" 的组合键，NUL 经 URL/JSON 都能传进来。
func validateTxnKeys(req TxnRequest) error {
	for _, cmp := range req.Compares {
		if err := validateKey(cmp.Key); err != nil {
			return fmt.Errorf("compares: %w", err)
		}
	}
	for _, ops := range [][]types.Operation{req.Success, req.Failure} {
		for _, op := range ops {
			if err := validateKey(op.Key); err != nil {
				return fmt.Errorf("%s operation: %w", op.Op, err)
			}
		}
	}
	return nil
}

type ErrorResponse struct {
	Error string `json:"error"`
}

// validateKey 拒绝会让 MVCC 组合键产生歧义或没有意义的 key：
// 空 key，以及包含 \x00 的 key（存储层用 \x00 分隔用户 key 和 8 字节 revision）。
// 注意 GET 路径参数会被 URL 解码，客户端发送 %00 同样能构造出 NUL 字节。
func validateKey(key string) error {
	if key == "" {
		return errors.New("key must not be empty")
	}
	if strings.ContainsRune(key, 0) {
		return errors.New("key must not contain NUL byte")
	}
	return nil
}

// handlePut 处理 POST /v1/kv
func (h *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request) {
	if h.proxyToLeader(w, r) {
		// 请求被代理到 Leader，当前节点不再处理
		return
	}
	// 当前节点是 Leader，继续处理请求

	var req PutRequest
	// 解析 JSON Body
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if err := validateKey(req.Key); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// 调用底层的 HandlePut（同步等待落盘）
	res := h.srv.HandlePut(req.Key, req.Value, req.LeaseID)
	if res.Err != nil {
		writeError(w, http.StatusInternalServerError, res.Err.Error())
		return
	}

	// 返回成功响应
	w.WriteHeader(http.StatusOK)                                      // 设置状态码为 200 OK
	json.NewEncoder(w).Encode(map[string]string{"status": "success"}) // 返回简单的 JSON 响应
}

// handleGet 处理 GET /v1/kv/{key} 和 GET /v1/kv/{key}?rev={rev}
func (h *HTTPServer) handleGet(w http.ResponseWriter, r *http.Request) {
	// 获取路径参数
	key := r.PathValue("key")
	if err := validateKey(key); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// 获取查询参数 revision (例如 ?revision=10)
	// 同时兼容提交说明中使用的 ?rev=10：否则未知参数会被静默忽略，
	// 客户端以为读的是历史版本，实际拿到的是最新值。
	revStr := r.URL.Query().Get("revision")
	if revStr == "" {
		revStr = r.URL.Query().Get("rev")
	}

	var val string
	var err error

	if revStr != "" {
		// 解析 revision
		rev, parseErr := strconv.ParseUint(revStr, 10, 64)
		if parseErr != nil {
			writeError(w, http.StatusBadRequest, "invalid revision")
			return
		}
		// 调用历史版本查询
		val, err = h.srv.HandleGetAtRevision(key, rev)
	} else {
		// 调用最新版本查询
		val, err = h.srv.HandleGet(key)
	}

	if err != nil {
		if errors.Is(err, mvcc.ErrKeyNotFound) {
			writeError(w, http.StatusNotFound, "key not found")
		} else {
			writeError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	// 返回成功响应
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(GetResponse{Key: key, Value: val})
}

// 统一的错误响应函数
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")   // 设置响应头为 JSON
	w.WriteHeader(status)                                // 设置 HTTP 状态码
	json.NewEncoder(w).Encode(ErrorResponse{Error: msg}) // 返回 JSON 错误信息
}

// handleDelete 处理 DELETE /v1/kv/{key}
func (h *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	if h.proxyToLeader(w, r) {
		// 请求被代理到 Leader，当前节点不再处理
		return
	}
	// 当前节点是 Leader，继续处理请求

	// 获取路径参数
	key := r.PathValue("key")
	if err := validateKey(key); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// 调用底层的 HandleDelete（同步等待落盘）
	res := h.srv.HandleDelete(key)
	if res.Err != nil {
		writeError(w, http.StatusInternalServerError, res.Err.Error())
		return
	}

	// 返回成功响应
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// handleRange 处理 GET /v1/kv/range?prefix={prefix}&limit={limit}
func (h *HTTPServer) handleRange(w http.ResponseWriter, r *http.Request) {
	// 解析查询参数
	prefix := r.URL.Query().Get("prefix")
	if err := validateKey(prefix); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	limitStr := r.URL.Query().Get("limit")
	limit := 100 // 默认限制为 100
	if limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "failed to parse limit: "+err.Error())
			return
		}
	}

	// 调用底层的 HandleRange
	kvs, err := h.srv.HandleRange(prefix, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// 构造响应
	resp := RangeResponse{KVs: make([]RangeKV, len(kvs))}
	for i, kv := range kvs {
		resp.KVs[i] = RangeKV{Key: kv.Key, Value: kv.Value}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleTxn 处理 POST /v1/kv/txn，例如：
//
//	{
//	  "compares": [{"key": "k", "op": "==", "value": "v"}],
//	  "success":  [{"op": "PUT", "key": "a", "value": "1"}],
//	  "failure":  [{"op": "DELETE", "key": "b"}]
//	}
func (h *HTTPServer) handleTxn(w http.ResponseWriter, r *http.Request) {
	if h.proxyToLeader(w, r) {
		// 请求被代理到 Leader，当前节点不再处理
		return
	}
	// 当前节点是 Leader，继续处理请求

	var req TxnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	if err := validateTxnKeys(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// 校验比较运算符和写操作类型：非法请求直接 400，不写进 Raft 日志
	txnCmd := types.TxnCommand{Compares: req.Compares, Success: req.Success, Failure: req.Failure}
	if err := types.ValidateTxnCommand(txnCmd); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// 调用底层的 HandleTxn（同步等待落盘）
	res := h.srv.HandleTxn(req.Compares, req.Success, req.Failure)
	if res.Err != nil {
		writeError(w, http.StatusInternalServerError, res.Err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(TxnResponse{
		Status:    "success",
		Succeeded: res.Succeeded,
	})
}

// proxyToLeader 检查当前节点是否是 Leader，如果不是，则将请求代理到 Leader 节点。
func (h *HTTPServer) proxyToLeader(w http.ResponseWriter, r *http.Request) bool {
	if h.srv.IsLeader() {
		return false // 自己是 Leader，不需要代理，放行
	}

	leaderAddr := h.srv.GetLeaderHTTPAddr()
	if leaderAddr == "" {
		// 没有选出 Leader，返回 503 Service Unavailable
		http.Error(w, "no leader elected yet", http.StatusServiceUnavailable)
		return true // 处理了，不放行
	}

	// 如果请求本身带有内部代理标记，但自己还是 Follower，直接报错
	if r.Header.Get("X-Internal-Proxy") == "true" {
		http.Error(w, "proxy loop detected or lost leadership", http.StatusServiceUnavailable)
		return true
	}

	// 构建目标 Leader 的 URL
	targetURL, _ := url.Parse("http://" + leaderAddr)
	proxy := httputil.NewSingleHostReverseProxy(targetURL) // 创建反向代理
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		// 代理请求失败时的处理
		log.Printf("[Proxy Error] 代理到 %s 失败: %v", targetURL, err)
		http.Error(w, "proxy failed: "+err.Error(), http.StatusBadGateway)
	}

	// 防止无限代理：加上内部标记
	r.Header.Set("X-Internal-Proxy", "true")

	// 执行代理转发
	log.Printf("[Proxy] 将请求代理到 Leader %s: %s %s", leaderAddr, r.Method, r.URL.Path)
	proxy.ServeHTTP(w, r)
	return true // 处理完毕，不放行原逻辑
}

// AddMemberRequest 是添加成员请求体。
type AddMemberRequest struct {
	ID       uint64 `json:"id"`
	RaftAddr string `json:"raft_addr"`
	HTTPAddr string `json:"http_addr"`
}

// handleAddMember 处理 POST /v1/cluster/members/add，添加新成员到集群
func (h *HTTPServer) handleAddMember(w http.ResponseWriter, r *http.Request) {
	if h.proxyToLeader(w, r) {
		return // 请求被代理到 Leader，当前节点不再处理
	}
	// 当前节点是 Leader，继续处理请求

	// 解析请求体
	var req AddMemberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	// 校验请求参数
	if req.ID == 0 {
		writeError(w, http.StatusBadRequest, "id must be non-zero")
		return
	}
	if req.RaftAddr == "" {
		writeError(w, http.StatusBadRequest, "raft_addr must not be empty")
		return
	}
	if req.HTTPAddr == "" {
		writeError(w, http.StatusBadRequest, "http_addr must not be empty")
		return
	}

	// 调用 server 的 HandleAddMember 方法处理添加成员请求
	if err := h.srv.HandleAddMember(req.ID, req.RaftAddr, req.HTTPAddr); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// handleRemoveMember 处理 POST /v1/cluster/members/remove，移除集群成员
func (h *HTTPServer) handleRemoveMember(w http.ResponseWriter, r *http.Request) {
	if h.proxyToLeader(w, r) {
		return // 请求被代理到 Leader，当前节点不再处理
	}
	// 当前节点是 Leader，继续处理请求

	var req struct {
		ID uint64 `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.ID == 0 {
		writeError(w, http.StatusBadRequest, "id must be non-zero")
		return
	}

	if err := h.srv.HandleRemoveMember(req.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// httpapi/watch.go
func (h *HTTPServer) handleWatch(w http.ResponseWriter, r *http.Request) {
	// 1. 设置 SSE 必须的 Header
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// 获取 Flusher（如果是 HTTP/2，可能不支持 Flusher，这里用的是 HTTP/1.1）
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	key := r.URL.Query().Get("key")
	isPrefix := r.URL.Query().Get("prefix") == "true" // 是否按前缀订阅
	startRevStr := r.URL.Query().Get("start_rev")

	// 解析 start_rev 参数
	var startRev uint64 = 0
	if startRevStr != "" {
		startRev, _ = strconv.ParseUint(startRevStr, 10, 64)
	}

	// 2. 注册 Channel，拿到接收事件的通道
	// 注意：要先注册 Channel，再查询历史事件，否则可能漏掉刚好在查询和注册之间发生的事件
	ch := h.srv.Watch(key, isPrefix)
	defer h.srv.Unwatch(key, ch, isPrefix) // 客户端断开时清理

	// 3. 如果指定了 start_rev，先获取历史事件并发送给客户
	if startRev > 0 {
		history, err := h.srv.GetHistory(key, isPrefix, startRev)
		if err != nil {
			log.Printf("Failed to get watch history: %v", err)
			return
		}
		// 补发历史事件
		for _, event := range history {
			fmt.Fprintf(w, "data: %s\n\n", event.JSON())
			flusher.Flush()
		}
	}

	// 3. 启动心跳 ticker，防止反向代理断开连接
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	// 4. 死循环阻塞读取
	for {
		select {
		case <-r.Context().Done():
			// 客户端主动断开了连接
			return
		case <-ticker.C:
			// 发送心跳注释，防止某些反向代理（如 Nginx）断开连接
			fmt.Fprintf(w, ": ping\n\n")
			flusher.Flush()
		case event := <-ch:
			// 收到业务事件，发送给客户端
			// 标准格式：data: {"key":"test","value":"new"}\n\n
			fmt.Fprintf(w, "data: %s\n\n", event.JSON())
			flusher.Flush()
		}
	}
}

// handleLeaseGrant 处理 POST /v1/lease/grant，创建一个新的 Lease
func (h *HTTPServer) handleLeaseGrant(w http.ResponseWriter, r *http.Request) {
	if h.proxyToLeader(w, r) {
		return // 请求被代理到 Leader，当前节点不再处理
	}
	// 当前节点是 Leader，继续处理请求

	var req struct {
		TTL int64 `json:"ttl"` // TTL 单位为秒
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.TTL <= 0 {
		writeError(w, http.StatusBadRequest, "ttl must be positive")
		return
	}

	leaseID, res := h.srv.HandleLeaseGrant(req.TTL)
	if res.Err != nil {
		writeError(w, http.StatusInternalServerError, res.Err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"lease_id": leaseID,
	})
}

// handleLeaseRevoke 处理 POST /v1/lease/revoke，撤销一个 Lease
func (h *HTTPServer) handleLeaseRevoke(w http.ResponseWriter, r *http.Request) {
	if h.proxyToLeader(w, r) {
		return // 请求被代理到 Leader，当前节点不再处理
	}
	// 当前节点是 Leader，继续处理请求

	var req struct {
		LeaseID int64 `json:"lease_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.LeaseID <= 0 {
		writeError(w, http.StatusBadRequest, "lease_id must be positive")
		return
	}

	res := h.srv.HandleLeaseRevoke(req.LeaseID)
	if res.Err != nil {
		writeError(w, http.StatusInternalServerError, res.Err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// handleLeaseKeepAlive 处理 POST /v1/lease/keepalive，续租一个 Lease
func (h *HTTPServer) handleLeaseKeepAlive(w http.ResponseWriter, r *http.Request) {
	if h.proxyToLeader(w, r) {
		return // 请求被代理到 Leader，当前节点不再处理
	}
	// 当前节点是 Leader，继续处理请求

	var req struct {
		LeaseID int64 `json:"lease_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.LeaseID <= 0 {
		writeError(w, http.StatusBadRequest, "lease_id must be positive")
		return
	}

	res := h.srv.HandleLeaseKeepAlive(req.LeaseID)
	if res.Err != nil {
		writeError(w, http.StatusInternalServerError, res.Err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}
