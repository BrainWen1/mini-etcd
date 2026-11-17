package httpapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"mini-etcd/internal/mvcc"
	"mini-etcd/internal/server"
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

	log.Printf("HTTP API Server 启动，监听 %s", port)
	return http.ListenAndServe(":"+port, mux)
}

// 定义请求和响应的 JSON 结构体
type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

// handlePut 处理 POST /v1/kv
func (h *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request) {
	var req PutRequest
	// 解析 JSON Body
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	// 调用底层的 HandlePut（同步等待落盘）
	if err := h.srv.HandlePut(req.Key, req.Value); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
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

	// 获取查询参数 rev (例如 ?rev=10)
	revStr := r.URL.Query().Get("revision")

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
