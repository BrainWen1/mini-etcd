// Package grpcapi 实现 mini-etcd 的 gRPC 接口，是节点唯一的对外网络入口。
//
// 职责与旧的 HTTP 层一一对应：
//   - 写请求（Put/Delete/Txn/LeaseGrant/LeaseRevoke/LeaseKeepAlive/MemberAdd/
//     MemberRemove）必须由 Leader 提议。Follower 收到后通过内部 gRPC 连接把
//     请求原样转发给 Leader，客户端不需要自己换节点重试。
//   - 读请求（Get/Range）借助 ReadIndex 屏障在本地完成，Follower 也能返回
//     线性一致的结果。
//   - Watch 是服务端流，直接读取本地已应用的状态机并持续推送。
//
// Raft 日志里仍然使用 internal/types 的 JSON 命令格式：Protobuf 只是客户端与
// 节点之间的传输格式，不改变共识层和状态机的数据格式。
package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"go.etcd.io/etcd/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	etcdpb "mini-etcd/api/proto"
	"mini-etcd/internal/mvcc"
	"mini-etcd/internal/server"
	"mini-etcd/internal/types"
)

// forwardedKey 标记请求已经经过一次 Follower -> Leader 转发。
// 没有它，两个都认为对方是 Leader 的节点（例如领导权刚切换）会互相转发，
// 形成不消耗网络带宽但永远不结束的转发环路。
const forwardedKey = "x-mini-etcd-forwarded"

// GRPCServer 同时实现 KV / Watch / Lease / Cluster 四个服务。
type GRPCServer struct {
	// 一个实例被注册到四个 service 上，共享同一份 Leader 转发连接池。
	etcdpb.UnimplementedKVServer
	etcdpb.UnimplementedWatchServer
	etcdpb.UnimplementedLeaseServer
	etcdpb.UnimplementedClusterServer

	srv  *server.Server
	grpc *grpc.Server

	// 到其它节点 gRPC 服务的连接池（key 是地址）。
	// grpc.ClientConn 并发安全，可以长期复用，不要在每次 RPC 时重新 dial。
	connMu sync.Mutex
	conns  map[string]*grpc.ClientConn
}

// NewGRPCServer 创建 gRPC 服务并完成服务注册。
func NewGRPCServer(srv *server.Server) *GRPCServer {
	g := &GRPCServer{
		srv:   srv,
		conns: make(map[string]*grpc.ClientConn),
	}
	g.grpc = grpc.NewServer()
	// 注册四个服务，grpc-go 会自动把方法名映射成 /package.service/method 的 gRPC 路径。
	etcdpb.RegisterKVServer(g.grpc, g)
	etcdpb.RegisterWatchServer(g.grpc, g)
	etcdpb.RegisterLeaseServer(g.grpc, g)
	etcdpb.RegisterClusterServer(g.grpc, g)
	// 开启反射，便于用 grpcurl 直接调试（无需携带 .proto 文件）。
	// 对公网暴露时可以在生产配置里关掉。
	reflection.Register(g.grpc)
	return g
}

// Start 在指定端口上提供 gRPC 服务，阻塞直到 Serve 返回。
func (g *GRPCServer) Start(port string) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("listen :%s: %w", port, err)
	}
	log.Printf("gRPC API Server 启动，监听 %s", lis.Addr())
	return g.grpc.Serve(lis)
}

// Stop 立即停止服务并关闭到其它节点的连接。
//
// 不用 GracefulStop：Watch 是长连接，GracefulStop 会一直等到客户端主动断开，
// 而这里的调用场景是进程退出。
func (g *GRPCServer) Stop() {
	g.grpc.Stop()
	g.closeConns()
}

// ---------------------------------------------------------------------------
// Leader 转发
// ---------------------------------------------------------------------------

// checkForward 判断当前节点是否需要把请求转发给 Leader。
// 返回 forward=true 表示调用方应转发；false 表示本节点就是 Leader；
// 返回 error 表示无法处理（没有 Leader，或请求已经转发过一次）。
func (g *GRPCServer) checkForward(ctx context.Context) (bool, error) {
	if g.srv.IsLeader() {
		return false, nil
	}
	// 已经是别的节点转发过来的请求，却落到一个仍认为自己是 Follower 的节点：
	// 说明领导权在切换中。直接失败让客户端重试，绝不能再转发。
	if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get(forwardedKey)) > 0 {
		return false, status.Error(codes.Unavailable, "node is not the leader and the request was already forwarded")
	}
	if g.srv.GetLeaderGRPCAddr() == "" {
		return false, status.Error(codes.Unavailable, "no leader elected yet")
	}
	return true, nil
}

// forwardedContext 给转发请求打上标记，避免 Leader 再次转发。
func forwardedContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, forwardedKey, "1")
}

// leaderConn 返回当前 Leader 的 gRPC 连接，按地址缓存。
func (g *GRPCServer) leaderConn() (*grpc.ClientConn, error) {
	addr := g.srv.GetLeaderGRPCAddr()
	if addr == "" {
		return nil, status.Error(codes.Unavailable, "no leader elected yet")
	}
	g.connMu.Lock()
	defer g.connMu.Unlock()
	if conn, ok := g.conns[addr]; ok {
		return conn, nil
	}
	// 集群内部通信，当前不启用 TLS。生产环境应在这里配置 mTLS。
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial leader %s: %v", addr, err)
	}
	g.conns[addr] = conn
	return conn, nil
}

// kvClient 返回当前 Leader 的 KVClient。
func (g *GRPCServer) kvClient() (etcdpb.KVClient, error) {
	conn, err := g.leaderConn()
	if err != nil {
		return nil, err
	}
	return etcdpb.NewKVClient(conn), nil
}

func (g *GRPCServer) leaseClient() (etcdpb.LeaseClient, error) {
	conn, err := g.leaderConn()
	if err != nil {
		return nil, err
	}
	return etcdpb.NewLeaseClient(conn), nil
}

func (g *GRPCServer) clusterClient() (etcdpb.ClusterClient, error) {
	conn, err := g.leaderConn()
	if err != nil {
		return nil, err
	}
	return etcdpb.NewClusterClient(conn), nil
}

// closeConns 关闭所有到其它节点的 gRPC 连接。
func (g *GRPCServer) closeConns() {
	g.connMu.Lock()
	defer g.connMu.Unlock()
	for addr, conn := range g.conns {
		_ = conn.Close()
		delete(g.conns, addr)
	}
}

// ---------------------------------------------------------------------------
// KV
// ---------------------------------------------------------------------------

// Put 写入一个 key。lease > 0 时把 key 绑定到租约，租约过期后 key 自动删除。
func (g *GRPCServer) Put(ctx context.Context, req *etcdpb.PutRequest) (*etcdpb.PutResponse, error) {
	// 校验参数
	// req.GetKey() 是由 protoc 自动生成的 getter 方法，用于获取 protobuf 字段的值。
	if err := validateKey(req.GetKey()); err != nil {
		return nil, invalidArgument(err)
	}
	// lease < 0 没有意义：租约 ID 是 uint64，负数在 bbolt 里会被当成大整数存储，导致租约永远不会过期。
	if req.GetLeaseId() < 0 {
		return nil, status.Error(codes.InvalidArgument, "lease must not be negative")
	}
	// 先检查是否需要转发给 Leader
	forward, err := g.checkForward(ctx)
	if err != nil {
		return nil, err
	}
	if forward {
		// 转发请求给 Leader，原样返回 Leader 的响应
		client, err := g.kvClient() // 拿到 Leader 的 gRPC 连接并创建 KVClient
		if err != nil {
			return nil, err
		}
		return client.Put(forwardedContext(ctx), req) // client.Put 会把 req 序列化成 protobuf 再发给 Leader
		// client 是 grpc.ClientConn 的封装，在 kvClient 里已经用 Leader 的连接初始化了 grpc.ClientConn
		// Put 内部会调用 grpc.Invoke，grpc.Invoke 会把请求序列化成 protobuf 并发给 client 里保存的 grpc.ClientCon
		// grpc.ClientConn 对应的就是 Leader，Leader 的 gRPC 服务会反序列化成 protobuf，再调用 GRPCServer.Put 方法处理请求
		// 最后把响应序列化成 protobuf 返回给 client，client 再反序列化成 PutResponse 返回给 Follower
	}

	// 本节点是 Leader，直接在状态机里处理请求
	if res := g.srv.HandlePut(req.GetKey(), req.GetValue(), req.GetLeaseId()); res.Err != nil {
		return nil, toStatus(res.Err)
	}
	return &etcdpb.PutResponse{}, nil
}

// Get 读取一个 key。revision=0 读最新值，否则读 revision <= 指定值的最新版本。
func (g *GRPCServer) Get(ctx context.Context, req *etcdpb.GetRequest) (*etcdpb.GetResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, invalidArgument(err)
	}

	var (
		value string
		err   error
	)
	// 根据是否指定 revision 调用不同的状态机方法。
	// HandleGetAtRevision 会在 bbolt 事务里遍历历史版本，找到 revision <= 指定值的最新版本。
	// HandleGet 直接读最新值。
	if req.GetRevision() > 0 {
		value, err = g.srv.HandleGetAtRevision(req.GetKey(), req.GetRevision())
	} else {
		value, err = g.srv.HandleGet(req.GetKey())
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &etcdpb.GetResponse{
		Kv: &etcdpb.KeyValue{Key: req.GetKey(), Value: value},
	}, nil
}

// Delete 删除一个 key（写入墓碑，历史版本仍然保留到 GC 为止）。
func (g *GRPCServer) Delete(ctx context.Context, req *etcdpb.DeleteRequest) (*etcdpb.DeleteResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, invalidArgument(err)
	}
	// 先检查是否需要转发给 Leader
	forward, err := g.checkForward(ctx)
	if err != nil {
		return nil, err
	}
	if forward {
		client, err := g.kvClient()
		if err != nil {
			return nil, err
		}
		return client.Delete(forwardedContext(ctx), req) // client.Delete 也会把 req 序列化成 protobuf 再发给 Leader
	}

	// 本节点是 Leader，直接在状态机里处理请求
	if res := g.srv.HandleDelete(req.GetKey()); res.Err != nil {
		return nil, toStatus(res.Err)
	}
	return &etcdpb.DeleteResponse{}, nil
}

// Range 返回所有以 prefix 开头的键值对。
func (g *GRPCServer) Range(ctx context.Context, req *etcdpb.RangeRequest) (*etcdpb.RangeResponse, error) {
	if err := validateKey(req.GetPrefix()); err != nil {
		return nil, invalidArgument(err)
	}

	kvs, err := g.srv.HandleRange(req.GetPrefix(), int(req.GetLimit()))
	if err != nil {
		return nil, toStatus(err)
	}

	resp := &etcdpb.RangeResponse{Kvs: make([]*etcdpb.KeyValue, len(kvs))}
	for i, kv := range kvs {
		resp.Kvs[i] = &etcdpb.KeyValue{Key: kv.Key, Value: kv.Value}
	}
	return resp, nil
}

// Txn 在状态机的同一个 bbolt 事务里评估 compares 并执行 success/failure 分支。
func (g *GRPCServer) Txn(ctx context.Context, req *etcdpb.TxnRequest) (*etcdpb.TxnResponse, error) {
	compares, err := comparesFromProto(req.GetCompares())
	if err != nil {
		return nil, invalidArgument(err)
	}
	success, err := operationsFromProto(req.GetSuccess())
	if err != nil {
		return nil, invalidArgument(err)
	}
	failure, err := operationsFromProto(req.GetFailure())
	if err != nil {
		return nil, invalidArgument(err)
	}

	// 先检查是否需要转发给 Leader
	forward, err := g.checkForward(ctx)
	if err != nil {
		return nil, err
	}
	if forward {
		client, err := g.kvClient()
		if err != nil {
			return nil, err
		}
		return client.Txn(forwardedContext(ctx), req)
	}

	// 本节点是 Leader，直接在状态机里处理请求
	res := g.srv.HandleTxn(compares, success, failure)
	if res.Err != nil {
		return nil, toStatus(res.Err)
	}
	return &etcdpb.TxnResponse{Succeeded: res.Succeeded}, nil
}

// ---------------------------------------------------------------------------
// Watch
// ---------------------------------------------------------------------------

// Watch 订阅一个 key（或前缀）的变更，先补发 start_revision 之后的历史事件，
// 再持续推送实时事件。
func (g *GRPCServer) Watch(req *etcdpb.WatchRequest, stream etcdpb.Watch_WatchServer) error {
	if err := validateKey(req.GetKey()); err != nil {
		return invalidArgument(err)
	}

	// 先注册 Channel，再查询历史事件。顺序反过来的话，查询完成到注册之间
	// 发生的变更既不在历史里、也不会被推送，客户端会永久漏掉这些事件。
	ch := g.srv.Watch(req.GetKey(), req.GetPrefix())
	defer g.srv.Unwatch(req.GetKey(), ch, req.GetPrefix())

	// lastRev 用于去重：历史补发与实时推送存在重叠窗口，
	// 同一个事件可能既出现在历史里、又出现在 Channel 里。
	var lastRev uint64
	if startRev := req.GetStartRevision(); startRev > 0 {
		history, err := g.srv.GetHistory(req.GetKey(), req.GetPrefix(), startRev)
		if err != nil {
			return toStatus(err)
		}
		for _, event := range history {
			if err := stream.Send(eventToProto(event)); err != nil {
				// stream 是 gRPC 的服务端流，用于把事件推送给客户端，相当于 HTTP 的 SSE（Server-Sent Events）。
				// 如果客户端断开连接，stream.Send 会返回错误。
				return err
			}
			lastRev = event.Revision
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			// 客户端取消或连接断开，Unwatch 由 defer 负责。
			return nil
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			// 只有新收到的事件的 revision 大于历史补发的最后一个 revision 才会发送给客户端
			if event.Revision <= lastRev {
				continue
			}
			lastRev = event.Revision // 更新 lastRev 为最新的 revision，避免重复发送
			if err := stream.Send(eventToProto(event)); err != nil {
				return err
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Lease
// ---------------------------------------------------------------------------

func (g *GRPCServer) LeaseGrant(ctx context.Context, req *etcdpb.LeaseGrantRequest) (*etcdpb.LeaseGrantResponse, error) {
	if req.GetTtl() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "ttl must be positive")
	}
	forward, err := g.checkForward(ctx)
	if err != nil {
		return nil, err
	}
	if forward {
		// 转发请求给 Leader，原样返回 Leader 的响应
		client, err := g.leaseClient()
		if err != nil {
			return nil, err
		}
		return client.LeaseGrant(forwardedContext(ctx), req)
	}

	// 本节点是 Leader，直接在状态机里处理请求
	leaseID, res := g.srv.HandleLeaseGrant(req.GetTtl())
	if res.Err != nil {
		return nil, toStatus(res.Err)
	}
	return &etcdpb.LeaseGrantResponse{LeaseId: leaseID}, nil
}

func (g *GRPCServer) LeaseRevoke(ctx context.Context, req *etcdpb.LeaseRevokeRequest) (*etcdpb.LeaseRevokeResponse, error) {
	if req.GetLeaseId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "lease_id must be positive")
	}
	forward, err := g.checkForward(ctx)
	if err != nil {
		return nil, err
	}
	if forward {
		// 转发请求给 Leader，原样返回 Leader 的响应
		client, err := g.leaseClient()
		if err != nil {
			return nil, err
		}
		return client.LeaseRevoke(forwardedContext(ctx), req)
	}

	// 本节点是 Leader，直接在状态机里处理请求
	if res := g.srv.HandleLeaseRevoke(req.GetLeaseId()); res.Err != nil {
		return nil, toStatus(res.Err)
	}
	return &etcdpb.LeaseRevokeResponse{}, nil
}

// LeaseKeepAlive 双向流：客户端每发一条续租请求，服务端回一条 ACK。
// 单条租约已过期只返回 succeeded=false，不终止整条流，
// 客户端可以在同一条连接上为多个租约续租。
func (g *GRPCServer) LeaseKeepAlive(stream etcdpb.Lease_LeaseKeepAliveServer) error {
	forward, err := g.checkForward(stream.Context())
	if err != nil {
		return err
	}
	if forward {
		// 转发请求给 Leader，原样返回 Leader 的响应
		return g.proxyKeepAlive(stream)
	}

	// 本节点是 Leader，直接在状态机里处理续租请求
	// 这里就不需要像 proxyKeepAlive 里一样充当代理了，客户端直接与 Leader 建立双向流。
	for {
		// stream.Recv() 会阻塞直到收到客户端的请求，或者连接断开。
		req, err := stream.Recv()
		if err == io.EOF {
			return nil // 客户端正常关闭发送方向
		}
		if err != nil {
			return err
		}
		if req.GetLeaseId() <= 0 {
			return status.Error(codes.InvalidArgument, "lease_id must be positive")
		}

		// 续租请求被提交到 Raft 共识层，可能会失败（失去领导权、超时等）。
		res := g.srv.HandleLeaseKeepAlive(req.GetLeaseId())
		if res.Err != nil {
			if errors.Is(res.Err, mvcc.ErrLeaseNotFound) {
				if err := stream.Send(&etcdpb.LeaseKeepAliveResponse{
					LeaseId:   req.GetLeaseId(),
					Succeeded: false,
				}); err != nil {
					return err
				}
				continue
			}
			// 提议失败（失去领导权、超时等）说明这条流已经无法可靠服务。
			return toStatus(res.Err)
		}

		ttl, err := g.srv.LeaseTTL(req.GetLeaseId())
		if err != nil {
			ttl = 0 // 续租已经成功，读不到 TTL 只是响应信息不完整，不影响语义
		}
		// 发送续租响应给客户端
		if err := stream.Send(&etcdpb.LeaseKeepAliveResponse{
			LeaseId:   req.GetLeaseId(),
			Ttl:       ttl,
			Succeeded: true,
		}); err != nil {
			return err
		}
	}
}

// proxyKeepAlive 把 Follower 收到的双向流逐条转发给 Leader。
// 当前执行此方法的 Follower 节点充当代理，客户端与 Leader 之间的双向流被拆成两条单向流：
//   - Follower 收到客户端的请求，原样转发给 Leader。
//   - Leader 收到请求后处理，原样返回响应给 Follower。
//   - Follower 收到响应后原样返回给客户端。
func (g *GRPCServer) proxyKeepAlive(stream etcdpb.Lease_LeaseKeepAliveServer) error {
	client, err := g.leaseClient()
	if err != nil {
		return err
	}
	// upstream 是 Leader 的双向流，Follower 收到的请求会原样转发给 Leader，Leader 的响应会原样返回给 Follower。
	upstream, err := client.LeaseKeepAlive(forwardedContext(stream.Context()))
	if err != nil {
		return toStatus(err)
	}

	// 两个方向各自阻塞，任一方向结束就返回；返回后 stream.Context() 被取消，
	// 另一个 goroutine 的阻塞调用会随之退出，不会泄漏。
	errc := make(chan error, 2)
	go func() {
		for {
			// stream.Recv() 会阻塞直到收到客户端的请求，或者连接断开。
			// 如果客户端断开连接，stream.Recv() 会返回错误，errc <- err 会让 proxyKeepAlive 返回，
			// stream.Context() 被取消，upstream.Send() 也会返回错误，errc <- err 会让 proxyKeepAlive 返回。
			// 这样就不会出现 Follower 收到的 Leader 响应被转发给一个已经断开连接的客户端的情况。
			req, err := stream.Recv()
			if err != nil {
				errc <- err
				return
			}
			if err := upstream.Send(req); err != nil {
				errc <- err
				return
			}
		}
	}()
	go func() {
		for {
			// upstream.Recv() 会阻塞直到收到 Leader 的响应，或者连接断开。
			// 如果 Leader 失去领导权，upstream.Recv() 会返回错误，errc <- err 会让 proxyKeepAlive 返回，
			// stream.Context() 被取消，stream.Recv() 也会返回错误，errc <- err 会让 proxyKeepAlive 返回。
			// 这样就不会出现 Follower 收到的请求被转发给一个已经失去领导权的 Leader 的情况。
			resp, err := upstream.Recv()
			if err != nil {
				errc <- err
				return
			}
			// stream.Send() 会阻塞直到把响应发送给客户端，或者连接断开。
			if err := stream.Send(resp); err != nil {
				errc <- err
				return
			}
		}
	}()

	// 阻塞直到任一方向出错（客户端断开、Leader 失去领导权等），然后返回错误。
	err = <-errc
	if err == io.EOF {
		// 客户端正常关闭发送方向，返回 nil 让 gRPC 框架把流标记为正常结束。
		return nil
	}
	return toStatus(err)
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

func (g *GRPCServer) MemberAdd(ctx context.Context, req *etcdpb.MemberAddRequest) (*etcdpb.MemberAddResponse, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "id must be non-zero")
	}
	if req.GetRaftAddr() == "" {
		return nil, status.Error(codes.InvalidArgument, "raft_addr must not be empty")
	}
	if req.GetGrpcAddr() == "" {
		return nil, status.Error(codes.InvalidArgument, "grpc_addr must not be empty")
	}
	forward, err := g.checkForward(ctx)
	if err != nil {
		return nil, err
	}
	if forward {
		// 转发请求给 Leader，原样返回 Leader 的响应
		client, err := g.clusterClient()
		if err != nil {
			return nil, err
		}
		return client.MemberAdd(forwardedContext(ctx), req)
	}

	// 本节点是 Leader，直接在状态机里处理成员添加请求
	if err := g.srv.HandleAddMember(req.GetId(), req.GetRaftAddr(), req.GetGrpcAddr()); err != nil {
		return nil, toStatus(err)
	}
	return &etcdpb.MemberAddResponse{}, nil
}

func (g *GRPCServer) MemberRemove(ctx context.Context, req *etcdpb.MemberRemoveRequest) (*etcdpb.MemberRemoveResponse, error) {
	if req.GetId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "id must be non-zero")
	}
	forward, err := g.checkForward(ctx)
	if err != nil {
		return nil, err
	}
	if forward {
		// 转发请求给 Leader，原样返回 Leader 的响应
		client, err := g.clusterClient()
		if err != nil {
			return nil, err
		}
		return client.MemberRemove(forwardedContext(ctx), req)
	}

	// 本节点是 Leader，直接在状态机里处理成员移除请求
	if err := g.srv.HandleRemoveMember(req.GetId()); err != nil {
		return nil, toStatus(err)
	}
	return &etcdpb.MemberRemoveResponse{}, nil
}

// ---------------------------------------------------------------------------
// 校验与转换
// ---------------------------------------------------------------------------

// validateKey 拒绝会让 MVCC 组合键产生歧义或没有意义的 key：
// 空 key，以及包含 \x00 的 key（存储层用 \x00 分隔用户 key 和 8 字节 revision）。
func validateKey(key string) error {
	if key == "" {
		return errors.New("key must not be empty")
	}
	if strings.ContainsRune(key, 0) {
		return errors.New("key must not contain NUL byte")
	}
	return nil
}

// invalidArgument 把内部错误包装成 gRPC InvalidArgument 错误码。
func invalidArgument(err error) error {
	return status.Error(codes.InvalidArgument, err.Error())
}

// toStatus 把内部错误映射成规范的 gRPC status code。
// 已经是 status.Error 的原样返回：转发场景下 Leader 的错误必须保持原语义。
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	// status.FromError 只会识别 status.Error 生成的 error，其他 error 都会被映射成 Internal。
	if _, ok := status.FromError(err); ok {
		return err
	}
	// 这里的映射规则参考了 etcd 的实现，尽量让客户端收到的错误码有意义。
	switch {
	case errors.Is(err, mvcc.ErrKeyNotFound), errors.Is(err, mvcc.ErrLeaseNotFound), errors.Is(err, server.ErrMemberNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, server.ErrMemberExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, server.ErrLastMember):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, raft.ErrProposalDropped):
		// 提议被丢弃通常意味着本节点已经不是 Leader，客户端应换节点重试。
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// comparesFromProto 把 protobuf 枚举转换成状态机使用的字符串运算符，
// 同时完成 key 校验。转换后的结果满足 types.ValidateTxnCommand。
func comparesFromProto(in []*etcdpb.Compare) ([]types.Compare, error) {
	out := make([]types.Compare, len(in))
	for i, cmp := range in {
		if err := validateKey(cmp.GetKey()); err != nil {
			return nil, fmt.Errorf("compares[%d]: %w", i, err)
		}
		op, err := compareOpFromProto(cmp.GetOp())
		if err != nil {
			return nil, fmt.Errorf("compares[%d]: %w", i, err)
		}
		out[i] = types.Compare{Key: cmp.GetKey(), Op: op, Value: cmp.GetValue()}
	}
	return out, nil
}

// compareOpFromProto 把 protobuf 枚举转换成状态机使用的字符串运算符。
func compareOpFromProto(op etcdpb.CompareOp) (string, error) {
	switch op {
	case etcdpb.CompareOp_COMPARE_OP_UNSPECIFIED, etcdpb.CompareOp_COMPARE_OP_EQUAL:
		return "==", nil
	case etcdpb.CompareOp_COMPARE_OP_NOT_EQUAL:
		return "!=", nil
	case etcdpb.CompareOp_COMPARE_OP_GREATER:
		return ">", nil
	case etcdpb.CompareOp_COMPARE_OP_GREATER_OR_EQUAL:
		return ">=", nil
	case etcdpb.CompareOp_COMPARE_OP_LESS:
		return "<", nil
	case etcdpb.CompareOp_COMPARE_OP_LESS_OR_EQUAL:
		return "<=", nil
	default:
		return "", fmt.Errorf("unsupported compare op %d", op)
	}
}

// operationsFromProto 把 protobuf 枚举转换成状态机使用的字符串操作类型，同时完成 key 校验。转换后的结果满足 types.ValidateTxnCommand。
func operationsFromProto(in []*etcdpb.Operation) ([]types.Operation, error) {
	out := make([]types.Operation, len(in))
	for i, op := range in {
		if err := validateKey(op.GetKey()); err != nil {
			return nil, fmt.Errorf("success/failure[%d]: %w", i, err)
		}
		if op.GetLeaseId() < 0 {
			return nil, fmt.Errorf("success/failure[%d]: lease must not be negative", i)
		}
		opType, err := operationTypeFromProto(op.GetOp())
		if err != nil {
			return nil, fmt.Errorf("success/failure[%d]: %w", i, err)
		}
		out[i] = types.Operation{
			Op:      opType,
			Key:     op.GetKey(),
			Value:   op.GetValue(),
			LeaseID: op.GetLeaseId(),
		}
	}
	return out, nil
}

// operationTypeFromProto 把 protobuf 枚举转换成状态机使用的字符串操作类型。
func operationTypeFromProto(op etcdpb.OperationType) (string, error) {
	switch op {
	case etcdpb.OperationType_OPERATION_TYPE_PUT:
		return "PUT", nil
	case etcdpb.OperationType_OPERATION_TYPE_DELETE:
		return "DELETE", nil
	default:
		return "", fmt.Errorf("unsupported operation type %d", op)
	}
}

// eventToProto 把状态机事件转换成 proto。
// DELETE 统一返回空 value：实时事件里携带的是存储层的墓碑哨兵值，
// 不应该泄漏给客户端，历史补发路径也已经统一成空字符串。
func eventToProto(event mvcc.Event) *etcdpb.Event {
	out := &etcdpb.Event{
		Key:      event.Key,
		Revision: event.Revision,
		Lease:    event.LeaseID,
	}
	// 设置 DELETE 事件的 Type 和 Value
	if event.Type == "DELETE" {
		out.Type = etcdpb.EventType_EVENT_TYPE_DELETE
		return out
	}
	// 设置 PUT 事件的 Type 和 Value
	out.Type = etcdpb.EventType_EVENT_TYPE_PUT
	out.Value = event.Value
	return out
}
