package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"mini-etcd/internal/config"
	"net"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	// maxMessageSize 是允许接收的单帧最大长度。
	// Raft 普通消息不超过 MaxSizePerMsg（本项目中为 1MB），快照消息可能更大；
	// 这里留出充足余量，同时避免对端发来畸形长度头时一次性申请超大内存。
	maxMessageSize = 64 << 20 // 64MiB

	dialTimeout   = 500 * time.Millisecond // 建立连接的超时时间
	writeTimeout  = 1 * time.Second        // 单次写入的超时时间
	retryInterval = 1 * time.Second        // 连接失败后的重试冷却时间
)

// peerConn 包装 net.Conn，额外提供写锁。
// 即使将来 Send 从多个 goroutine 调用，同一条连接上的帧也不会互相交错。
type peerConn struct {
	netConn net.Conn
	connMu  sync.Mutex // 保护 Conn 的读写，避免并发读写导致数据错乱

	id    uint64              // 目标节点 ID
	sendC chan raftpb.Message // 异步发送消息的通道
	stopC chan struct{}       // 停止信号，用于唤醒阻塞中的发送 goroutine
}

// TCPTransport 是一个基于 TCP 的 Raft 消息传输实现
type TCPTransport struct {
	id       uint64              // 节点 ID
	peers    map[uint64]string   // 所有节点的 ID -> 网络地址映射
	listener net.Listener        // TCP 监听器
	recvc    chan raftpb.Message // 接收消息的通道
	done     chan struct{}       // 关闭信号，用于唤醒阻塞中的收发 goroutine

	mu         sync.RWMutex         // 保护 conns、failedTime、closed 的读写锁
	conns      map[uint64]*peerConn // 已建立的连接
	failedTime map[uint64]time.Time // 记录连接失败的时间，用于重试连接
	closed     bool                 // 是否已经关闭
	closeOnce  sync.Once            // 保证 Close 幂等
}

// NewTCPTransport 创建 TCPTransport 并立即完成端口监听。
// 监听失败会返回错误，而不是在后台 goroutine 里 log.Fatalf：
// 调用方需要明确知道节点是否真的开始监听，否则会误以为节点已就绪。
func NewTCPTransport(cfg config.Config) (*TCPTransport, error) {
	t := &TCPTransport{
		id:         cfg.ID,
		peers:      cfg.Peers,
		recvc:      make(chan raftpb.Message, 1024), // 缓冲区大小可调
		done:       make(chan struct{}),
		conns:      make(map[uint64]*peerConn),
		failedTime: make(map[uint64]time.Time),
	}

	// 先启动 TCP 监听器，再启动一个 goroutine 接受连接请求，确保 NewTCPTransport 返回时节点已经在监听端口。
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", cfg.ListenAddr, err)
	}
	t.listener = ln
	go t.acceptLoop()

	return t, nil
}

// acceptLoop 接受来自其他节点的连接请求
func (t *TCPTransport) acceptLoop() {
	for {
		conn, err := t.listener.Accept() // Accept 会阻塞直到有新的连接到来
		if err != nil {
			// Close 关闭监听器后 Accept 会立刻返回错误，此时必须退出，
			// 否则循环会以最高速度空转并刷屏日志。
			select {
			case <-t.done: // 关闭信号，退出循环
				return
			default: // 其他错误
			}
			if errors.Is(err, net.ErrClosed) {
				// 监听器已关闭，退出循环
				return
			}
			// 其他错误，打印日志并继续循环
			log.Printf("failed to accept connection: %v", err)
			continue
		}
		// 启动一个 goroutine 处理该连接
		go t.handleConn(conn)
	}
}

// handleConn 处理来自其他节点的连接，读取消息并发送到 recvc 通道
func (t *TCPTransport) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		var length uint32
		// binary.Read 会阻塞直到读取到完整的长度头，需要指定使用大端序读取
		if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) {
				log.Printf("failed to read message length from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}

		// 长度头非法说明字节流已经错位（或对端在发畸形数据），
		// 继续读只会读到垃圾，直接断开连接。长度上限同时防止超大内存分配。
		if length == 0 || length > maxMessageSize {
			log.Printf("invalid message length %d from %s, closing connection", length, conn.RemoteAddr())
			return
		}

		data := make([]byte, length)
		// io.ReadFull 会阻塞直到读取到完整的数据
		if _, err := io.ReadFull(conn, data); err != nil {
			log.Printf("failed to read message data from %s: %v", conn.RemoteAddr(), err)
			return
		}

		// 反序列化为 raftpb.Message
		var msg raftpb.Message
		if err := msg.Unmarshal(data); err != nil {
			log.Printf("failed to unmarshal message from %s: %v", conn.RemoteAddr(), err)
			return
		}

		// 丢弃目的地不是本节点的消息，避免配置错误时把别人的消息喂给本地 Raft。
		if msg.To != t.id {
			log.Printf("dropping message addressed to node %d (local node is %d)", msg.To, t.id)
			continue
		}

		// 将消息发送到 recvc 通道；关闭时退出，避免 goroutine 永久阻塞
		select {
		case t.recvc <- msg: // 成功发送到通道
		case <-t.done: // 关闭信号，退出循环
			return
		}
	}
}

// Send 方法用于发送 Raft 消息给其他节点
func (t *TCPTransport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.To == raft.None {
			// 忽略无效的目标节点
			continue
		}

		// 获取逻辑连接
		pc := t.getConn(m.To)
		if pc == nil {
			continue
		}

		// 非阻塞推入队列
		select {
		case pc.sendC <- m:
			// 入队成功
		default:
			// 队列已满（网络太慢），直接丢弃
			// 不要在这里阻塞，Raft 的心跳和重试机制会保证最终一致。
			log.Printf("[Node %d] send queue full for node %d, dropping message", t.id, m.To)
		}
	}
}

// Recv 返回一个接收消息的通道，供 Raft 节点使用
func (t *TCPTransport) Recv() <-chan raftpb.Message {
	return t.recvc
}

// Close 关闭 TCPTransport，释放资源。可重复调用。
func (t *TCPTransport) Close() {
	t.closeOnce.Do(func() { // 保证 Close 只执行一次
		t.mu.Lock()
		t.closed = true
		conns := t.conns                     // 复制当前连接，可以在释放锁后慢慢关闭，避免阻塞其他 goroutine
		t.conns = make(map[uint64]*peerConn) // 清空连接映射，避免其他 goroutine 再使用旧连接
		t.mu.Unlock()

		close(t.done) // 通知 acceptLoop 和 handleConn 退出

		// 关闭监听器
		if t.listener != nil {
			t.listener.Close()
		}

		// 关闭所有已建立的连接
		for _, pc := range conns {
			close(pc.stopC) // 通知 writeLoop 退出
			pc.connMu.Lock()
			if pc.netConn != nil {
				pc.netConn.Close() // 关闭底层 TCP
			}
			pc.connMu.Unlock()
		}
	})
}

// getConn 根据节点 ID 获取对应的 TCP 连接，如果不存在则创建新的连接
func (t *TCPTransport) getConn(to uint64) *peerConn {
	// 如果已经有逻辑连接，直接返回
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return nil
	}
	if pc, ok := t.conns[to]; ok {
		t.mu.RUnlock()
		return pc
	}
	t.mu.RUnlock()

	// 创建逻辑连接
	pc := &peerConn{
		id:    to,
		sendC: make(chan raftpb.Message, 1024), // 缓冲区大小可调
		stopC: make(chan struct{}),
	}

	// 在写锁内二次检查：Close 可能刚把 conns 换成了新 map，
	// 这时再插入会得到一个永远不会被 Close 回收的逻辑连接；
	// 并发调用 Send 时也可能重复创建，必须复用已有的那一个。
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	if existing, ok := t.conns[to]; ok {
		t.mu.Unlock()
		return existing
	}
	t.conns[to] = pc
	t.mu.Unlock()

	go t.writeLoop(pc) // 启动发送 goroutine

	return pc
}

// writeLoop 异步处理发送队列
func (t *TCPTransport) writeLoop(pc *peerConn) {
	for {
		select {
		case <-t.done:
			return
		case <-pc.stopC:
			return
		case m := <-pc.sendC:
			// 真正发送消息
			t.sendMsgToPeer(pc, m)
		}
	}
}

func (t *TCPTransport) sendMsgToPeer(pc *peerConn, m raftpb.Message) {
	data, err := m.Marshal()
	if err != nil {
		log.Printf("failed to marshal message to node %d: %v", pc.id, err)
		return
	}
	// 发送端也要限制大小：接收端对超过 maxMessageSize 的帧会直接断开连接，
	// 如果这里放行，超大消息会让两边陷入"断开-重连-再发同一条消息"的循环。
	if len(data) > maxMessageSize {
		log.Printf("message to node %d too large (%d bytes), dropped", pc.id, len(data))
		return
	}

	// 组帧
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(data)))
	copy(frame[4:], data)

	// 1. 获取物理连接（如果不存在或已断开，尝试重连）
	conn := t.dialIfNeeded(pc)
	if conn == nil {
		return // 连接不可用，丢弃这条消息
	}

	// 2. 写入数据（带超时）
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = conn.Write(frame)
	if err != nil {
		log.Printf("failed to write to node %d: %v", pc.id, err)
		// 只丢弃坏掉的物理连接，保留逻辑连接和它的 writeLoop：
		//   - 若把 pc 从 conns 里删掉，这个 writeLoop 就再也没人引用，
		//     会一直阻塞在 sendC 上，且 stopC 永远不关闭 -> goroutine/fd 泄漏；
		//   - 同时 Send 会为同一节点再建一个 pc，导致一个节点对应多条 TCP
		//     连接、两个发送 goroutine 并发写，消息顺序也无法保证。
		// 置 nil 后，下一条消息会在冷却期结束后由同一个 writeLoop 重新建连。
		pc.connMu.Lock()
		if pc.netConn != nil {
			pc.netConn.Close()
			pc.netConn = nil
		}
		pc.connMu.Unlock()
		t.recordFailure(pc.id)
	}
}

// dialIfNeeded 负责建立或获取物理连接，带失败冷却机制
func (t *TCPTransport) dialIfNeeded(pc *peerConn) net.Conn {
	pc.connMu.Lock()
	defer pc.connMu.Unlock()

	// 连接还在，直接用
	if pc.netConn != nil {
		return pc.netConn
	}

	// 检查冷却期；同时确认 transport 没有在拨号期间被关闭，
	// 否则 Close 之后 writeLoop 仍可能新建一条无人关闭的出站连接。
	t.mu.RLock()
	closed := t.closed
	lastFailed, failedBefore := t.failedTime[pc.id]
	t.mu.RUnlock()

	if closed {
		// transport 已关闭，放弃拨号
		return nil
	}
	if failedBefore && time.Since(lastFailed) < retryInterval {
		return nil // 冷却期内，放弃重试
	}

	addr, ok := t.peers[pc.id]
	if !ok || addr == "" {
		return nil
	}

	// 真正建立 TCP 连接
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		t.recordFailure(pc.id) // 记录失败时间
		log.Printf("failed to connect to node %d (%s): %v", pc.id, addr, err)
		return nil
	}

	pc.netConn = conn
	t.mu.Lock()
	delete(t.failedTime, pc.id) // 连接成功，清除冷却
	t.mu.Unlock()

	return conn
}

// recordFailure 记录一次建连/写失败，使 dialIfNeeded 在冷却期内不再重试
func (t *TCPTransport) recordFailure(to uint64) {
	t.mu.Lock()
	t.failedTime[to] = time.Now()
	t.mu.Unlock()
}
