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
	net.Conn
	writeMu sync.Mutex
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

		// 将消息序列化为字节流
		data, err := m.Marshal()
		if err != nil {
			log.Printf("failed to marshal message to node %d: %v", m.To, err)
			continue
		}
		if len(data) > maxMessageSize {
			// 消息过大，直接丢弃。Raft 会通过重试/心跳重新发送。
			log.Printf("message to node %d too large (%d bytes), dropped", m.To, len(data))
			continue
		}

		// 根据 m.To 找到目标节点地址
		conn := t.getConn(m.To)
		if conn == nil {
			// 连接不可用：丢弃该消息，Raft 会通过重试/心跳重新发送。
			// 这里不打印日志，避免对端长期宕机时按心跳频率刷屏；
			// getConn 对建连失败已经做了限频日志。
			continue
		}

		// 长度头和消息体合并成一次 Write：既避免两次写之间被其他写入打断，
		// 也保证接收端不会读到半截帧。
		frame := make([]byte, 4+len(data))
		binary.BigEndian.PutUint32(frame[:4], uint32(len(data)))
		copy(frame[4:], data)

		// 写超时必不可少：对端主机失联（半开连接）时，TCP 重传可能让
		// conn.Write 阻塞数分钟，而 Send 是在 Raft 的单线程主循环里调用的，
		// 会连带卡死 Tick、Ready 处理以及对其他节点的回复。
		conn.writeMu.Lock()
		_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		_, err = conn.Write(frame)
		conn.writeMu.Unlock()
		if err != nil {
			log.Printf("failed to write message to node %d: %v", m.To, err)
			// 连接已不可用（可能只写了一半），关闭后由下一次 Send 重新建连
			t.closeConn(m.To, conn)
			continue
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
		for _, conn := range conns {
			conn.Close()
		}
	})
}

// getConn 根据节点 ID 获取对应的 TCP 连接，如果不存在则创建新的连接
func (t *TCPTransport) getConn(to uint64) *peerConn {
	// 如果已经有连接，直接返回
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return nil
	}
	conn, ok := t.conns[to]
	lastFailed, failedBefore := t.failedTime[to]
	t.mu.RUnlock()
	if ok {
		return conn
	}

	// 如果之前连接失败，且在冷却期内，直接放弃重连
	if failedBefore && time.Since(lastFailed) < retryInterval {
		return nil
	}

	addr, ok := t.peers[to]
	if !ok || addr == "" {
		t.recordFailure(to)
		log.Printf("no address configured for node %d", to)
		return nil
	}

	newConn, err := net.DialTimeout("tcp", addr, dialTimeout) // 建立 TCP 连接
	if err != nil {
		// 记录失败时间，进入冷却期，避免每条消息都阻塞在 Dial 上
		t.recordFailure(to)
		// 只在这里（冷却期过后）打印日志
		log.Printf("failed to connect to node %d (%s): %v", to, addr, err)
		return nil
	}
	pc := &peerConn{Conn: newConn}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		// 建连期间 Close 被调用，不要再把连接放回池子
		newConn.Close()
		return nil
	}
	// Double check (防止并发时重复创建)
	if existing, ok := t.conns[to]; ok {
		newConn.Close()
		return existing
	}
	// 连接成功，清除失败时间记录
	delete(t.failedTime, to)
	t.conns[to] = pc
	return pc
}

// recordFailure 记录一次建连失败，使 getConn 在冷却期内不再重试
func (t *TCPTransport) recordFailure(to uint64) {
	t.mu.Lock()
	t.failedTime[to] = time.Now()
	t.mu.Unlock()
}

// closeConn 关闭 to 的连接：只有 conn 仍是当前在用的那条连接时才关闭并移除，
// 避免误关其他 goroutine 新建的连接。
func (t *TCPTransport) closeConn(to uint64, conn *peerConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.conns[to]; ok && cur == conn {
		cur.Close()
		delete(t.conns, to)
	}
}
