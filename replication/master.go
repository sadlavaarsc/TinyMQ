// Package replication 实现 TinyMQ 的主从复制与故障切换。
package replication

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"TinyMQ/storage"
)

// Master 是主节点，负责接收写请求并向 Slave 同步数据。
type Master struct {
	mu sync.RWMutex

	// addr 是主节点监听地址
	addr string

	// slaves 维护所有已连接的从节点
	slaves map[string]*SlaveConn

	// storage 是底层存储引擎
	storage *storage.Storage

	// listener 是 TCP 监听器
	listener net.Listener

	// ctx 用于优雅退出
	ctx    context.Context
	cancel context.CancelFunc
}

// SlaveConn 代表一个从节点连接。
type SlaveConn struct {
	ID       string
	Conn     net.Conn
	LastSync time.Time
	Offset   int64
}

// NewMaster 创建 Master 实例。
func NewMaster(addr string, st *storage.Storage) *Master {
	ctx, cancel := context.WithCancel(context.Background())
	return &Master{
		addr:    addr,
		slaves:  make(map[string]*SlaveConn),
		storage: st,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start 启动 Master 监听。
func (m *Master) Start() error {
	ln, err := net.Listen("tcp", m.addr)
	if err != nil {
		return fmt.Errorf("master listen failed: %w", err)
	}
	m.listener = ln

	go m.acceptLoop()
	go m.syncLoop()
	return nil
}

// Stop 停止 Master。
func (m *Master) Stop() {
	m.cancel()
	if m.listener != nil {
		m.listener.Close()
	}
}

// acceptLoop 接受 Slave 连接。
func (m *Master) acceptLoop() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			select {
			case <-m.ctx.Done():
				return
			default:
				continue
			}
		}
		go m.handleSlave(conn)
	}
}

// handleSlave 处理单个 Slave 连接。
func (m *Master) handleSlave(conn net.Conn) {
	// 简化实现：直接注册 Slave
	slaveID := conn.RemoteAddr().String()
	sc := &SlaveConn{
		ID:       slaveID,
		Conn:     conn,
		LastSync: time.Now(),
		Offset:   0,
	}

	m.mu.Lock()
	m.slaves[slaveID] = sc
	m.mu.Unlock()

	// 等待连接关闭
	buf := make([]byte, 1)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			break
		}
	}

	m.mu.Lock()
	delete(m.slaves, slaveID)
	m.mu.Unlock()
	conn.Close()
}

// syncLoop 定期向所有 Slave 推送增量数据。
func (m *Master) syncLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.mu.RLock()
			slaves := make([]*SlaveConn, 0, len(m.slaves))
			for _, sc := range m.slaves {
				slaves = append(slaves, sc)
			}
			m.mu.RUnlock()

			for _, sc := range slaves {
				// 实际场景中，此处读取存储增量数据并发送给 Slave
				sc.LastSync = time.Now()
			}
		case <-m.ctx.Done():
			return
		}
	}
}

// RegisterSlave 手动注册一个 Slave（用于测试或静态配置）。
func (m *Master) RegisterSlave(id string, conn net.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.slaves[id] = &SlaveConn{
		ID:       id,
		Conn:     conn,
		LastSync: time.Now(),
		Offset:   0,
	}
}

// SlaveCount 返回当前连接的 Slave 数量。
func (m *Master) SlaveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.slaves)
}
