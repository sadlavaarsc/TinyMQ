// Package replication 实现 TinyMQ 的主从复制与故障切换。
package replication

import (
	"fmt"
	"net"
	"sync"

	"TinyMQ/broker"
)

// Master 是主节点，负责接收写请求并向 Slave 同步数据。
type Master struct {
	mu      sync.RWMutex
	slaves  map[string]*SlaveConn // addr -> conn
	msgChan chan *broker.Message
}

// SlaveConn 代表一个从节点连接。
type SlaveConn struct {
	Addr string
	Conn net.Conn
}

// NewMaster 创建主节点。
func NewMaster() *Master {
	return &Master{
		slaves:  make(map[string]*SlaveConn),
		msgChan: make(chan *broker.Message, 1000),
	}
}

// AddSlave 添加从节点。
func (m *Master) AddSlave(addr string, conn net.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.slaves[addr] = &SlaveConn{Addr: addr, Conn: conn}
	fmt.Println("[Master] slave added:", addr)
}

// Replicate 复制消息到所有从节点。
func (m *Master) Replicate(msg *broker.Message) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for addr, slave := range m.slaves {
		// 简化：直接发送序列化消息
		_, err := slave.Conn.Write(msg.Payload)
		if err != nil {
			fmt.Println("[Master] replicate to", addr, "failed:", err)
		}
	}
}

// Start 启动复制监听，循环处理 msgChan 中的消息。
func (m *Master) Start() {
	for msg := range m.msgChan {
		m.Replicate(msg)
	}
}
