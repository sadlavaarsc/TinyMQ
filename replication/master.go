// Package replication 实现主从复制
package replication

import (
	"fmt"
	"net"
	"sync"

	"github.com/sadlavaarsc/TinyMQ/broker"
)

// Master 主节点
type Master struct {
	mu      sync.RWMutex
	slaves  map[string]*SlaveConn // addr -> conn
	msgChan chan *broker.Message
}

// SlaveConn 从节点连接
type SlaveConn struct {
	Addr string
	Conn net.Conn
}

// NewMaster 创建主节点
func NewMaster() *Master {
	return &Master{
		slaves:  make(map[string]*SlaveConn),
		msgChan: make(chan *broker.Message, 1000),
	}
}

// AddSlave 添加从节点
func (m *Master) AddSlave(addr string, conn net.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.slaves[addr] = &SlaveConn{Addr: addr, Conn: conn}
	fmt.Println("[Master] slave added:", addr)
}

// Replicate 复制消息到所有从节点
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

// Start 启动复制监听
func (m *Master) Start() {
	for msg := range m.msgChan {
		m.Replicate(msg)
	}
}
