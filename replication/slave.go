package replication

import (
	"fmt"
	"io"
	"net"
	"time"
)

// Slave 从节点
type Slave struct {
	masterAddr string
	conn       net.Conn
}

// NewSlave 创建从节点
func NewSlave(masterAddr string) *Slave {
	return &Slave{masterAddr: masterAddr}
}

// Sync 连接到主节点并同步数据
func (s *Slave) Sync() error {
	conn, err := net.DialTimeout("tcp", s.masterAddr, 5*time.Second)
	if err != nil {
		return err
	}
	s.conn = conn
	fmt.Println("[Slave] connected to master:", s.masterAddr)
	// 后台持续读取复制数据
	go s.receive()
	return nil
}

func (s *Slave) receive() {
	buf := make([]byte, 4096)
	for {
		n, err := s.conn.Read(buf)
		if err == io.EOF {
			fmt.Println("[Slave] master disconnected")
			return
		}
		if err != nil {
			return
		}
		// 处理复制的消息
		fmt.Println("[Slave] received", n, "bytes")
	}
}

// Close 关闭从节点连接
func (s *Slave) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
