package connection

import (
	"net"
	"sync"
)

// connection to redis-cli
type Connection struct {
	conn    net.Conn
	mutex   sync.Mutex
	wg      sync.WaitGroup
	dbIndex int
}

func (conn *Connection) Close() error {
	_ = conn.conn.Close()
	return nil
}

func (conn *Connection) RemoteAddr() net.Addr {
	return conn.conn.RemoteAddr()
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

func (conn *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	conn.mutex.Lock()
	conn.wg.Add(1)
	defer func() {
		conn.wg.Done()
		conn.mutex.Unlock()
	}()

	_, err := conn.conn.Write(b)
	return err

}

func (conn *Connection) GetDBIndex() int {
	return conn.dbIndex
}

func (conn *Connection) SelectDB(index int) {
	conn.dbIndex = index
}
