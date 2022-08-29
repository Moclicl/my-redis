package connection

import "net"

// connection to redis-cli
type Connection struct {
	conn net.Conn
}

func (conn *Connection) Close() error {
	_ = conn.conn.Close()
	return nil
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}
