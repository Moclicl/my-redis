package server

import (
	"context"
	database2 "my-redis/database"
	"my-redis/interface/database"
	"my-redis/lib/logger"
	"my-redis/redis/connection"
	"my-redis/redis/parse"
	"net"
	"sync"
	"sync/atomic"
)

type Handler struct {
	activeConn sync.Map
	db         database.DB
	close      atomic.Value
}

// 创建数据处理器
func MakeHandler() *Handler {
	var db database.DB
	db = database2.CreateRedisServer()
	return &Handler{db: db}
}

func (h *Handler) Handler(ctx context.Context, conn net.Conn) {

	if val := h.close.Load(); val == true {
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, 1)

	parse.ParseStream(conn)

}

func (h *Handler) Close() error {

	logger.Info("handler正在关闭...")
	h.close.Store(true)
	h.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	return nil
}
