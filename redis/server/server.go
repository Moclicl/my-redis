package server

import (
	"context"
	"io"
	database2 "my-redis/database"
	"my-redis/interface/database"
	"my-redis/lib/logger"
	"my-redis/redis/connection"
	"my-redis/redis/parse"
	"my-redis/redis/protocol"
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

	ch := parse.ParseStream(conn)

	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF {
				h.closeClient(client)
				logger.Info("连接已关闭" + client.RemoteAddr().String())
				return
			}
			errReply := protocol.MakeStatusErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("连接已关闭" + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("数据为空")
			continue
		}

		val, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("数据错误")
			continue
		}
		result := h.db.Exec(client, val.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write([]byte("-ERR unknown\r\n"))
		}
	}

}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.ClientClose(client)
	h.activeConn.Delete(client)
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
