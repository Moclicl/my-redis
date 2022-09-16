package database

import (
	"my-redis/interface/redis"
	"my-redis/redis/protocol"
)

func init() {
	RegisterCommand("ping", Ping, noPrepare, nil, 1, true)
}

func Ping(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &protocol.PongReply{}
	} else if len(args) == 1 {
		return protocol.MakeStatusReply(string(args[0]))
	} else {
		return protocol.MakeStatusErrReply("ERR wrong number of arguments for 'ping' command")
	}
}
