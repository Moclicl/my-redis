package database

import (
	"my-redis/interface/redis"
	"my-redis/redis/protocol"
)

func init() {
	RegisterCommand("KEYS", execKeys, noPrepare, nil)
}

func execKeys(db *DB, args [][]byte) redis.Reply {
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		result = append(result, []byte(key))
		return true
	})
	return protocol.MakeMultiBulkReply(result)
}
