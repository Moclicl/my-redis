package database

import (
	"my-redis/interface/redis"
	"time"
)

type DB interface {
	Exec(client redis.Connection, cmd [][]byte) redis.Reply
	Close()
	ClientClose(client redis.Connection)
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)
	GetDBSize(dbIndex int) (int, int)
}

type DataEntity struct {
	Data interface{}
}
