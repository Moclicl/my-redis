package database

import "my-redis/interface/redis"

type DB interface {
	Exec(client redis.Connection, cmd [][]byte) redis.Reply
	Close()
	ClientClose(client redis.Connection)
}

type DataEntity struct {
	Data interface{}
}
