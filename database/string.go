package database

import (
	"my-redis/interface/database"
	"my-redis/interface/redis"
	"my-redis/redis/protocol"
)

func init() {
	RegisterCommand("SET", execSet, writeFirstKey, nil, 3, false)
	RegisterCommand("GET", execGet, writeFirstKey, nil, 2, true)
}

func execGet(db *DB, args [][]byte) redis.Reply {

	key := string(args[0])
	data, err := db.getString(key)
	if err != nil {
		return err
	}
	if data == nil {
		return &protocol.NullReply{}
	}
	return protocol.MakeBulkReply(data)

}

func execSet(db *DB, args [][]byte) redis.Reply {

	key := string(args[0])
	val := args[1]

	entity := &database.DataEntity{Data: val}

	result := db.PutEntity(key, entity)

	//TODO aof

	if result > 0 {
		return &protocol.OkReply{}
	}
	return &protocol.NullReply{}

}

func (db *DB) getString(key string) ([]byte, redis.Reply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, &protocol.NullReply{}
	}
	data, ok := entity.Data.([]byte)
	return data, nil
}
