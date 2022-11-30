package database

import (
	"my-redis/datastruct/dict"
	"my-redis/interface/database"
	"my-redis/interface/redis"
	"my-redis/lib/utils"
	"my-redis/redis/protocol"
)

func init() {
	RegisterCommand("HSET", execHSet, writeFirstKey, nil, 4, false)
	RegisterCommand("HGET", execHGet, writeFirstKey, nil, 3, true)
	RegisterCommand("HSETNX", execHSetNX, writeFirstKey, nil, 4, false)
	RegisterCommand("HGETALL", execHGetAll, writeFirstKey, nil, 2, true)
}

func (db DB) getDict(key string) (dict.Dict, redis.Reply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, protocol.MakeStatusErrReply("get entity fail")
	}
	_dict, ok := entity.Data.(dict.Dict)
	if !ok {
		return nil, protocol.MakeStatusErrReply("get dict fail")
	}
	return _dict, nil
}

func (db *DB) getOrInitDict(key string) (dict.Dict, bool, redis.Reply) {
	_dict, _ := db.getDict(key)
	var initialized = false
	if _dict == nil {
		_dict = dict.MakeSimple()
		db.PutEntity(key, &database.DataEntity{Data: _dict})
		initialized = true
	}
	return _dict, initialized, nil

}

func execHSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	_dict, _, err := db.getOrInitDict(key)
	if err != nil {
		return err
	}

	res := _dict.Put(field, value)
	db.aof(utils.ToCmdLine2("hset", args...))
	return protocol.MakeStatusIntReply(int64(res))

}

func execHSetNX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	value := args[2]

	_dict, _, err := db.getOrInitDict(key)
	if err != nil {
		return err
	}
	res := _dict.PutIfAbsent(field, value)
	db.aof(utils.ToCmdLine2("hsetnx", args...))
	return protocol.MakeStatusIntReply(int64(res))

}

func execHGetAll(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	_dict, err := db.getDict(key)

	if err != nil {
		return err
	}
	if _dict == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	size := _dict.Len()
	result := make([][]byte, size*2)
	i := 0
	_dict.ForEach(func(key string, val interface{}) bool {
		result[i] = []byte(key)
		i++
		result[i], _ = val.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(result)

}

func execHGet(db *DB, args [][]byte) redis.Reply {

	key := string(args[0])
	field := string(args[1])

	_dict, err := db.getDict(key)
	if err != nil {
		return err
	}
	if _dict == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	val, ok := _dict.Get(field)
	if !ok {
		return &protocol.NullReply{}
	}

	value := val.([]byte)
	return protocol.MakeBulkReply(value)

}
