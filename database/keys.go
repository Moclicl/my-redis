package database

import (
	"my-redis/datastruct/dict"
	"my-redis/datastruct/list"
	"my-redis/datastruct/set"
	"my-redis/datastruct/sortedset"
	"my-redis/interface/redis"
	"my-redis/redis/protocol"
	"time"
)

func init() {
	RegisterCommand("KEYS", execKeys, noPrepare, nil, 2, true)
	RegisterCommand("type", execType, noPrepare, nil, 2, true)
	RegisterCommand("TTL", execTTL, noPrepare, nil, 2, true)
	RegisterCommand("DEL", execDel, noPrepare, nil, 2, true)
}

func execDel(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	deleted := db.Removes(keys...)
	//TODO AOF
	return protocol.MakeStatusIntReply(int64(deleted))
}

func execTTL(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, ok := db.GetEntity(key)
	if !ok {
		return protocol.MakeStatusIntReply(-2)
	}
	val, ok := db.ttlMap.Get(key)
	if !ok {
		return protocol.MakeStatusIntReply(-1)
	}
	expireTime, _ := val.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return protocol.MakeStatusIntReply(int64(ttl / time.Second))
}

func execType(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	entity, ok := db.GetEntity(key)
	if !ok {
		return protocol.MakeStatusReply("nil")
	}
	switch entity.Data.(type) {
	case []byte:
		return protocol.MakeStatusReply("string")
	case list.List:
		return protocol.MakeStatusReply("list")
	case dict.Dict:
		return protocol.MakeStatusReply("hash")
	case *set.Set:
		return protocol.MakeStatusReply("set")
	case *sortedset.SortedSet:
		return protocol.MakeStatusReply("zset")
	}
	return &protocol.UnknownErrReply{}
}

func execKeys(db *DB, args [][]byte) redis.Reply {
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		result = append(result, []byte(key))
		return true
	})
	return protocol.MakeMultiBulkReply(result)
}
