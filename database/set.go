package database

import (
	HashSet "my-redis/datastruct/set"
	"my-redis/interface/database"
	"my-redis/interface/redis"
	"my-redis/redis/protocol"
)

func init() {
	RegisterCommand("SADD", execSAdd, writeFirstKey, nil, -3, false)
	RegisterCommand("SCARD", execSCard, writeFirstKey, nil, 2, true)
	RegisterCommand("SMEMBERS", execSMembers, writeFirstKey, nil, 2, true)
	RegisterCommand("SREM", execSRem, writeFirstKey, nil, -3, false)
}

func execSRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]
	set, err := db.getSet(key)
	if err != nil {
		return err
	}
	if set == nil {
		return protocol.MakeStatusErrReply("can not find the key")
	}

	count := 0
	for _, m := range values {
		count += set.Remove(string(m))
	}

	if set.Len() == 0 {
		db.Remove(key)
	}

	return protocol.MakeStatusIntReply(int64(count))

}

func execSMembers(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, err := db.getSet(key)
	if err != nil {
		return err
	}
	if set == nil {
		return protocol.MakeStatusErrReply("can not find the key")
	}
	i := 0
	arr := make([][]byte, set.Len())

	set.ForEach(func(member string) bool {
		arr[i] = []byte(member)
		i++
		return true
	})

	return protocol.MakeMultiBulkReply(arr)

}

func execSCard(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	set, err := db.getSet(key)
	if err != nil {
		return err
	}
	if set == nil {
		return protocol.MakeStatusErrReply("can not find the key")
	}

	return protocol.MakeStatusIntReply(int64(set.Len()))

}

func execSAdd(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]

	set, err := db.getOrInitSet(key)
	if err != nil {
		return err
	}
	count := 0
	for _, value := range values {
		count += set.Add(string(value))
	}

	return protocol.MakeStatusIntReply(int64(count))

}

func (db *DB) getOrInitSet(key string) (set *HashSet.Set, reply redis.Reply) {

	set, err := db.getSet(key)
	if err != nil {
		return nil, err
	}
	if set == nil {
		set = HashSet.MakeSet()
		db.PutEntity(key, &database.DataEntity{Data: set})
	}

	return set, nil

}

func (db *DB) getSet(key string) (set *HashSet.Set, reply redis.Reply) {

	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	data, ok := entity.Data.(*HashSet.Set)
	if !ok {
		return nil, protocol.MakeStatusErrReply("covert fail")
	}
	return data, nil
}
