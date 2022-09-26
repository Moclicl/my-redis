package database

import (
	List "my-redis/datastruct/list"
	"my-redis/interface/database"
	"my-redis/interface/redis"
	"my-redis/lib/utils"
	"my-redis/redis/protocol"
	"strconv"
)

func init() {
	RegisterCommand("LPUSH", execLPush, writeFirstKey, nil, -3, false)
	RegisterCommand("LRANGE", execLRange, writeFirstKey, nil, 4, true)
	RegisterCommand("LLEN", execLLen, writeFirstKey, nil, 2, true)
	RegisterCommand("LSET", execLSet, writeFirstKey, nil, 4, false)
	RegisterCommand("LREM", execLRem, writeFirstKey, nil, 4, false)
}

func (db *DB) getList(key string) (List.List, redis.Reply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	list, ok := entity.Data.(List.List)
	if !ok {
		return nil, protocol.MakeStatusErrReply("convert fail")
	}
	return list, nil

}

func (db *DB) getOrInitList(key string) (list List.List, reply redis.Reply) {
	list, err := db.getList(key)
	if err != nil {
		return nil, err
	}
	if list == nil {
		list = List.MakeSimpleList()
		db.PutEntity(key, &database.DataEntity{Data: list})
	}
	return list, nil
}

func execLPush(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	values := args[1:]

	list, err := db.getOrInitList(key)
	if err != nil {
		return err
	}

	for _, value := range values {
		list.Insert(0, value)
	}

	return protocol.MakeStatusIntReply(int64(list.Len()))

}

func execLRange(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	start64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeStatusErrReply("fail to parse int")
	}
	start := int(start64)
	end64, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeStatusErrReply("fail to parse int")
	}
	end := int(end64)

	list, errReply := db.getList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	size := list.Len()
	if start < size*-1 {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return &protocol.EmptyMultiBulkReply{}
	}

	if end < end*-1 {
		end = 0
	} else if end < 0 {
		end = size + end + 1
	} else if end < size {
		end += 1
	} else {
		end = size
	}

	if end < start {
		end = start
	}

	slice := list.Range(start, end)
	result := make([][]byte, len(slice))
	for i, raw := range slice {
		bytes, _ := raw.([]byte)
		result[i] = bytes
	}

	return protocol.MakeMultiBulkReply(result)

}

func execLLen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	list, err := db.getList(key)
	if err != nil {
		return err
	}
	if list == nil {
		return protocol.MakeStatusIntReply(0)
	}

	size := int64(list.Len())
	return protocol.MakeStatusIntReply(size)

}

func execLSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	index64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeStatusErrReply("parse int fail")
	}
	index := int(index64)
	value := args[2]

	list, errReply := db.getList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeStatusErrReply("no suck key")
	}
	size := list.Len()
	if index < size*-1 {
		return protocol.MakeStatusErrReply("index out of bound")
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return protocol.MakeStatusErrReply("index out of bound")
	}
	list.Set(index, value)
	return &protocol.OkReply{}

}

func execLRem(db *DB, args [][]byte) redis.Reply {

	key := string(args[0])

	count64, err := strconv.ParseInt(string(args[1]), 10, 64)

	if err != nil {
		return protocol.MakeStatusErrReply("parse int fail")
	}
	count := int(count64)
	value := args[2]

	list, errReply := db.getList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return protocol.MakeStatusErrReply("no suck key")
	}

	removed := 0
	if count == 0 {
		removed = list.RemoveAllByVal(func(a interface{}) bool {
			return utils.Equals(a, value)
		})
	}

	if list.Len() == 0 {
		db.Remove(key)
	}

	return protocol.MakeStatusIntReply(int64(removed))

}
