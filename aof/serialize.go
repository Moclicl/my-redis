package aof

import (
	"my-redis/datastruct/dict"
	"my-redis/datastruct/list"
	"my-redis/datastruct/set"
	"my-redis/datastruct/sortedset"
	"my-redis/interface/database"
	"my-redis/redis/protocol"
	"strconv"
	"time"
)

func Entity2Cmd(key string, entity *database.DataEntity) *protocol.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *protocol.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = string2Cmd(key, val)
	case list.List:
		cmd = list2Cmd(key, val)
	case dict.Dict:
		cmd = hash2Cmd(key, val)
	case *set.Set:
		cmd = set2Cmd(key, val)
	case *sortedset.SortedSet:
		cmd = zSet2Cmd(key, val)
	}
	return cmd
}

var stringCmd = []byte("SET")

func string2Cmd(key string, val []byte) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = stringCmd
	args[1] = []byte(key)
	args[2] = val
	return protocol.MakeMultiBulkReply(args)
}

var listCmd = []byte("RPUSH")

func list2Cmd(key string, val list.List) *protocol.MultiBulkReply {
	args := make([][]byte, 2+val.Len())
	args[0] = listCmd
	args[1] = []byte(key)
	i := 0
	val.ForEach(func(v interface{}) bool {
		args[2+i] = v.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var hashCmd = []byte("HMSET")

func hash2Cmd(key string, val dict.Dict) *protocol.MultiBulkReply {
	args := make([][]byte, 2+val.Len()*2)
	args[0] = hashCmd
	args[1] = []byte(key)
	i := 0
	val.ForEach(func(key string, val interface{}) bool {
		args[2*i+2] = []byte(key)
		args[2*i+3] = val.([]byte)
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var setCmd = []byte("SADD")

func set2Cmd(key string, val *set.Set) *protocol.MultiBulkReply {
	args := make([][]byte, 2+val.Len())
	args[0] = setCmd
	args[1] = []byte(key)
	i := 0
	val.ForEach(func(member string) bool {
		args[2+2*i] = []byte(member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var zSetCmd = []byte("ZADD")

func zSet2Cmd(key string, val *sortedset.SortedSet) *protocol.MultiBulkReply {
	args := make([][]byte, 2+val.Len()*2)
	args[0] = zSetCmd
	args[1] = []byte(key)
	i := 0
	val.ForEach(int64(0), val.Len(), true, func(element *sortedset.Element) bool {
		value := strconv.FormatFloat(element.Score, 'f', -1, 64)
		args[2+2*i] = []byte(value)
		args[3+2*i] = []byte(element.Member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var expireCmd = []byte("PEXPIREAT")

func expire2Cmd(key string, expireAt time.Time) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = expireCmd
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return protocol.MakeMultiBulkReply(args)
}
