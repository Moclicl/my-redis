package database

import (
	"my-redis/datastruct/sortedset"
	"my-redis/interface/database"
	"my-redis/interface/redis"
	"my-redis/redis/protocol"
	"strconv"
	"strings"
)

func init() {
	RegisterCommand("ZADD", execZAdd, writeFirstKey, nil, -4, false)
	RegisterCommand("ZCARD", execZCard, writeFirstKey, nil, -4, true)
	RegisterCommand("ZRANGE", execZRange, writeFirstKey, nil, -4, true)
	RegisterCommand("ZREM", execZRem, writeFirstKey, nil, -4, false)
}

func (db *DB) getOrInitSortedSet(key string) (*sortedset.SortedSet, redis.Reply) {
	sortedSet, _ := db.getSortedSet(key)
	if sortedSet == nil {
		sortedSet = sortedset.MakeSortedSet()
		db.PutEntity(key, &database.DataEntity{Data: sortedSet})
	}
	return sortedSet, nil
}

func (db *DB) getSortedSet(key string) (*sortedset.SortedSet, redis.Reply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	sortedSet, _ := entity.Data.(*sortedset.SortedSet)
	return sortedSet, nil
}

func execZAdd(db *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 1 {
		return protocol.MakeStatusErrReply("sortedset argument wrong")
	}
	key := string(args[0])
	size := (len(args) - 1) / 2
	elements := make([]*sortedset.Element, size)
	for i := 0; i < size; i++ {
		_score := args[2*i+1]
		member := string(args[2*i+2])
		score, err := strconv.ParseFloat(string(_score), 64)
		if err != nil {
			return protocol.MakeStatusErrReply("parseFloat fail")
		}
		elements[i] = &sortedset.Element{Score: score, Member: member}
	}

	sortedSet, err := db.getOrInitSortedSet(key)
	if err != nil {
		return err
	}
	i := 0
	for _, val := range elements {
		if sortedSet.Add(val.Member, val.Score) {
			i++
		}
	}

	return protocol.MakeStatusIntReply(int64(i))

}

func execZCard(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	sortedSet, errRep := db.getSortedSet(key)
	if errRep != nil {
		return errRep
	}
	return protocol.MakeStatusIntReply(sortedSet.Len())
}

func execZRange(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeStatusErrReply("ERR wrong number of arguments for 'zrange' command")
	}
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(string(args[3])) != "WITHSCORES" {
			return protocol.MakeStatusErrReply("syntax error")
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeStatusErrReply("ERR start not an integer")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeStatusErrReply("ERR stop not an integer")
	}
	return range0(db, key, start, stop, withScores, false)
}

func range0(db *DB, key string, start int64, stop int64, scores bool, b bool) redis.Reply {

}

func execZRem(db *DB, args [][]byte) redis.Reply {

}
