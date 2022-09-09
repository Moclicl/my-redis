package database

import (
	"fmt"
	"my-redis/interface/redis"
	"my-redis/lib/logger"
	"my-redis/redis/protocol"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
)

type RedisDB struct {
	dbList []*atomic.Value
}

// 默认16个redis库
var databases = 16

func CreateRedisServer() *RedisDB {

	db := &RedisDB{}

	db.dbList = make([]*atomic.Value, databases)

	for i := range db.dbList {
		singleDB := MakeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		db.dbList[i] = holder
	}

	return db

}

func (r *RedisDB) Exec(c redis.Connection, cmd [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("数据库执行发生错误:%v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmd[0]))

	if cmdName == "flushall" {
		r.flushAll()
	} else if cmdName == "flushdb" {
		return r.flushDB(c.GetDBIndex())
	} else if cmdName == "select" {
		return execSelect(c, r, cmd[1:])
	}

	dbIndex := c.GetDBIndex()
	db, err := r.selectDB(dbIndex)
	if err != nil {
		return err
	}
	return db.Exec(c, cmd)

}

func execSelect(c redis.Connection, r *RedisDB, args [][]byte) redis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeStatusErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(r.dbList) || dbIndex < 0 {
		return protocol.MakeStatusErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return &protocol.OkReply{}
}

func (r *RedisDB) selectDB(dbIndex int) (*DB, *protocol.StatusErrReply) {
	if dbIndex >= len(r.dbList) || dbIndex < 0 {
		return nil, protocol.MakeStatusErrReply("ERR DB index is out of range")
	}
	return r.dbList[dbIndex].Load().(*DB), nil
}

func (r *RedisDB) Close() {
	panic("implement me")
}

func (r *RedisDB) ClientClose(c redis.Connection) {

}

func (r *RedisDB) flushAll() redis.Reply {
	for i := range r.dbList {
		r.flushDB(i)
	}
	//TODO aof rdb
	return &protocol.OkReply{}
}

func (r *RedisDB) flushDB(index int) redis.Reply {
	if index >= len(r.dbList) || index < 0 {
		return protocol.MakeStatusErrReply("ERR DB index is out of range")
	}
	newDB := MakeDB()
	r.loadDB(index, newDB)
	return &protocol.OkReply{}
}

func (r *RedisDB) loadDB(index int, newDB *DB) redis.Reply {
	//TODO aof
	newDB.index = index
	r.dbList[index].Store(newDB)
	return &protocol.OkReply{}
}
