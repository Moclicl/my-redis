package database

import (
	"fmt"
	"my-redis/aof"
	"my-redis/config"
	"my-redis/interface/database"
	"my-redis/interface/redis"
	"my-redis/lib/logger"
	"my-redis/rdb"
	"my-redis/redis/protocol"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type RedisDB struct {
	dbList []*atomic.Value

	rdbHandler *rdb.Handler
	aofHandler *aof.Handler
}

// 默认16个redis库
var databases = 16

func CreateRedisServer() *RedisDB {

	db := &RedisDB{}

	if config.Properties.Databases == 0 {
		config.Properties.Databases = databases
	}
	db.dbList = make([]*atomic.Value, config.Properties.Databases)

	for i := range db.dbList {
		singleDB := MakeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		db.dbList[i] = holder
	}

	db.rdbHandler = rdb.NewHandler(db)

	aofOpen := false
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(db)
		if err != nil {
			panic(err)
		}
		db.aofHandler = aofHandler
		for _, mdb := range db.dbList {
			singleDB := mdb.Load().(*DB)
			singleDB.aof = func(line [][]byte) {
				db.aofHandler.AddAof(singleDB.index, line)
			}
		}
		aofOpen = true
	}

	if config.Properties.RDBFilename != "" && !aofOpen {
		loadRdbFile(db)
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
	} else if cmdName == "save" {
		return SaveRDB(r)
	} else if cmdName == "bgsave" {
		return BGSaveRDB(r)
	} else if cmdName == "bgrewriteaof" {
		return BGRewriteAOF(r)
	} else if cmdName == "rewriteaof" {
		return RewriteAOF(r)
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

func (r *RedisDB) multiSelectDB(dbIndex int) *DB {
	db, err := r.selectDB(dbIndex)
	if err != nil {
		panic(err)
	}
	return db
}

func (r *RedisDB) Close() {
	if r.aofHandler != nil {
		r.aofHandler.Close()
	}
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

func (r *RedisDB) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	r.multiSelectDB(dbIndex).ForEach(cb)
}

func (r *RedisDB) GetDBSize(dbIndex int) (int, int) {
	db := r.multiSelectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
}

func SaveRDB(db *RedisDB) redis.Reply {
	err := db.rdbHandler.Rewrite2RDB()
	if err != nil {
		logger.Error(err)
	}
	return &protocol.OkReply{}
}

func BGSaveRDB(db *RedisDB) redis.Reply {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error(err)
			}
		}()
		err := db.rdbHandler.Rewrite2RDB()
		if err != nil {
			logger.Error(err)
		}
	}()
	return protocol.MakeStatusReply("Background saving started")
}

func BGRewriteAOF(r *RedisDB) redis.Reply {
	go r.aofHandler.Rewrite()
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

func RewriteAOF(r *RedisDB) redis.Reply {
	err := r.aofHandler.Rewrite()
	if err != nil {
		return protocol.MakeStatusErrReply(err.Error())
	}
	return &protocol.OkReply{}
}
