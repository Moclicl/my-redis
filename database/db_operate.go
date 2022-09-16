package database

import (
	"my-redis/datastruct/dict"
	"my-redis/interface/database"
	"my-redis/interface/redis"
	"my-redis/redis/protocol"
	"strings"
)

type DB struct {
	index  int       //数据库下标
	data   dict.Dict //数据库操作
	ttlMap dict.Dict //过期时间表
}

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

func MakeDB() *DB {

	return &DB{
		data:   dict.MakeShard(dataDictSize),
		ttlMap: dict.MakeShard(ttlDictSize),
	}
}

func (db *DB) Exec(c redis.Connection, cmd [][]byte) redis.Reply {

	_ = strings.ToLower(string(cmd[0]))
	return db.execNormalCommand(cmd)
}

func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeStatusErrReply("ERR unknown command '" + cmdName + "'")
	}

	if !validateNum(cmd.num, cmdLine) {
		return protocol.MakeStatusErrReply("ERR wrong number of arguments")
	}
	prepare := cmd.prepare
	_, _ = prepare(cmdLine[1:])
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

func validateNum(num int, cmdLine [][]byte) bool {
	if num >= 0 {
		return len(cmdLine) == num
	}
	return len(cmdLine) >= -num
}

func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	val, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	//TODO key过期判断
	entity := val.(*database.DataEntity)
	return entity, true
}

func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, ok := db.data.Get(key)
		if ok {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Remove(key string) {
	db.data.Remove(key)
	db.ttlMap.Remove(key)
}
