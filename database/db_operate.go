package database

import (
	"my-redis/datastruct/dict"
	"my-redis/interface/database"
	"my-redis/interface/redis"
	"my-redis/lib/lock"
	"my-redis/lib/time_utils"
	"my-redis/redis/protocol"
	"strings"
	"time"
)

type DB struct {
	index  int            //数据库下标
	data   dict.Dict      //数据库操作
	ttlMap dict.Dict      //过期时间表
	aof    func([][]byte) //AOF
	locker *lock.Locks
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

func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.locker.RWLocks(writeKeys, readKeys)
}

func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.locker.RWUnlocks(writeKeys, readKeys)
}

func genExpireTask(key string) string {
	return "expire" + key
}

func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	time_utils.Add(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			return
		}
		expireTime, _ = rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			db.Remove(key)
		}
	})
}

func (db DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, val interface{}) bool {
		entity, _ := val.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}
		return cb(key, entity, expiration)
	})
}
