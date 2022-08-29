package database

import "sync/atomic"

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

func (r *RedisDB) Exec() {
	panic("implement me")
}

func (r *RedisDB) Close() {
	panic("implement me")
}
