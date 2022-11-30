package database

import (
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
	rdb "github.com/hdt3213/rdb/parser"
	"my-redis/config"
	"my-redis/datastruct/dict"
	_list "my-redis/datastruct/list"
	_set "my-redis/datastruct/set"
	"my-redis/datastruct/sortedset"
	"my-redis/interface/database"
	"my-redis/lib/logger"
	"os"
)

func loadRdbFile(mdb *RedisDB) {
	rdbFile, err := os.Open(config.Properties.RDBFilename)
	if err != nil {
		logger.Error("open rdb file failed" + err.Error())
		return
	}

	defer func() {
		_ = rdbFile.Close()
	}()
	decoder := rdb.NewDecoder(rdbFile)
	err = dumpRDB(decoder, mdb)
	if err != nil {
		logger.Error("dump rdb file failed" + err.Error())
		return
	}

}

func dumpRDB(decoder *core.Decoder, mdb *RedisDB) error {
	return decoder.Parse(func(obj model.RedisObject) bool {
		db := mdb.multiSelectDB(obj.GetDBIndex())
		switch obj.GetType() {
		case rdb.StringType:
			str := obj.(*rdb.StringObject)
			db.PutEntity(obj.GetKey(), &database.DataEntity{Data: str})
		case rdb.ListType:
			listObj := obj.(*rdb.ListObject)
			list := _list.MakeSimpleList()
			for _, v := range listObj.Values {
				list.Add(v)
			}
			db.PutEntity(obj.GetKey(), &database.DataEntity{Data: list})
		case rdb.HashType:
			hashObj := obj.(*rdb.HashObject)
			hash := dict.MakeSimple()
			for k, v := range hashObj.Hash {
				hash.Put(k, v)
			}
			db.PutEntity(obj.GetKey(), &database.DataEntity{Data: hash})
		case rdb.SetType:
			setObj := obj.(*rdb.SetObject)
			set := _set.MakeSet()
			for _, v := range setObj.Members {
				set.Add(string(v))
			}
			db.PutEntity(obj.GetKey(), &database.DataEntity{Data: set})
		case rdb.ZSetType:
			zsetObj := obj.(*rdb.ZSetObject)
			zset := sortedset.MakeSortedSet()
			for _, v := range zsetObj.Entries {
				zset.Add(v.Member, v.Score)
			}
			db.PutEntity(obj.GetKey(), &database.DataEntity{Data: zset})
		}
		if obj.GetExpiration() != nil {
			db.Expire(obj.GetKey(), *obj.GetExpiration())
		}
		return true
	})
}
