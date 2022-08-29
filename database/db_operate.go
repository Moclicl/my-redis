package database

import "my-redis/datastruct/dict"

type DB struct {
	index int       //数据库下标
	data  dict.Dict //数据库操作
}

const (
	dataDictSize = 1 << 16
)

func MakeDB() *DB {

	return &DB{
		data: dict.MakeShard(dataDictSize),
	}
}
