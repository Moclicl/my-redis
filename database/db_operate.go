package database

import (
	"my-redis/datastruct/dict"
	"my-redis/interface/redis"
	"my-redis/redis/protocol"
	"strings"
)

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
	prepare := cmd.prepare
	_, _ = prepare(cmdLine[1:])
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}
