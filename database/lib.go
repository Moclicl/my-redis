package database

import (
	"my-redis/interface/redis"
	"strings"
)

var cmdTable = make(map[string]*command)

type Cmd = [][]byte

type ExecFunc func(db *DB, args [][]byte) redis.Reply // 执行命令
type PreFunc func(args [][]byte) ([]string, []string) // 执行前的准备,对key进行加锁
type UndoFunc func(db *DB, args [][]byte) []Cmd       // 仅在事务中使用

type command struct {
	executor ExecFunc
	prepare  PreFunc
	undo     UndoFunc
	num      int
	readOnly bool
}

func RegisterCommand(name string, executor ExecFunc, prepare PreFunc, undo UndoFunc, num int, readOnly bool) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		prepare:  prepare,
		undo:     undo,
		num:      num,
		readOnly: readOnly,
	}
}
