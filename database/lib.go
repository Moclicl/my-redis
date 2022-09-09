package database

import (
	"my-redis/interface/redis"
	"strings"
)

var cmdTable = make(map[string]*command)

type Cmd = [][]byte

type ExecFunc func(db *DB, args [][]byte) redis.Reply
type PreFunc func(args [][]byte) ([]string, []string)
type UndoFunc func(db *DB, args [][]byte) []Cmd

type command struct {
	executor ExecFunc
	prepare  PreFunc
	undo     UndoFunc
}

func RegisterCommand(name string, executor ExecFunc, prepare PreFunc, undo UndoFunc) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		prepare:  prepare,
		undo:     undo,
	}
}
