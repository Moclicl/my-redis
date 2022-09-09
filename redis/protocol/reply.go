package protocol

import (
	"bytes"
	"strconv"
)

var CRLF = "\r\n"

// ---------- 状态返回 ----------

type StatusReply struct {
	Status string
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{Status: status}
}

// ---------- Err返回 ----------

type StatusErrReply struct {
	Status string
}

func (r *StatusErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func MakeStatusErrReply(status string) *StatusErrReply {
	return &StatusErrReply{Status: status}
}

// ---------- Int返回 ----------

type StatusIntReply struct {
	Code int64
}

func (r *StatusIntReply) ToBytes() []byte {
	return []byte("-" + strconv.FormatInt(r.Code, 10) + CRLF)
}

func MakeStatusIntReply(code int64) *StatusIntReply {
	return &StatusIntReply{Code: code}
}

// ---------- 数组返回 ----------

type MultiBulkReply struct {
	Args [][]byte
}

func (r *MultiBulkReply) ToBytes() []byte {
	_len := len(r.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(_len) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF)
			buf.WriteString(string(arg) + CRLF)
		}
	}
	return buf.Bytes()

}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{Args: args}
}

// ---------- 单个字符返回 ----------

type BulkReply struct {
	Arg []byte
}

func (r *BulkReply) ToBytes() []byte {
	if len(r.Arg) == 0 {
		return []byte("$-1")
	}

	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)

}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{Arg: arg}
}

// ---------- 错误信息返回 ----------
type UnknownErrReply struct{}

func (r *UnknownErrReply) ToBytes() []byte {
	return []byte("-Err unknown\r\n")
}

// ---------- PONG返回 ----------
type PongReply struct{}

func (r *PongReply) ToBytes() []byte {
	return []byte("+PONG\r\n")
}

// ---------- OK返回 ----------
type OkReply struct{}

func (r *OkReply) ToBytes() []byte {
	return []byte("+OK\r\n")
}
