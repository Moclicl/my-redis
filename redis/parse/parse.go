package parse

import (
	"bufio"
	"errors"
	"io"
	"my-redis/interface/redis"
	"my-redis/lib/logger"
	"my-redis/redis/protocol"
	"runtime/debug"
	"strconv"
	"strings"
)

// 响应
type Payload struct {
	Data redis.Reply
	Err  error
}

type State struct {
	bulkLen          int64
	readingMultiBulk bool
	argsCount        int
	msgType          byte
	args             [][]byte
}

func (s *State) finished() bool {
	return s.argsCount > 0 && s.argsCount == len(s.args)
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse(reader, ch)
	return ch
}

func parse(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()
	var state State
	bufReader := bufio.NewReader(reader)
	for {
		msg, ioErr, err := readLine(bufReader, &state)
		if err != nil {
			if ioErr {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}

			ch <- &Payload{Err: err}
			state = State{}
			continue
		}
		// 正常解码
		if !state.readingMultiBulk {

			if msg[0] == '*' {
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{Err: err}
					state = State{}
					continue
				}
				if state.argsCount == 0 {
					ch <- &Payload{Data: &protocol.EmptyMultiBulkReply{}}
					state = State{}
					continue
				}
			} else if msg[0] == '$' {
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{Err: err}
					state = State{}
					continue
				}

			} else {
				res, err := parseSingleLine(msg)
				ch <- &Payload{Data: res, Err: err}
				state = State{}
				continue
			}

		} else {

			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{Err: err}
				state = State{}
				continue
			}

			if state.finished() {
				var res redis.Reply
				if state.msgType == '*' {
					res = protocol.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					res = protocol.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{Data: res, Err: err}
				state = State{}
			}

		}

	}
}

func readBody(msg []byte, state *State) error {

	var err error
	line := msg[0 : len(msg)-2]
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("格式错误" + string(msg))
		}
		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}

func readLine(bufReader *bufio.Reader, state *State) ([]byte, bool, error) {
	var msg []byte
	var err error
	if state.bulkLen == 0 {
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("格式错误" + string(msg))
		}
	} else {
		// 获得具体数据体
		bulkLen := state.bulkLen + 2
		msg = make([]byte, bulkLen)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

func parseMultiBulkHeader(msg []byte, state *State) error {
	line, err := strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("格式错误" + string(msg))
	}
	if line == 0 {
		state.argsCount = 0
		return nil
	} else if line > 0 {
		state.msgType = msg[0]
		state.readingMultiBulk = true
		state.argsCount = int(line)
		state.args = make([][]byte, 0, line)
		return nil
	} else {
		return errors.New("格式错误" + string(msg))
	}
}

func parseBulkHeader(msg []byte, state *State) error {
	line, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("格式错误" + string(msg))
	}
	if line > 0 {
		state.bulkLen = line
		state.msgType = msg[0]
		state.readingMultiBulk = true
		state.argsCount = 1
		state.args = make([][]byte, 0, 1)
	}
	return errors.New("格式错误" + string(msg))
}

func parseSingleLine(msg []byte) (redis.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var res redis.Reply
	switch msg[0] {
	case '+':
		res = protocol.MakeStatusReply(str[1:])
	case '-':
		res = protocol.MakeStatusErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("格式错误" + string(msg))
		}
		res = protocol.MakeStatusIntReply(val)
	}
	return res, nil
}
