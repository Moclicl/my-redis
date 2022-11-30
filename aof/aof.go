package aof

import (
	"my-redis/config"
	"my-redis/interface/database"
	"my-redis/lib/logger"
	"my-redis/lib/utils"
	"my-redis/redis/protocol"
	"os"
	"strconv"
	"sync"
)

const aofQueueSize = 1 << 16

type payload struct {
	dbIndex int
	cmdLine [][]byte
}

type Handler struct {
	db          database.DB
	aofChan     chan *payload
	aofFileName string
	aofFile     *os.File
	aofFinished chan bool
	currentDB   int
	pausingAof  *sync.RWMutex
}

func NewAOFHandler(db database.DB) (*Handler, error) {
	handler := &Handler{}
	handler.db = db
	handler.aofChan = make(chan *payload, aofQueueSize)
	handler.aofFileName = config.Properties.AppendFilename
	aofFile, err := os.OpenFile(handler.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofFinished = make(chan bool)
	return handler, nil
}

func (h *Handler) AddAof(dbIndex int, cmdLine [][]byte) {
	if config.Properties.AppendOnly && h.aofChan != nil {
		h.aofChan <- &payload{
			dbIndex: dbIndex,
			cmdLine: cmdLine,
		}
	}
}

func (h *Handler) HandleAof() {
	h.currentDB = 0
	for p := range h.aofChan {
		h.pausingAof.RLock()
		if p.dbIndex != h.currentDB {
			data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := h.aofFile.Write(data)
			if err != nil {
				logger.Error(err)
				continue
			}
			h.currentDB = p.dbIndex
		}
		data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := h.aofFile.Write(data)
		if err != nil {
			logger.Error(err)
		}
		h.pausingAof.RUnlock()
	}
	h.aofFinished <- true
}

func (h *Handler) Close() {
	if h.aofFile != nil {
		close(h.aofChan)
		<-h.aofFinished
		err := h.aofFile.Close()
		if err != nil {
			logger.Error(err)
		}
	}
}
