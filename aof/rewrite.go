package aof

import (
	"io"
	"io/ioutil"
	"my-redis/config"
	"my-redis/interface/database"
	"my-redis/lib/logger"
	"my-redis/lib/utils"
	"my-redis/redis/protocol"
	"os"
	"strconv"
	"time"
)

type RewriteCtx struct {
	tmpFile  *os.File
	fileSize int64
	dbIndex  int
}

func (h *Handler) Rewrite() error {
	ctx, err := h.startRewrite()
	if err != nil {
		return err
	}
	err = h.doRewrite(ctx)
	if err != nil {
		return err
	}
	h.finishRewrite(ctx)
	return nil
}

func (h Handler) startRewrite() (*RewriteCtx, error) {
	h.pausingAof.Lock()
	defer h.pausingAof.Unlock()

	err := h.aofFile.Sync()
	if err != nil {
		logger.Error("file sync failed")
		return nil, err
	}

	fileInfo, _ := os.Stat(h.aofFileName)
	fileSize := fileInfo.Size()

	file, _ := ioutil.TempFile("", "*.aof")
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: fileSize,
		dbIndex:  h.currentDB,
	}, nil
}

func (h *Handler) doRewrite(ctx *RewriteCtx) error {
	tmpFile := ctx.tmpFile
	for i := 0; i < config.Properties.Databases; i++ {
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}

		h.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			cmd := Entity2Cmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd := expire2Cmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}

func (h *Handler) finishRewrite(ctx *RewriteCtx) {

	h.pausingAof.Lock()
	defer h.pausingAof.Unlock()

	tmpFile := ctx.tmpFile
	src, _ := os.Open(h.aofFileName)

	defer func() {
		_ = src.Close()
	}()

	_, err := src.Seek(ctx.fileSize, 0)
	if err != nil {
		logger.Error("seek failed: " + err.Error())
		return
	}

	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIndex))).ToBytes()
	_, err = tmpFile.Write(data)
	if err != nil {
		logger.Error("tmp file rewrite failed: " + err.Error())
		return
	}

	_, err = io.Copy(tmpFile, src)
	if err != nil {
		logger.Error("copy aof filed failed: " + err.Error())
		return
	}

	_ = h.aofFile.Close()
	_ = os.Rename(tmpFile.Name(), h.aofFileName)

	aofFile, err := os.OpenFile(h.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	h.aofFile = aofFile

	// 再写一次保证
	data = protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(h.currentDB))).ToBytes()
	_, err = h.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
