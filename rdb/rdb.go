package rdb

import (
	rdb "github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
	"io/ioutil"
	"my-redis/config"
	"my-redis/datastruct/dict"
	"my-redis/datastruct/list"
	"my-redis/datastruct/set"
	"my-redis/datastruct/sortedset"
	"my-redis/interface/database"
	"my-redis/lib/logger"
	"os"
	"strconv"
	"time"
)

type Handler struct {
	db database.DB
}

func NewHandler(db database.DB) *Handler {
	return &Handler{
		db: db,
	}
}

func (h *Handler) Rewrite2RDB() error {
	file, err := ioutil.TempFile("", "*.rdb")
	if err != nil {
		logger.Error("tmp file create failed")
		return err
	}

	err = h.startRewrite2RDB(file)

	rdbFileName := config.Properties.RDBFilename
	if rdbFileName == "" {
		rdbFileName = "dump.rdb"
	}

	err = file.Close()
	if err != nil {
		return err
	}
	err = os.Rename(file.Name(), rdbFileName)
	if err != nil {
		return err
	}
	return nil

}

func (h *Handler) startRewrite2RDB(file *os.File) error {
	encoder := rdb.NewEncoder(file).EnableCompress()
	err := encoder.WriteHeader()
	if err != nil {
		return err
	}

	auxMap := map[string]string{
		"redis-ver":    "6.0.0",
		"redis-bits":   "64",
		"aof-preamble": "0",
		"ctime":        strconv.FormatInt(time.Now().Unix(), 10),
	}
	for k, v := range auxMap {
		err := encoder.WriteAux(k, v)
		if err != nil {
			return err
		}
	}

	for i := 0; i < config.Properties.Databases; i++ {
		keyCount, ttlCount := h.db.GetDBSize(i)
		if keyCount == 0 {
			continue
		}
		err = encoder.WriteDBHeader(uint(i), uint64(keyCount), uint64(ttlCount))
		if err != nil {
			return err
		}

		var extErr error

		h.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			var opts []interface{}
			if expiration != nil {
				opts = append(opts, rdb.WithTTL(uint64(expiration.UnixNano()/1e6)))
			}
			switch obj := entity.Data.(type) {
			case []byte:
				err = encoder.WriteStringObject(key, obj, opts...)
			case list.List:
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(v interface{}) bool {
					bytes, _ := v.([]byte)
					vals = append(vals, bytes)
					return true
				})
				err = encoder.WriteListObject(key, vals, opts...)
			case dict.Dict:
				hash := make(map[string][]byte)
				obj.ForEach(func(key string, val interface{}) bool {
					bytes, _ := val.([]byte)
					hash[key] = bytes
					return true
				})
				err = encoder.WriteHashMapObject(key, hash, opts...)
			case *set.Set:
				vals := make([][]byte, obj.Len())
				obj.ForEach(func(member string) bool {
					vals = append(vals, []byte(member))
					return true
				})
				err = encoder.WriteSetObject(key, vals, opts...)
			case *sortedset.SortedSet:
				var vals []*model.ZSetEntry
				obj.ForEach(int64(0), obj.Len(), true, func(element *sortedset.Element) bool {
					vals = append(vals, &model.ZSetEntry{
						Member: element.Member,
						Score:  element.Score,
					})
					return true
				})
				err = encoder.WriteZSetObject(key, vals, opts...)
			}
			if err != nil {
				extErr = err
				return false
			}

			return true
		})
		if extErr != nil {
			return extErr
		}
	}
	err = encoder.WriteEnd()
	if err != nil {
		return err
	}
	return nil
}
