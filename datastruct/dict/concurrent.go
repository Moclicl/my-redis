package dict

import (
	"sync"
	"sync/atomic"
)

type CurrentDict struct {
	table      []*shard
	count      int32
	shardCount int
}

type shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

func MakeShard(shardCount int) *CurrentDict {

	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}

	return &CurrentDict{table: table, count: 0, shardCount: shardCount}

}

// hash算法
func fnv(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= uint32(16777619)
		hash ^= uint32(key[i])
	}
	return hash
}

// 获得下标
func (currentDict *CurrentDict) spread(code uint32) uint32 {
	tableSize := uint32(len(currentDict.table))
	return (tableSize - 1) & code
}

// 获得存储块
func (currentDict *CurrentDict) getShard(index uint32) *shard {
	return currentDict.table[index]
}

func (currentDict *CurrentDict) addCount() {

	atomic.AddInt32(&currentDict.count, 1)

}

func (currentDict *CurrentDict) Put(key string, val interface{}) (result int) {
	if currentDict == nil {
		panic("CurrentDict is null")
	}
	hashCode := fnv(key)
	index := currentDict.spread(hashCode)
	shard := currentDict.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 0
	}
	shard.m[key] = val
	currentDict.addCount()
	return 1
}

func (currentDict *CurrentDict) Get(key string) (val interface{}, exist bool) {
	if currentDict == nil {
		panic("CurrentDict is null")
	}
	hashCode := fnv(key)
	index := currentDict.spread(hashCode)
	shard := currentDict.getShard(index)
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()
	val, exist = shard.m[key]
	return
}

func (currentDict *CurrentDict) ForEach(consumer Consumer) {
	if currentDict == nil {
		panic("CurrentDict is null")
	}

	for _, shard := range currentDict.table {
		shard.mutex.RLock()
		func() {
			defer shard.mutex.RUnlock()
			for key, value := range shard.m {
				container := consumer(key, value)
				if !container {
					return
				}
			}
		}()
	}

}
