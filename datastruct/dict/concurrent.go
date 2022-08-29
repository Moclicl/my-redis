package dict

import "sync"

type CurrentDict struct {
	table      []*Shard
	count      int
	shardCount int
}

type Shard struct {
	m     map[string]interface{}
	mutex sync.Mutex
}

func MakeShard(shardCount int) *CurrentDict {

	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{
			m: make(map[string]interface{}),
		}
	}

	return &CurrentDict{table: table, count: 0, shardCount: shardCount}

}

func (currentDict *CurrentDict) Put(key string, val interface{}) (result int) {
	return
}

func (currentDict *CurrentDict) Get(key string) (val interface{}, exist bool) {
	return
}
