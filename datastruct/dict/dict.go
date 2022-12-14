package dict

type Consumer func(key string, val interface{}) bool

type Dict interface {
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int)
	Get(key string) (val interface{}, exist bool)
	ForEach(consumer Consumer)
	Remove(key string) (result int)
	Len() int
}
