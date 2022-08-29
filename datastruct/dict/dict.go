package dict

type Dict interface {
	Put(key string, val interface{}) (result int)
	Get(key string) (val interface{}, exist bool)
}
