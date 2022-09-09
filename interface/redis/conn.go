package redis

type Connection interface {
	GetDBIndex() int
	SelectDB(int)
}
