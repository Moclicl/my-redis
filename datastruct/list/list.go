package list

type Expected func(a interface{}) bool
type Consumer func(v interface{}) bool

type List interface {
	Add(val interface{})
	Len() int
	Insert(index int, val interface{})
	Range(start int, end int) []interface{}
	Set(index int, value interface{})
	RemoveAllByVal(expected Expected) int
	ForEach(consumer Consumer)
}
