package list

import "container/list"

type SimpleList struct {
	data *list.List
	size int
}

func MakeSimpleList() *SimpleList {
	return &SimpleList{data: list.New()}
}

func (sl *SimpleList) Add(val interface{}) {
	sl.size++
	if sl.data.Len() == 0 {
		page := make([]interface{}, 0, 1024)
		page = append(page, val)
		sl.data.PushBack(page)
		return
	}

	backNode := sl.data.Back()
	backPage := backNode.Value.([]interface{})
	if len(backPage) == cap(backPage) {
		page := make([]interface{}, 0, 1024)
		page = append(page, val)
		sl.data.PushBack(page)
		return
	}
	backPage = append(backPage, val)
	backNode.Value = backPage
}

func (sl *SimpleList) Len() int {
	return sl.size
}

func (sl *SimpleList) Insert(index int, val interface{}) {
	if index == sl.size {
		sl.Add(val)
		return
	}
	it := sl.find(index)
	page := it.n.Value.([]interface{})
	offset := it.offset
	if len(page) < 1024 {
		page = append(page[:offset+1], page[offset:]...)
		page[offset] = val
		it.n.Value = page
		sl.size++
		return
	}

}

type iterator struct {
	n      *list.Element
	offset int
	list   *SimpleList
}

func (iter *iterator) get() interface{} {
	return iter.page()[iter.offset]
}

func (iter *iterator) page() []interface{} {
	return iter.n.Value.([]interface{})
}

func (iter *iterator) next() bool {
	page := iter.page()
	if iter.offset < len(page)-1 {
		iter.offset++
		return true
	}

	if iter.n == iter.list.data.Back() {
		iter.offset = len(page)
		return false
	}

	// 下一个块
	iter.offset = 0
	iter.n = iter.n.Next()
	return true

}

func (sl *SimpleList) find(index int) *iterator {
	if sl == nil {
		panic("list is nil")
	}
	if index < 0 || index >= sl.size {
		panic("index out of bound")
	}

	var n *list.Element
	var page []interface{}
	var pageCount int
	if index < sl.size/2 {
		// 从头开始遍历
		n = sl.data.Front()
		pageCount = 0
		for {
			page = n.Value.([]interface{})
			if pageCount+len(page) > index {
				break
			}
			pageCount += len(page)
			n = n.Next()
		}
	} else {
		// 从尾开始遍历
		n = sl.data.Back()
		pageCount = sl.size
		for {
			page = n.Value.([]interface{})
			pageCount -= len(page)
			if pageCount <= index {
				break
			}
			n = n.Next()
		}
	}
	offset := index - pageCount
	return &iterator{n: n, offset: offset, list: sl}

}

func (sl *SimpleList) Range(start int, end int) []interface{} {
	if start < 0 || start >= sl.Len() {
		panic("start out of range")
	}
	if end < start || end > sl.Len() {
		panic("end out of range")
	}

	size := end - start
	result := make([]interface{}, 0, size)
	it := sl.find(start)
	for i := 0; i < size; i++ {
		result = append(result, it.get())
		it.next()
	}
	return result
}

func (sl *SimpleList) Set(index int, value interface{}) {
	it := sl.find(index)
	it.page()[it.offset] = value
}

func (sl *SimpleList) RemoveAllByVal(expected Expected) int {
	iter := sl.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
		} else {
			iter.next()
		}
	}
	return removed
}

func (iter *iterator) atEnd() bool {
	if iter.list.Len() == 0 {
		return true
	}
	if iter.n != iter.list.data.Back() {
		return false
	}
	page := iter.page()
	// 是否到尾
	return iter.offset == len(page)
}

func (iter *iterator) remove() interface{} {
	page := iter.page()
	val := page[iter.offset]
	page = append(page[:iter.offset], page[iter.offset+1:]...)
	if len(page) > 0 {
		iter.n.Value = page
		if iter.offset == len(page) {
			if iter.n != iter.list.data.Back() {
				iter.n = iter.n.Next()
				iter.offset = 0
			}
		}
	} else {
		if iter.n == iter.list.data.Back() {
			iter.list.data.Remove(iter.n)
			iter.n = nil
			iter.offset = 0
		} else {
			nextNode := iter.n.Next()
			iter.list.data.Remove(iter.n)
			iter.n = nextNode
			iter.offset = 0
		}
	}
	iter.list.size--
	return val
}

func (sl *SimpleList) ForEach(consumer Consumer) {
	if sl == nil {
		panic("list is nil")
	}
	if sl.Len() == 0 {
		return
	}

	iter := sl.find(0)
	for {
		next := consumer(iter.get())
		if !next {
			break
		}
		if !iter.next() {
			break
		}
	}

}
