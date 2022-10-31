package sortedset

type SortedSet struct {
	dict     map[string]*Element
	skipList *SkipList
}

func MakeSortedSet() *SortedSet {
	return &SortedSet{dict: make(map[string]*Element), skipList: MakeSkipList()}
}

func (s *SortedSet) Add(member string, score float64) bool {
	element, ok := s.dict[member]
	s.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	if ok {
		if score != element.Score {
			s.skipList.remove(member, element.Score)
			s.skipList.insert(member, score)
		}
		return false
	}

	s.skipList.insert(member, score)

	return true
}

func (s *SortedSet) Len() int64 {
	return int64(len(s.dict))
}

func (s *SortedSet) Range(start int64, stop int64, desc bool) []*Element {
	sliceSize := int(stop - start)
	slice := make([]*Element, sliceSize)
	i := 0
	s.ForEach(start, stop, desc, func(element *Element) bool {
		slice[i] = element
		i++
		return true
	})
	return slice
}

// [start,stop)
func (s *SortedSet) ForEach(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
	size := s.Len()

	var node *node

	if desc {
		node = s.skipList.tail
		if start > 0 {
			node = s.skipList.getByRank(size - start)
		}
	} else {
		node = s.skipList.header.level[0].forward
		if start > 0 {
			node = s.skipList.getByRank(start + 1)
		}
	}

	silceSize := int(stop - start)
	for i := 0; i < silceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}

}

func (s *SortedSet) Remove(member string) bool {
	v, ok := s.dict[member]
	if ok {
		s.skipList.remove(member, v.Score)
		delete(s.dict, member)
		return true
	}
	return false
}
