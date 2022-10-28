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
