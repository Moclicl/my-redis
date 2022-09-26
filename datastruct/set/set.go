package set

import "my-redis/datastruct/dict"

type Set struct {
	dict dict.Dict
}

func (s *Set) Add(m string) int {
	return s.dict.Put(m, nil)
}

func (s *Set) Len() int {
	return s.dict.Len()
}

func MakeSet(member ...string) *Set {
	set := &Set{dict: dict.MakeSimple()}

	for _, m := range member {
		set.Add(m)
	}

	return set

}

func (s *Set) ForEach(consumer func(member string) bool) {
	s.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

func (s *Set) Remove(key string) int {
	return s.dict.Remove(key)
}
