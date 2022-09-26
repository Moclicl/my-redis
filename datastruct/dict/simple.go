package dict

type SimpleDict struct {
	m map[string]interface{}
}

func MakeSimple() *SimpleDict {
	return &SimpleDict{m: make(map[string]interface{})}
}

func (dict *SimpleDict) Put(key string, val interface{}) (result int) {
	_, ok := dict.m[key]
	dict.m[key] = val
	if ok {
		return 0
	}
	return 1
}

func (dict *SimpleDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, ok := dict.m[key]
	if ok {
		return 0
	}
	dict.m[key] = val
	return 1
}

func (dict *SimpleDict) Get(key string) (val interface{}, exist bool) {
	val, ok := dict.m[key]
	return val, ok
}

func (dict *SimpleDict) ForEach(consumer Consumer) {
	for k, v := range dict.m {
		if !consumer(k, v) {
			break
		}
	}
}

func (dict *SimpleDict) Remove(key string) (result int) {
	_, ok := dict.m[key]
	delete(dict.m, key)
	if ok {
		return 1
	}
	return 0
}

func (dict *SimpleDict) Len() int {
	if dict.m == nil {
		panic("m is nil")
	}
	return len(dict.m)
}
