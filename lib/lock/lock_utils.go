package lock

import (
	"container/list"
	"sort"
	"sync"
)

type Locks struct {
	table []*sync.RWMutex
}

func (locks *Locks) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	slices := locks.toLockSlice(keys, false)
	writeSlices := locks.toLockSlice(writeKeys, false)

	writeIndexDict := make(map[uint32]bool)
	for _, dict := range writeSlices {
		writeIndexDict[dict] = true
	}

	for _, dict := range slices {
		_, val := writeIndexDict[dict]
		mu := locks.table[dict]
		if val {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}

}

func (locks Locks) RWUnlocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	slices := locks.toLockSlice(keys, false)
	writeSlices := locks.toLockSlice(writeKeys, true)

	writeIndexDict := make(map[uint32]bool)
	for _, dict := range writeSlices {
		writeIndexDict[dict] = true
	}

	for _, dict := range slices {
		_, val := writeIndexDict[dict]
		mu := locks.table[dict]
		if val {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}

func (locks *Locks) toLockSlice(keys []string, reverse bool) []uint32 {

	indexes := list.New()
	for _, key := range keys {
		indexes.PushBack(locks.spread(fnv32(key)))
	}

	slices := make([]uint32, indexes.Len())

	for val := indexes.Front(); val != nil; val.Next() {
		slices = append(slices, val.Value.(uint32))
	}

	sort.Slice(slices, func(i, j int) bool {
		if !reverse {
			return slices[i] < slices[j]
		} else {
			return slices[i] > slices[j]
		}
	})
	return slices
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= uint32(16777619)
		hash ^= uint32(key[i])
	}
	return hash
}

func (locks *Locks) spread(hashCode uint32) uint32 {
	if locks == nil {
		panic("locks is nil")
	}
	tableSize := uint32(len(locks.table))
	return (tableSize - 1) & hashCode
}
