package sortedset

import "math/rand"

type SkipList struct {
	header *node
	tail   *node
	length int64
	level  int16
}

type Level struct {
	forward *node
	span    int64
}

type node struct {
	Element
	backward *node
	level    []*Level
}

type Element struct {
	Score  float64
	Member string
}

const maxLevel = 16

func MakeSkipList() *SkipList {
	return &SkipList{level: 1, header: makeNode(1, 0, "")}
}

func makeNode(level int16, score float64, member string) *node {
	n := &node{
		Element: Element{
			Score:  score,
			Member: member,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31()&0xFFFF) < (0.25 * 0xFFFF) {
		level++
	}
	if level < maxLevel {
		return level
	}
	return maxLevel
}

func (s *SkipList) insert(member string, score float64) *node {

	// 每层的前驱节点
	update := make([]*node, maxLevel)

	node := s.header

	for i := s.level - 1; i >= 0; i-- {
		if node.level[i] != nil {
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score ||
					(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
				node = node.level[i].forward
			}
		}
		update[i] = node
	}

	level := randomLevel()

	node = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node
	}

	if update[0] == s.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		s.tail = node
	}
	s.length++
	return node

}

func (s *SkipList) remove(member string, score float64) bool {
	return true
}
