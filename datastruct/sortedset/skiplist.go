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

	update := make([]*node, maxLevel)
	node := s.header
	for level := s.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil &&
			(node.level[level].forward.Score < score ||
				(node.level[level].forward.Score == score && node.level[level].forward.Member < member)) {
			node = node.level[level].forward
		}
		update[level] = node
	}

	node = node.level[0].forward
	if node != nil && member == node.Member && score == node.Score {
		s.removeNode(node, update)
		return true
	}
	return false
}

func (s *SkipList) getByRank(rank int64) *node {
	i := int64(0)
	n := s.header
	for level := s.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && (i+n.level[level].span) <= rank {
			i += n.level[level].span
			n = n.level[level].forward
		}
		if i == rank {
			return n
		}
	}
	return nil
}

func (s *SkipList) removeNode(node *node, update []*node) {

	for i := int16(0); i < s.level; i++ {
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span
			update[i].level[i].forward = node.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}

	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		s.tail = node.backward
	}

	for s.level > 1 && s.header.level[s.level-1].forward == nil {
		s.level--
	}
	s.length--
}
