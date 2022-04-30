package sortedset

import "math/rand"

const (
	maxLevel = 16
)

// Element is a key-score pair
type Element struct {
	Member string
	Score  float64
}

// Level aspect of a node
type Level struct {
	forward *node // forward node has greater score
	span    int64
}

// node stores the Element, the prev node, and the next nodes stored in the level
type node struct {
	Element
	backward *node
	level    []*Level
}

// skipList stores the header node and the tail node, of course include the length and the level
type skipList struct {
	header *node
	tail   *node
	length int64
	level  int16
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

func makeSkipList() *skipList {
	return &skipList{
		level:  1,
		header: makeNode(maxLevel, 0, ""),
	}
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

func (s *skipList) insert(member string, score float64) *node {

	update := make([]*node, maxLevel)
	rank := make([]int64, maxLevel)

	// find position to insert
	node := s.header
	for i := s.level - 1; i >= 0; i-- {
		// rank[i] stores the cross span
		if i == s.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		if node.level[i] != nil {
			// traverse the skip list
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score || (node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}

	level := randomLevel()
	// extend skip list
	if level > s.level {
		for i := s.level; i < level; i++ {
			rank[i] = 0
			update[i] = s.header
			update[i].level[i].span = s.length
		}
		s.level = level
	}

	// make node and link into skip list
	node = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node
		// update span covered by update[i] after node is inserted
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// increase span for untouched levels
	for i := level; i < s.level; i++ {
		update[i].level[i].span++
	}

	// set backward node
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

// removeNode remove node, update is the backward nodes
func (s *skipList) removeNode(node *node, update []*node) {
	// 更新 span 以及 forward 指针
	for i := int16(0); i < s.level; i++ {
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node
		} else {
			update[i].level[i].span--
		}
	}

	// 更新 backward 指针
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		s.tail = node.backward
	}
	// 更新 level
	for s.level > 1 && s.header.level[s.level-1].forward == nil {
		s.level--
	}
	s.length--
}

// remove requires node has found and remove it
func (s *skipList) remove(member string, score float64) bool {
	// find backward node or the last node of each level whose forward node is in need of updating
	update := make([]*node, maxLevel)
	node := s.header
	for i := s.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score &&
					node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		update[i] = node
	}
	node = node.level[0].forward
	if node != nil && score == node.Score && node.Member == member {
		s.removeNode(node, update)
		// free x
		return true
	}
	return false
}

// getRank return the rank of the node, 0 if the member not found
func (s *skipList) getRank(member string, score float64) int64 {
	var rank int64 = 0
	x := s.header
	for i := s.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.Score < score ||
				(x.level[i].forward.Score == score &&
					x.level[i].forward.Member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		// make sure member
		if x.Member == member {
			return rank
		}
	}
	return 0
}

// getByRank returns the node based the rank from 1
func (s *skipList) getByRank(rank int64) *node {
	var i int64 = 0
	n := s.header
	// scan from top level
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

func (s *skipList) hasInRange(min *ScoreBorder, max *ScoreBorder) bool {
	// min & max = empty
	if min.Value > max.Value || (min.Value == max.Value && (min.Exclude || max.Exclude)) {
		return false
	}
	// min > tail
	n := s.tail
	if n == nil || !min.less(n.Score) {
		return false
	}
	// max < head
	n = s.header.level[0].forward
	if n == nil || !max.greater(n.Score) {
		return false
	}
	return true
}

func (s *skipList) getFirstInScoreRange(min *ScoreBorder, max *ScoreBorder) *node {
	if !s.hasInRange(min, max) {
		return nil
	}
	n := s.header
	// scan from top level
	for level := s.level - 1; level >= 0; level-- {
		// if forward is not in range then move forward
		for n.level[level].forward != nil && !min.less(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}
	// this is an inner range, so the next node can not be null
	n = n.level[0].forward
	if !max.greater(n.Score) {
		return nil
	}
	return n
}

func (s *skipList) getLastInScoreRange(min *ScoreBorder, max *ScoreBorder) *node {
	if !s.hasInRange(min, max) {
		return nil
	}
	n := s.header
	// scan from top level
	for level := s.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && max.greater(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}
	if !min.less(n.Score) {
		return nil
	}
	return n
}

func (s *skipList) RemoveRangeByScore(min *ScoreBorder, max *ScoreBorder) []*Element {
	update := make([]*node, maxLevel)
	removed := make([]*Element, 0)
	// find backward nodes
	node := s.header
	for i := s.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil {
			if min.less(node.level[i].forward.Score) {
				// already in range
				break
			}
			node = node.level[i].forward
		}
		update[i] = node
	}

	// node is the first node within range
	node = node.level[0].forward

	// remove nodes in range
	for node != nil {
		if !max.greater(node.Score) {
			// already out of range
			break
		}
		next := node.level[0].forward
		removeElement := node.Element
		removed = append(removed, &removeElement)
		s.removeNode(node, update)
		node = next
	}
	return removed
}

func (s *skipList) RemoveRangeByRank(start int64, stop int64) []*Element {
	var i int64 = 0 // rank of iterator
	update := make([]*node, maxLevel)
	removed := make([]*Element, 0)

	// scan from top level
	node := s.header
	for level := s.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && (i+node.level[level].span) < start {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}

	i++
	node = node.level[0].forward // first node in range

	// remove nodes in range
	for node != nil && i < stop {
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		s.removeNode(node, update)
		node = next
		i++
	}
	return removed
}
