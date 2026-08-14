package store

import "math/rand"

// Skiplist parameters, matching Redis zskiplist.
const (
	skiplistMaxLevel = 32
	skiplistP        = 0.25
)

type skiplistLevel struct {
	forward *skiplistNode
	span    int64
}

// skiplistNode holds one member/score pair at one or more levels.
type skiplistNode struct {
	member   []byte
	score    float64
	backward *skiplistNode
	level    []skiplistLevel
}

// skiplist is a sorted, probabilistically-balanced linked list over (score,
// member) pairs, ordered by score then member.
type skiplist struct {
	header *skiplistNode
	tail   *skiplistNode
	length int64
	level  int
	rnd    *rand.Rand
}

func newSkiplist() *skiplist {
	h := &skiplistNode{level: make([]skiplistLevel, skiplistMaxLevel)}
	return &skiplist{
		header: h,
		level:  1,
		rnd:    rand.New(rand.NewSource(rand.Int63())),
	}
}

// compare orders nodes by score first, then member bytes (as Redis does).
func skiplistCompare(score1 float64, m1 []byte, score2 float64, m2 []byte) int {
	if score1 < score2 {
		return -1
	}
	if score1 > score2 {
		return 1
	}
	return stringCompare(m1, m2)
}

func stringCompare(a, b []byte) int {
	la, lb := len(a), len(b)
	n := la
	if lb < n {
		n = lb
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	switch {
	case la < lb:
		return -1
	case la > lb:
		return 1
	default:
		return 0
	}
}

func (sl *skiplist) randomLevel() int {
	level := 1
	for level < skiplistMaxLevel && sl.rnd.Float64() < skiplistP {
		level++
	}
	return level
}

// Insert adds a (score, member) pair and returns the new node.
func (sl *skiplist) Insert(score float64, member []byte) *skiplistNode {
	var update [skiplistMaxLevel]*skiplistNode
	var rank [skiplistMaxLevel]int64

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil &&
			skiplistCompare(x.level[i].forward.score, x.level[i].forward.member, score, member) < 0 {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	level := sl.randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].level[i].span = sl.length
		}
		sl.level = level
	}

	node := &skiplistNode{score: score, member: member, level: make([]skiplistLevel, level)}
	for i := 0; i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}
	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == sl.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		sl.tail = node
	}
	sl.length++
	return node
}

// Delete removes the (score, member) pair, reporting whether it was found.
func (sl *skiplist) Delete(score float64, member []byte) bool {
	var update [skiplistMaxLevel]*skiplistNode

	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			skiplistCompare(x.level[i].forward.score, x.level[i].forward.member, score, member) < 0 {
			x = x.level[i].forward
		}
		update[i] = x
	}

	x = x.level[0].forward
	if x != nil && x.score == score && bytesEqual(x.member, member) {
		sl.deleteNode(x, update[:])
		return true
	}
	return false
}

func (sl *skiplist) deleteNode(x *skiplistNode, update []*skiplistNode) {
	for i := 0; i < sl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		sl.tail = x.backward
	}
	for sl.level > 1 && sl.header.level[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
}

// GetRank returns the 1-based rank of (score, member), or 0 when not found.
func (sl *skiplist) GetRank(score float64, member []byte) int64 {
	var rank int64
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(skiplistCompare(x.level[i].forward.score, x.level[i].forward.member, score, member) < 0 ||
				(x.level[i].forward.score == score && bytesEqual(x.level[i].forward.member, member))) {
			rank += x.level[i].span
			x = x.level[i].forward
		}
		if x != sl.header && x.score == score && bytesEqual(x.member, member) {
			return rank
		}
	}
	return 0
}

// nodeByRank returns the node at 1-based rank, or nil when out of range.
func (sl *skiplist) nodeByRank(rank int64) *skiplistNode {
	var traversed int64
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && traversed+x.level[i].span <= rank {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		if traversed == rank && x != sl.header {
			return x
		}
	}
	return nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
