package store

// listNode is a single element of a List.
type listNode struct {
	val  []byte
	prev *listNode
	next *listNode
}

// List is a doubly-linked list of byte slices with O(1) push/pop at both ends.
type List struct {
	head *listNode
	tail *listNode
	len  int
}

func newList() *List { return &List{} }

// Len returns the number of elements.
func (l *List) Len() int { return l.len }

// PushFront inserts a value at the head.
func (l *List) PushFront(v []byte) {
	n := &listNode{val: v}
	if l.head == nil {
		l.head, l.tail = n, n
	} else {
		n.next = l.head
		l.head.prev = n
		l.head = n
	}
	l.len++
}

// PushBack inserts a value at the tail.
func (l *List) PushBack(v []byte) {
	n := &listNode{val: v}
	if l.tail == nil {
		l.head, l.tail = n, n
	} else {
		n.prev = l.tail
		l.tail.next = n
		l.tail = n
	}
	l.len++
}

// PopFront removes and returns the head value. ok is false when the list is
// empty.
func (l *List) PopFront() ([]byte, bool) {
	if l.head == nil {
		return nil, false
	}
	v := l.head.val
	l.head = l.head.next
	if l.head == nil {
		l.tail = nil
	} else {
		l.head.prev = nil
	}
	l.len--
	return v, true
}

// PopBack removes and returns the tail value. ok is false when the list is
// empty.
func (l *List) PopBack() ([]byte, bool) {
	if l.tail == nil {
		return nil, false
	}
	v := l.tail.val
	l.tail = l.tail.prev
	if l.tail == nil {
		l.head = nil
	} else {
		l.tail.next = nil
	}
	l.len--
	return v, true
}

// nodeAt returns the node at index i (0-based, negative from the end) or nil
// when out of range.
func (l *List) nodeAt(i int) *listNode {
	if l.len == 0 {
		return nil
	}
	if i < 0 {
		i += l.len
	}
	if i < 0 || i >= l.len {
		return nil
	}
	// walk from the closer end
	if i < l.len/2 {
		n := l.head
		for ; i > 0; i-- {
			n = n.next
		}
		return n
	}
	n := l.tail
	for i = l.len - 1 - i; i > 0; i-- {
		n = n.prev
	}
	return n
}

// Index returns the value at index i (0-based, negative from the end). ok is
// false when out of range.
func (l *List) Index(i int) ([]byte, bool) {
	n := l.nodeAt(i)
	if n == nil {
		return nil, false
	}
	return n.val, true
}

// Set replaces the value at index i. ok is false when out of range.
func (l *List) Set(i int, v []byte) bool {
	n := l.nodeAt(i)
	if n == nil {
		return false
	}
	n.val = v
	return true
}

// Range returns values in the inclusive index range [start, stop]. Negative
// indices count from the end and out-of-range values are clamped, mirroring
// Redis LRANGE.
func (l *List) Range(start, stop int) [][]byte {
	if l.len == 0 {
		return nil
	}
	if start < 0 {
		start += l.len
	}
	if stop < 0 {
		stop += l.len
	}
	if start < 0 {
		start = 0
	}
	if stop >= l.len {
		stop = l.len - 1
	}
	if start > stop || start >= l.len {
		return nil
	}
	out := make([][]byte, 0, stop-start+1)
	n := l.nodeAt(start)
	for i := start; i <= stop; i++ {
		out = append(out, n.val)
		n = n.next
	}
	return out
}
