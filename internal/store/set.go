package store

import (
	"math/rand"
	"sort"
)

// SetIntersectCard returns the cardinality of the intersection of the given
// sets. A limit greater than zero stops counting after that many elements.
func (s *Store) SetIntersectCard(keys [][]byte, limit int) (int, error) {
	sets, err := s.setLists(keys)
	if err != nil {
		return 0, err
	}
	if len(sets) == 0 {
		return 0, nil
	}
	// Start from the smallest set to keep the work down.
	smallest := 0
	for i, set := range sets {
		if len(set) < len(sets[smallest]) {
			smallest = i
		}
	}
	n := 0
	for m := range sets[smallest] {
		inAll := true
		for i, set := range sets {
			if i == smallest {
				continue
			}
			if _, ok := set[m]; !ok {
				inAll = false
				break
			}
		}
		if !inAll {
			continue
		}
		n++
		if limit > 0 && n >= limit {
			break
		}
	}
	return n, nil
}

// SetMove moves a member from one set to another. It reports whether the
// member was moved.
func (s *Store) SetMove(source, dest, member []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	se, ok := s.data[string(source)]
	if !ok || se.Expired(now) {
		return false, nil
	}
	if se.Type != TypeSet {
		return false, ErrWrongType
	}
	name := string(member)
	if _, ok := se.Set[name]; !ok {
		return false, nil
	}
	de, ok := s.data[string(dest)]
	if !ok {
		de = &Entry{Type: TypeSet, Set: make(map[string]struct{}), cost: len(string(dest))}
		s.usedBytes += len(string(dest))
		s.data[string(dest)] = de
	} else if de.Type != TypeSet {
		return false, ErrWrongType
	}
	s.touchLocked(string(source))
	s.touchLocked(string(dest))
	// Remove from the source.
	delete(se.Set, name)
	s.addCost(se, -(len(name) + elemOverhead))
	if len(se.Set) == 0 {
		s.usedBytes -= se.cost
		delete(s.data, string(source))
	}
	// Add to the destination.
	if _, exists := de.Set[name]; !exists {
		de.Set[name] = struct{}{}
		s.addCost(de, len(name)+elemOverhead)
	}
	return true, nil
}

// SetPop removes and returns random members of the set. A count of zero pops
// a single member.
func (s *Store) SetPop(key []byte, count int) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entryLocked(key)
	if !ok {
		return nil, nil
	}
	if e.Type != TypeSet {
		return nil, ErrWrongType
	}
	if len(e.Set) == 0 {
		return nil, nil
	}
	s.touchLocked(string(key))
	members := make([]string, 0, len(e.Set))
	for m := range e.Set {
		members = append(members, m)
	}
	sort.Strings(members)
	if count <= 0 {
		count = 1
	}
	if count > len(members) {
		count = len(members)
	}
	// Pick the members at random, then remove them.
	perm := rand.Perm(len(members))
	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		m := members[perm[i]]
		out = append(out, []byte(m))
		delete(e.Set, m)
		s.addCost(e, -(len(m) + elemOverhead))
	}
	if len(e.Set) == 0 {
		s.usedBytes -= e.cost
		delete(s.data, string(key))
	}
	return out, nil
}

// SetRandomMembers returns random members of the set. A negative count
// returns |count| members, possibly with repetitions, like Redis does.
func (s *Store) SetRandomMembers(key []byte, count int) ([][]byte, error) {
	members, err := s.setMemberList(key)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}
	if count == 0 {
		return [][]byte{members[rand.Intn(len(members))]}, nil
	}
	if count < 0 {
		out := make([][]byte, 0, -count)
		for i := 0; i < -count; i++ {
			out = append(out, members[rand.Intn(len(members))])
		}
		return out, nil
	}
	if count > len(members) {
		count = len(members)
	}
	perm := rand.Perm(len(members))
	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, members[perm[i]])
	}
	return out, nil
}

// setMemberList returns the members of the set under key.
func (s *Store) setMemberList(key []byte) ([][]byte, error) {
	out, ok, err := readEntry(s, key, func(e *Entry) ([][]byte, error) {
		if e.Type != TypeSet {
			return nil, ErrWrongType
		}
		members := make([][]byte, 0, len(e.Set))
		for m := range e.Set {
			members = append(members, []byte(m))
		}
		sort.Slice(members, func(i, j int) bool {
			return string(members[i]) < string(members[j])
		})
		return members, nil
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return out, nil
}

// SetIsMemberMany reports for every member whether it belongs to the set.
func (s *Store) SetIsMemberMany(key []byte, members [][]byte) ([]bool, error) {
	out := make([]bool, len(members))
	_, _, err := readEntry(s, key, func(e *Entry) (bool, error) {
		if e.Type != TypeSet {
			return false, ErrWrongType
		}
		for i, m := range members {
			_, out[i] = e.Set[string(m)]
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SetDiff returns the members of the first set that are not in the others.
func (s *Store) SetDiff(keys [][]byte) ([][]byte, error) {
	sets, err := s.setLists(keys)
	if err != nil {
		return nil, err
	}
	if len(sets) == 0 {
		return nil, nil
	}
	others := make(map[string]struct{})
	for _, set := range sets[1:] {
		for m := range set {
			others[m] = struct{}{}
		}
	}
	out := make([][]byte, 0, len(sets[0]))
	for m := range sets[0] {
		if _, ok := others[m]; !ok {
			out = append(out, []byte(m))
		}
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i]) < string(out[j]) })
	return out, nil
}

// SetInter returns the members present in every set.
func (s *Store) SetInter(keys [][]byte) ([][]byte, error) {
	sets, err := s.setLists(keys)
	if err != nil {
		return nil, err
	}
	if len(sets) == 0 {
		return nil, nil
	}
	// Start from the smallest set.
	smallest := 0
	for i, set := range sets {
		if len(set) < len(sets[smallest]) {
			smallest = i
		}
	}
	out := make([][]byte, 0, len(sets[smallest]))
	for m := range sets[smallest] {
		inAll := true
		for i, set := range sets {
			if i == smallest {
				continue
			}
			if _, ok := set[m]; !ok {
				inAll = false
				break
			}
		}
		if inAll {
			out = append(out, []byte(m))
		}
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i]) < string(out[j]) })
	return out, nil
}

// SetUnion returns the members present in any of the sets.
func (s *Store) SetUnion(keys [][]byte) ([][]byte, error) {
	sets, err := s.setLists(keys)
	if err != nil {
		return nil, err
	}
	all := make(map[string]struct{})
	for _, set := range sets {
		for m := range set {
			all[m] = struct{}{}
		}
	}
	out := make([][]byte, 0, len(all))
	for m := range all {
		out = append(out, []byte(m))
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i]) < string(out[j]) })
	return out, nil
}

// setLists returns the member sets of the given keys. A missing key yields
// an empty set, like Redis does.
func (s *Store) setLists(keys [][]byte) ([]map[string]struct{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.clock()
	out := make([]map[string]struct{}, 0, len(keys))
	for _, k := range keys {
		e, ok := s.data[string(k)]
		if !ok || e.Expired(now) {
			out = append(out, map[string]struct{}{})
			continue
		}
		if e.Type != TypeSet {
			return nil, ErrWrongType
		}
		out = append(out, e.Set)
	}
	return out, nil
}

// SetStore replaces the set under key with the given members and returns the
// number of members stored.
func (s *Store) SetStore(key []byte, members [][]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	if e, ok := s.data[string(key)]; ok && !e.Expired(now) {
		if e.Type != TypeSet {
			return 0, ErrWrongType
		}
		s.usedBytes -= e.cost
		delete(s.data, string(key))
	}
	set := make(map[string]struct{}, len(members))
	cost := len(string(key))
	for _, m := range members {
		if _, exists := set[string(m)]; exists {
			continue
		}
		set[string(m)] = struct{}{}
		cost += len(m) + elemOverhead
	}
	e := &Entry{Type: TypeSet, Set: set, cost: cost}
	e.lastAccess.Store(now.UnixNano())
	s.usedBytes += cost
	s.data[string(key)] = e
	return len(set), nil
}
