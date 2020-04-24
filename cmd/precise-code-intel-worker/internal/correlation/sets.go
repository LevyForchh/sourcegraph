package correlation

import (
	"sort"
)

type IDSet map[string]struct{}

func (set IDSet) Add(id string) {
	set[id] = struct{}{}
}

func (set IDSet) AddAll(other IDSet) {
	for k := range other {
		set.Add(k)
	}
}

func (set IDSet) Contains(id string) bool {
	_, ok := set[id]
	return ok
}

func (set IDSet) Keys() []string {
	var keys []string
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

func (set IDSet) Choose() (string, bool) {
	if len(set) == 0 {
		return "", false
	}

	return set.Keys()[0], true
}

//
//
//

type DefaultIDSetMap map[string]IDSet

func (sm DefaultIDSetMap) GetOrCreate(key string) IDSet {
	if s, ok := sm[key]; ok {
		return s
	}

	s := IDSet{}
	sm[key] = s
	return s
}

//
//
//

type DisjointIDSet map[string]IDSet

func (set DisjointIDSet) Union(id1, id2 string) {
	set.getOrCreateSet(id1).Add(id2)
	set.getOrCreateSet(id2).Add(id1)
}

func (set DisjointIDSet) ExtractSet(id string) IDSet {
	s := IDSet{}

	frontier := []string{id}
	for len(frontier) > 0 {
		v := frontier[0]
		frontier = frontier[1:]

		if !s.Contains(v) {
			s.Add(v)
			frontier = append(frontier, set[v].Keys()...)
		}
	}

	return s
}

func (set DisjointIDSet) getOrCreateSet(id string) IDSet {
	s, ok := set[id]
	if !ok {
		s = IDSet{}
		set[id] = s
	}

	return s
}
