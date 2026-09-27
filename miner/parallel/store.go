package parallel

import (
	"cmp"
	"maps"
	"slices"
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// store holds every value written during a build, versioned by the position
// of the task that wrote it.
type store struct {
	mu            sync.RWMutex
	versions      map[key][]version
	contractCodes map[common.Hash][]byte
}

type version struct {
	pos int
	val value
}

func newStore() *store {
	return &store{
		versions:      make(map[key][]version),
		contractCodes: make(map[common.Hash][]byte),
	}
}

// read returns the newest version of k written by a position below pos.
func (s *store) read(k key, pos int) (value, int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	vs := s.versions[k]
	// versions are sorted by position, so the search lands on the first one
	// at or after pos and vs[i-1] is the newest one before it.
	i, _ := slices.BinarySearchFunc(vs, pos, func(v version, p int) int {
		return cmp.Compare(v.pos, p)
	})
	if i == 0 {
		return value{}, 0, false
	}
	return vs[i-1].val, vs[i-1].pos, true
}

// publish records the writes of the task at pos, replacing values it
// published before.
func (s *store) publish(pos int, writes map[key]value, contractCodes map[common.Hash][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range writes {
		vs := s.versions[k]
		i, found := slices.BinarySearchFunc(vs, pos, func(v version, p int) int {
			return cmp.Compare(v.pos, p)
		})
		if found {
			vs[i].val = v
		} else {
			s.versions[k] = slices.Insert(vs, i, version{pos: pos, val: v})
		}
	}
	maps.Copy(s.contractCodes, contractCodes)
}

// unpublish removes the versions the task at pos published for the given keys.
func (s *store) unpublish(pos int, writes map[key]value) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k := range writes {
		vs := slices.DeleteFunc(s.versions[k], func(v version) bool {
			return v.pos == pos
		})
		if len(vs) == 0 {
			delete(s.versions, k)
		} else {
			s.versions[k] = vs
		}
	}
}

// code returns contract code published under hash.
func (s *store) code(hash common.Hash) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c, ok := s.contractCodes[hash]
	return c, ok
}
