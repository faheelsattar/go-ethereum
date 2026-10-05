package parallel

import (
	"cmp"
	"maps"
	"slices"
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

const storeShards = 64

// store holds every value written during a build, versioned by the position
// of the task that wrote it. Keys are spread over shards with their own lock
// so that one worker publishing rarely blocks the others reading.
type store struct {
	shards        [storeShards]shard
	codeMu        sync.RWMutex
	contractCodes map[common.Hash][]byte
}

type shard struct {
	mu       sync.RWMutex
	versions map[key][]version
}

type version struct {
	pos int
	val value
}

func newStore() *store {
	s := &store{contractCodes: make(map[common.Hash][]byte)}
	for i := range s.shards {
		s.shards[i].versions = make(map[key][]version)
	}
	return s
}

func (s *store) shardOf(k key) *shard {
	return &s.shards[(k.addr[19]^k.slot[31])%storeShards]
}

// read returns the newest version of k written by a position below pos.
func (s *store) read(k key, pos int) (value, int, bool) {
	sh := s.shardOf(k)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	vs := sh.versions[k]
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
	for k, v := range writes {
		sh := s.shardOf(k)
		sh.mu.Lock()
		vs := sh.versions[k]
		i, found := slices.BinarySearchFunc(vs, pos, func(v version, p int) int {
			return cmp.Compare(v.pos, p)
		})
		if found {
			vs[i].val = v
		} else {
			sh.versions[k] = slices.Insert(vs, i, version{pos: pos, val: v})
		}
		sh.mu.Unlock()
	}
	if len(contractCodes) > 0 {
		s.codeMu.Lock()
		maps.Copy(s.contractCodes, contractCodes)
		s.codeMu.Unlock()
	}
}

// unpublish removes the versions the task at pos published for the given keys.
func (s *store) unpublish(pos int, writes map[key]value) {
	for k := range writes {
		sh := s.shardOf(k)
		sh.mu.Lock()
		vs := slices.DeleteFunc(sh.versions[k], func(v version) bool {
			return v.pos == pos
		})
		if len(vs) == 0 {
			delete(sh.versions, k)
		} else {
			sh.versions[k] = vs
		}
		sh.mu.Unlock()
	}
}

// code returns contract code published under hash.
func (s *store) code(hash common.Hash) ([]byte, bool) {
	s.codeMu.RLock()
	defer s.codeMu.RUnlock()

	c, ok := s.contractCodes[hash]
	return c, ok
}
