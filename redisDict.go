package redisemu

import (
	"math/bits"
	"math/rand"
)

type (
	redisDict struct {
		count    int
		removals int
		dirty    bool
		buckets  []*redisDictItem // only one item per bucket
	}

	redisDictItem struct {
		fullHash uint64
		key      string
		value    any
	}

	redisDictIter struct {
		dict         *redisDict
		bucketNumber uint32
		key          string
		value        any
	}
)

func newRedisDict() *redisDict {
	dict := &redisDict{
		buckets: make([]*redisDictItem, 16),
	}
	return dict
}

func newRedisDictFromStringTable(m map[string]string) *redisDict {
	dict := &redisDict{
		buckets: make([]*redisDictItem, 16),
	}
	for k, v := range m {
		dict.store(k, v)
	}

	dict.dirty = false
	return dict
}

func newRedisDictFromKeyTable(m map[string]struct{}) *redisDict {
	dict := &redisDict{
		buckets: make([]*redisDictItem, 16),
	}
	for k := range m {
		dict.store(k, struct{}{})
	}

	dict.dirty = false
	return dict
}

func (rdi *redisDictIter) next() (more bool) {
	for rdi.bucketNumber < uint32(len(rdi.dict.buckets)) {
		item := rdi.dict.buckets[rdi.bucketNumber]
		rdi.bucketNumber++
		if item != nil {
			rdi.key = item.key
			rdi.value = item.value
			more = true
			return
		}
	}
	return
}

func (rd *redisDict) createIterator() *redisDictIter {
	return &redisDictIter{dict: rd}
}

func (rd *redisDict) hashToIndex(fullHash uint64, bucketCount uint32) uint32 {
	mask := bucketCount - 1
	bucketNumber := uint32(fullHash) & mask
	bitPos := bitPosition(bucketCount)
	shifted := bucketNumber << (32 - bitPos)
	return bits.Reverse32(shifted)
}

func (rd *redisDict) findBucket(key string, fullHash uint64) (bucket *redisDictItem, bucketNumber uint32) {
	bucketCount := uint32(len(rd.buckets))
	bucketNumber = rd.hashToIndex(fullHash, bucketCount)
	bucket = rd.buckets[bucketNumber]
	return
}

func (rd *redisDict) hash(key string) (fullHash uint64) {
	return calcSipHash(key)
}

func (rd *redisDict) rehash(bucketCount uint32) {
	buckets := make([]*redisDictItem, bucketCount)

	for _, item := range rd.buckets {
		if item != nil {
			newIndex := rd.hashToIndex(item.fullHash, bucketCount)
			buckets[newIndex] = item
		}
	}

	rd.buckets = buckets
}

func (rd *redisDict) store(key string, val any) {
	rd.dirty = true
	fullHash := rd.hash(key)
	item, bucketNumber := rd.findBucket(key, fullHash)
	if item != nil {
		if item.key != key {
			// determine the required size to eliminate the hash collision
			// (relying heavily on the randomness and size of the hash function here)
			n := uint32(len(rd.buckets))
			for {
				n *= 2
				mask := uint64(n - 1)
				if (item.fullHash & mask) != (fullHash & mask) {
					rd.rehash(n)
					bucketNumber = rd.hashToIndex(fullHash, n)
					break
				}
			}
		} else {
			item.value = val
			return
		}
	}

	if rd.buckets[bucketNumber] != nil {
		panic("store is about to overwite data")
	}

	item = &redisDictItem{
		fullHash: fullHash,
		key:      key,
		value:    val,
	}
	rd.buckets[bucketNumber] = item
	rd.count++
}

func (rd *redisDict) remove(key string) (exists bool) {
	hash := rd.hash(key)
	item, bucketNumber := rd.findBucket(key, hash)
	if item == nil || item.key != key {
		return
	}
	rd.buckets[bucketNumber] = nil
	rd.count--
	exists = true

	rd.removals++
	rd.dirty = true
	if rd.removals > len(rd.buckets)/2 {
		// check if the hash table size can be reduced
		rd.removals = 0
		reducable := len(rd.buckets) > 16 // keep a minimum size
		if reducable {
			// when an even and an odd bucket is filled, it means that
			// the array cannot be reduced without causing a collision
			for i := 0; i < len(rd.buckets); i += 2 {
				if rd.buckets[i] != nil && rd.buckets[i+1] != nil {
					reducable = false
					break
				}
			}

			if reducable {
				rd.rehash(uint32(len(rd.buckets)) / 2)
			}
		}
	}

	return
}

func (rd *redisDict) get(key string) (value any, exists bool) {
	hash := rd.hash(key)
	item, _ := rd.findBucket(key, hash)
	if item != nil && item.key == key {
		value = item.value
		exists = true
	}
	return
}

func (rd *redisDict) pickRandomItems(count, sparseThreshold int) (items []*redisDictItem) {
	items = make([]*redisDictItem, 0, count)

	// Algorithm that is expensive when sparseness is high; we rely on
	// the hash function to reduce that possibility.
	for i := 0; i < count; i++ {
		for {
			r := rand.Intn(len(rd.buckets))
			if rd.buckets[r] == nil {
				continue
			}
			items = append(items, rd.buckets[r])
			break
		}
	}
	return
}

func (rd *redisDict) pickUniqueRandomItems(count, sparseThreshold int) (items []*redisDictItem) {
	if count > rd.count {
		count = rd.count
	}
	items = make([]*redisDictItem, 0, count)

	// Algorithm that is expensive when sparseness is high; we rely on
	// the hash function to reduce that possibility.
	occupied := map[int]struct{}{}
	for i := 0; i < count; i++ {
		for {
			r := rand.Intn(len(rd.buckets))
			if rd.buckets[r] == nil {
				continue
			}
			if _, isOccupied := occupied[r]; isOccupied {
				continue
			}
			occupied[r] = struct{}{}
			items = append(items, rd.buckets[r])
			break
		}
	}
	return
}

func (rd *redisDict) clone() *redisDict {
	dict := &redisDict{
		buckets:  make([]*redisDictItem, len(rd.buckets)),
		count:    rd.count,
		removals: rd.removals,
	}
	copy(dict.buckets, rd.buckets)
	return dict
}

func (rd *redisDict) toStringTable() map[string]string {
	result := make(map[string]string, rd.count)
	for i := rd.createIterator(); i.next(); {
		result[i.key] = i.value.(string)
	}
	return result
}

func (rd *redisDict) toKeyTable() map[string]struct{} {
	result := make(map[string]struct{}, rd.count)
	for i := rd.createIterator(); i.next(); {
		result[i.key] = struct{}{}
	}
	return result
}
