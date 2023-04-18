package redisemu

import (
	"strings"
	"time"

	"github.com/jimsnab/go-lane"
)

const (
	FLAG_KEY_TYPE_STRING bitflags = 1 << iota
	FLAG_KEY_TYPE_HASH_TABLE
	FLAG_KEY_TYPE_SET
	FLAG_KEY_TYPE_LIST
)

type (
	storeKey struct {
		id         uint64
		flags      bitflags
		lastAccess time.Time
		expiresAt  time.Time
		payload    any
	}

	storeList struct {
		head  *listItem
		tail  *listItem
		count int
	}

	listItem struct {
		next    *listItem
		prev    *listItem
		element []byte
	}
)

// primative that duplicates a store key, caller is responsible for the id field
func (sk *storeKey) clone(newId uint64) *storeKey {
	var payload any
	if sk.payload != nil {
		if flagHasOne(sk.flags, FLAG_KEY_TYPE_STRING) {
			skBytes := sk.payload.([]byte)
			bytes := make([]byte, len(skBytes))
			copy(bytes, skBytes)
			payload = bytes
		} else if flagHasOne(sk.flags, FLAG_KEY_TYPE_LIST) {
			sl := sk.payload.(*storeList)
			newSl := storeList{}
			for p := sl.head; p != nil; p = p.next {
				element := make([]byte, len(p.element))
				copy(element, p.element)
				item := &listItem{
					prev:    newSl.tail,
					element: element,
				}
				newSl.tail = item
				if newSl.head == nil {
					newSl.head = item
				}
			}
			payload = &newSl
		} else if flagHasOne(sk.flags, FLAG_KEY_TYPE_HASH_TABLE) {
			m := sk.payload.(map[string]string)
			newMap := make(map[string]string, len(m))
			for k, v := range m {
				newMap[k] = v
			}
			payload = newMap
		} else if flagHasOne(sk.flags, FLAG_KEY_TYPE_SET) {
			m := sk.payload.(map[string]struct{})
			newMap := make(map[string]struct{}, len(m))
			for k := range m {
				newMap[k] = struct{}{}
			}
			payload = newMap
		} else {
			panic("unexpected payload type")
		}
	}

	return &storeKey{
		id:         newId,
		flags:      sk.flags,
		lastAccess: sk.lastAccess,
		expiresAt:  sk.expiresAt,
		payload:    payload,
	}
}

func (sk *storeKey) getStringBytes() []byte {
	if flagHasOne(sk.flags, FLAG_KEY_TYPE_STRING) {
		return sk.payload.([]byte)
	} else {
		return nil
	}
}

func (sk *storeKey) getList() *storeList {
	if flagHasOne(sk.flags, FLAG_KEY_TYPE_LIST) {
		return sk.payload.(*storeList)
	} else {
		return nil
	}
}

func (sk *storeKey) getHashTable() *redisDict {
	if flagHasOne(sk.flags, FLAG_KEY_TYPE_HASH_TABLE) {
		return sk.payload.(*redisDict)
	} else {
		return nil
	}
}

func (sk *storeKey) getSet() *redisDict {
	if flagHasOne(sk.flags, FLAG_KEY_TYPE_SET) {
		return sk.payload.(*redisDict)
	} else {
		return nil
	}
}

func storeKeyTypeFlag(keyType string) bitflags {
	switch strings.ToLower(keyType) {
	case "string":
		return FLAG_KEY_TYPE_STRING
	case "hash":
		return FLAG_KEY_TYPE_HASH_TABLE
	case "set":
		return FLAG_KEY_TYPE_SET
	case "list":
		return FLAG_KEY_TYPE_LIST
	}

	return 0
}

func storeKeyType(keyTypeFlag bitflags) string {
	switch keyTypeFlag {
	case FLAG_KEY_TYPE_STRING:
		return "string"
	case FLAG_KEY_TYPE_HASH_TABLE:
		return "hash"
	case FLAG_KEY_TYPE_SET:
		return "set"
	case FLAG_KEY_TYPE_LIST:
		return "list"
	}

	return "none"
}

func (sl *storeList) dump(l lane.Lane) {
	l.Tracef("list count: %d", sl.count)

	for li := sl.head; li != nil; li = li.next {
		l.Tracef("  %s", string(li.element))
	}
}
