package redisemu

import (
	"encoding/gob"
	"fmt"
	"os"
	"time"
)

type (
	persistHeader struct {
		Version          uint32
		Count            uint32
		Removals         uint32
		DataObjectNumber uint64
	}

	persistKeyHeader struct {
		Key        string
		Id         uint64
		Flags      bitflags
		LastAccess time.Time
		ExpiresAt  time.Time
	}
)

func (ds *dataStore) save(fileName string) (err error) {
	// open output file
	f, err := os.Create(fileName)
	if err != nil {
		return
	}

	// close f on exit and check for its returned error
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()

	enc := gob.NewEncoder(f)

	// write the header
	ph := persistHeader{
		Version:          1,
		Count:            uint32(ds.data.count),
		Removals:         uint32(ds.data.removals),
		DataObjectNumber: ds.dataObjectNumber,
	}

	if err = enc.Encode(ph); err != nil {
		return
	}

	// write the data
	for _, item := range ds.data.buckets {
		if item == nil {
			continue
		}
		sk, isSk := item.value.(*storeKey)
		if !isSk {
			panic("unexpected value type in data store")
		}

		// copy only persistable members
		pkh := persistKeyHeader{
			Key:        item.key,
			Id:         sk.id,
			Flags:      sk.flags,
			LastAccess: sk.lastAccess,
			ExpiresAt:  sk.expiresAt,
		}

		if err = enc.Encode(pkh); err != nil {
			return
		}

		if flagHasOne(sk.flags, FLAG_KEY_TYPE_STRING) {
			err = enc.Encode(sk.payload.([]byte))
		} else if flagHasOne(sk.flags, FLAG_KEY_TYPE_HASH_TABLE) {
			table := sk.payload.(*redisDict)
			err = enc.Encode(table.toStringTable())
		} else if flagHasOne(sk.flags, FLAG_KEY_TYPE_SET) {
			table := sk.payload.(*redisDict)
			err = enc.Encode(table.toKeyTable())
		} else if flagHasOne(sk.flags, FLAG_KEY_TYPE_LIST) {
			// can't serialize pointers; extract the raw data
			list := sk.payload.(*storeList)
			payload := make([][]byte, 0, list.count)
			for p := list.head; p != nil; p = p.next {
				payload = append(payload, p.element)
			}

			err = enc.Encode(payload)
		} else {
			panic("should be unreachable")
		}

		if err != nil {
			return
		}
	}

	return
}

func (ds *dataStore) load(fileName string) (err error) {
	// open input file
	f, err := os.Open(fileName)
	if err != nil {
		return
	}

	// close f on exit and check for its returned error
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()

	dec := gob.NewDecoder(f)

	// read the header
	var ph persistHeader
	if err = dec.Decode(&ph); err != nil {
		return
	}
	if ph.Version != 1 {
		err = fmt.Errorf("unsupported file version %d", ph.Version)
		return
	}

	// make a new dictionary from the persisted data
	data := newRedisDict()
	for i := 0; i < int(ph.Count); i++ {
		var pkh persistKeyHeader
		if err = dec.Decode(&pkh); err != nil {
			return
		}

		var payload any
		if flagHasOne(pkh.Flags, FLAG_KEY_TYPE_STRING) {
			var str []byte
			err = dec.Decode(&str)
			payload = str
		} else if flagHasOne(pkh.Flags, FLAG_KEY_TYPE_HASH_TABLE) {
			var table map[string]string
			err = dec.Decode(&table)
			payload = newRedisDictFromStringTable(table)
		} else if flagHasOne(pkh.Flags, FLAG_KEY_TYPE_SET) {
			var table map[string]struct{}
			err = dec.Decode(&table)
			payload = newRedisDictFromKeyTable(table)
		} else if flagHasOne(pkh.Flags, FLAG_KEY_TYPE_LIST) {
			var rawList [][]byte
			err = dec.Decode(&rawList)
			if err == nil {
				// make a linked list from raw data
				list := &storeList{}

				for _, element := range rawList {
					item := &listItem{
						prev:    list.tail,
						element: element,
					}
					if list.head == nil {
						list.head = item
					} else {
						list.tail.next = item
					}
					list.tail = item
				}

				list.count = len(rawList)
				payload = list
			}
		} else {
			err = fmt.Errorf("invalid value type - database file is corrupt")
			return
		}

		if err != nil {
			err = fmt.Errorf("unexpected value type - database file is corrupt. Error: %s", err.Error())
			return
		}

		sk := &storeKey{
			id:         pkh.Id,
			flags:      pkh.Flags,
			lastAccess: pkh.LastAccess,
			expiresAt:  pkh.ExpiresAt,
			payload:    payload,
		}
		data.store(pkh.Key, sk)
	}

	data.removals = int(ph.Removals)
	data.dirty = false

	ds.data = data
	ds.dataObjectNumber = ph.DataObjectNumber

	return
}
