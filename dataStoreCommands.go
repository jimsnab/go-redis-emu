package redisemu

import (
	byteUtils "bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jimsnab/go-lane"
)

const (
	SET_NOT_EXIST bitflags = 1 << iota
	SET_EXISTS
	SET_GET
	SET_KEEP_TTL
	SET_APPEND
)

const (
	BF_GET bitfieldOperation = iota
	BF_SET
	BF_INCRBY
)

const (
	OFLOW_WRAP overflowType = iota
	OFLOW_SAT
	OFLOW_FAIL
)

const (
	RESULT_COMPLETED keyOpResult = iota
	RESULT_MISSING_SOURCE
	RESULT_DESTINATION_EXISTS
)

const (
	VALUE_EXISTS valueExists = iota
	VALUE_DOESNT_EXIST
	VALUE_WRONG_TYPE
	VALUE_WRONG_FORMAT
	VALUE_OVERFLOW
)

var maxTime = time.Date(9999, 12, 31, 23, 59, 59, 999, time.UTC)
var minTime = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
var wrongTypeError = respErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")

type (
	dataStoreCommand struct {
		id uint32 // command counter
		ds *dataStore
	}

	bitfieldOperation int
	overflowType      int
	keyOpResult       int
	valueExists       int

	bitfieldOp struct {
		argIndex  int
		op        bitfieldOperation
		value     int64
		signed    bool
		bitOffset int
		endOffset int
		width     int
		oflow     overflowType
	}

	unblockKey struct {
		keyName  string
		elements int
	}

	sortVal struct {
		data        string
		sortByStr   string
		sortByFloat float64
	}
)

// lock is re-enterant to support MULTI
func (dsc *dataStoreCommand) lock() {
	if !atomic.CompareAndSwapUint32(&dsc.ds.multiLock, dsc.id, dsc.id) {
		// multi-lock not acquired, acquire a single lock
		dsc.ds.mu.Lock()
	}
}

func (dsc *dataStoreCommand) unlock() {
	if !atomic.CompareAndSwapUint32(&dsc.ds.multiLock, dsc.id, dsc.id) {
		// release the single lock
		dsc.ds.mu.Unlock()
	}
}

func (dsc *dataStoreCommand) unlockAndUnblock(uk *unblockKey) {
	dsc.ds.unblockListUnlocked(uk.keyName, uk.elements)
	if !atomic.CompareAndSwapUint32(&dsc.ds.multiLock, dsc.id, dsc.id) {
		// release the single lock
		dsc.ds.mu.Unlock()
	}
}

func (dsc *dataStoreCommand) acquireExclusive() {
	// give ownership to the caller
	dsc.ds.mu.Lock()

	// bypass the lock on nested callers
	atomic.StoreUint32(&dsc.ds.multiLock, dsc.id)
}

func (dsc *dataStoreCommand) releaseExclusive() {
	// release the multi-lock; subsequent commands are all blocked on dsc.ds.mu
	atomic.StoreUint32(&dsc.ds.multiLock, dsc.id)
	// release and let the next subsequent command execute (if any)
	dsc.ds.mu.Unlock()
}

func (dsc *dataStoreCommand) dumpKey(l lane.Lane, keyName string) {
	// stop the unused warning for some functions we'd like to keep for reference and development
	var _ = dsc.getHashTableSet
	var _ = dsc.getSetMember
	var _ = dsc.getSet
	var _ = dsc.deleteSetMembers

	sk, exists := dsc.getKeyObject(keyName)
	if !exists {
		l.Tracef("key '%s' does not exist", keyName)
	} else {
		l.Tracef("key '%s' last access %v, expires %v", keyName, sk.lastAccess, sk.expiresAt)
		strBytes := sk.getStringBytes()
		if strBytes != nil {
			l.Tracef("key '%s' string '%s'", keyName, string(strBytes))
		}
		list := sk.getList()
		if list != nil {
			list.dump(l)
		}
	}
}

func (dsc *dataStoreCommand) setKey(keyName, str string, options bitflags, expiration time.Time) (val respValue, valid valueExists) {
	argBytes := []byte(str)
	val.data = rstrOK

	dsc.lock()
	defer dsc.unlock()

	oldSk, exists := dsc.getKeyObjectUnlocked(keyName)
	if exists {
		strBytes := oldSk.getStringBytes()

		if flagHasOne(options, bitflags(SET_GET)) {
			if strBytes == nil {
				val.data = nil
				valid = VALUE_WRONG_TYPE
				return
			}
			val.data = respBulkString(strBytes)
		}

		if flagHasOne(options, SET_NOT_EXIST) {
			if !flagHasOne(options, bitflags(SET_GET)) {
				val.data = nil
			}
			return
		}

		if flagHasOne(options, SET_KEEP_TTL) {
			expiration = time.Time(oldSk.expiresAt)
		}

		if flagHasOne(options, SET_APPEND) {
			if strBytes == nil {
				val.data = nil
				valid = VALUE_WRONG_TYPE
				return
			}
			argBytes = append(strBytes, argBytes...)
		}

	} else {
		if flagHasOne(options, SET_EXISTS) {
			val.data = nil
			return
		}
		if flagHasOne(options, SET_GET) {
			val.data = nil
		}
	}

	newSk := dsc.ds.newStoreKeyUnlocked(keyName)
	newSk.flags = FLAG_KEY_TYPE_STRING
	newSk.payload = argBytes
	newSk.expiresAt = expiration
	return
}

func (dsc *dataStoreCommand) setKeys(keys []string, values []string, options bitflags) (result respValue) {
	dsc.lock()
	defer dsc.unlock()

	if flagHasOne(options, SET_NOT_EXIST) {
		for _, keyName := range keys {
			_, exists := dsc.getKeyObjectUnlocked(keyName)
			if exists {
				result.data = respInt(0)
				return
			}
		}
		result.data = respInt(1)
	} else {
		result.data = rstrOK
	}

	for idx, keyName := range keys {
		val := values[idx]

		newSk := dsc.ds.newStoreKeyUnlocked(keyName)
		newSk.flags = FLAG_KEY_TYPE_STRING
		newSk.payload = []byte(val)
		newSk.expiresAt = maxTime
	}

	return
}

func (dsc *dataStoreCommand) setRange(keyName string, offset int, substring string) (result respValue) {
	dsc.lock()
	defer dsc.unlock()

	var setBytes []byte
	expiration := maxTime
	oldSk, exists := dsc.getKeyObjectUnlocked(keyName)
	if exists {
		setBytes = oldSk.getStringBytes()
		if setBytes == nil {
			result.data = wrongTypeError
			return
		}
		expiration = time.Time(oldSk.expiresAt)
	} else {
		setBytes = []byte{}
	}

	if len(setBytes) < offset {
		expanded := make([]byte, offset)
		copy(expanded, setBytes)
		setBytes = expanded
	}

	substrBytes := []byte(substring)
	end := offset + len(substrBytes)
	suffix := []byte{}
	if end < len(setBytes) {
		suffix = setBytes[end:]
	}

	newStrBytes := make([]byte, 0, offset+len(substrBytes)+len(suffix))
	newStrBytes = append(newStrBytes, setBytes[0:offset]...)
	newStrBytes = append(newStrBytes, substrBytes...)
	newStrBytes = append(newStrBytes, suffix...)

	newSk := dsc.ds.newStoreKeyUnlocked(keyName)
	newSk.flags = FLAG_KEY_TYPE_STRING
	newSk.expiresAt = expiration
	newSk.payload = newStrBytes

	result.data = respInt(len(newStrBytes))
	return
}

func (sk *storeKey) isExpiredUnlocked() bool {
	return time.Now().After(sk.expiresAt)
}

func (dsc *dataStoreCommand) getKeyObjectUnlocked(keyName string) (sk *storeKey, exists bool) {
	sk, exists = dsc.ds.getStoreKey(keyName)
	if !exists {
		return
	}
	if sk.isExpiredUnlocked() {
		exists = false
		sk = nil
		return
	}

	return
}

func (dsc *dataStoreCommand) setDirty() {
	dsc.ds.data.dirty = true
}

func (dsc *dataStoreCommand) getKeyObject(keyName string) (sk *storeKey, exists bool) {
	dsc.lock()
	defer dsc.unlock()

	return dsc.getKeyObjectUnlocked(keyName)
}

func (dsc *dataStoreCommand) getKey(keyName string) (val string, exists valueExists) {
	dsc.lock()
	defer dsc.unlock()

	return dsc.getKeyUnlocked(keyName)
}

func (dsc *dataStoreCommand) getKeyUnlocked(keyName string) (val string, exists valueExists) {
	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		exists = VALUE_DOESNT_EXIST
		return
	}

	strBytes := sk.getStringBytes()
	if strBytes == nil {
		exists = VALUE_WRONG_TYPE
		return
	}
	val = string(strBytes)
	return
}

func (dsc *dataStoreCommand) getKeyBytes(keyName string) (val []byte, exists valueExists) {
	sk, objExists := dsc.getKeyObject(keyName)
	if !objExists {
		exists = VALUE_DOESNT_EXIST
		return
	}

	val = sk.getStringBytes()
	if val == nil {
		exists = VALUE_WRONG_TYPE
		return
	}
	return
}

func (dsc *dataStoreCommand) getKeys(keyName ...string) (vals []*string, wrongType bool) {
	vals = []*string{}

	dsc.lock()
	defer dsc.unlock()

	for _, key := range keyName {
		sk, objExists := dsc.getKeyObjectUnlocked(key)

		var val *string
		if objExists {
			strBytes := sk.getStringBytes()
			if strBytes != nil {
				str := string(strBytes)
				val = &str
			} else {
				wrongType = true
			}
		}
		vals = append(vals, val)
	}

	return
}

func (dsc *dataStoreCommand) getKeySetExpiration(keyName string, expiration time.Time) (val string, exists valueExists) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if objExists {
		strBytes := sk.getStringBytes()
		if strBytes != nil {
			val = string(strBytes)
			sk.expiresAt = expiration
		} else {
			exists = VALUE_WRONG_TYPE
		}
	} else {
		exists = VALUE_DOESNT_EXIST
	}
	return
}

func (dsc *dataStoreCommand) getDeleteKey(keyName string) (val string, exists valueExists) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if objExists {
		strBytes := sk.getStringBytes()
		if strBytes != nil {
			val = string(strBytes)
			dsc.ds.data.remove(keyName)
		} else {
			exists = VALUE_WRONG_TYPE
		}
	} else {
		exists = VALUE_DOESNT_EXIST
	}
	return
}

func (dsc *dataStoreCommand) keys(pattern string) (list []string) {
	list = []string{}

	dsc.lock()
	defer dsc.unlock()

	pat := []rune(pattern)

	for i := dsc.ds.data.createIterator(); i.next(); {
		sv := i.value.(*storeKey)
		if sv.isExpiredUnlocked() {
			continue
		}
		if !redisGlob(pat, []rune(i.key)) {
			continue
		}

		list = append(list, i.key)
	}

	return
}

func (dsc *dataStoreCommand) addInt(keyName string, delta int64) (value int64, exists valueExists) {
	dsc.lock()
	defer dsc.unlock()

	value = delta
	expiration := maxTime

	oldSk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if objExists {
		strBytes := oldSk.getStringBytes()
		if strBytes == nil {
			exists = VALUE_WRONG_TYPE
			return
		}

		var err error
		value, err = strconv.ParseInt(string(strBytes), 10, 64)
		if err != nil {
			exists = VALUE_WRONG_FORMAT
			return
		}

		// detect overflow
		newVal := value + delta
		if (newVal > value) != (delta > 0) {
			exists = VALUE_OVERFLOW
			return
		}
		value = newVal
		expiration = time.Time(oldSk.expiresAt)
	}

	newSk := dsc.ds.newStoreKeyUnlocked(keyName)
	newSk.flags = FLAG_KEY_TYPE_STRING
	newSk.expiresAt = expiration
	newSk.payload = []byte(fmt.Sprintf("%d", value))

	return
}

func (dsc *dataStoreCommand) addFloat(keyName string, delta float64) (value float64, valid valueExists) {
	dsc.lock()
	defer dsc.unlock()

	expiration := maxTime

	oldSk, exists := dsc.getKeyObjectUnlocked(keyName)
	if exists {
		strBytes := oldSk.getStringBytes()
		if strBytes == nil {
			valid = VALUE_WRONG_TYPE
			return
		}

		var err error
		value, err = strconv.ParseFloat(string(strBytes), 64)
		if err != nil {
			valid = VALUE_WRONG_FORMAT
			return
		}

		value += delta
		expiration = time.Time(oldSk.expiresAt)
	} else {
		value = delta
	}

	bytes := []byte(strconv.FormatFloat(value, 'f', -1, 64))

	newSk := dsc.ds.newStoreKeyUnlocked(keyName)
	newSk.flags = FLAG_KEY_TYPE_STRING
	newSk.expiresAt = expiration
	newSk.payload = bytes

	return
}

func (dsc *dataStoreCommand) getIds(keyNames ...string) (ids []uint64) {
	dsc.lock()
	defer dsc.unlock()

	ids = make([]uint64, 0, len(keyNames))
	for _, keyName := range keyNames {
		sk, exists := dsc.getKeyObjectUnlocked(keyName)
		if exists {
			ids = append(ids, sk.id)
		} else {
			ids = append(ids, 0)
		}
	}
	return
}

func (dsc *dataStoreCommand) bitfieldWrite(keyName string, ops []*bitfieldOp) (output respValue) {
	// find the byte array minimum length
	length := 0
	for _, op := range ops {
		if op.op == BF_GET {
			continue
		}
		n := op.endOffset
		if n > length {
			length = n
		}
	}
	length = (length / 8) + 1

	// get the stored value
	dsc.lock()
	defer dsc.unlock()

	var strBytes []byte
	var expiration time.Time
	sk, exists := dsc.getKeyObjectUnlocked(keyName)
	if exists {
		// ensure the stored byte array is long enough, and preserve expiration
		strBytes = sk.getStringBytes()
		if strBytes == nil {
			output.data = wrongTypeError
			return
		}
		if len(strBytes) < length {
			expanded := make([]byte, length)
			copy(expanded, strBytes)
			strBytes = expanded
		}
		expiration = sk.expiresAt
	} else {
		// make a brand new byte array
		strBytes = make([]byte, length)
		expiration = maxTime
	}

	results := make([]any, 0, len(ops))
	changed := false

	for _, op := range ops {
		bits := op.width

		n := extractBitfield(strBytes, op.bitOffset, op.endOffset)
		if op.signed {
			n = signExtend(n, bits)
		}

		if op.op == BF_GET {
			results = append(results, n)
		} else {
			newValue := op.value
			if op.op == BF_INCRBY {
				newValue = n + newValue
			}

			// detect underflow and overflow
			var outOfBounds bool
			if op.signed {
				outOfBounds = isSignedSumOverflow(n, op.value, bits)
			} else {
				// unsigned underflows when it goes negative
				outOfBounds = newValue < 0 || isUnsignedOverflow(newValue, bits)
			}
			if outOfBounds {
				switch op.oflow {
				case OFLOW_WRAP:
					newValue &= (1 << bits) - 1
					if op.signed {
						newValue = signExtend(newValue, bits)
					}
				case OFLOW_SAT:
					newValue = saturateValue(op.signed, newValue, bits)
				case OFLOW_FAIL:
					results = append(results, nil)
					continue
				}
			}

			setBitfield(strBytes, op.bitOffset, op.width, newValue)
			changed = true

			if op.op == BF_INCRBY {
				results = append(results, respInt(newValue))
			} else {
				results = append(results, respInt(n))
			}
		}
	}

	if changed {
		newSk := dsc.ds.newStoreKeyUnlocked(keyName)
		newSk.flags = FLAG_KEY_TYPE_STRING
		newSk.expiresAt = expiration
		newSk.payload = strBytes
	}

	return nativeValueToResp(results)
}

func (dsc *dataStoreCommand) invertBits(srcKeyName, destKeyName string) (output respValue) {
	var invertedBytes []byte

	dsc.lock()
	defer dsc.unlock()

	sk, exists := dsc.getKeyObjectUnlocked(srcKeyName)
	if exists {
		strBytes := sk.getStringBytes()
		if strBytes == nil {
			output.data = wrongTypeError
			return
		}
		invertedBytes = make([]byte, len(strBytes))
		for idx, b := range strBytes {
			invertedBytes[idx] = ^b
		}
	} else {
		invertedBytes = make([]byte, 0)
	}

	newSk := dsc.ds.newStoreKeyUnlocked(destKeyName)
	newSk.flags = FLAG_KEY_TYPE_STRING
	newSk.expiresAt = maxTime
	newSk.payload = invertedBytes

	output.data = respInt(len(invertedBytes))
	return
}

func (dsc *dataStoreCommand) changeBits(destKeyName string, srcKeyNames []string, op func(a, b byte) byte) (output respValue) {

	values := make([][]byte, 0, len(srcKeyNames))
	longest := 0

	dsc.lock()
	defer dsc.unlock()

	for _, srcKeyName := range srcKeyNames {
		sk, exists := dsc.getKeyObjectUnlocked(srcKeyName)
		if exists {
			strBytes := sk.getStringBytes()
			if strBytes == nil {
				output.data = wrongTypeError
				return
			}
			if len(strBytes) > longest {
				longest = len(strBytes)
			}
			values = append(values, strBytes)
		} else {
			values = append(values, []byte{})
		}
	}

	var resultBytes []byte

	for _, str := range values {
		data := []byte(str)
		if resultBytes == nil {
			resultBytes = make([]byte, longest)
			copy(resultBytes, data)
		} else {
			for i := 0; i < longest; i++ {
				a := extractBitfieldByte(resultBytes, i)
				b := extractBitfieldByte(data, i)
				resultBytes[i] = op(a, b)
			}
		}
	}

	newSk := dsc.ds.newStoreKeyUnlocked(destKeyName)
	newSk.flags = FLAG_KEY_TYPE_STRING
	newSk.expiresAt = maxTime
	newSk.payload = resultBytes

	dsc.ds.data.store(destKeyName, newSk)
	output.data = respInt(len(resultBytes))
	return
}

func (dsc *dataStoreCommand) copy(srcKeyName, destKeyName string, dds *dataStore, replace bool) (result keyOpResult) {
	if dsc.ds == dds {
		// copy within the same data store uses only the data store lock
		dsc.lock()
		defer dsc.unlock()
		dds = dsc.ds
	} else {
		// to acquire two data store locks, the global lock must be held, to prevent
		// a deadlock from two conflicting multi-data store operations
		multiDataStoreLock.Lock()
		defer multiDataStoreLock.Unlock()

		dsc.lock()
		defer dsc.unlock()

		// create a temporary data store command object in order to lock the dest data store
		ddsc := dds.newDataStoreCommand()
		ddsc.lock()
		defer ddsc.unlock()
	}

	newSk, destExists := dsc.ds.copyStoreKeyUnlocked(srcKeyName, destKeyName, dds, replace)
	if newSk == nil {
		if destExists {
			return RESULT_DESTINATION_EXISTS
		} else {
			return RESULT_MISSING_SOURCE
		}
	} else {
		return RESULT_COMPLETED
	}
}

func (dsc *dataStoreCommand) move(srcKeyName, destKeyName string, dds *dataStore, replace bool) (result keyOpResult) {
	if dds == dsc.ds {
		// copy within the same database uses only the database lock
		dsc.lock()
		defer dsc.unlock()
	} else {
		// to acquire two data store locks, the global lock must be held, to prevent
		// a deadlock from two conflicting multi-data store operations
		multiDataStoreLock.Lock()
		defer multiDataStoreLock.Unlock()

		dsc.lock()
		defer dsc.unlock()

		// create a temporary data store command object in order to lock the dest data store
		ddsc := dds.newDataStoreCommand()
		ddsc.lock()
		defer ddsc.unlock()
	}

	newSk, destExists := dsc.ds.moveStoreKeyUnlocked(srcKeyName, destKeyName, dds, replace)
	if newSk == nil {
		if destExists {
			return RESULT_DESTINATION_EXISTS
		} else {
			return RESULT_MISSING_SOURCE
		}
	} else {
		return RESULT_COMPLETED
	}
}

func (dsc *dataStoreCommand) del(keyNames []string, reclaim bool) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	count := 0
	for _, keyName := range keyNames {
		sk, exists := dsc.getKeyObjectUnlocked(keyName)
		if exists {
			count++

			if reclaim {
				dsc.ds.data.remove(keyName)
			} else {
				sk.expiresAt = minTime
			}
		} else if reclaim {
			// remove expired now (if it exists)
			dsc.ds.data.remove(keyName)
		}
	}

	output.data = respInt(count)
	return
}

func (dsc *dataStoreCommand) exists(keyNames []string) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	count := 0
	for _, keyName := range keyNames {
		_, exists := dsc.getKeyObjectUnlocked(keyName)
		if exists {
			count++
		}
	}

	output.data = respInt(count)
	return
}

func simpleChecksum(data []byte) []byte {
	checksum := uint64(0)
	for _, b := range data {
		checksum = bits.RotateLeft64(checksum, 10)
		checksum ^= uint64(b)
	}

	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, checksum)
	return result
}

func (dsc *dataStoreCommand) dump(keyName string) (output respValue) {
	sk, exists := dsc.getKeyObject(keyName)
	if !exists {
		return
	}

	serial := []byte{1, byte(sk.flags)}

	beLen := make([]byte, 4)

	strBytes := sk.getStringBytes()
	if strBytes != nil {
		binary.BigEndian.PutUint32(beLen, uint32(len(strBytes))+1)
	}
	serial = append(serial, beLen...)
	if strBytes != nil {
		serial = append(serial, strBytes...)
	}

	checksum := simpleChecksum(serial)
	serial = append(serial, checksum...)

	output.data = respBulkString(serial)
	return
}

func (dsc *dataStoreCommand) restore(keyName, serializedData string, ttl int64, absttl, replace bool) (output respValue) {
	serial := []byte(serializedData)

	if len(serial) < 10 || serial[0] != 1 {
		output.data = respErrorString("ERR DUMP payload version or checksum are wrong")
		return
	}

	content := serial[:len(serial)-8]
	checksum := serial[len(serial)-8:]
	expected := simpleChecksum(content)
	if !byteUtils.Equal(checksum, expected) {
		output.data = respErrorString("ERR DUMP payload version or checksum are wrong")
		return
	}

	var expiration time.Time
	if ttl != 0 {
		if absttl {
			expiration = time.UnixMilli(ttl)
		} else {
			expiration = time.Now().Add(time.Millisecond * time.Duration(ttl))
		}
	} else {
		expiration = maxTime
	}

	dsc.lock()
	defer dsc.unlock()

	if !replace {
		_, exists := dsc.getKeyObjectUnlocked(keyName)
		if exists {
			output.data = respErrorString("BUSYKEY Target key name already exists.")
			return
		}
	}

	len := binary.BigEndian.Uint32(content[2:6])
	var serialBytes []byte
	if len > 0 {
		serialBytes = content[6 : 6+len-1]
	}

	newSk := dsc.ds.newStoreKeyUnlocked(keyName)
	newSk.flags = bitflags(content[1])
	newSk.expiresAt = expiration
	newSk.payload = serialBytes

	output.data = rstrOK
	return
}

func (dsc *dataStoreCommand) expire(keyName string, expiration time.Time, nx, xx, gt, lt bool) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	sk, exists := dsc.getKeyObjectUnlocked(keyName)
	if !exists {
		output.data = respInt(0)
		return
	}

	if nx {
		if sk.expiresAt.Before(maxTime) {
			output.data = respInt(0)
			return
		}
	} else if xx {
		if !sk.expiresAt.Before(maxTime) {
			output.data = respInt(0)
			return
		}
	} else if gt {
		if !expiration.After(sk.expiresAt) {
			output.data = respInt(0)
			return
		}
	} else if lt {
		if !expiration.Before(sk.expiresAt) {
			output.data = respInt(0)
			return
		}
	}

	sk.expiresAt = expiration
	output.data = respInt(1)
	return
}

func (dsc *dataStoreCommand) expireTime(keyName string) (expiration time.Time, valid int) {
	sk, exists := dsc.getKeyObject(keyName)
	if !exists {
		valid = -2
		return
	}
	if !sk.expiresAt.Before(maxTime) {
		valid = -1
		return
	}
	expiration = sk.expiresAt
	return
}

func (dsc *dataStoreCommand) persist(keyName string) (output respValue) {
	sk, exists := dsc.getKeyObject(keyName)
	if !exists || !sk.expiresAt.Before(maxTime) {
		output.data = respInt(0)
		return
	}
	sk.expiresAt = maxTime
	output.data = respInt(1)
	return
}

func (dsc *dataStoreCommand) randomKey() (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	if dsc.ds.data.count > 0 {
		l := len(dsc.ds.data.buckets)
		n := rand.Intn(l)

		for {
			item := dsc.ds.data.buckets[n]
			if item != nil {
				output.data = respBulkString(item.key)
				return
			}
			n++
			if n >= l {
				n = 0
			}
		}
	}
	return
}

func (dsc *dataStoreCommand) dictScanUnlocked(data *redisDict, cursor uint32, pattern string,
	count int,
	isMatch func(item *redisDictItem) any) (output respValue) {
	result := make([]any, 2)
	matches := make([]any, 0, count)

	highBit := uint32(len(data.buckets)) // always a power of 2
	shift := 32 - bitPosition(highBit)
	mask := highBit - 1

	var pat []rune
	if pattern != "" {
		pat = []rune(pattern)
	}

	cursor &= mask
	for count > 0 {
		index := bits.Reverse32(uint32(cursor) << shift)
		item := data.buckets[index]
		if item != nil {
			match := isMatch(item)
			if match != nil {
				if redisGlob(pat, []rune(item.key)) {
					matches = append(matches, item.key)
					if match != item {
						matches = append(matches, match)
					}
					count--
				}
			}
		}
		nextCursor := uint32(index) + 1
		for nextCursor < highBit {
			if data.buckets[nextCursor] != nil {
				break
			}
			nextCursor++
		}
		cursor = bits.Reverse32(nextCursor << shift)
		if cursor == 0 {
			break
		}
	}

	result[0] = fmt.Sprintf("%d", cursor)
	result[1] = matches

	output = nativeValueToResp(result)
	return
}

func (dsc *dataStoreCommand) scan(cursor uint32, pattern string, count int, requiredFlags bitflags) (output respValue) {

	dsc.lock()
	defer dsc.unlock()

	return dsc.dictScanUnlocked(dsc.ds.data, cursor, pattern, count, func(item *redisDictItem) any {
		sv := item.value.(*storeKey)
		if !sv.isExpiredUnlocked() && flagHasAll(sv.flags, requiredFlags) {
			return item
		} else {
			return nil
		}
	})
}

func (dsc *dataStoreCommand) touch(keyName string) (exists bool) {
	_, exists = dsc.getKeyObject(keyName)
	return
}

func (dsc *dataStoreCommand) getKeyType(keyName string) (keyType string) {
	sk, exists := dsc.getKeyObject(keyName)
	if !exists {
		return "none"
	}

	return storeKeyType(sk.flags)
}

func (dsc *dataStoreCommand) getListUnlocked(keyName string) (list *storeList, err *respErrorString) {
	sk, exists := dsc.getKeyObjectUnlocked(keyName)

	if !exists {
		return
	}

	list = sk.getList()
	if list == nil {
		err = &wrongTypeError
	}
	return
}

func (dsc *dataStoreCommand) ensureListUnlocked(keyName string) (list *storeList, err *respErrorString) {
	list, err = dsc.getListUnlocked(keyName)
	if err != nil {
		return
	}

	if list == nil {
		list = &storeList{}

		newSk := dsc.ds.newStoreKeyUnlocked(keyName)
		newSk.flags = FLAG_KEY_TYPE_LIST
		newSk.expiresAt = maxTime
		newSk.payload = list

		dsc.ds.data.store(keyName, newSk)
	}
	return
}

func (dsc *dataStoreCommand) newListUnlocked(keyName string) (list *storeList) {
	list, _ = dsc.getListUnlocked(keyName)

	if list == nil {
		list = &storeList{}

		newSk := dsc.ds.newStoreKeyUnlocked(keyName)
		newSk.flags = FLAG_KEY_TYPE_LIST
		newSk.expiresAt = maxTime
		newSk.payload = list

		dsc.ds.data.store(keyName, newSk)
	}
	return
}

func (dsc *dataStoreCommand) lpushUnlocked(keyName string, list *storeList, element []byte) {
	item := listItem{
		next:    list.head,
		element: element,
	}
	if list.head == nil {
		list.tail = &item
	} else {
		list.head.prev = &item
	}
	list.head = &item
	list.count++
	dsc.setDirty()
}

func (dsc *dataStoreCommand) lpush(keyName string, values [][]byte) (output respValue) {
	uk := unblockKey{keyName: keyName}

	dsc.lock()
	defer dsc.unlockAndUnblock(&uk)

	list, err := dsc.ensureListUnlocked(keyName)
	if err != nil {
		output.data = *err
		return
	}

	for _, element := range values {
		dsc.lpushUnlocked(keyName, list, element)
	}
	uk.elements = len(values)

	output.data = respInt(list.count)
	return
}

func (dsc *dataStoreCommand) lpushx(keyName string, values [][]byte) (output respValue) {
	uk := unblockKey{keyName: keyName}

	dsc.lock()
	defer dsc.unlockAndUnblock(&uk)

	list, err := dsc.getListUnlocked(keyName)
	if err != nil {
		output.data = *err
		return
	}

	if list == nil || list.count == 0 {
		output.data = respInt(0)
		return
	}

	for _, element := range values {
		dsc.lpushUnlocked(keyName, list, element)
	}
	uk.elements = len(values)

	output.data = respInt(list.count)
	return
}

func (dsc *dataStoreCommand) lpopUnlocked(keyName string, list *storeList, item *listItem) {
	list.head = item.next
	if list.head == nil {
		list.tail = nil
	} else {
		list.head.prev = nil
	}
	list.count--

	// item removed, dereference
	item.next = nil
	item.prev = nil

	// clean up if source list became empty
	if list.count == 0 {
		dsc.ds.data.remove(keyName)
	}

	dsc.setDirty()
}

func (dsc *dataStoreCommand) lpop(keyName string, count int) (values [][]byte, err *respErrorString) {
	dsc.lock()
	defer dsc.unlock()

	list, err := dsc.getListUnlocked(keyName)
	if err != nil || list == nil {
		return
	}

	values = make([][]byte, 0, count)

	for ; count > 0; count-- {
		item := list.head
		if item == nil {
			break
		}
		values = append(values, item.element)
		dsc.lpopUnlocked(keyName, list, item)
	}
	return
}

func (dsc *dataStoreCommand) rpushUnlocked(keyName string, list *storeList, element []byte) {
	item := listItem{
		prev:    list.tail,
		element: element,
	}
	if list.tail == nil {
		list.head = &item
	} else {
		list.tail.next = &item
	}
	list.tail = &item
	list.count++
	dsc.setDirty()
}

func (dsc *dataStoreCommand) rpush(keyName string, values [][]byte) (output respValue) {
	uk := unblockKey{keyName: keyName}

	dsc.lock()
	defer dsc.unlockAndUnblock(&uk)

	list, err := dsc.ensureListUnlocked(keyName)
	if err != nil {
		output.data = *err
		return
	}

	for _, element := range values {
		dsc.rpushUnlocked(keyName, list, element)
	}
	uk.elements = len(values)

	output.data = respInt(list.count)
	return
}

func (dsc *dataStoreCommand) rpushx(keyName string, values [][]byte) (output respValue) {
	uk := unblockKey{keyName: keyName}

	dsc.lock()
	defer dsc.unlockAndUnblock(&uk)

	list, err := dsc.getListUnlocked(keyName)
	if err != nil {
		output.data = *err
		return
	}

	if list == nil || list.count == 0 {
		output.data = respInt(0)
		return
	}

	for _, element := range values {
		dsc.rpushUnlocked(keyName, list, element)
	}
	uk.elements = len(values)

	output.data = respInt(list.count)
	return
}

func (dsc *dataStoreCommand) rpopUnlocked(keyName string, list *storeList, item *listItem) {
	list.tail = item.prev
	if list.tail == nil {
		list.head = nil
	} else {
		list.tail.next = nil
	}
	list.count--

	// item removed, dereference
	item.next = nil
	item.prev = nil

	// clean up if source list became empty
	if list.count == 0 {
		dsc.ds.data.remove(keyName)
	}

	dsc.setDirty()
}

func (dsc *dataStoreCommand) rpop(keyName string, count int) (values [][]byte, err *respErrorString) {
	dsc.lock()
	defer dsc.unlock()

	list, err := dsc.getListUnlocked(keyName)
	if err != nil || list == nil {
		return
	}

	values = make([][]byte, 0, count)

	for ; count > 0; count-- {
		item := list.tail
		if item == nil {
			break
		}
		values = append(values, item.element)
		dsc.rpopUnlocked(keyName, list, item)
	}
	return
}

func (dsc *dataStoreCommand) lindex(keyName string, index int) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	list, err := dsc.getListUnlocked(keyName)
	if err != nil {
		output.data = *err
		return
	}

	if list == nil {
		return
	}

	if index >= 0 {
		if index >= list.count {
			return
		}

		var item *listItem
		for item = list.head; item != nil && index > 0; item = item.next {
			index--
		}
		if index == 0 {
			output.data = respBulkString(string(item.element))
		}
	} else {
		index = -(index + 1)
		if index >= list.count {
			return
		}

		var item *listItem
		for item = list.tail; item != nil && index > 0; item = item.prev {
			index--
		}
		if index == 0 {
			output.data = respBulkString(string(item.element))
		}
	}
	return
}

func (dsc *dataStoreCommand) linsertBeforeUnlocked(list *storeList, pivotItem *listItem, element []byte) {
	newItem := listItem{
		element: element,
	}

	newItem.next = pivotItem
	newItem.prev = pivotItem.prev
	if newItem.prev != nil {
		newItem.prev.next = &newItem
	} else {
		list.head = &newItem
	}
	pivotItem.prev = &newItem

	list.count++
	dsc.setDirty()
}

func (dsc *dataStoreCommand) linsertAfterUnlocked(list *storeList, pivotItem *listItem, element []byte) {
	newItem := listItem{
		element: element,
	}

	newItem.prev = pivotItem
	newItem.next = pivotItem.next
	if newItem.next != nil {
		newItem.next.prev = &newItem
	} else {
		list.tail = &newItem
	}
	pivotItem.next = &newItem

	list.count++
	dsc.setDirty()
}

func (dsc *dataStoreCommand) linsert(keyName string, before bool, pivot, element string) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	list, err := dsc.getListUnlocked(keyName)
	if err != nil {
		output.data = *err
		return
	}

	if list == nil || list.count == 0 {
		output.data = respInt(0)
		return
	}

	var pivotItem *listItem
	for pivotItem = list.head; pivotItem != nil; pivotItem = pivotItem.next {
		if string(pivotItem.element) == pivot {
			break
		}
	}

	if pivotItem == nil {
		output.data = respInt(-1)
		return
	}

	if before {
		dsc.linsertBeforeUnlocked(list, pivotItem, []byte(element))
	} else {
		dsc.linsertAfterUnlocked(list, pivotItem, []byte(element))
	}

	output.data = respInt(list.count)
	return
}

func (dsc *dataStoreCommand) llen(keyName string) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	list, err := dsc.getListUnlocked(keyName)
	if err != nil {
		output.data = *err
		return
	}

	if list == nil {
		output.data = respInt(0)
		return
	}

	output.data = respInt(list.count)
	return
}

func (dsc *dataStoreCommand) lrange(keyName string, start, stop int) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	list, err := dsc.getListUnlocked(keyName)
	if err != nil {
		output.data = *err
		return
	}

	values := []any{}

	if list != nil {
		// convert negative indexes
		if start < 0 {
			start = list.count + start
		}
		if stop < 0 {
			stop = list.count + stop
		}

		// enforce boundaries
		if start < 0 {
			start = 0
		}

		// find the start item
		item := list.head
		var offset int
		for offset = 0; offset < start; offset++ {
			if item == nil {
				break
			}
			item = item.next
		}

		// capture all items in the range
		for ; offset <= stop; offset++ {
			if item == nil {
				break
			}
			values = append(values, string(item.element))
			item = item.next
		}
	}

	output = nativeValueToResp(values)
	return
}

func (dsc *dataStoreCommand) lmove(srcKeyName, destKeyName string, srcLeft, destLeft bool) (output respValue) {
	uk := unblockKey{keyName: destKeyName}

	dsc.lock()
	defer dsc.unlockAndUnblock(&uk)

	srcList, err := dsc.getListUnlocked(srcKeyName)
	if err != nil {
		output.data = *err
		return
	}

	if srcList == nil || srcList.count == 0 {
		return
	}

	destList, err := dsc.ensureListUnlocked(destKeyName)
	if err != nil {
		output.data = *err
		return
	}

	// remove the item from the source list
	var item *listItem
	if srcLeft {
		item = srcList.head
		dsc.lpopUnlocked(srcKeyName, srcList, item)
	} else {
		item = srcList.tail
		dsc.rpopUnlocked(srcKeyName, srcList, item)
	}
	element := item.element

	// place the item into the dest list
	if destLeft {
		dsc.lpushUnlocked(destKeyName, destList, element)
	} else {
		dsc.rpushUnlocked(destKeyName, destList, element)
	}
	uk.elements = 1

	output.data = respBulkString(element)
	return
}

func (dsc *dataStoreCommand) lmpop(keyNames []string, left bool, count int) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	var result []any
	elements := make([]any, 0, count)

	for _, keyName := range keyNames {
		list, err := dsc.getListUnlocked(keyName)
		if err != nil {
			output.data = *err
			return
		}

		if list != nil && list.count > 0 {
			if left {
				for {
					item := list.head
					if item == nil || count <= 0 {
						break
					}
					elements = append(elements, string(item.element))
					dsc.lpopUnlocked(keyName, list, item)
					count--
				}
			} else {
				for {
					item := list.tail
					if item == nil || count <= 0 {
						break
					}
					elements = append(elements, string(item.element))
					dsc.rpopUnlocked(keyName, list, item)
					count--
				}
			}

			result = []any{keyName, elements}
			break
		}
	}

	if len(result) == 0 {
		return
	}

	return nativeValueToResp(result)
}

func (dsc *dataStoreCommand) lpos(keyName string, element string, forward bool, rank, count, maxLength int) (matches []int, err *respErrorString) {
	dsc.lock()
	defer dsc.unlock()

	list, err := dsc.getListUnlocked(keyName)
	if err != nil {
		return
	}

	if list == nil || list.count == 0 {
		return
	}

	if maxLength == 0 {
		maxLength = list.count
	}

	if count == 0 {
		count = list.count
	}

	if rank > 0 {
		rank--
	}

	if forward {
		pos := 0
		for item := list.head; item != nil && maxLength > 0 && count > 0; item = item.next {
			if string(item.element) == element {
				if rank > 0 {
					rank--
				} else {
					matches = append(matches, pos)
					count--
				}
			}
			pos++
			maxLength--
		}
	} else {
		pos := list.count - 1
		for item := list.tail; item != nil && maxLength > 0 && count > 0; item = item.prev {
			if string(item.element) == element {
				if rank > 0 {
					rank--
				} else {
					matches = append(matches, pos)
					count--
				}
			}
			pos--
			maxLength--
		}
	}

	return
}

func (dsc *dataStoreCommand) removeUnlocked(list *storeList, item *listItem) {
	if item.prev != nil {
		item.prev.next = item.next
	} else {
		list.head = item.next
	}
	if item.next != nil {
		item.next.prev = item.prev
	} else {
		list.tail = item.prev
	}
	list.count--

	// item removed, dereference
	item.next = nil
	item.prev = nil

	dsc.setDirty()
}

func (dsc *dataStoreCommand) lremove(keyName string, element string, count int) (removed int, err *respErrorString) {
	dsc.lock()
	defer dsc.unlock()

	list, err := dsc.getListUnlocked(keyName)
	if err != nil {
		return
	}

	if list == nil || list.count == 0 {
		return
	}

	if count == 0 {
		count = list.count
	}

	if count >= 0 {
		for item := list.head; item != nil && count > removed; {
			next := item.next
			if string(item.element) == element {
				dsc.removeUnlocked(list, item)
				removed++
			}
			item = next
		}
	} else {
		count = -count
		for item := list.tail; item != nil && count > removed; {
			next := item.prev
			if string(item.element) == element {
				dsc.removeUnlocked(list, item)
				removed++
			}
			item = next
		}
	}

	return
}

func (dsc *dataStoreCommand) findListItem(list *storeList, count int) (foundItem *listItem) {
	if count < 0 || count >= list.count {
		return
	}
	if count < (list.count / 2) {
		pos := 0
		for item := list.head; item != nil; item = item.next {
			if pos == count {
				foundItem = item
				break
			}
			pos++
		}
	} else {
		pos := list.count - 1
		for item := list.tail; item != nil; item = item.prev {
			if pos == count {
				foundItem = item
				break
			}
			pos--
		}
	}
	return
}

func (dsc *dataStoreCommand) lset(keyName string, element string, count int) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	list, err := dsc.getListUnlocked(keyName)
	if err != nil {
		output.data = *err
		return
	}

	if list == nil || list.count == 0 {
		output.data = respErrorString("ERR no such key")
		return
	}

	// convert negative list position arg
	if count < 0 {
		count = list.count + count
	}

	item := dsc.findListItem(list, count)
	if item == nil {
		output.data = respErrorString("ERR index out of range")
		return
	}

	item.element = []byte(element)
	output.data = rstrOK
	return
}

func (dsc *dataStoreCommand) ltrim(keyName string, start, stop int) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	list, err := dsc.getListUnlocked(keyName)
	if err != nil {
		output.data = *err
		return
	}

	if list == nil {
		output.data = rstrOK
		return
	}

	// convert negative list position args
	if start < 0 {
		start = list.count + start
	}
	if stop < 0 {
		stop = list.count + stop
	}

	// enforce bounds
	if start < 0 {
		start = 0
	} else if start > list.count {
		start = list.count
	}

	if stop < start {
		start = 0
		stop = -1
	} else if stop > list.count {
		stop = list.count
	}

	for start > 0 {
		dsc.lpopUnlocked(keyName, list, list.head)
		start--
		stop--
	}

	stop++
	for stop < list.count {
		dsc.rpopUnlocked(keyName, list, list.tail)
	}

	output.data = rstrOK
	return
}

func (dsc *dataStoreCommand) getHashTableField(keyName, fieldName string) (val string, ve valueExists) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		ve = VALUE_DOESNT_EXIST
		return
	}

	m := sk.getHashTable()
	if m == nil {
		ve = VALUE_WRONG_TYPE
		return
	}

	v, exists := m.get(fieldName)
	if !exists {
		ve = VALUE_DOESNT_EXIST
	} else {
		val = v.(string)
	}
	return
}

func (dsc *dataStoreCommand) getHashTable(keyName string) (val respValue, ve valueExists) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		ve = VALUE_DOESNT_EXIST
		val = nativeValueToResp(map[string]any{})
		return
	}

	m := sk.getHashTable()
	if m == nil {
		ve = VALUE_WRONG_TYPE
		return
	}

	rm := newRespMapSized(m.count)
	for it := m.createIterator(); it.next(); {
		rm.set(nativeValueToResp(it.key), nativeValueToResp(it.value.(string)))
	}
	val.data = rm
	return
}

func (dsc *dataStoreCommand) getHashTableSet(keyName string) (val respValue, ve valueExists) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		ve = VALUE_DOESNT_EXIST
		val.data = respSet{}
		return
	}

	m := sk.getHashTable()
	if m == nil {
		ve = VALUE_WRONG_TYPE
		return
	}

	s := make(respSet, m.count)
	for it := m.createIterator(); it.next(); {
		s[nativeValueToResp(it.key)] = struct{}{}
	}
	val.data = s
	return
}

func (dsc *dataStoreCommand) setHashTableWorker(keyName string, fieldNames, values []string, options bitflags) (added int, wrongType bool) {
	dsc.lock()
	defer dsc.unlock()

	oldSk, exists := dsc.getKeyObjectUnlocked(keyName)

	var m *redisDict
	if exists {
		m = oldSk.getHashTable()
		if m == nil {
			wrongType = true
			return
		}
	} else {
		// new key
		m = newRedisDict()

		newSk := dsc.ds.newStoreKeyUnlocked(keyName)
		newSk.flags = FLAG_KEY_TYPE_HASH_TABLE
		newSk.payload = m
		newSk.expiresAt = maxTime
	}

	for idx, fieldName := range fieldNames {
		_, exists := m.get(fieldName)
		if exists {
			if flagHasOne(options, SET_NOT_EXIST) {
				continue
			}
		} else {
			added++
		}
		m.store(fieldName, values[idx])
		dsc.setDirty()
	}
	return
}

func (dsc *dataStoreCommand) setHashTableFields(keyName string, fieldNames, values []string) (added int, wrongType bool) {
	return dsc.setHashTableWorker(keyName, fieldNames, values, 0)
}

func (dsc *dataStoreCommand) deleteHashTableFields(keyName string, fieldNames []string) (removed int, wrongType bool) {
	dsc.lock()
	defer dsc.unlock()

	sk, exists := dsc.getKeyObjectUnlocked(keyName)

	if exists {
		m := sk.getHashTable()
		if m == nil {
			wrongType = true
			return
		}

		for _, fieldName := range fieldNames {
			if m.remove(fieldName) {
				removed++
				dsc.setDirty()

				if m.count == 0 {
					dsc.ds.data.remove(keyName)
					break
				}
			}
		}
	}
	return
}

func (dsc *dataStoreCommand) fieldAddInt(keyName, fieldName string, delta int64) (value int64, ve valueExists) {
	dsc.lock()
	defer dsc.unlock()

	value = delta

	oldSk, exists := dsc.getKeyObjectUnlocked(keyName)

	var m *redisDict
	if exists {
		m = oldSk.getHashTable()
		if m == nil {
			ve = VALUE_WRONG_TYPE
			return
		}
	} else {
		// new key
		m = newRedisDict()

		newSk := dsc.ds.newStoreKeyUnlocked(keyName)
		newSk.flags = FLAG_KEY_TYPE_HASH_TABLE
		newSk.payload = m
		newSk.expiresAt = maxTime
	}

	oldVal, exists := m.get(fieldName)
	if exists {
		var err error
		oldInt, err := strconv.ParseInt(oldVal.(string), 10, 64)
		if err != nil {
			ve = VALUE_WRONG_FORMAT
			return
		}
		newVal := oldInt + delta
		if (newVal > value) != (delta > 0) {
			ve = VALUE_OVERFLOW
			return
		}
		value = newVal
		ve = VALUE_EXISTS
	} else {
		ve = VALUE_DOESNT_EXIST
	}
	m.store(fieldName, fmt.Sprintf("%d", value))
	dsc.setDirty()

	return
}

func (dsc *dataStoreCommand) fieldAddFloat(keyName, fieldName string, delta float64) (value float64, ve valueExists) {
	value = delta

	dsc.lock()
	defer dsc.unlock()

	if math.IsInf(delta, 0) || math.IsNaN(delta) {
		ve = VALUE_OVERFLOW
		return
	}

	oldSk, exists := dsc.getKeyObjectUnlocked(keyName)

	var m *redisDict
	if exists {
		m = oldSk.getHashTable()
		if m == nil {
			ve = VALUE_WRONG_TYPE
			return
		}
	} else {
		// new key
		m = newRedisDict()

		newSk := dsc.ds.newStoreKeyUnlocked(keyName)
		newSk.flags = FLAG_KEY_TYPE_HASH_TABLE
		newSk.payload = m
		newSk.expiresAt = maxTime
	}

	oldVal, exists := m.get(fieldName)
	if exists {
		var err error
		value, err = strconv.ParseFloat(oldVal.(string), 64)
		if err != nil {
			ve = VALUE_WRONG_FORMAT
			return
		}
		value += delta
		if math.IsInf(value, 0) || math.IsNaN(value) {
			ve = VALUE_OVERFLOW
			return
		}
		dsc.setDirty()
		ve = VALUE_EXISTS
	} else {
		ve = VALUE_DOESNT_EXIST
	}

	m.store(fieldName, strconv.FormatFloat(value, 'f', -1, 64))
	return
}

func (dsc *dataStoreCommand) getHashTableFieldValues(keyName string, fieldNames ...string) (vals []*string, wrongType bool) {
	vals = make([]*string, 0, len(fieldNames))

	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		vals = make([]*string, len(fieldNames))
		return
	}

	m := sk.getHashTable()
	if m == nil {
		wrongType = true
		return
	}

	for _, fieldName := range fieldNames {
		dictVal, exists := m.get(fieldName)

		var val *string
		var str string
		if exists {
			str = dictVal.(string)
			val = &str
		}
		vals = append(vals, val)
	}

	return
}

func (dsc *dataStoreCommand) getHashTableRandField(keyName string, count *int, withValues bool) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		if count != nil {
			if withValues {
				output.data = newRespMap()
			} else {
				output.data = respArray{}
			}
		}
		return
	}

	m := sk.getHashTable()
	if m == nil {
		output.data = wrongTypeError
		return
	}

	var items []*redisDictItem
	var arraySize int
	if count == nil || *count < 0 {
		if count == nil {
			arraySize = 1
		} else {
			arraySize = -(*count)
		}

		items = m.pickRandomItems(arraySize, 85)
	} else {
		arraySize := *count
		items = m.pickUniqueRandomItems(arraySize, 85)
	}

	if withValues {
		if count == nil {
			panic("unexpected argument combination")
		}

		// strange redis behavior - RESP2 returns flat array, RESP3 returns array of pairs (a pair is an array of 2)
		pairs := make(respPairs, 0, arraySize)
		for _, item := range items {
			pair := respPair{
				key:   nativeValueToResp(item.key),
				value: nativeValueToResp(item.value),
			}
			pairs = append(pairs, pair)
		}
		output.data = pairs
	} else {
		a := make([]string, 0, arraySize)
		for _, item := range items {
			a = append(a, item.key)
		}

		if count != nil {
			output = nativeValueToResp(a)
		} else {
			output.data = respBulkString(a[0])
		}
	}
	return
}

func (dsc *dataStoreCommand) getHashTableFields(keyName string) (fields []string, wrongType bool) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		fields = []string{}
		return
	}

	m := sk.getHashTable()
	if m == nil {
		wrongType = true
		return
	}

	fields = make([]string, 0, m.count)
	for i := m.createIterator(); i.next(); {
		fields = append(fields, i.key)
	}

	return
}

func (dsc *dataStoreCommand) getHashTableValues(keyName string) (values []string, wrongType bool) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		values = []string{}
		return
	}

	m := sk.getHashTable()
	if m == nil {
		wrongType = true
		return
	}

	values = make([]string, 0, m.count)
	for i := m.createIterator(); i.next(); {
		values = append(values, i.value.(string))
	}

	return
}

func (dsc *dataStoreCommand) getHashTableCount(keyName string) (count int, wrongType bool) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		return
	}

	m := sk.getHashTable()
	if m == nil {
		wrongType = true
		return
	}

	count = m.count
	return
}

func (dsc *dataStoreCommand) hashTableScan(keyName string, cursor uint32, pattern string, count int) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		result := make([]any, 2)
		result[0] = "0"
		result[1] = []any{}
		output = nativeValueToResp(result)
		return
	}

	m := sk.getHashTable()
	if m == nil {
		output.data = wrongTypeError
		return
	}

	return dsc.dictScanUnlocked(
		m, cursor, pattern, count,
		func(item *redisDictItem) any {
			return item.value
		})
}

func (dsc *dataStoreCommand) getSetMember(keyName, memberName string) (val string, ve valueExists) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		ve = VALUE_DOESNT_EXIST
		return
	}

	m := sk.getSet()
	if m == nil {
		ve = VALUE_WRONG_TYPE
		return
	}

	v, exists := m.get(memberName)
	if !exists {
		ve = VALUE_DOESNT_EXIST
	} else {
		val = v.(string)
	}
	return
}

func (dsc *dataStoreCommand) getSet(keyName string) (val respValue, ve valueExists) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		ve = VALUE_DOESNT_EXIST
		val = nativeValueToResp([]any{})
		return
	}

	m := sk.getSet()
	if m == nil {
		ve = VALUE_WRONG_TYPE
		return
	}

	a := make([]string, 0, m.count*2)
	for it := m.createIterator(); it.next(); {
		a = append(a, it.key, it.value.(string))
	}
	val = nativeValueToResp(a)
	return
}

func (dsc *dataStoreCommand) setAddWorker(keyName string, memberNames []string, options bitflags) (added int, wrongType bool) {
	dsc.lock()
	defer dsc.unlock()

	return dsc.setAddWorkerUnlocked(keyName, memberNames, options)
}

func (dsc *dataStoreCommand) setAddWorkerUnlocked(keyName string, memberNames []string, options bitflags) (added int, wrongType bool) {
	oldSk, exists := dsc.getKeyObjectUnlocked(keyName)

	var m *redisDict
	if exists {
		m = oldSk.getSet()
		if m == nil {
			wrongType = true
			return
		}
	} else {
		// new key
		m = newRedisDict()

		newSk := dsc.ds.newStoreKeyUnlocked(keyName)
		newSk.flags = FLAG_KEY_TYPE_SET
		newSk.payload = m
		newSk.expiresAt = maxTime
	}

	for _, memberName := range memberNames {
		_, exists := m.get(memberName)
		if exists {
			if flagHasOne(options, SET_NOT_EXIST) {
				continue
			}
		} else {
			added++
		}
		m.store(memberName, struct{}{})
		dsc.setDirty()
	}
	return
}

func (dsc *dataStoreCommand) setSetMembers(keyName string, memberNames []string) (added int, wrongType bool) {
	return dsc.setAddWorker(keyName, memberNames, 0)
}

func (dsc *dataStoreCommand) deleteSetMembers(keyName string, memberNames []string) (removed int, wrongType bool) {
	dsc.lock()
	defer dsc.unlock()

	sk, exists := dsc.getKeyObjectUnlocked(keyName)

	if exists {
		m := sk.getSet()
		if m == nil {
			wrongType = true
			return
		}

		for _, memberName := range memberNames {
			if m.remove(memberName) {
				removed++
				dsc.setDirty()

				if m.count == 0 {
					dsc.ds.data.remove(keyName)
					break
				}
			}
		}
	}
	return
}

func (dsc *dataStoreCommand) getSetRandMember(keyName string, count *int) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		if count != nil {
			output = nativeValueToResp([]any{})
		}
		return
	}

	m := sk.getSet()
	if m == nil {
		output.data = wrongTypeError
		return
	}

	var items []*redisDictItem
	var arraySize int
	if count == nil || *count < 0 {
		if count == nil {
			arraySize = 1
		} else {
			arraySize = -(*count)
		}

		items = m.pickRandomItems(arraySize, 85)
	} else {
		arraySize := *count
		items = m.pickUniqueRandomItems(arraySize, 85)
	}

	a := make([]string, 0, arraySize)
	for _, item := range items {
		a = append(a, item.key)
	}

	if count != nil {
		output = nativeValueToResp(a)
	} else {
		output.data = respBulkString(a[0])
	}
	return
}

func (dsc *dataStoreCommand) getSetMembers(keyName string) (members []string, wrongType bool) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		members = []string{}
		return
	}

	m := sk.getSet()
	if m == nil {
		wrongType = true
		return
	}

	members = make([]string, 0, m.count)
	for i := m.createIterator(); i.next(); {
		members = append(members, i.key)
	}

	return
}

func (dsc *dataStoreCommand) getSetCount(keyName string) (count int, wrongType bool) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		return
	}

	m := sk.getSet()
	if m == nil {
		wrongType = true
		return
	}

	count = m.count
	return
}

func (dsc *dataStoreCommand) setScan(keyName string, cursor uint32, pattern string, count int) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		result := make([]any, 2)
		result[0] = "0"
		result[1] = []any{}
		output = nativeValueToResp(result)
		return
	}

	m := sk.getSet()
	if m == nil {
		output.data = wrongTypeError
		return
	}

	return dsc.dictScanUnlocked(
		m, cursor, pattern, count,
		func(item *redisDictItem) any {
			return item
		})
}

func (dsc *dataStoreCommand) setHasMember(keyName, memberName string) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		output.data = respInt(0)
		return
	}

	m := sk.getSet()
	if m == nil {
		output.data = wrongTypeError
		return
	}

	_, exists := m.get(memberName)
	if exists {
		output.data = respInt(1)
	} else {
		output.data = respInt(0)
	}
	return
}

func (dsc *dataStoreCommand) setHasMembers(keyName string, memberNames ...string) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	answers := make([]int, len(memberNames))

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if objExists {
		m := sk.getSet()
		if m == nil {
			output.data = wrongTypeError
			return
		}

		for idx, memberName := range memberNames {
			_, exists := m.get(memberName)
			if exists {
				answers[idx] = 1
			} else {
				answers[idx] = 0
			}
		}
	}

	output = nativeValueToResp(answers)
	return
}

func (dsc *dataStoreCommand) setOperation(
	keyName string,
	op func(firstKeyName string, keyNames ...string) (*redisDict, bool),
	withKeyNames ...string) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	d, wrongType := op(keyName, withKeyNames...)
	if wrongType {
		output.data = wrongTypeError
		return
	}

	a := make([]any, 0, d.count)
	for i := d.createIterator(); i.next(); {
		a = append(a, i.key)
	}
	output = nativeValueToResp(a)
	return
}

func (dsc *dataStoreCommand) setOperationStore(
	destination, keyName string,
	op func(firstKeyName string, keyNames ...string) (*redisDict, bool),
	withKeyNames ...string) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	d, wrongType := op(keyName, withKeyNames...)
	if wrongType {
		output.data = wrongTypeError
		return
	}

	newSk := dsc.ds.newStoreKeyUnlocked(destination)
	newSk.flags = FLAG_KEY_TYPE_SET
	newSk.payload = d
	newSk.expiresAt = maxTime

	output.data = respInt(d.count)
	return
}

func (dsc *dataStoreCommand) setOperationCount(
	limit int,
	op func(limit int, keyNames ...string) (*redisDict, bool),
	keyNames ...string) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	d, wrongType := op(limit, keyNames...)
	if wrongType {
		output.data = wrongTypeError
		return
	}

	output.data = respInt(d.count)
	return
}

func (dsc *dataStoreCommand) diffWorker(firstKey string, keyNames ...string) (d *redisDict, wrongType bool) {
	sk, objExists := dsc.getKeyObjectUnlocked(firstKey)
	if !objExists {
		d = newRedisDict()
		return
	}

	m := sk.getSet()
	if m == nil {
		wrongType = true
		return
	}

	d = m.clone()

	for _, keyName := range keyNames {
		sk2, objExists := dsc.getKeyObjectUnlocked(keyName)
		if !objExists {
			continue
		}
		m2 := sk2.getSet()
		if m2 == nil {
			wrongType = true
			return
		}

		for i := m2.createIterator(); i.next(); {
			d.remove(i.key)
		}
	}

	return
}

func (dsc *dataStoreCommand) diffSet(keyName string, withKeyNames ...string) (output respValue) {
	return dsc.setOperation(keyName, dsc.diffWorker, withKeyNames...)
}

func (dsc *dataStoreCommand) diffSetStore(destination, keyName string, withKeyNames ...string) (output respValue) {
	return dsc.setOperationStore(destination, keyName, dsc.diffWorker, withKeyNames...)
}

func (dsc *dataStoreCommand) intersectWorker(firstKey string, keyNames ...string) (d *redisDict, wrongType bool) {
	sk, objExists := dsc.getKeyObjectUnlocked(firstKey)
	if !objExists {
		d = newRedisDict()
		return
	}

	m := sk.getSet()
	if m == nil {
		wrongType = true
		return
	}

	d = m.clone()

	for _, keyName := range keyNames {
		sk2, objExists := dsc.getKeyObjectUnlocked(keyName)
		if !objExists {
			d = newRedisDict()
			return
		}
		m2 := sk2.getSet()
		if m2 == nil {
			wrongType = true
			return
		}

		removalNames := []string{}

		for i := m.createIterator(); i.next(); {
			_, exists := m2.get(i.key)
			if !exists {
				removalNames = append(removalNames, i.key)
			}
		}

		for _, removalName := range removalNames {
			d.remove(removalName)
		}
	}

	return
}

func (dsc *dataStoreCommand) intersectWithLimitWorker(limit int, keyNames ...string) (d *redisDict, wrongType bool) {
	sets := make([]*redisDict, 0, len(keyNames))
	for _, keyName := range keyNames {
		sk, objExists := dsc.getKeyObjectUnlocked(keyName)
		if !objExists {
			d = newRedisDict()
			return
		}

		m := sk.getSet()
		if m == nil {
			wrongType = true
			return
		}

		sets = append(sets, m)
	}

	d = newRedisDict()
	if len(sets) < 2 {
		return
	}

	s1 := sets[0]
	for iter := s1.createIterator(); iter.next(); {
		found := true
		for i := 1; i < len(sets); i++ {
			s2 := sets[i]
			_, exists := s2.get(iter.key)
			if !exists {
				found = false
				break
			}
		}
		if found {
			d.store(iter.key, iter.value)
			if limit > 0 && d.count == limit {
				return
			}
		}
	}

	return
}

func (dsc *dataStoreCommand) intersectSet(keyName string, withKeyNames ...string) (output respValue) {
	return dsc.setOperation(keyName, dsc.intersectWorker, withKeyNames...)
}

func (dsc *dataStoreCommand) intersectSetStore(destination, keyName string, withKeyNames ...string) (output respValue) {
	return dsc.setOperationStore(destination, keyName, dsc.intersectWorker, withKeyNames...)
}

func (dsc *dataStoreCommand) intersectSetCount(limit int, keyNames ...string) (output respValue) {
	return dsc.setOperationCount(limit, dsc.intersectWithLimitWorker, keyNames...)
}

func (dsc *dataStoreCommand) unionWorker(firstKey string, keyNames ...string) (d *redisDict, wrongType bool) {
	sk, objExists := dsc.getKeyObjectUnlocked(firstKey)

	var m *redisDict
	if !objExists {
		m = newRedisDict()
	} else {
		m = sk.getSet()
		if m == nil {
			wrongType = true
			return
		}
	}

	d = m.clone()

	for _, keyName := range keyNames {
		sk2, objExists := dsc.getKeyObjectUnlocked(keyName)
		if !objExists {
			continue
		}
		m2 := sk2.getSet()
		if m2 == nil {
			wrongType = true
			return
		}

		for i := m2.createIterator(); i.next(); {
			d.store(i.key, struct{}{})
		}
	}

	return
}

func (dsc *dataStoreCommand) unionSet(keyName string, withKeyNames ...string) (output respValue) {
	return dsc.setOperation(keyName, dsc.unionWorker, withKeyNames...)
}

func (dsc *dataStoreCommand) unionSetStore(destination, keyName string, withKeyNames ...string) (output respValue) {
	return dsc.setOperationStore(destination, keyName, dsc.unionWorker, withKeyNames...)
}

func (dsc *dataStoreCommand) setMove(source, destination, memberName string) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	ssk, objExists := dsc.getKeyObjectUnlocked(source)
	if !objExists {
		output.data = respInt(0)
		return
	}

	ss := ssk.getSet()
	if ss == nil {
		output.data = wrongTypeError
		return
	}

	_, exists := ss.get(memberName)
	if !exists {
		output.data = respInt(0)
		return
	}

	added, wrongType := dsc.setAddWorkerUnlocked(destination, []string{memberName}, SET_NOT_EXIST)
	if wrongType {
		output.data = wrongTypeError
		return
	}

	ss.remove(memberName)

	output.data = respInt(added)
	return
}

func (dsc *dataStoreCommand) setRemove(keyName string, members []string) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	sk, objExists := dsc.getKeyObjectUnlocked(keyName)
	if !objExists {
		output.data = respInt(0)
		return
	}

	m := sk.getSet()
	if m == nil {
		output.data = wrongTypeError
		return
	}

	removals := 0
	for _, member := range members {
		if m.remove(member) {
			removals++
		}
	}

	output.data = respInt(removals)
	return
}

func (dsc *dataStoreCommand) save(l lane.Lane, path string) (err error) {
	if dsc.ds.data.dirty {
		dsc.lock()
		defer dsc.unlock()

		if err = dsc.ds.save(path); err != nil {
			l.Errorf("Unable to save to %s. Error: %s", path, err)
			return
		}

		dsc.ds.data.dirty = false
		l.Tracef("Changes saved to %s", path)
	}

	return
}

func (dsc *dataStoreCommand) load(l lane.Lane, path string) (err error) {
	dsc.lock()
	defer dsc.unlock()

	if err = dsc.ds.load(path); err != nil {
		l.Errorf("Failed to load saved data from %s. Error: %s", path, err)
		return
	}

	l.Infof("Loaded saved data from %s", path)
	return
}

func (dsc *dataStoreCommand) sort(sourceKeyName, byPattern, destKeyName string, startAt, count int, getPatterns []string, limit, desc, alpha bool) (output respValue) {
	dsc.lock()
	defer dsc.unlock()

	// get the values to sort
	var vals []sortVal
	list, _ := dsc.getListUnlocked(sourceKeyName)
	if list != nil {
		// convert linked list into a value array
		vals = make([]sortVal, 0, list.count)
		for i := list.head; i != nil; i = i.next {
			sv := sortVal{
				data: string(i.element),
			}
			vals = append(vals, sv)
		}
	} else {
		sk, objExists := dsc.getKeyObjectUnlocked(sourceKeyName)
		if !objExists {
			output = nativeValueToResp([]any{})
			return
		}

		ss := sk.getSet()
		if ss != nil {
			// convert set (a hash table) into a value array
			vals = make([]sortVal, 0, ss.count)
			for i := ss.createIterator(); i.next(); {
				sv := sortVal{
					data: i.value.(string),
				}
				vals = append(vals, sv)
			}
		} else {
			output.data = wrongTypeError
			return
		}
	}

	dontSort := false
	if byPattern != "" {
		if !strings.Contains(byPattern, "*") {
			dontSort = true
		} else {
			for idx, val := range vals {
				pat := strings.Replace(byPattern, "*", val.data, 1)
				byVal, byValExists := dsc.getKeyUnlocked(pat)
				if byValExists != VALUE_EXISTS {
					val.sortByStr = "0"
					val.sortByFloat = 0
				} else {
					val.sortByStr = byVal
					if !alpha {
						f64, parseErr := strconv.ParseFloat(byVal, 64)
						if parseErr != nil {
							output.data = respErrorString("ERR One or more scores can't be converted into double")
							return
						}
						val.sortByFloat = f64
					}
				}
				vals[idx] = val
			}
		}
	}

	if !dontSort {
		// pick a sorting strategy
		if alpha {
			if !desc {
				// asc alpha
				sort.Slice(vals, func(i, j int) bool {
					return vals[i].sortByStr < vals[j].sortByStr
				})
			} else {
				// desc alpha
				sort.Slice(vals, func(i, j int) bool {
					return vals[j].sortByStr < vals[i].sortByStr
				})
			}
		} else {
			if !desc {
				// asc numeric
				sort.Slice(vals, func(i, j int) bool {
					return vals[i].sortByFloat < vals[j].sortByFloat
				})
			} else {
				// desc numeric
				sort.Slice(vals, func(i, j int) bool {
					return vals[j].sortByFloat < vals[i].sortByFloat
				})
			}
		}
	}

	if limit {
		start := startAt
		if start < 0 {
			start = 0
		}

		if count < 0 {
			count = len(vals)
		}

		end := start + count
		if start >= len(vals) {
			start = 0
			end = 0
		} else if end < start {
			end = start
		} else if end >= len(vals) {
			end = len(vals)
		}
		vals = vals[start:end]
	}

	// produce the output array
	if len(getPatterns) == 0 {
		getPatterns = []string{"#"}
	}

	a := make([]respValue, 0, len(vals)*len(getPatterns))
	nilVal := respValue{}

	for _, val := range vals {
		for _, getPattern := range getPatterns {
			if getPattern == "#" {
				rv := respValue{data: respBulkString(val.data)}
				a = append(a, rv)
			} else {
				// replace the first asterisk in the get pattern with the list item value
				if !strings.Contains(getPattern, "*") {
					a = append(a, nilVal)
				} else {
					pat := strings.Replace(getPattern, "*", val.data, 1)
					sk, exists := dsc.getKeyUnlocked(pat)
					if exists != VALUE_EXISTS {
						a = append(a, nilVal)
					} else {
						str := respValue{data: respBulkString(sk)}
						a = append(a, str)
					}
				}
			}
		}
	}

	if destKeyName != "" {
		list := dsc.newListUnlocked(destKeyName)

		for _, element := range a {
			str, _ := element.toString()
			dsc.rpushUnlocked(destKeyName, list, []byte(str))
		}

		output.data = respInt(list.count)
	} else {
		output = nativeValueToResp(a)
	}
	return
}
