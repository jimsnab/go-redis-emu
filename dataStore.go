package goredisemu

import (
	"sync"
	"sync/atomic"
	"time"
)

var multiDataStoreLock sync.Mutex

type (
	dataStore struct {
		dataObjectNumber uint64
		mu               sync.Mutex
		commandNumber    uint32
		multiLock        uint32
		data             *redisDict
		cursors          map[int64]*storeKey
		cursorsSize      int
		waitingClients   *waitTable
	}
)

func newDataStore() *dataStore {
	return &dataStore{
		data:           newRedisDict(),
		cursors:        make(map[int64]*storeKey, 2),
		cursorsSize:    2,
		waitingClients: newWaitTable(),
	}
}

// creates an object used for data store locking
func (ds *dataStore) newDataStoreCommand() *dataStoreCommand {
	id := (atomic.AddUint32(&ds.commandNumber, 1) & 0x7FFFFFFF) | 0x8000000
	return &dataStoreCommand{
		id: id,
		ds: ds,
	}
}

func (ds *dataStore) getStoreKey(keyName string) (sk *storeKey, exists bool) {
	val, exists := ds.data.get(keyName)
	if exists {
		sk = val.(*storeKey)
		sk.lastAccess = time.Now()
	}
	return
}

func (ds *dataStore) hasChangedUnlocked(keyName string, id uint64) bool {
	sk, exists := ds.getStoreKey(keyName)
	if !exists {
		return id != 0
	} else {
		return id != sk.id
	}
}

func (ds *dataStore) newStoreKeyUnlocked(keyName string) *storeKey {
	ds.dataObjectNumber++
	sk := &storeKey{
		id:         ds.dataObjectNumber,
		lastAccess: time.Now(),
	}
	ds.data.store(keyName, sk)
	return sk
}

// makes a full copy of a store key, optionally into a different data store
func (ds *dataStore) copyStoreKeyUnlocked(srcKeyName, destKeyName string, dds *dataStore, overwrite bool) (newSk *storeKey, destExists bool) {
	sk, exists := ds.getStoreKey(srcKeyName)
	if !exists {
		return
	}

	if !overwrite {
		_, destExists = dds.getStoreKey(destKeyName)
		if destExists {
			return
		}
	}

	dds.dataObjectNumber++
	newSk = sk.clone(dds.dataObjectNumber)
	dds.data.store(destKeyName, newSk)
	return
}

// moves a store key, optionally into a different data store
func (ds *dataStore) moveStoreKeyUnlocked(srcKeyName, destKeyName string, dds *dataStore, overwrite bool) (newSk *storeKey, destExists bool) {
	sk, exists := ds.getStoreKey(srcKeyName)
	if !exists {
		return
	}

	if !overwrite {
		_, destExists = dds.getStoreKey(destKeyName)
		if destExists {
			return
		}
	}

	// detach sk from the source db
	ds.data.remove(srcKeyName)

	// give sk a new id and link it to the dest db
	dds.dataObjectNumber++
	sk.id = dds.dataObjectNumber
	dds.data.store(destKeyName, sk)

	newSk = sk
	return
}

func (ds *dataStore) enterListBlock(keyName string) (ws *wakeSignal) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.waitingClients.enterWait(keyName)
}

func (ds *dataStore) enterListMultiBlock(keyNames []string) (ws *wakeSignal) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.waitingClients.enterMultiWait(keyNames)
}

func (ds *dataStore) leaveListBlock(ws *wakeSignal) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.waitingClients.disposeWakeSignal(ws)
}

func (ds *dataStore) unblockListUnlocked(keyName string, elements int) {
	ds.waitingClients.unblock(keyName, elements)
}
