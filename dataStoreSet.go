package redisemu

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/jimsnab/go-lane"
)

type (
	dataStoreSet struct {
		mu       sync.Mutex
		basePath string
		dbs      map[int]*dataStore
		users    map[string]*dataStoreUser
	}
)

func newDataStoreSet(l lane.Lane, basePath string) *dataStoreSet {
	dss := &dataStoreSet{
		basePath: basePath,
		dbs:      map[int]*dataStore{},
		users:    map[string]*dataStoreUser{"default": newDataStoreUser()},
	}

	dss.createDbUnlocked(0)
	if basePath != "" {
		// Search the file system for persisted data, and load each data store
		dir, fileBase := filepath.Split(basePath)
		if dir == "" {
			dir = "."
		}

		// data store files are <base-name>.db<n> where <base-name> is user provided
		// and <n> is the data store index.
		fileBase += ".db"

		filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if !d.IsDir() {
				if strings.HasPrefix(d.Name(), fileBase) {
					n64, parseErr := strconv.ParseInt(d.Name()[len(fileBase):], 10, 32)
					n := int(n64)
					if parseErr == nil {
						// found a data store file - load it
						if n != 0 {
							dss.createDbUnlocked(n)
						}
						dsc := dss.dbs[n].newDataStoreCommand()
						loadErr := dsc.load(l, path)
						if loadErr != nil {
							return loadErr
						}
					}
				}
			}

			return nil
		})
	}
	return dss
}

func (dss *dataStoreSet) save(l lane.Lane) error {
	for index, ds := range dss.dbs {
		dsc := ds.newDataStoreCommand()
		err := dsc.save(l, dss.dataStoreFileName(index))
		if err != nil {
			return err
		}
	}
	return nil
}

func (dss *dataStoreSet) dataStoreFileName(index int) string {
	if dss.basePath == "" {
		return ""
	}
	return fmt.Sprintf("%s.db%d", dss.basePath, index)
}

func (dss *dataStoreSet) createDbUnlocked(index int) (ds *dataStore, valid bool) {
	if index < 0 || index > 15 {
		return
	}
	ds, exists := dss.dbs[index]
	if !exists {
		ds = newDataStore()
		dss.dbs[index] = ds
	}

	return ds, true
}

func (dss *dataStoreSet) getDb(index int, create bool) (ds *dataStore, valid bool) {
	dss.mu.Lock()
	defer dss.mu.Unlock()

	ds, exists := dss.dbs[index]
	if !exists {
		if create {
			if ds, valid = dss.createDbUnlocked(index); !valid {
				return
			}
		} else {
			return
		}
	}

	valid = true
	return
}

func (dss *dataStoreSet) flushDb(index int) {
	dss.mu.Lock()
	defer dss.mu.Unlock()

	delete(dss.dbs, index)
}

func (dss *dataStoreSet) flushAll() {
	dss.mu.Lock()
	defer dss.mu.Unlock()

	dss.dbs = map[int]*dataStore{}
}

func (dss *dataStoreSet) getUser(userName string) (dsu *dataStoreUser, exists bool) {
	dsu, exists = dss.users[userName]
	return
}

func (dss *dataStoreSet) dbSize(index int) (size respInt, valid bool) {
	dss.mu.Lock()
	defer dss.mu.Unlock()

	ds, exists := dss.dbs[index]
	if exists {
		size = respInt(ds.data.count)
		valid = true
	}

	return
}
