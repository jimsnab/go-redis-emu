package redisemu

import (
	"fmt"
)

type (
	// wakeSignal corresponds to a client that is waiting
	wakeSignal struct {
		ready chan struct{}

		// objects that will raise the signal if written to
		objectsHead *signalListTuple
		objectsTail *signalListTuple
		id          int // debugging only
	}

	// signalListTuple connects a waiting client to a queue the client's signal is in; the
	// signal can be in more than one wait list at a time
	signalListTuple struct {
		// the signal and the linkage to the other lists it is in
		signal      *wakeSignal
		objectsNext *signalListTuple
		objectsPrev *signalListTuple

		// the list for this reference; many signalListTuples can point to the same list
		waitList  *objectWaitList
		queueNext *signalListTuple
		queuePrev *signalListTuple
	}

	// an object wait list is an ordered list of signals waiting for the object to change
	objectWaitList struct {
		name      string
		queueHead *signalListTuple
		queueTail *signalListTuple
	}

	// a table mapping one wait list per object name
	waitTable struct {
		table map[string]*objectWaitList
	}
)

var signals int

func newWaitTable() *waitTable {
	return &waitTable{
		table: map[string]*objectWaitList{},
	}
}

// creates a wake signal object, one-to-one mapping to a client
// (two clients cannot wait on the same wake signal)
func newWakeSignal() *wakeSignal {
	signals++
	ws := &wakeSignal{
		ready: make(chan struct{}, 1),
		id:    signals,
	}
	return ws
}

// add this wake signal to the end of the specific wait list and to
// a list of all objects that can raise the signal
func (ws *wakeSignal) joinWaitList(owl *objectWaitList) {
	ref := &signalListTuple{
		signal:      ws,
		waitList:    owl,
		objectsPrev: ws.objectsTail,
		queuePrev:   owl.queueTail,
	}

	// place at the end of the object's queue
	if owl.queueTail == nil {
		owl.queueHead = ref
	} else {
		owl.queueTail.queueNext = ref
	}
	owl.queueTail = ref

	// track in the wake signal's list of lists
	if ws.objectsTail == nil {
		ws.objectsHead = ref
	} else {
		ws.objectsTail.objectsNext = ref
	}
	ws.objectsTail = ref
}

// take the ref out of the object's blocked queue and out of the
// wake signal's objects list, and return true if the objects
// list became empty
func (ref *signalListTuple) unlink() (listEmpty bool) {
	// remove from the list of objects
	if ref.objectsPrev != nil {
		ref.objectsPrev.objectsNext = ref.objectsNext
	} else {
		ref.signal.objectsHead = ref.objectsNext
	}
	if ref.objectsNext != nil {
		ref.objectsNext.objectsPrev = ref.objectsPrev
	} else {
		ref.signal.objectsTail = ref.objectsPrev
	}

	// remove from the object queue
	if ref.queuePrev != nil {
		ref.queuePrev.queueNext = ref.queueNext
	} else {
		ref.waitList.queueHead = ref.queueNext
	}
	if ref.queueNext != nil {
		ref.queueNext.queuePrev = ref.queuePrev
	} else {
		ref.waitList.queueTail = ref.queuePrev
	}

	listEmpty = (ref.waitList.queueHead == nil)

	// disconnect links
	ref.signal = nil
	ref.waitList = nil
	ref.objectsPrev = nil
	ref.objectsNext = nil
	ref.queuePrev = nil
	ref.queueNext = nil
	return
}

// adds a client that is waiting on a single object; client then
// can wait on ws.ready channel
func (wt *waitTable) enterWait(name string) (ws *wakeSignal) {
	ws = newWakeSignal()

	list, exists := wt.table[name]
	if !exists {
		list = &objectWaitList{
			name: name,
		}
		wt.table[name] = list
	}

	ws.joinWaitList(list)
	return
}

// adds a client that is waiting on many objects; client then can
// wait on ws.ready channel
func (wt *waitTable) enterMultiWait(names []string) (ws *wakeSignal) {
	ws = newWakeSignal()

	// get in each object's list
	for _, name := range names {
		list, exists := wt.table[name]
		if !exists {
			list = &objectWaitList{
				name: name,
			}
			wt.table[name] = list
		}
		ws.joinWaitList(list)
	}

	return
}

// Removes a client wake signal from all wait lists it is in, because
// the wait is now unblocked, or the client cancels the wait.
func (wt *waitTable) unlinkWakeSignal(ws *wakeSignal) {
	// leave each list
	for ws.objectsHead != nil {
		ref := ws.objectsHead
		name := ref.waitList.name
		if ref.unlink() {
			delete(wt.table, name)
		}
	}
}

// Cleans up a client wake signal.
func (wt *waitTable) disposeWakeSignal(ws *wakeSignal) {
	wt.unlinkWakeSignal(ws)
	close(ws.ready)
}

// removes the head of the list for the named object (if any),
// and releases a semaphore to unblock the waiting client (if
// still waiting)
func (wt *waitTable) unblock(name string, elements int) {
	list, exists := wt.table[name]
	if exists {
		for i := 0; i < elements; i++ {
			ref := list.queueHead
			if ref == nil {
				break // less blocked clients than pushes
			}
			ws := ref.signal
			wt.unlinkWakeSignal(ws)
			ws.ready <- struct{}{}
		}
	}
}

func (wt *waitTable) dump() {
	signals := map[int]*wakeSignal{}

	// go through all of the keys, and develop a list of waiting clients
	for _, list := range wt.table {
		for ref := list.queueHead; ref != nil; ref = ref.queueNext {
			signals[ref.signal.id] = ref.signal
		}
	}

	// verify all of the signals are in lists
	for id, signal := range signals {
		if id != signal.id {
			panic("id != signal.id")
		}

		for ref := signal.objectsHead; ref != nil; ref = ref.objectsNext {
			list, exists := wt.table[ref.waitList.name]
			if !exists {
				panic("signal links to name that is missing from index")
			}
			found := false
			for signalListTuple := list.queueTail; signalListTuple != nil; signalListTuple = signalListTuple.queuePrev {
				if signalListTuple.signal.id == id {
					found = true
					break
				}
			}
			if !found {
				panic("could not find a signal in the expected list queue")
			}
		}
	}

	fmt.Printf("Waiting clients:\n")
	for _, signal := range signals {
		if signal.objectsHead == signal.objectsTail {
			ref := signal.objectsHead
			if ref.objectsNext != nil || ref.objectsPrev != nil {
				panic("signal's list list is corrupt")
			}
			fmt.Printf(" Signal %d is waiting on object %s\n", signal.id, ref.waitList.name)
		} else {
			fmt.Printf(" Signal %d waits on multiple objects:\n", signal.id)
			for ref := signal.objectsHead; ref != nil; ref = ref.objectsNext {
				fmt.Printf("  waiting on object %s\n", ref.waitList.name)
			}
		}
	}
	fmt.Println()

	fmt.Printf("Object wait queues:\n")
	for name, list := range wt.table {
		fmt.Printf(" Object %s list:\n", name)
		for w := list.queueHead; w != nil; w = w.queueNext {
			fmt.Printf("  signal %d waiting\n", w.signal.id)
		}
	}
	fmt.Println("----")
}
