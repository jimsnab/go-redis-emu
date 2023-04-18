package redisemu

import "testing"

type (
	testWakeSignal struct {
		wakeSignal
		ch chan struct{}
	}
)

func newTestWakeSignal(ws *wakeSignal) *testWakeSignal {
	return &testWakeSignal{
		wakeSignal: *ws,
		ch:         make(chan struct{}, 1),
	}
}

// this function abstracts the channel so that we could potentially use other
// sync methods such as a semaphore
func (tws *testWakeSignal) isReady() chan struct{} {
	return tws.ready
}

func TestWaitTableSingle(t *testing.T) {
	wt := newWaitTable()
	ws := newTestWakeSignal(wt.enterWait("key1"))

	select {
	case <-ws.isReady():
		t.Fatal("signal without unblock")
	default:
		// expected
	}
	wt.dump()
	wt.unblock("key1", 1)
	wt.dump()
	select {
	case <-ws.isReady():
		// expected
	default:
		t.Fatal("no signal after unblock")
	}
}

func TestWaitTableSingle2(t *testing.T) {
	wt := newWaitTable()
	ws1 := newTestWakeSignal(wt.enterWait("key1"))
	ws2 := newTestWakeSignal(wt.enterWait("key1"))

	select {
	case <-ws1.isReady():
		t.Fatal("signal 1 without unblock")
	case <-ws2.isReady():
		t.Fatal("signal 2 without unblock")
	default:
		// expected
	}
	wt.dump()

	wt.unblock("key1", 1)
	wt.dump()
	select {
	case <-ws1.isReady():
		// expected
	case <-ws2.isReady():
		t.Fatal("signals out of order")
	default:
		t.Fatal("no signal after unblock")
	}

	wt.unblock("key1", 1)
	wt.dump()
	select {
	case <-ws1.isReady():
		t.Fatal("signals out of order")
	case <-ws2.isReady():
		// expected
	default:
		t.Fatal("no signal after unblock")
	}
}

func TestWaitTableSingleAndMulti(t *testing.T) {
	wt := newWaitTable()
	ws1 := newTestWakeSignal(wt.enterWait("key1"))
	ws2 := newTestWakeSignal(wt.enterWait("key1"))
	ws3 := newTestWakeSignal(wt.enterMultiWait([]string{"key1", "key2"}))

	select {
	case <-ws1.isReady():
		t.Fatal("signal 1 without unblock")
	case <-ws2.isReady():
		t.Fatal("signal 2 without unblock")
	case <-ws3.isReady():
		t.Fatal("signal 3 without unblock")
	default:
		// expected
	}
	wt.dump()

	wt.unblock("key1", 1)
	wt.dump()
	select {
	case <-ws1.isReady():
		// expected
	case <-ws2.isReady():
		t.Fatal("signals out of order")
	case <-ws3.isReady():
		t.Fatal("signals out of order")
	default:
		t.Fatal("no signal after unblock")
	}

	wt.unblock("key1", 1)
	wt.dump()
	select {
	case <-ws1.isReady():
		t.Fatal("signals out of order")
	case <-ws2.isReady():
		// expected
	case <-ws3.isReady():
		t.Fatal("signals out of order")
	default:
		t.Fatal("no signal after unblock")
	}

	wt.unblock("key1", 1)
	wt.dump()
	select {
	case <-ws1.isReady():
		t.Fatal("signals out of order")
	case <-ws2.isReady():
		t.Fatal("signals out of order")
	case <-ws3.isReady():
		// expected
	default:
		t.Fatal("no signal after unblock")
	}
}

func TestWaitTableSingleAndMulti2(t *testing.T) {
	wt := newWaitTable()
	ws1 := newTestWakeSignal(wt.enterWait("key1"))
	ws2 := newTestWakeSignal(wt.enterWait("key1"))
	ws3 := newTestWakeSignal(wt.enterMultiWait([]string{"key1", "key2"}))
	select {
	case <-ws1.isReady():
		t.Fatal("signal 1 without unblock")
	case <-ws2.isReady():
		t.Fatal("signal 2 without unblock")
	case <-ws3.isReady():
		t.Fatal("signal 3 without unblock")
	default:
		// expected
	}
	wt.dump()

	wt.unblock("key2", 1)
	wt.dump()
	select {
	case <-ws1.isReady():
		t.Fatal("signals out of order")
	case <-ws2.isReady():
		t.Fatal("signals out of order")
	case <-ws3.isReady():
		// expected
	default:
		t.Fatal("no signal after unblock")
	}

	wt.unblock("key1", 1)
	wt.dump()
	select {
	case <-ws1.isReady():
		// expected
	case <-ws2.isReady():
		t.Fatal("signals out of order")
	case <-ws3.isReady():
		t.Fatal("signals out of order")
	default:
		t.Fatal("no signal after unblock")
	}

	wt.unblock("key1", 1)
	wt.dump()
	select {
	case <-ws1.isReady():
		t.Fatal("signals out of order")
	case <-ws2.isReady():
		// expected
	case <-ws3.isReady():
		t.Fatal("signals out of order")
	default:
		t.Fatal("no signal after unblock")
	}
}

func TestWaitTableSingleAndMultiTwoElements(t *testing.T) {
	wt := newWaitTable()
	ws1 := newTestWakeSignal(wt.enterWait("key1"))
	ws2 := newTestWakeSignal(wt.enterMultiWait([]string{"key1", "key2"}))
	ws3 := newTestWakeSignal(wt.enterWait("key1"))

	select {
	case <-ws1.isReady():
		t.Fatal("signal 1 without unblock")
	case <-ws2.isReady():
		t.Fatal("signal 2 without unblock")
	case <-ws3.isReady():
		t.Fatal("signal 3 without unblock")
	default:
		// expected
	}
	wt.dump()

	wt.unblock("key1", 2)
	wt.dump()

	hasOne := 0
	hasTwo := 0
	select {
	case <-ws1.isReady():
		hasOne++
	case <-ws2.isReady():
		hasTwo++
	case <-ws3.isReady():
		t.Fatal("signals out of order")
	default:
		t.Fatal("no signal after unblock")
	}
	select {
	case <-ws1.isReady():
		hasOne++
	case <-ws2.isReady():
		hasTwo++
	case <-ws3.isReady():
		t.Fatal("signals out of order")
	default:
		t.Fatal("no signal after unblock")
	}

	if hasOne != 1 || hasTwo != 1 {
		t.Fatal("signals out of order")
	}

	wt.unblock("key1", 1)
	wt.dump()
	select {
	case <-ws1.isReady():
		t.Fatal("signals out of order")
	case <-ws2.isReady():
		t.Fatal("signals out of order")
	case <-ws3.isReady():
		// expected
	default:
		t.Fatal("no signal after unblock")
	}
}

func TestWaitTableSingleAndMultiTwoElements2(t *testing.T) {
	wt := newWaitTable()
	ws1 := newTestWakeSignal(wt.enterWait("key1"))
	ws2 := newTestWakeSignal(wt.enterMultiWait([]string{"key1", "key2"}))
	ws3 := newTestWakeSignal(wt.enterWait("key1"))

	select {
	case <-ws1.isReady():
		t.Fatal("signal 1 without unblock")
	case <-ws2.isReady():
		t.Fatal("signal 2 without unblock")
	case <-ws3.isReady():
		t.Fatal("signal 3 without unblock")
	default:
		// expected
	}
	wt.dump()

	wt.unblock("key2", 2)
	wt.dump()

	select {
	case <-ws1.isReady():
		t.Fatal("signals out of order")
	case <-ws2.isReady():
		// expected
	case <-ws3.isReady():
		t.Fatal("signals out of order")
	default:
		t.Fatal("no signal after unblock")
	}

	wt.unblock("key1", 1)
	wt.dump()
	select {
	case <-ws1.isReady():
		// expected
	case <-ws2.isReady():
		t.Fatal("signals out of order")
	case <-ws3.isReady():
		t.Fatal("signals out of order")
	default:
		t.Fatal("no signal after unblock")
	}

	wt.unblock("key1", 1)
	wt.dump()
	select {
	case <-ws1.isReady():
		t.Fatal("signals out of order")
	case <-ws2.isReady():
		t.Fatal("signals out of order")
	case <-ws3.isReady():
		// expected
	default:
		t.Fatal("no signal after unblock")
	}
}
