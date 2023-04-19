package redisemu

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

type (
	RedisTestClient interface {
		// provides the logging lane interface for the client connection
		Lane() lane.Lane

		// makes another test client for the same data store set
		AdditionalClient() RedisTestClient

		// issues a command to the test server
		ProcessCommand(cmdName string, args ...any) (output respValue)

		// test diagnostics - logs the content of a key
		DumpKey(keyName string)

		// test diagnostics - logs the content of a bitmap
		DumpBitmapKey(keyName string)

		// blocking shutdown/cleanup for any test client resources
		Terminate()

		// called at the end of each test for cleanup
		Close()

		// provides client connection settings (such as socket IP addresses) for CLIENT INFO and CLIENT LIST commands
		ClientInfo() []string

		// gives client connection the opportunity to evaluate filter properties such as addr and laddr
		MatchFilter(filter map[string]string) bool

		// sets the client to close the connection once the current command (if any) completes
		RequestClose()

		// indicates that client close is pending or has been completed
		IsCloseRequested() bool

		// provides the client ID
		ClientID() int64

		// provides the state of the no-evict setting
		IsNoEvict() bool

		// provides the test local server IP address
		ServerAddr() string

		// provides the test client IP address
		ClientAddr() string

		// returns "now" time according to the server clock (+/- a few ms)
		ServerNow() time.Time
	}

	testClient struct {
		mu         sync.Mutex
		started    time.Time
		dss        *dataStoreSet
		disp       *cmdDispatcher
		cs         *clientState
		processing int32
		terminated bool
		addr       string
		laddr      string
	}
)

var testPort int32 = 50000

func NewRedisTestClient(t *testing.T) RedisTestClient {
	return NewRedisTestClientResp3(t)
}

func NewRedisTestClientResp2(t *testing.T) RedisTestClient {
	if testRealRedis {
		return newRealRedisClient()
	}

	l := lane.NewLogLane(context.Background())

	rd := newRespDeserializerFromResource(l, cmdSpec)
	value, _, valid := rd.deserializeNext()
	if !valid {
		l.Fatal("invalid cmdSpec definition content")
	}

	cmds := redisCommands{}
	if valid = cmds.respDeserialize(l, value); !valid {
		l.Fatal("failed to deserialize command definitions")
	}

	ri := newRespDeserializerFromResource(l, cmdInfoSpec)
	value, _, valid = ri.deserializeNext()
	if !valid {
		l.Fatal("invalid cmdInfoSpec definition content")
	}

	info := newRedisInfoTable()
	if valid = info.respDeserialize(l, value); !valid {
		l.Fatal("failed to deserialize command info definitions")
	}

	port := atomic.AddInt32(&testPort, 1)

	ts := &testClient{
		started: time.Now(),
		dss:     newDataStoreSet(l, ""),
		addr:    fmt.Sprintf("1.2.3.4:%d", port),
		laddr:   "127.0.0.1:6379",
	}

	ts.disp = newCmdDispatcher(cmds, info, ts.dss)
	ts.cs = newClientState(l, ts, ts.disp)
	return ts
}

func NewRedisTestClientResp3(t *testing.T) RedisTestClient {
	client := newRealRedisClient()
	output := client.ProcessCommand("HELLO", "3")
	if _, ok := output.data.(respMap); !ok {
		t.Fatal("resp3 hello failed")
	}
	return client
}

func (ts *testClient) Lane() lane.Lane {
	return ts.cs.l
}

func (ts *testClient) AdditionalClient() RedisTestClient {
	client := &testClient{
		started: time.Now(),
		dss:     ts.dss,
	}
	client.cs = newClientState(ts.cs.l.Derive(), client, ts.cs.disp)
	return client
}

func (ts *testClient) ProcessCommand(cmdName string, args ...any) (output respValue) {
	if !atomic.CompareAndSwapInt32(&ts.processing, 0, 1) {
		panic("another command is pending")
	}
	nativeArray := []any{}
	nativeArray = append(nativeArray, cmdName)
	nativeArray = append(nativeArray, args...)
	dispatchArray := nativeValueToResp(nativeArray)

	output = ts.cs.dispatch(dispatchArray)

	atomic.StoreInt32(&ts.processing, 0)
	return
}

func (ts *testClient) DumpKey(keyName string) {
	dsc := ts.cs.ds.newDataStoreCommand()
	dsc.dumpKey(ts.cs.l, keyName)
}

func (ts *testClient) DumpBitmapKey(keyName string) {
	dsc := ts.cs.ds.newDataStoreCommand()
	logBitmapKey(ts.cs.l, dsc, keyName)
}

func (ts *testClient) Terminate() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if !ts.terminated {
		ts.terminated = true
		ts.cs.unblock("Error: Server closed the connection", true)
	}
}

func (ts *testClient) Close() {
	ts.Terminate()
	ts.cs.unregister()
}

func (ts *testClient) ClientInfo() []string {
	since := time.Since(ts.started)
	return []string{
		"age=" + fmt.Sprintf("%d", int64(since.Seconds())),
	}
}

func (ts *testClient) MatchFilter(filter map[string]string) bool {
	for k, v := range filter {
		switch k {
		case "addr":
			if v != ts.addr {
				return false
			}

		case "laddr":
			if v != ts.laddr {
				return false
			}
		}
	}
	return true
}

func (ts *testClient) RequestClose() {
	ts.Terminate()
}

func (ts *testClient) IsCloseRequested() bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	return ts.terminated
}

func (ts *testClient) ClientID() int64 {
	return ts.cs.id
}

func (ts *testClient) IsNoEvict() bool {
	return ts.cs.noEvict
}

func (ts *testClient) ServerAddr() string {
	return ts.laddr
}

func (ts *testClient) ClientAddr() string {
	return ts.addr
}

func (ts *testClient) ServerNow() time.Time {
	return time.Now()
}
