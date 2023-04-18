package goredisemu

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRedisEcho(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("echo", "Hello World!")
	if !output.isString("Hello World!") {
		t.Fatal("echo fail")
	}
}

func TestRedisPing(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("ping")
	if !output.isString("PONG") {
		t.Fatal("ping fail")
	}

	output = ts.ProcessCommand("ping", "Hello World!")
	if !output.isString("Hello World!") {
		t.Fatal("ping message fail")
	}
}

func TestRedisClientName(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("client", "setname", "invalid name")
	if !output.isErrorType() {
		t.Fatal("invalid name fail")
	}

	output = ts.ProcessCommand("client", "getname")
	if !output.isNull() {
		t.Fatal("get empty name fail")
	}

	output = ts.ProcessCommand("client", "setname", "the-name")
	if !output.isString("OK") {
		t.Fatal("set name fail")
	}

	output = ts.ProcessCommand("client", "getname")
	if !output.isString("the-name") {
		t.Fatal("get non-empty name fail")
	}

	output = ts.ProcessCommand("client", "setname", "")
	if !output.isString("OK") {
		t.Fatal("clear client name fail")
	}

	output = ts.ProcessCommand("client", "getname")
	if !output.isNull() {
		t.Fatal("get cleared name fail")
	}
}

func TestRedisClientId(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("client", "id")
	if !output.isInt64(ts.ClientID()) {
		t.Fatal("get id fail")
	}
}

func TestRedisClientInfo(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("client", "info")
	if !output.isStringType() {
		t.Fatal("get info fail")
	}

	output = ts.ProcessCommand("multi")
	if !output.isString("OK") {
		t.Fatal("start multi fail")
	}

	output = ts.ProcessCommand("client", "info")
	if !output.isString(strQueued) {
		t.Fatal("queue get info fail")
	}

	output = ts.ProcessCommand("exec")
	a, isArray := output.data.(respArray)
	if !isArray {
		t.Fatal("response is not an array")
	}
	for _, v := range a {
		if !v.isStringType() {
			t.Fatal("get info in multi fail")
		}
	}
}

func TestRedisClientKillOldSyntax(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("client", "kill", "1.2.3.4:80")
	if !output.isErrorType() || ts.IsCloseRequested() {
		t.Fatal("kill old syntax fail")
	}

	output = ts.ProcessCommand("client", "kill", ts.ClientAddr())
	if !output.isString("OK") || !ts.IsCloseRequested() {
		t.Fatal("kill old syntax valid address fail")
	}
}

func TestRedisClientKillId(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	output := ts2.ProcessCommand("client", "kill", "id", fmt.Sprintf("%d", ts.ClientID()))
	if !output.isAtLeast(1) || !ts.IsCloseRequested() {
		t.Fatal("kill by id fail")
	}
}

func TestRedisClientKillTypeNormal(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	output := ts2.ProcessCommand("client", "kill", "type", "normal")
	if !output.isAtLeast(1) || !ts.IsCloseRequested() {
		t.Fatal("kill type normal fail")
	}
}

func TestRedisClientKillTypeMaster(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	output := ts2.ProcessCommand("client", "kill", "type", "master")
	if !output.isInt(0) || ts.IsCloseRequested() {
		t.Fatal("kill type master fail")
	}
}

func TestRedisClientKillTypeSlaveReplicaPubsub(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	output := ts2.ProcessCommand("client", "kill", "type", "slave")
	if !output.isInt(0) || ts.IsCloseRequested() {
		t.Fatal("kill type slave fail")
	}

	output = ts2.ProcessCommand("client", "kill", "type", "replica")
	if !output.isInt(0) || ts.IsCloseRequested() {
		t.Fatal("kill type replica fail")
	}

	output = ts2.ProcessCommand("client", "kill", "type", "pubsub")
	if !output.isInt(0) || ts.IsCloseRequested() {
		t.Fatal("kill type pubsub fail")
	}
}

func TestRedisClientKillUser(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	output := ts2.ProcessCommand("client", "kill", "user", "test") // ERR No such user 'test'
	if !output.isErrorType() || ts.IsCloseRequested() {
		t.Fatal("kill wrong user fail")
	}

	output = ts2.ProcessCommand("client", "kill", "user", "default")
	if !output.isAtLeast(1) || !ts.IsCloseRequested() {
		t.Fatal("kill user fail")
	}
}

func TestRedisClientKillAddr(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	output := ts2.ProcessCommand("client", "kill", "addr", "3.4.5.6:2000")
	if !output.isInt(0) || ts.IsCloseRequested() {
		t.Fatal("kill wrong ip fail")
	}

	output = ts2.ProcessCommand("client", "kill", "addr", ts.ClientAddr())
	if !output.isAtLeast(1) || !ts.IsCloseRequested() {
		t.Fatal("kill ip:port fail")
	}
}

func TestRedisClientKillLAddr(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	output := ts2.ProcessCommand("client", "kill", "laddr", "3.4.5.6:2000")
	if !output.isInt(0) || ts.IsCloseRequested() {
		t.Fatal("kill wrong laddr fail")
	}

	output = ts2.ProcessCommand("client", "kill", "laddr", ts.ServerAddr())
	if !output.isAtLeast(1) || !ts.IsCloseRequested() {
		t.Fatal("kill laddr fail")
	}
}

func TestRedisClientKillSkipMe(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	output := ts2.ProcessCommand("client", "kill", "skipme", "yes")
	if !output.isAtLeast(1) || ts2.IsCloseRequested() {
		t.Fatal("kill skipme yes fail")
	}

	output = ts2.ProcessCommand("client", "kill", "skipme", "no")
	if !output.isAtLeast(1) || !ts2.IsCloseRequested() {
		t.Fatal("kill skipme no fail")
	}
}

func TestRedisClientKillSyntax(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("client", "kill", "skipme", "maybe")
	if !output.isErrorType() {
		t.Fatal("kill skipme bad syntax fail")
	}

	output = ts.ProcessCommand("client", "kill", "1.2.3.4:5", "2.3.4.5:6")
	if !output.isErrorType() {
		t.Fatal("kill ip bad syntax fail")
	}
}

func TestRedisClientKillRepeated(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("client", "kill", "skipme", "no", "skipme", "yes")
	if !output.isInt(0) || ts.IsCloseRequested() {
		t.Fatal("kill skipme repeated fail")
	}

	output = ts.ProcessCommand("client", "kill", "skipme", "yes", "skipme", "no")
	if !output.isAtLeast(1) || !ts.IsCloseRequested() {
		t.Fatal("kill skipme repeated yes fail")
	}
}

func countOccurences(text, searchText string) (count int) {
	pos := 0
	for pos < len(text) {
		subIndex := strings.Index(text[pos:], searchText)
		if subIndex < 0 {
			break
		}
		count++
		pos += subIndex + len(searchText)
	}
	return
}

func TestRedisClientList(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	output := ts.ProcessCommand("client", "list")
	str, ok := output.toString()
	if !ok || (countOccurences(str, "flags=") != 2) {
		t.Fatal("list two clients fail")
	}

	output = ts.ProcessCommand("client", "list", "id", fmt.Sprintf("%d", ts.ClientID()))
	str, ok = output.toString()
	if !ok || (countOccurences(str, "flags=") != 1) {
		t.Fatal("list one by id fail")
	}

	output = ts.ProcessCommand("client", "list", "id", fmt.Sprintf("%d", ts.ClientID()), fmt.Sprintf("%d", ts2.ClientID()))
	str, ok = output.toString()
	if !ok || (countOccurences(str, "flags=") != 2) {
		t.Fatal("list two by id fail")
	}

	output = ts.ProcessCommand("client", "list", "id", fmt.Sprintf("%d", ts.ClientID()), fmt.Sprintf("%d", ts2.ClientID()), "100")
	str, ok = output.toString()
	if !ok || (countOccurences(str, "flags=") != 2) {
		t.Fatal("list two by three ids fail")
	}

	output = ts.ProcessCommand("client", "list", "id", "1000000", "1000001", "1000002")
	str, ok = output.toString()
	if !ok || str != "" {
		t.Fatal("list by three non-existing ids fail")
	}

	output = ts.ProcessCommand("client", "list", "type", "pubsub")
	str, ok = output.toString()
	if !ok || str != "" {
		t.Fatal("list pubsub fail")
	}

	output = ts.ProcessCommand("client", "list", "type", "normal")
	str, ok = output.toString()
	if !ok || (countOccurences(str, "flags=") != 2) {
		t.Fatal("list normal fail")
	}

	output = ts.ProcessCommand("client", "list", "type", "master")
	str, ok = output.toString()
	if !ok || str != "" {
		t.Fatal("list master fail")
	}
}

func TestRedisClientNoEvict(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("client", "no-evict", "on")
	if !output.isString(strOK) || !ts.IsNoEvict() {
		t.Fatal("no-evict on fail")
	}

	output = ts.ProcessCommand("client", "no-evict", "off")
	if !output.isString(strOK) || ts.IsNoEvict() {
		t.Fatal("no-evict off fail")
	}
}

func TestRedisClientSelect(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("set", "dog", "bark")
	if !output.isString(strOK) {
		t.Fatal("make key in db 0 fail")
	}

	output = ts.ProcessCommand("select", "1")
	if !output.isString(strOK) {
		t.Fatal("select 1 fail")
	}

	output = ts.ProcessCommand("get", "dog")
	if !output.isNull() {
		t.Fatal("select get db 0 key in db 1 fail")
	}

	output = ts.ProcessCommand("set", "cat", "meow")
	if !output.isString(strOK) {
		t.Fatal("make key in db 1 fail")
	}

	output = ts.ProcessCommand("select", "0")
	if !output.isString(strOK) {
		t.Fatal("select 0 fail")
	}

	output = ts.ProcessCommand("get", "dog")
	if !output.isString("bark") {
		t.Fatal("select get db 0 key in db 1 fail")
	}

	output = ts.ProcessCommand("get", "cat")
	if !output.isNull() {
		t.Fatal("select get db 1 key in db 0 fail")
	}

	output = ts.ProcessCommand("select", "1")
	if !output.isString(strOK) {
		t.Fatal("select 1 again fail")
	}

	output = ts.ProcessCommand("get", "cat")
	if !output.isString("meow") {
		t.Fatal("select get db 0 key in db 1 fail")
	}

	output = ts.ProcessCommand("select", "-1")
	if !output.isErrorType() {
		t.Fatal("select -1 fail")
	}

	output = ts.ProcessCommand("select", "16")
	if !output.isErrorType() {
		t.Fatal("select 16 fail")
	}

	output = ts.ProcessCommand("select", "15")
	if !output.isString(strOK) {
		t.Fatal("select 15 fail")
	}

	output = ts.ProcessCommand("client", "info")
	str, valid := output.toString()
	if !valid {
		t.Fatal("select get client info fail")
	}
	if !strings.Contains(str, "db=15") {
		t.Fatal("select check client info fail")
	}

	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	output = ts.ProcessCommand("client", "list")
	str, valid = output.toString()
	if !valid {
		t.Fatal("select client list fail")
	}
	if !strings.Contains(str, "db=15") || !strings.Contains(str, "db=0") {
		t.Fatal("select check client list fail")
	}
}

func TestRedisUnblock(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	// unblock with timeout
	var wg sync.WaitGroup
	waitFail := true
	wg.Add(1)
	go func() {
		output := ts.ProcessCommand("blmove", "k1", "k2", "right", "left", "0")
		waitFail = !output.isNull()
		wg.Done()
	}()

	time.Sleep(time.Millisecond)
	output := ts2.ProcessCommand("client", "unblock", fmt.Sprintf("%d", ts.ClientID()))
	if !output.isInt(1) {
		t.Fatal("client unblock fail")
	}

	wg.Wait()
	if waitFail {
		t.Fatal("unblock wait fail")
	}

	// unblock with error
	wg.Add(1)
	go func() {
		output := ts.ProcessCommand("blmove", "k1", "k2", "right", "left", "0")
		str, _ := output.toString()
		waitFail = !output.isErrorType() || !strings.Contains(str, "UNBLOCKED")
		wg.Done()
	}()

	time.Sleep(time.Millisecond)
	output = ts2.ProcessCommand("client", "unblock", fmt.Sprintf("%d", ts.ClientID()), "error")
	if !output.isInt(1) {
		t.Fatal("client unblock error fail")
	}

	wg.Wait()
	if waitFail {
		t.Fatal("unblock wait error fail")
	}

	// unblock with timeout token
	wg.Add(1)
	go func() {
		output := ts.ProcessCommand("blmove", "k1", "k2", "right", "left", "0")
		waitFail = !output.isNull()
		wg.Done()
	}()

	time.Sleep(time.Millisecond)
	output = ts2.ProcessCommand("client", "unblock", fmt.Sprintf("%d", ts.ClientID()), "timeout")
	if !output.isInt(1) {
		t.Fatal("client unblock timeout token fail")
	}

	wg.Wait()
	if waitFail {
		t.Fatal("unblock wait timeout token fail")
	}
}
