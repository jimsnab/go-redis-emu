package redisemu

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestRedisAppend(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// create key test
	output := ts.ProcessCommand("append", "key1", "cat")
	if !output.isInt(3) {
		t.Fatal("append step 1 fail")
	}

	// append key test
	output = ts.ProcessCommand("append", "key1", "bat")
	if !output.isInt(6) {
		t.Fatal("append step 2 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("catbat") {
		t.Fatal("append result string is invalid")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("append can't make list")
	}

	output = ts.ProcessCommand("append", "list", "10")
	if !output.isErrorType() {
		t.Fatal("append invalid op on list")
	}
}

func TestRedisDecr(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// decr from 1 to -1
	output := ts.ProcessCommand("set", "key1", "1")
	if !output.isString("OK") {
		t.Fatal("decr step 1 fail")
	}

	output = ts.ProcessCommand("decr", "key1")
	if !output.isInt(0) {
		t.Fatal("decr step 2 fail")
	}

	output = ts.ProcessCommand("decr", "key1")
	if !output.isInt(-1) {
		t.Fatal("decr step 3 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("-1") {
		t.Fatal("decr result string is invalid")
	}

	// wrong data test
	output = ts.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("decr can't set string")
	}

	output = ts.ProcessCommand("decr", "key1")
	if !output.isErrorType() {
		t.Fatal("decr invalid op on text")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("decr can't make list")
	}

	output = ts.ProcessCommand("decr", "list")
	if !output.isErrorType() {
		t.Fatal("decr invalid op on list")
	}
}

func TestRedisDecrBy(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// test various decrement intervals
	output := ts.ProcessCommand("set", "key1", "1")
	if !output.isString("OK") {
		t.Fatal("decrby step 1 fail")
	}

	output = ts.ProcessCommand("decrby", "key1", "10")
	if !output.isInt(-9) {
		t.Fatal("decrby step 2 fail")
	}

	output = ts.ProcessCommand("decrby", "key1", "0")
	if !output.isInt(-9) {
		t.Fatal("decrby step 3 fail")
	}

	output = ts.ProcessCommand("decrby", "key1", "-5")
	if !output.isInt(-4) {
		t.Fatal("decrby step 4 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("-4") {
		t.Fatal("decrby result string is invalid")
	}

	// overflow tests
	output = ts.ProcessCommand("set", "key1", "-2")
	if !output.isString("OK") {
		t.Fatal("decrby step 5 fail")
	}

	output = ts.ProcessCommand("decrby", "key1", fmt.Sprintf("%d", math.MaxInt64))
	if !output.isErrorType() {
		t.Fatal("decrby step 6 fail")
	}

	output = ts.ProcessCommand("set", "key1", "1")
	if !output.isString("OK") {
		t.Fatal("decrby step 7 fail")
	}

	output = ts.ProcessCommand("decrby", "key1", fmt.Sprintf("%d", -math.MaxInt64))
	if !output.isErrorType() {
		t.Fatal("decrby step 8 fail")
	}

	// wrong data test
	output = ts.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("decrby can't set string")
	}

	output = ts.ProcessCommand("decrby", "key1", "1")
	if !output.isErrorType() {
		t.Fatal("decrby invalid op on text")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("decrby can't make list")
	}

	output = ts.ProcessCommand("decrby", "list", "1")
	if !output.isErrorType() {
		t.Fatal("decrby invalid op on list")
	}
}

func TestRedisGet(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// no args test
	output := ts.ProcessCommand("get")
	if !output.isErrorType() {
		t.Fatal("get no args fail")
	}

	// simple value get
	output = ts.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("get step 1 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("cat") {
		t.Fatal("get step 2 fail")
	}

	// missing key test
	output = ts.ProcessCommand("get", "key2")
	if !output.isNull() {
		t.Fatal("get step 3 fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("get can't make list")
	}

	output = ts.ProcessCommand("get", "list")
	if !output.isErrorType() {
		t.Fatal("get invalid op on list")
	}
}

func TestRedisGetDel(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// simple get & del test
	output := ts.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("getdel step 1 fail")
	}

	output = ts.ProcessCommand("getdel", "key1")
	if !output.isString("cat") {
		t.Fatal("getdel step 2 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isNull() {
		t.Fatal("getdel step 3 fail")
	}

	// missing key test
	output = ts.ProcessCommand("getdel", "key1")
	if !output.isNull() {
		t.Fatal("getdel step 3 fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("getdel can't make list")
	}

	output = ts.ProcessCommand("getdel", "list")
	if !output.isErrorType() {
		t.Fatal("getdel invalid op on list")
	}
}

func TestRedisGetEx(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// one second expire test
	output := ts.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("getex create second expire key fail")
	}

	output = ts.ProcessCommand("getex", "key1", "ex", "1")
	if !output.isString("cat") {
		t.Fatal("getex set 1 second expire fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("cat") {
		t.Fatal("getex get before second expire fail")
	}

	time.Sleep(1001 * time.Millisecond)

	output = ts.ProcessCommand("get", "key1")
	if !output.isNull() {
		t.Fatal("getex get after second expire fail")
	}

	// expire on expired key test
	output = ts.ProcessCommand("getex", "key1", "ex", "1")
	if !output.isNull() {
		t.Fatal("getex step 5 fail")
	}

	// immediate expiration test
	output = ts.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("getex set key for immediate expiration fail")
	}

	output = ts.ProcessCommand("getex", "key1", "ex", "0") // "ERR invalid expire time in 'getex' command"
	if !output.isErrorType() {
		t.Fatal("getex expire 0s fail")
	}

	output = ts.ProcessCommand("getex", "key1", "ex", "-1") // "ERR invalid expire time in 'getex' command"
	if !output.isErrorType() {
		t.Fatal("getex expire -1s fail")
	}

	// millisecond expiration tests
	output = ts.ProcessCommand("set", "key2", "dog")
	if !output.isString("OK") {
		t.Fatal("getex create ms expire key fail")
	}

	output = ts.ProcessCommand("getex", "key2", "px", "250")
	if !output.isString("dog") {
		t.Fatal("getex set ms expiration fail")
	}

	output = ts.ProcessCommand("getex", "key2", "px", "250")
	if !output.isString("dog") {
		t.Fatal("getex set ms expiration again fail")
	}

	output = ts.ProcessCommand("getex", "key2", "px", "0") // "ERR invalid expire time in 'getex' command"
	if !output.isErrorType() {
		t.Fatal("getex expire 0 ms fail")
	}

	output = ts.ProcessCommand("getex", "key2", "px", "-1") // "ERR invalid expire time in 'getex' command"
	if !output.isErrorType() {
		t.Fatal("getex expire -1 ms fail")
	}

	output = ts.ProcessCommand("get", "key2")
	if !output.isString("dog") {
		t.Fatal("getex value with ms expiration fail")
	}

	time.Sleep(251 * time.Millisecond)

	output = ts.ProcessCommand("get", "key2")
	if !output.isNull() {
		t.Fatal("getex value after ms expiration fail")
	}

	// persist test
	output = ts.ProcessCommand("set", "key2", "dog")
	if !output.isString("OK") {
		t.Fatal("getex create persist key fail")
	}

	output = ts.ProcessCommand("getex", "key2", "px", "250")
	if !output.isString("dog") {
		t.Fatal("getex set persist ms expiration fail")
	}

	time.Sleep(50 * time.Millisecond)

	output = ts.ProcessCommand("getex", "key2", "persist")
	if !output.isString("dog") {
		t.Fatal("getex apply persist fail")
	}

	time.Sleep(350 * time.Millisecond)

	output = ts.ProcessCommand("get", "key2")
	if !output.isString("dog") {
		t.Fatal("getex get persisted key after potential expiration fail")
	}

	// expire at tests
	output = ts.ProcessCommand("set", "key3", "fox")
	if !output.isString("OK") {
		t.Fatal("getex create expire at key fail")
	}

	output = ts.ProcessCommand("getex", "key3", "exat", "0")
	if !output.isErrorType() {
		t.Fatal("getex expire at 0 fail")
	}

	output = ts.ProcessCommand("getex", "key3", "exat", "-1")
	if !output.isErrorType() {
		t.Fatal("getex expire at -1 fail")
	}

	now := ts.ServerNow()
	output = ts.ProcessCommand("getex", "key3", "exat", fmt.Sprintf("%d", now.Unix()+1))
	if !output.isString("fox") {
		t.Fatal("getex set expire at in 1s fail")
	}

	output = ts.ProcessCommand("get", "key3")
	if !output.isString("fox") {
		t.Fatal("getex get before expire at fail")
	}

	time.Sleep(1001 * time.Millisecond)

	output = ts.ProcessCommand("get", "key3")
	if !output.isNull() {
		t.Fatal("getex get after expire at fail")
	}

	output = ts.ProcessCommand("set", "key4", "duck")
	if !output.isString("OK") {
		t.Fatal("getex set expire at ms fail")
	}

	output = ts.ProcessCommand("getex", "key4", "pxat", "0")
	if !output.isErrorType() {
		t.Fatal("getex expire at 0 ms fail")
	}

	output = ts.ProcessCommand("getex", "key4", "pxat", "-1")
	if !output.isErrorType() {
		t.Fatal("getex expire at -1 ms fail")
	}

	now = ts.ServerNow()
	output = ts.ProcessCommand("getex", "key4", "pxat", fmt.Sprintf("%d", now.UnixMilli()+100))
	if !output.isString("duck") {
		t.Fatal("getex set expire at +100ms fail")
	}

	output = ts.ProcessCommand("get", "key4")
	if !output.isString("duck") {
		t.Fatal("getex get before expire at ms fail")
	}

	time.Sleep(101 * time.Millisecond)

	output = ts.ProcessCommand("get", "key4")
	if !output.isNull() {
		t.Fatal("getex get after expire at ms fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("getex can't make list")
	}

	output = ts.ProcessCommand("getex", "list", "ex", "10")
	if !output.isErrorType() {
		t.Fatal("getex invalid op on list")
	}
}

func testSubstr(t *testing.T, cmdName string) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// various substr range tests
	output := ts.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal(cmdName + " step 1 fail")
	}

	output = ts.ProcessCommand(cmdName, "key1", "1", "1")
	if !output.isString("a") {
		t.Fatal(cmdName + " step 2 fail")
	}

	output = ts.ProcessCommand(cmdName, "key1", "0", "2")
	if !output.isString("cat") {
		t.Fatal(cmdName + " step 3 fail")
	}

	output = ts.ProcessCommand(cmdName, "key1", "0", "3")
	if !output.isString("cat") {
		t.Fatal(cmdName + " step 4 fail")
	}

	output = ts.ProcessCommand(cmdName, "key1", "-1", "2")
	if !output.isString("t") {
		t.Fatal(cmdName + " step 5 fail")
	}

	output = ts.ProcessCommand(cmdName, "key1", "-2", "-1")
	if !output.isString("at") {
		t.Fatal(cmdName + " step 6 fail")
	}

	// out of range tests
	output = ts.ProcessCommand(cmdName, "key1", "0", "500")
	if !output.isString("cat") {
		t.Fatal(cmdName + " large positive end range fail")
	}

	output = ts.ProcessCommand(cmdName, "key1", "-500", "-1")
	if !output.isString("cat") {
		t.Fatal(cmdName + " large negative start range fail")
	}

	output = ts.ProcessCommand(cmdName, "key1", "500", "-1")
	if !output.isString("") {
		t.Fatal(cmdName + " large positive start range fail")
	}

	output = ts.ProcessCommand(cmdName, "key1", "0", "-500")

	_, isTestClient := ts.(*testClient)
	if isTestClient {
		if !output.isString("") {
			t.Fatal(cmdName + " large negative end range fail")
		}
	} else {
		// BUGBUG https://github.com/redis/redis/issues/11738
		if !output.isString("c") {
			t.Fatal(cmdName + " large negative end range fail")
		}
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal(cmdName + " can't make list")
	}

	output = ts.ProcessCommand(cmdName, "list", "0", "-1")
	if !output.isErrorType() {
		t.Fatal(cmdName + " invalid op on list")
	}

}

func TestRedisGetRange(t *testing.T) {
	testSubstr(t, "getrange")
}

func TestRedisGetSet(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// get & set basic test
	output := ts.ProcessCommand("getset", "key1", "cat")
	if !output.isNull() {
		t.Fatal("getset step 1 fail")
	}

	output = ts.ProcessCommand("getset", "key1", "dog")
	if !output.isString("cat") {
		t.Fatal("getset step 2 fail")
	}

	output = ts.ProcessCommand("getset", "key1", "fox")
	if !output.isString("dog") {
		t.Fatal("getset step 3 fail")
	}

	// missing key test
	output = ts.ProcessCommand("get", "key1")
	if !output.isString("fox") {
		t.Fatal("getset step 4 fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("getset can't make list")
	}

	output = ts.ProcessCommand("getset", "list", "10")
	if !output.isErrorType() {
		t.Fatal("getset invalid op on list")
	}
}

func TestRedisIncr(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// negative to positive
	output := ts.ProcessCommand("set", "key1", "-1")
	if !output.isString("OK") {
		t.Fatal("incr step 1 fail")
	}

	output = ts.ProcessCommand("incr", "key1")
	if !output.isInt(0) {
		t.Fatal("incr step 2 fail")
	}

	output = ts.ProcessCommand("incr", "key1")
	if !output.isInt(1) {
		t.Fatal("incr step 3 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("1") {
		t.Fatal("incr result string is invalid")
	}

	// wrong data test
	output = ts.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("incr can't set string")
	}

	output = ts.ProcessCommand("incr", "key1")
	if !output.isErrorType() {
		t.Fatal("incr invalid op on text")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("incr can't make list")
	}

	output = ts.ProcessCommand("incr", "list")
	if !output.isErrorType() {
		t.Fatal("incr invalid op on list")
	}
}

func TestRedisIncrBy(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// incrby various intervals, exercise sign changes
	output := ts.ProcessCommand("set", "key1", "-1")
	if !output.isString("OK") {
		t.Fatal("incrby step 1 fail")
	}

	output = ts.ProcessCommand("incrby", "key1", "10")
	if !output.isInt(9) {
		t.Fatal("incrby step 2 fail")
	}

	output = ts.ProcessCommand("incrby", "key1", "0")
	if !output.isInt(9) {
		t.Fatal("incrby step 3 fail")
	}

	output = ts.ProcessCommand("incrby", "key1", "-5")
	if !output.isInt(4) {
		t.Fatal("incrby step 4 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("4") {
		t.Fatal("incrby result string is invalid")
	}

	// overflow tests
	output = ts.ProcessCommand("set", "key1", "1")
	if !output.isString("OK") {
		t.Fatal("incrby step 5 fail")
	}

	output = ts.ProcessCommand("incrby", "key1", fmt.Sprintf("%d", math.MaxInt64))
	if !output.isErrorType() {
		t.Fatal("incrby step 6 fail")
	}

	output = ts.ProcessCommand("set", "key1", "-2")
	if !output.isString("OK") {
		t.Fatal("incrby step 7 fail")
	}

	output = ts.ProcessCommand("incrby", "key1", fmt.Sprintf("%d", -math.MaxInt64))
	if !output.isErrorType() {
		t.Fatal("incrby step 8 fail")
	}

	// wrong data test
	output = ts.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("incrby can't set string")
	}

	output = ts.ProcessCommand("incrby", "key1", "10")
	if !output.isErrorType() {
		t.Fatal("incrby invalid op on text")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("incrby can't make list")
	}

	output = ts.ProcessCommand("incrby", "list", "10")
	if !output.isErrorType() {
		t.Fatal("incrby invalid op on list")
	}
}

func TestRedisIncrFloat(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// various float add tests
	output := ts.ProcessCommand("set", "key1", "-1.5")
	if !output.isString("OK") {
		t.Fatal("incrbyfloat step 1 fail")
	}

	output = ts.ProcessCommand("incrbyfloat", "key1", "1.2")
	if !output.isFloat(-0.3, 4) {
		t.Fatal("incrbyfloat step 2 fail")
	}

	output = ts.ProcessCommand("incrbyfloat", "key1", "0")
	if !output.isFloat(-0.3, 4) {
		t.Fatal("incrbyfloat step 3 fail")
	}

	output = ts.ProcessCommand("incrbyfloat", "key1", "-.4")
	if !output.isFloat(-0.7, 4) {
		t.Fatal("incrbyfloat step 4 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isFloat(-0.7, 4) {
		t.Fatal("incrbyfloat result string is invalid")
	}

	// range tests
	output = ts.ProcessCommand("set", "key1", "1e300")
	if !output.isString("OK") {
		t.Fatal("incrbyfloat step 5 fail")
	}

	output = ts.ProcessCommand("incrbyfloat", "key1", fmt.Sprintf("%f", math.Inf(1))) // "ERR increment would produce NaN or Infinity"
	if !output.isErrorType() {
		t.Fatal("incrbyfloat inf incr fail")
	}

	output = ts.ProcessCommand("incrbyfloat", "key1", fmt.Sprintf("%f", math.Inf(-1))) // "ERR increment would produce NaN or Infinity"
	if !output.isErrorType() {
		t.Fatal("incrbyfloat -inf incr fail")
	}

	// BUGBUG how is Inf reached in real redis?
	_, isTestClient := ts.(*testClient)
	if isTestClient {
		output = ts.ProcessCommand("set", "key1", "1e300")
		if !output.isString("OK") {
			t.Fatal("incrbyfloat set big positive number fail")
		}

		output = ts.ProcessCommand("incrbyfloat", "key1", fmt.Sprintf("%f", math.MaxFloat64))
		if !output.isString("+Inf") {
			t.Fatal("incrbyfloat overflow positive to inf fail")
		}

		output = ts.ProcessCommand("set", "key1", "-1e300")
		if !output.isString("OK") {
			t.Fatal("incrbyfloat set big negative number fail")
		}

		output = ts.ProcessCommand("incrbyfloat", "key1", fmt.Sprintf("%f", -math.MaxFloat64))
		if !output.isString("-Inf") {
			t.Fatal("incrbyfloat overflow negative to inf fail")
		}
	}

	// add to missing key
	output = ts.ProcessCommand("incrbyfloat", "new", "10")
	if !output.isString("10") {
		t.Fatal("incrbyfloat add to non existing key fail")
	}

	// wrong data test
	output = ts.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("incrbyfloat can't set string")
	}

	output = ts.ProcessCommand("incrbyfloat", "key1", "10")
	if !output.isErrorType() {
		t.Fatal("incrbyfloat invalid op on text")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("incrbyfloat can't make list")
	}

	output = ts.ProcessCommand("incrbyfloat", "list", "10")
	if !output.isErrorType() {
		t.Fatal("incrbyfloat invalid op on list")
	}
}

func TestRedisLcs(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// basic lcs len test
	output := ts.ProcessCommand("set", "key1", "ohmytext")
	if !output.isString("OK") {
		t.Fatal("lcs step 1 fail")
	}

	output = ts.ProcessCommand("set", "key2", "mynewtext")
	if !output.isString("OK") {
		t.Fatal("lcs step 2 fail")
	}

	output = ts.ProcessCommand("lcs", "key1", "key2")
	if !output.isString("mytext") {
		t.Fatal("lcs step 3 fail")
	}

	output = ts.ProcessCommand("lcs", "key1", "key2", "len")
	if !output.isInt(6) {
		t.Fatal("lcs step 4 fail")
	}

	expected := []any{
		"matches",
		[]any{
			[]any{
				[]any{4, 7},
				[]any{5, 8},
			},
			[]any{
				[]any{2, 3},
				[]any{0, 1},
			},
		},
		"len",
		6,
	}

	// basic lcs idx test
	output = ts.ProcessCommand("lcs", "key1", "key2", "idx")
	if !output.isValue(expected) {
		t.Fatal("lcs step 5 fail")
	}

	// missing key tests
	output = ts.ProcessCommand("lcs", "missing", "key2")
	if !output.isString("") {
		t.Fatal("lcs missing key 1 fail")
	}

	output = ts.ProcessCommand("lcs", "key1", "missing")
	if !output.isString("") {
		t.Fatal("lcs missing key 2 fail")
	}

	// minmatchlen test
	expected = []any{
		"matches",
		[]any{
			[]any{
				[]any{4, 7},
				[]any{5, 8},
			},
		},
		"len",
		6,
	}
	output = ts.ProcessCommand("lcs", "key1", "key2", "idx", "minmatchlen", "4")
	if !output.isValue(expected) {
		t.Fatal("lcs step 6 fail")
	}

	// withmatchlen test
	expected = []any{
		"matches",
		[]any{
			[]any{
				[]any{4, 7},
				[]any{5, 8},
				4,
			},
		},
		"len",
		6,
	}
	output = ts.ProcessCommand("lcs", "key1", "key2", "idx", "minmatchlen", "4", "withmatchlen")
	if !output.isValue(expected) {
		t.Fatal("lcs step 6 fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("lcs can't make list")
	}

	output = ts.ProcessCommand("lcs", "list", "list", "idx")
	if !output.isErrorType() {
		t.Fatal("lcs invalid op on list")
	}
}

func TestRedisMget(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key args test
	output := ts.ProcessCommand("mget")
	if !output.isErrorType() {
		t.Fatal("mget missing arg fail")
	}

	// two key test
	output = ts.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("mget step 1 fail")
	}

	output = ts.ProcessCommand("set", "key2", "dog")
	if !output.isString("OK") {
		t.Fatal("mget step 2 fail")
	}

	output = ts.ProcessCommand("mget", "key1")
	if !output.isValue([]any{"cat"}) {
		t.Fatal("mget step 3 fail")
	}

	output = ts.ProcessCommand("mget", "key1", "key2")
	if !output.isValue([]any{"cat", "dog"}) {
		t.Fatal("mget step 4 fail")
	}

	// two existing keys, other non existing keys
	output = ts.ProcessCommand("mget", "key1", "key2", "key3")
	if !output.isValue([]any{"cat", "dog", nil}) {
		t.Fatal("mget step 5 fail")
	}

	output = ts.ProcessCommand("mget", "key0", "key1", "key2", "key3")
	if !output.isValue([]any{nil, "cat", "dog", nil}) {
		t.Fatal("mget step 6 fail")
	}

	output = ts.ProcessCommand("mget", "key1", "key0", "key3", "key2")
	if !output.isValue([]any{"cat", nil, nil, "dog"}) {
		t.Fatal("mget step 7 fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("mget can't make list")
	}

	output = ts.ProcessCommand("mget", "list")
	if !output.isValue([]any{nil}) {
		t.Fatal("mget invalid op on list")
	}
}

func TestRedisMset(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// invalid arg tests
	output := ts.ProcessCommand("mset")
	if !output.isErrorType() {
		t.Fatal("mset step 1 fail")
	}

	output = ts.ProcessCommand("mset", "key1")
	if !output.isErrorType() {
		t.Fatal("mset step 2 fail")
	}

	// set one new test
	output = ts.ProcessCommand("mset", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("mset step 3 fail")
	}

	// set one overwrite test
	output = ts.ProcessCommand("mset", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("mset step 4 fail")
	}

	// set incomplete second pair test
	output = ts.ProcessCommand("mset", "key1", "cat", "dog")
	if !output.isErrorType() {
		t.Fatal("mset step 5 fail")
	}

	// set two, overwrite one test
	output = ts.ProcessCommand("mset", "key1", "cat", "key2", "dog")
	if !output.isString("OK") {
		t.Fatal("mset step 6 fail")
	}

	// overwrite other type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("mset can't make list")
	}

	output = ts.ProcessCommand("mset", "list", "cat")
	if !output.isString("OK") {
		t.Fatal("mset invalid op on list")
	}
}

func TestRedisMsetNx(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// invalid arg tests
	output := ts.ProcessCommand("msetnx")
	if !output.isErrorType() {
		t.Fatal("msetnx step 1 fail")
	}

	output = ts.ProcessCommand("msetnx", "key1")
	if !output.isErrorType() {
		t.Fatal("msetnx step 2 fail")
	}

	// not exists test
	output = ts.ProcessCommand("msetnx", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("msetnx step 3 fail")
	}

	// exists test
	output = ts.ProcessCommand("msetnx", "key1", "cat")
	if !output.isInt(0) {
		t.Fatal("msetnx step 4 fail")
	}

	// invalid arg test
	output = ts.ProcessCommand("msetnx", "key1", "cat", "dog")
	if !output.isErrorType() {
		t.Fatal("msetnx step 5 fail")
	}

	// already exists test
	output = ts.ProcessCommand("msetnx", "key1", "cat", "key2", "dog")
	if !output.isInt(0) {
		t.Fatal("msetnx step 6 fail")
	}

	// doesn't exist, two pairs
	output = ts.ProcessCommand("msetnx", "keyA", "cat", "keyB", "dog")
	if !output.isInt(1) {
		t.Fatal("msetnx step 7 fail")
	}

	// collide with other type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("msetnx can't make list")
	}

	output = ts.ProcessCommand("msetnx", "list", "cat")
	if !output.isInt(0) {
		t.Fatal("msetnx invalid op on list")
	}
}

func TestRedisPSetEx(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// ms expiration test
	output := ts.ProcessCommand("psetex", "key1", "200", "cat")
	if !output.isString("OK") {
		t.Fatal("psetex step 1 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("cat") {
		t.Fatal("psetex step 2 fail")
	}

	time.Sleep(201 * time.Millisecond)

	output = ts.ProcessCommand("get", "key1")
	if !output.isNull() {
		t.Fatal("psetex step 3 fail")
	}

	// overwrite other type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("psetex can't make list")
	}

	output = ts.ProcessCommand("psetex", "list", "200", "cat")
	if !output.isString("OK") {
		t.Fatal("psetex invalid op on list")
	}
}

func TestRedisSet(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing arg test
	output := ts.ProcessCommand("set", "key1")
	if !output.isErrorType() {
		t.Fatal("set step 1 fail")
	}

	// simple set test
	output = ts.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("set step 2 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("cat") {
		t.Fatal("set step 3 fail")
	}

	// not exists (nx) and exists (xx) tests
	output = ts.ProcessCommand("set", "key1", "dog", "nx")
	if !output.isNull() {
		t.Fatal("set step 4 fail")
	}

	output = ts.ProcessCommand("set", "key1", "dog", "xx")
	if !output.isString("OK") {
		t.Fatal("set step 5 fail")
	}

	output = ts.ProcessCommand("set", "key2", "fox", "xx")
	if !output.isNull() {
		t.Fatal("set step 6 fail")
	}

	output = ts.ProcessCommand("set", "key2", "fox", "nx")
	if !output.isString("OK") {
		t.Fatal("set step 7 fail")
	}

	output = ts.ProcessCommand("set", "key3", "cat", "get")
	if !output.isNull() {
		t.Fatal("set step 8 fail")
	}

	output = ts.ProcessCommand("set", "key3", "dog", "get")
	if !output.isString("cat") {
		t.Fatal("set step 9 fail")
	}

	output = ts.ProcessCommand("set", "key4", "cow", "nx", "get")
	if !output.isNull() {
		t.Fatal("set step 10 fail")
	}

	output = ts.ProcessCommand("set", "key4", "fox", "xx", "get")
	if !output.isString("cow") {
		t.Fatal("set step 11 fail")
	}

	output = ts.ProcessCommand("set", "key5", "fox", "xx", "get")
	if !output.isNull() {
		t.Fatal("set step 12 fail")
	}

	output = ts.ProcessCommand("set", "key4", "duck", "nx", "get")
	if !output.isString("fox") {
		t.Fatal("set step 13 fail")
	}

	// expiration tests
	output = ts.ProcessCommand("set", "key5", "pig", "ex", "1")
	if !output.isString("OK") {
		t.Fatal("set step 14 fail")
	}

	time.Sleep(500 * time.Millisecond)

	output = ts.ProcessCommand("get", "key5")
	if !output.isString("pig") {
		t.Fatal("set step 15 fail")
	}

	time.Sleep(501 * time.Millisecond)

	output = ts.ProcessCommand("get", "key5")
	if !output.isNull() {
		t.Fatal("set step 16 fail")
	}

	output = ts.ProcessCommand("set", "key6", "horse", "px", "100")
	if !output.isString("OK") {
		t.Fatal("set step 17 fail")
	}

	time.Sleep(50 * time.Millisecond)

	output = ts.ProcessCommand("get", "key6")
	if !output.isString("horse") {
		t.Fatal("set step 18 fail")
	}

	time.Sleep(51 * time.Millisecond)

	output = ts.ProcessCommand("get", "key6")
	if !output.isNull() {
		t.Fatal("set step 19 fail")
	}

	output = ts.ProcessCommand("set", "key7", "pig", "get", "ex", "1")
	if !output.isNull() {
		t.Fatal("set step 20 fail")
	}

	output = ts.ProcessCommand("set", "key7", "goat", "get", "ex", "1")
	if !output.isString("pig") {
		t.Fatal("set step 21 fail")
	}

	time.Sleep(500 * time.Millisecond)

	output = ts.ProcessCommand("get", "key7")
	if !output.isString("goat") {
		t.Fatal("set step 22 fail")
	}

	time.Sleep(501 * time.Millisecond)

	output = ts.ProcessCommand("get", "key7")
	if !output.isNull() {
		t.Fatal("set step 23 fail")
	}

	output = ts.ProcessCommand("set", "key8", "horse", "get", "px", "100")
	if !output.isNull() {
		t.Fatal("set step 24 fail")
	}

	output = ts.ProcessCommand("set", "key8", "hen", "get", "px", "100")
	if !output.isString("horse") {
		t.Fatal("set step 25 fail")
	}

	time.Sleep(50 * time.Millisecond)

	output = ts.ProcessCommand("get", "key8")
	if !output.isString("hen") {
		t.Fatal("set step 26 fail")
	}

	time.Sleep(51 * time.Millisecond)

	output = ts.ProcessCommand("get", "key8")
	if !output.isNull() {
		t.Fatal("set step 27 fail")
	}

	// keepttl tests
	output = ts.ProcessCommand("set", "key9", "turkey", "px", "100")
	if !output.isString("OK") {
		t.Fatal("set step 28 fail")
	}

	time.Sleep(50 * time.Millisecond)

	output = ts.ProcessCommand("set", "key9", "", "get", "keepttl")
	if !output.isString("turkey") {
		t.Fatal("set step 29 fail")
	}

	time.Sleep(51 * time.Millisecond)

	ts.DumpKey("key9")

	output = ts.ProcessCommand("set", "key9", "", "get", "keepttl")
	if !output.isNull() {
		t.Fatal("set step 30 fail")
	}

	// expire at tests
	output = ts.ProcessCommand("set", "key10", "lamb", "pxat", fmt.Sprintf("%d", ts.ServerNow().Add(100*time.Millisecond).UnixMilli()))
	if !output.isString("OK") {
		t.Fatal("set step 31 fail")
	}

	time.Sleep(50 * time.Millisecond)

	output = ts.ProcessCommand("get", "key10")
	if !output.isString("lamb") {
		t.Fatal("set step 32 fail")
	}

	time.Sleep(51 * time.Millisecond)

	output = ts.ProcessCommand("get", "key10")
	if !output.isNull() {
		t.Fatal("set step 33 fail")
	}

	_, isTestClient := ts.(*testClient)
	if isTestClient {
		// test client timing is precise
		output = ts.ProcessCommand("set", "key11", "sheep", "exat", fmt.Sprintf("%d", ts.ServerNow().Unix()+1))
		if !output.isString("OK") {
			t.Fatal("set step 34 fail")
		}

		time.Sleep(500 * time.Millisecond)

		output = ts.ProcessCommand("get", "key11")
		if !output.isString("sheep") {
			t.Fatal("set step 35 fail")
		}

		time.Sleep(501 * time.Millisecond)
	} else {
		// redis server clock can be out of sync with test box, or not rounded precisely enough
		output = ts.ProcessCommand("set", "key11", "sheep", "exat", fmt.Sprintf("%d", ts.ServerNow().Unix()+3))
		if !output.isString("OK") {
			t.Fatal("set step 34 fail")
		}

		time.Sleep(1500 * time.Millisecond)

		output = ts.ProcessCommand("get", "key11")
		if !output.isString("sheep") {
			t.Fatal("set step 35 fail")
		}

		time.Sleep(2501 * time.Millisecond)
	}

	output = ts.ProcessCommand("get", "key11")
	if !output.isNull() {
		t.Fatal("set step 36 fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "foo")
	if !output.isInt(1) {
		t.Fatal("set step 37 fail")
	}

	output = ts.ProcessCommand("set", "list", "cow", "get")
	if !output.isErrorType() {
		t.Fatal("set step 39 fail")
	}

	// overwrite other type test
	output = ts.ProcessCommand("set", "list", "bar")
	if !output.isString("OK") {
		t.Fatal("set step 39 fail")
	}
}

func TestRedisSetArgPos(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// argument position test
	output := ts.ProcessCommand("set", "poskey", "pig", "ex", "5", "NX")
	if !output.isString("OK") {
		t.Fatal("ex before nx fail")
	}
}

func TestRedisSetEx(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// second expiration
	output := ts.ProcessCommand("setex", "key1", "1", "cat")
	if !output.isString("OK") {
		t.Fatal("setex step 1 fail")
	}

	time.Sleep(500 * time.Millisecond)

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("cat") {
		t.Fatal("setex step 2 fail")
	}

	time.Sleep(501 * time.Millisecond)

	output = ts.ProcessCommand("get", "key1")
	if !output.isNull() {
		t.Fatal("setex step 3 fail")
	}

	// overwrite different type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("setex can't make list")
	}

	output = ts.ProcessCommand("setex", "list", "1", "cat")
	if !output.isString("OK") {
		t.Fatal("setex invalid op on list")
	}
}

func TestRedisSetNx(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// set non existant test
	output := ts.ProcessCommand("setnx", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("setnx step 1 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("cat") {
		t.Fatal("setnx step 2 fail")
	}

	// set exists test
	output = ts.ProcessCommand("setnx", "key1", "dog")
	if !output.isInt(0) {
		t.Fatal("setnx step 3 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("cat") {
		t.Fatal("setnx step 4 fail")
	}

	// collide on different type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("setnx can't make list")
	}

	output = ts.ProcessCommand("setnx", "list", "cat")
	if !output.isInt(0) {
		t.Fatal("setnx invalid op on list")
	}
}

func TestRedisSetRange(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// replace first char test
	output := ts.ProcessCommand("set", "key1", "hello")
	if !output.isString("OK") {
		t.Fatal("setrange step 1 fail")
	}

	output = ts.ProcessCommand("setrange", "key1", "0", "y")
	if !output.isInt(5) {
		t.Fatal("setrange step 2 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("yello") {
		t.Fatal("setrange step 3 fail")
	}

	// replace last char test
	output = ts.ProcessCommand("setrange", "key1", "5", "w")
	if !output.isInt(6) {
		t.Fatal("setrange step 4 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("yellow") {
		t.Fatal("setrange step 5 fail")
	}

	// replace middle test
	output = ts.ProcessCommand("setrange", "key1", "1", "ELL")
	if !output.isInt(6) {
		t.Fatal("setrange step 6 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("yELLow") {
		t.Fatal("setrange step 7 fail")
	}

	// append test
	output = ts.ProcessCommand("setrange", "key1", "8", "!!")
	if !output.isInt(10) {
		t.Fatal("setrange step 8 fail")
	}

	output = ts.ProcessCommand("get", "key1")
	if !output.isString("yELLow\x00\x00!!") {
		t.Fatal("setrange step 9 fail")
	}

	// make a new key test
	output = ts.ProcessCommand("setrange", "new", "0", "y")
	if !output.isInt(1) {
		t.Fatal("setrange step 10 fail")
	}

	output = ts.ProcessCommand("get", "new")
	if !output.isString("y") {
		t.Fatal("setrange step 11 fail")
	}

	// collide with a different key type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("setrange step 12 fail")
	}

	output = ts.ProcessCommand("setrange", "list", "0", "y")
	if !output.isErrorType() {
		t.Fatal("setrange step 13 fail")
	}
}

func TestRedisStrLen(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// basic test
	output := ts.ProcessCommand("set", "key1", "hello")
	if !output.isString("OK") {
		t.Fatal("strlen step 1 fail")
	}

	output = ts.ProcessCommand("strlen", "key1")
	if !output.isInt(5) {
		t.Fatal("strlen step 2 fail")
	}

	// non existing key test
	output = ts.ProcessCommand("strlen", "key2")
	if !output.isInt(0) {
		t.Fatal("strlen step 3 fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("strlen can't make list")
	}

	output = ts.ProcessCommand("strlen", "list")
	if !output.isErrorType() {
		t.Fatal("strlen invalid op on list")
	}
}

func TestRedisSubStr(t *testing.T) {
	testSubstr(t, "substr")
}
