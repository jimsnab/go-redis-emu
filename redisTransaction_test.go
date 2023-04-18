package redisemu

import "testing"

func TestRedisMissingMulti(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("exec")
	if !output.isErrorType() {
		t.Fatal("missing multi step 1 fail")
	}
}

func TestRedisEmptyMulti(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("multi")
	if !output.isString("OK") {
		t.Fatal("empty multi step 1 fail")
	}

	output = ts.ProcessCommand("exec")
	if !output.isValue([]any{}) {
		t.Fatal("empty multi step 2 fail")
	}
}

func TestRedisOneMulti(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("multi")
	if !output.isString("OK") {
		t.Fatal("one multi step 1 fail")
	}

	output = ts.ProcessCommand("set", "key1", "cat")
	if !output.isString(strQueued) {
		t.Fatal("one multi step 2 fail")
	}

	output = ts.ProcessCommand("exec")
	if !output.isValue([]any{"OK"}) {
		t.Fatal("one multi step 3 fail")
	}
}

func TestRedisDoubleMulti(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("multi")
	if !output.isString("OK") {
		t.Fatal("one double multi step 1 fail")
	}

	output = ts.ProcessCommand("multi")
	if !output.isErrorType() {
		t.Fatal("one double multi step 2 fail")
	}
}

func TestRedisOneWatch(t *testing.T) {
	ts1 := NewRedisTestClient()
	defer ts1.Close()
	ts2 := ts1.AdditionalClient()
	defer ts2.Close()

	output := ts1.ProcessCommand("watch", "key1")
	if !output.isString("OK") {
		t.Fatal("one watch a key fail")
	}

	output = ts1.ProcessCommand("multi")
	if !output.isString("OK") {
		t.Fatal("one watch start multi fail")
	}

	output = ts1.ProcessCommand("watch", "key2")
	if !output.isErrorType() { // "ERR WATCH inside MULTI is not allowed"
		t.Fatal("one watch inside multi fail")
	}

	output = ts2.ProcessCommand("set", "key1", "dog")
	if !output.isString("OK") {
		t.Fatal("one watch set key from another client fail")
	}

	output = ts1.ProcessCommand("set", "key1", "cat")
	if !output.isString(strQueued) {
		t.Fatal("one watch queue conflicting set fail")
	}

	output = ts1.ProcessCommand("exec")
	if !output.isNull() {
		t.Fatal("one watch exec collision fail")
	}
}

func TestRedisOneWatchPreexisting(t *testing.T) {
	ts1 := NewRedisTestClient()
	defer ts1.Close()
	ts2 := ts1.AdditionalClient()
	defer ts2.Close()

	output := ts1.ProcessCommand("set", "key1", "cat")
	if !output.isString("OK") {
		t.Fatal("preexisting watch step 1 fail")
	}

	output = ts1.ProcessCommand("watch", "key1")
	if !output.isString("OK") {
		t.Fatal("preexisting watch step 2 fail")
	}

	output = ts1.ProcessCommand("multi")
	if !output.isString("OK") {
		t.Fatal("preexisting watch step 3 fail")
	}

	output = ts2.ProcessCommand("set", "key1", "dog")
	if !output.isString("OK") {
		t.Fatal("preexisting watch step 4 fail")
	}

	output = ts1.ProcessCommand("set", "key1", "cat")
	if !output.isString(strQueued) {
		t.Fatal("preexisting watch step 5 fail")
	}

	output = ts1.ProcessCommand("exec")
	if !output.isNull() {
		t.Fatal("preexisting watch step 6 fail")
	}
}

func TestRedisOneWatchNoChange(t *testing.T) {
	ts1 := NewRedisTestClient()
	defer ts1.Close()

	output := ts1.ProcessCommand("set", "key1", "dog")
	if !output.isString("OK") {
		t.Fatal("nochange create key fail")
	}

	output = ts1.ProcessCommand("watch", "key1")
	if !output.isString("OK") {
		t.Fatal("nochange watch the key fail")
	}

	output = ts1.ProcessCommand("multi")
	if !output.isString("OK") {
		t.Fatal("nochange start multi fail")
	}

	output = ts1.ProcessCommand("set", "key1", "cat")
	if !output.isString(strQueued) {
		t.Fatal("nochange change the value fail")
	}

	output = ts1.ProcessCommand("exec")
	if !output.isValue([]any{"OK"}) {
		t.Fatal("nochange exec fail")
	}
}

func TestRedisTwoMulti(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("multi")
	if !output.isString("OK") {
		t.Fatal("two multi step 1 fail")
	}

	output = ts.ProcessCommand("set", "key1", "cat")
	if !output.isString(strQueued) {
		t.Fatal("two multi step 2 fail")
	}

	output = ts.ProcessCommand("incr", "key2")
	if !output.isString(strQueued) {
		t.Fatal("two multi step 3 fail")
	}

	output = ts.ProcessCommand("exec")
	if !output.isValue([]any{"OK", 1}) {
		t.Fatal("two multi step 4 fail")
	}
}

func TestRedisUnwatch(t *testing.T) {
	ts1 := NewRedisTestClient()
	defer ts1.Close()
	ts2 := ts1.AdditionalClient()
	defer ts2.Close()

	output := ts1.ProcessCommand("watch", "key1")
	if !output.isString("OK") {
		t.Fatal("unwatch set initial watch fail")
	}

	output = ts1.ProcessCommand("multi")
	if !output.isString("OK") {
		t.Fatal("unwatch step 1 fail")
	}

	output = ts2.ProcessCommand("set", "key1", "dog")
	if !output.isString("OK") {
		t.Fatal("unwatch set value in other client fail")
	}

	output = ts1.ProcessCommand("unwatch")
	if !output.isString(strQueued) {
		t.Fatal("unwatch queue unwatch fail")
	}

	output = ts1.ProcessCommand("set", "key1", "cat")
	if !output.isString(strQueued) {
		t.Fatal("unwatch set in multi fail")
	}

	output = ts1.ProcessCommand("exec")
	if !output.isNull() {
		t.Fatal("unwatch exec fail")
	}
}

func TestRedisAbortExec(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("multi")
	if !output.isString("OK") {
		t.Fatal("abort exec start multi fail")
	}

	output = ts.ProcessCommand("set", "key1", "cat")
	if !output.isString(strQueued) {
		t.Fatal("abort exec queue set fail")
	}

	output = ts.ProcessCommand("incr", "key2")
	if !output.isString(strQueued) {
		t.Fatal("abort exec queue incr fail")
	}

	output = ts.ProcessCommand("discard")
	if !output.isString("OK") {
		t.Fatal("abort exec discard multi fail")
	}

	output = ts.ProcessCommand("exec")
	if !output.isErrorType() {
		t.Fatal("abort exec after discard fail")
	}

	output = ts.ProcessCommand("discard")
	if !output.isErrorType() { // "ERR DISCARD without MULTI"
		t.Fatal("abort exec step 6 fail")
	}
}
