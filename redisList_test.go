package redisemu

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

func TestRedisLPushPop(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// pop missing key tests
	output := ts.ProcessCommand("lpop", "key1")
	if !output.isNull() {
		t.Fatal("lpushpop step 1 fail")
	}

	output = ts.ProcessCommand("lpop", "key1", "1")
	if !output.isNull() {
		t.Fatal("lpushpop step 2 fail")
	}

	// basic push pop
	output = ts.ProcessCommand("lpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("lpushpop step 3 fail")
	}

	output = ts.ProcessCommand("lpop", "key1")
	if !output.isString("cat") {
		t.Fatal("lpushpop step 4 fail")
	}

	// pop empty list
	output = ts.ProcessCommand("lpop", "key1")
	if !output.isNull() {
		t.Fatal("lpushpop step 5 fail")
	}

	// count tests
	output = ts.ProcessCommand("lpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("lpushpop step 6 fail")
	}

	output = ts.ProcessCommand("lpop", "key1", "1")
	if !output.isValue([]any{"cat"}) {
		t.Fatal("lpushpop step 7 fail")
	}

	output = ts.ProcessCommand("lpop", "key1")
	if !output.isNull() {
		t.Fatal("lpushpop step 8 fail")
	}

	output = ts.ProcessCommand("lpop", "key1", "1")
	if !output.isNull() {
		t.Fatal("lpushpop step 9 fail")
	}

	// push or pop to string test
	output = ts.ProcessCommand("set", "key2", "dog")
	if !output.isString("OK") {
		t.Fatal("lpushpop can't make string key fail")
	}

	output = ts.ProcessCommand("lpush", "key2", "try")
	if !output.isErrorType() {
		t.Fatal("lpushpop push to string fail")
	}

	output = ts.ProcessCommand("lpop", "key2")
	if !output.isErrorType() {
		t.Fatal("lpushpop pop from string fail")
	}

	// zero count test
	output = ts.ProcessCommand("lpush", "key1", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("lpushpop step 12 fail")
	}

	output = ts.ProcessCommand("lpop", "key1", "0")
	if !output.isValue([]any{}) {
		t.Fatal("lpushpop step 13 fail")
	}

	// count greater than list length test
	output = ts.ProcessCommand("lpop", "key1", "3")
	if !output.isValue([]any{"dog", "cat"}) {
		t.Fatal("lpushpop step 14 fail")
	}

	output = ts.ProcessCommand("lpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("lpushpop step 15 fail")
	}

	// invalid count test
	output = ts.ProcessCommand("lpush", "key1", "dog", "cow", "duck")
	if !output.isInt(4) {
		t.Fatal("lpushpop step 16 fail")
	}

	output = ts.ProcessCommand("lpop", "key1", "-1")
	if !output.isErrorType() {
		t.Fatal("lpushpop step 17 fail")
	}

	// count less than length of list test
	output = ts.ProcessCommand("lpop", "key1", "2")
	if !output.isValue([]any{"duck", "cow"}) {
		t.Fatal("lpushpop step 18 fail")
	}

	// count equals length of list test
	output = ts.ProcessCommand("lpop", "key1", "2")
	if !output.isValue([]any{"dog", "cat"}) {
		t.Fatal("lpushpop step 19 fail")
	}

	// expired key test
	output = ts.ProcessCommand("lpush", "key1", "dog", "cow", "duck")
	if !output.isInt(3) {
		t.Fatal("lpushpop push expired list fail")
	}

	output = ts.ProcessCommand("pexpire", "key1", "10")
	if !output.isInt(1) {
		t.Fatal("lpushpop expire the list fail")
	}

	time.Sleep(11 * time.Millisecond)

	output = ts.ProcessCommand("lpop", "key1")
	if !output.isNull() {
		t.Fatal("lpushpop pop expired fail")
	}
}

func TestRedisRPushPop(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key tests
	output := ts.ProcessCommand("rpop", "key1")
	if !output.isNull() {
		t.Fatal("rpushpop step 1 fail")
	}

	output = ts.ProcessCommand("rpop", "key1", "1")
	if !output.isNull() {
		t.Fatal("rpushpop step 2 fail")
	}

	// basic push pop test
	output = ts.ProcessCommand("rpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("rpushpop step 3 fail")
	}

	output = ts.ProcessCommand("rpop", "key1")
	if !output.isString("cat") {
		t.Fatal("rpushpop step 4 fail")
	}

	// pop empty list test
	output = ts.ProcessCommand("rpop", "key1")
	if !output.isNull() {
		t.Fatal("rpushpop step 5 fail")
	}

	// count test
	output = ts.ProcessCommand("rpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("rpushpop step 6 fail")
	}

	output = ts.ProcessCommand("rpop", "key1", "1")
	if !output.isValue([]any{"cat"}) {
		t.Fatal("rpushpop step 7 fail")
	}

	output = ts.ProcessCommand("rpop", "key1")
	if !output.isNull() {
		t.Fatal("rpushpop step 8 fail")
	}

	// count and empty list test
	output = ts.ProcessCommand("rpop", "key1", "1")
	if !output.isNull() {
		t.Fatal("rpushpop step 9 fail")
	}

	// push and pop on string test
	output = ts.ProcessCommand("set", "key2", "dog")
	if !output.isString("OK") {
		t.Fatal("rpushpop can't set string key fail")
	}

	output = ts.ProcessCommand("rpush", "key2", "try")
	if !output.isErrorType() {
		t.Fatal("rpushpop push string fail")
	}

	output = ts.ProcessCommand("rpop", "key2")
	if !output.isErrorType() {
		t.Fatal("rpushpop pop string fail")
	}

	// zero count test
	output = ts.ProcessCommand("rpush", "key1", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("rpushpop step 12 fail")
	}

	output = ts.ProcessCommand("rpop", "key1", "0")
	if !output.isValue([]any{}) {
		t.Fatal("rpushpop step 13 fail")
	}

	// count > list length test
	output = ts.ProcessCommand("rpop", "key1", "3")
	if !output.isValue([]any{"dog", "cat"}) {
		t.Fatal("rpushpop step 14 fail")
	}

	// return val of second push test
	output = ts.ProcessCommand("rpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("rpushpop step 15 fail")
	}

	output = ts.ProcessCommand("rpush", "key1", "dog", "cow", "duck")
	if !output.isInt(4) {
		t.Fatal("rpushpop step 16 fail")
	}

	// negative count test
	output = ts.ProcessCommand("rpop", "key1", "-1")
	if !output.isErrorType() {
		t.Fatal("rpushpop step 17 fail")
	}

	// pop less than list length test
	output = ts.ProcessCommand("rpop", "key1", "2")
	if !output.isValue([]any{"duck", "cow"}) {
		t.Fatal("rpushpop step 18 fail")
	}

	// pop list length test
	output = ts.ProcessCommand("rpop", "key1", "2")
	if !output.isValue([]any{"dog", "cat"}) {
		t.Fatal("rpushpop step 19 fail")
	}

	// expired key test
	output = ts.ProcessCommand("rpush", "key1", "dog", "cow", "duck")
	if !output.isInt(3) {
		t.Fatal("rpushpop push expired list fail")
	}

	output = ts.ProcessCommand("pexpire", "key1", "10")
	if !output.isInt(1) {
		t.Fatal("rpushpop expire the list fail")
	}

	time.Sleep(11 * time.Millisecond)

	output = ts.ProcessCommand("rpop", "key1")
	if !output.isNull() {
		t.Fatal("rpushpop pop expired fail")
	}
}

func TestRedisPushLPopR(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	output := ts.ProcessCommand("lpush", "key1", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("pushlpopr step 1 fail")
	}

	output = ts.ProcessCommand("rpop", "key1")
	if !output.isString("cat") {
		t.Fatal("pushlpopr step 2 fail")
	}

	output = ts.ProcessCommand("rpop", "key1", "2")
	if !output.isValue([]any{"dog", "pig"}) {
		t.Fatal("pushlpopr step 3 fail")
	}
}

func TestRedisPushRPopL(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	output := ts.ProcessCommand("rpush", "key1", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("pushrpopl step 1 fail")
	}

	output = ts.ProcessCommand("lpop", "key1")
	if !output.isString("cat") {
		t.Fatal("pushrpopl step 2 fail")
	}

	output = ts.ProcessCommand("lpop", "key1", "2")
	if !output.isValue([]any{"dog", "pig"}) {
		t.Fatal("pushrpopl step 3 fail")
	}
}

func TestRedisLIndex(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key tests
	output := ts.ProcessCommand("lindex", "key1", "0")
	if !output.isNull() {
		t.Fatal("lindex step 1 fail")
	}

	output = ts.ProcessCommand("lindex", "key1", "-1")
	if !output.isNull() {
		t.Fatal("lindex step 2 fail")
	}

	// head test
	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("lindex step 3 fail")
	}

	output = ts.ProcessCommand("lindex", "key1", "0")
	if !output.isString("cat") {
		t.Fatal("lindex step 4 fail")
	}

	// tail test
	output = ts.ProcessCommand("lindex", "key1", "-1")
	if !output.isString("pig") {
		t.Fatal("lindex step 5 fail")
	}

	// last item from positive index test
	output = ts.ProcessCommand("lindex", "key1", "2")
	if !output.isString("pig") {
		t.Fatal("lindex step 6 fail")
	}

	// first item from negative index test
	output = ts.ProcessCommand("lindex", "key1", "-3")
	if !output.isString("cat") {
		t.Fatal("lindex step 7 fail")
	}

	// out of range tests
	output = ts.ProcessCommand("lindex", "key1", "3")
	if !output.isNull() {
		t.Fatal("lindex step 8 fail")
	}

	output = ts.ProcessCommand("lindex", "key1", "-4")
	if !output.isNull() {
		t.Fatal("lindex step 9 fail")
	}

	// lindex on string test
	output = ts.ProcessCommand("set", "str", "test")
	if !output.isString("OK") {
		t.Fatal("lindex set string key fail")
	}

	output = ts.ProcessCommand("lindex", "str", "0")
	if !output.isErrorType() {
		t.Fatal("lindex op on string key fail")
	}
}

func TestRedisLInsert(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key tests
	output := ts.ProcessCommand("linsert", "key1", "before", "cat", "dog")
	if !output.isInt(0) {
		t.Fatal("linsert before missing key fail")
	}

	output = ts.ProcessCommand("linsert", "key1", "after", "cat", "dog")
	if !output.isInt(0) {
		t.Fatal("linsert after missing key fail")
	}

	// insert before tests
	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("linsert list setup fail")
	}
	ts.DumpKey("key1")

	output = ts.ProcessCommand("linsert", "key1", "before", "cat", "cow")
	if !output.isInt(4) {
		t.Fatal("linsert before first fail")
	}
	ts.DumpKey("key1")

	output = ts.ProcessCommand("linsert", "key1", "before", "dog", "chicken")
	if !output.isInt(5) {
		t.Fatal("linsert before last fail")
	}
	ts.DumpKey("key1")

	// insert after tests
	output = ts.ProcessCommand("linsert", "key1", "after", "cat", "goat")
	if !output.isInt(6) {
		t.Fatal("linsert after second fail")
	}
	ts.DumpKey("key1")

	output = ts.ProcessCommand("linsert", "key1", "after", "pig", "duck")
	if !output.isInt(7) {
		t.Fatal("linsert after last fail")
	}
	ts.DumpKey("key1")

	// verify full list
	output = ts.ProcessCommand("lpop", "key1", "7")
	if !output.isValue([]any{"cow", "cat", "goat", "chicken", "dog", "pig", "duck"}) {
		t.Fatal("linsert step 8 fail")
	}

	// insert no pivot test
	output = ts.ProcessCommand("linsert", "key1", "after", "fox", "hen")
	if !output.isInt(0) {
		t.Fatal("linsert no list key fail")
	}

	output = ts.ProcessCommand("rpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("linsert push key1 one item fail")
	}

	output = ts.ProcessCommand("linsert", "key1", "after", "fox", "hen")
	if !output.isInt(-1) {
		t.Fatal("linsert no list key fail")
	}

	// linsert on string test
	output = ts.ProcessCommand("set", "str", "test")
	if !output.isString("OK") {
		t.Fatal("linsert set string key fail")
	}

	output = ts.ProcessCommand("linsert", "str", "after", "cat", "goat")
	if !output.isErrorType() {
		t.Fatal("linsert op on string key fail")
	}
}

func TestRedisLLen(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	output := ts.ProcessCommand("llen", "key1")
	if !output.isInt(0) {
		t.Fatal("llen step 1 fail")
	}

	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("llen step 2 fail")
	}

	output = ts.ProcessCommand("llen", "key1")
	if !output.isInt(3) {
		t.Fatal("llen step 3 fail")
	}

	output = ts.ProcessCommand("set", "key2", "foo")
	if !output.isString("OK") {
		t.Fatal("llen step 4 fail")
	}

	output = ts.ProcessCommand("llen", "key2")
	if !output.isErrorType() {
		t.Fatal("llen step 5 fail")
	}
}

func TestRedisLMove(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key test
	output := ts.ProcessCommand("lmove", "key1", "key2", "left", "left")
	if !output.isNull() {
		t.Fatal("lmove step 1 fail")
	}

	output = ts.ProcessCommand("lmove", "key1", "key2", "left", "right")
	if !output.isNull() {
		t.Fatal("lmove step 2 fail")
	}

	output = ts.ProcessCommand("lmove", "key1", "key2", "right", "left")
	if !output.isNull() {
		t.Fatal("lmove step 3 fail")
	}

	output = ts.ProcessCommand("lmove", "key1", "key2", "right", "right")
	if !output.isNull() {
		t.Fatal("lmove step 4 fail")
	}

	// missing dest test
	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("lmove step 5 fail")
	}

	output = ts.ProcessCommand("lmove", "key1", "key2", "left", "left")
	if !output.isString("cat") {
		t.Fatal("lmove step 6 fail")
	}

	output = ts.ProcessCommand("lpop", "key2", "10")
	if !output.isValue([]any{"cat"}) {
		t.Fatal("lmove step 7 fail")
	}

	// empty list test
	output = ts.ProcessCommand("rpush", "key3", "cat")
	if !output.isInt(1) {
		t.Fatal("lmove step 8 fail")
	}

	output = ts.ProcessCommand("rpop", "key3")
	if !output.isString("cat") {
		t.Fatal("lmove step 9 fail")
	}

	output = ts.ProcessCommand("lmove", "key3", "key4", "left", "left")
	if !output.isNull() {
		t.Fatal("lmove step 10 fail")
	}

	// two lists test -- all four directions
	output = ts.ProcessCommand("rpush", "key5", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("lmove step 11 fail")
	}

	output = ts.ProcessCommand("rpush", "key6", "pig", "mouse")
	if !output.isInt(2) {
		t.Fatal("lmove step 12 fail")
	}

	output = ts.ProcessCommand("lmove", "key5", "key6", "left", "left") // *cat,dog:pig,mouse -> dog:*cat,pig,mouse
	if !output.isString("cat") {
		t.Fatal("lmove step 13 fail")
	}

	output = ts.ProcessCommand("lrange", "key6", "0", "-1")
	if !output.isValue([]any{"cat", "pig", "mouse"}) {
		t.Fatal("lmove step 14 fail")
	}

	output = ts.ProcessCommand("lmove", "key6", "key5", "left", "right") // *cat,pig,mouse:dog -> pig,mouse:dog,*cat
	if !output.isString("cat") {
		t.Fatal("lmove step 15 fail")
	}

	output = ts.ProcessCommand("lrange", "key5", "0", "-1")
	if !output.isValue([]any{"dog", "cat"}) {
		t.Fatal("lmove step 16 fail")
	}

	output = ts.ProcessCommand("lmove", "key6", "key5", "right", "left") // pig,*mouse:dog,cat -> pig:*mouse,dog,cat
	if !output.isString("mouse") {
		t.Fatal("lmove step 17 fail")
	}

	ts.DumpKey("key6")
	ts.DumpKey("key5")

	output = ts.ProcessCommand("lrange", "key5", "0", "-1")
	if !output.isValue([]any{"mouse", "dog", "cat"}) {
		t.Fatal("lmove step 18 fail")
	}

	output = ts.ProcessCommand("lmove", "key5", "key6", "right", "right") // mouse,dog,*cat:pig -> mouse,dog:pig,*cat
	if !output.isString("cat") {
		t.Fatal("lmove step 18 fail")
	}

	output = ts.ProcessCommand("lrange", "key6", "0", "-1")
	if !output.isValue([]any{"pig", "cat"}) {
		t.Fatal("lmove step 19 fail")
	}

	// invalid key type test
	output = ts.ProcessCommand("set", "str", "abc")
	if !output.isString("OK") {
		t.Fatal("lmove step 20 fail")
	}

	output = ts.ProcessCommand("lmove", "str", "str2", "left", "right")
	if !output.isErrorType() {
		t.Fatal("lmove step 21 fail")
	}

	// rotation test
	output = ts.ProcessCommand("rpush", "rot", "cat", "dog", "mouse", "lamb", "fox")
	if !output.isInt(5) {
		t.Fatal("lmove step 22 fail")
	}

	output = ts.ProcessCommand("lmove", "rot", "rot", "right", "left")
	if !output.isString("fox") {
		t.Fatal("lmove step 23 fail")
	}

	output = ts.ProcessCommand("lrange", "rot", "0", "-1")
	if !output.isValue([]any{"fox", "cat", "dog", "mouse", "lamb"}) {
		t.Fatal("lmove step 24 fail")
	}

	output = ts.ProcessCommand("lmove", "rot", "rot", "left", "right")
	if !output.isString("fox") {
		t.Fatal("lmove step 25 fail")
	}

	output = ts.ProcessCommand("lrange", "rot", "0", "-1")
	if !output.isValue([]any{"cat", "dog", "mouse", "lamb", "fox"}) {
		t.Fatal("lmove step 26 fail")
	}

	// wrong key type test
	output = ts.ProcessCommand("set", "str", "text")
	if !output.isString("OK") {
		t.Fatal("lmove make string key fail")
	}

	output = ts.ProcessCommand("lmove", "str", "rot", "right", "left")
	if !output.isErrorType() {
		t.Fatal("lmove wrong source key type fail")
	}

	output = ts.ProcessCommand("lmove", "rot", "str", "right", "left")
	if !output.isErrorType() {
		t.Fatal("lmove wrong dest key type fail")
	}
}

func TestRedisLMoveSrcKeyGone(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	output := ts.ProcessCommand("rpush", "key1", "fox")
	if !output.isInt(1) {
		t.Fatal("rpush error")
	}

	output = ts.ProcessCommand("lmove", "key1", "key2", "left", "left")
	if !output.isValue("fox") {
		t.Fatal("lmove fail")
	}

	output = ts.ProcessCommand("keys", "*")
	if !output.isValue([]any{"key2"}) {
		t.Fatal("key move fail")
	}
}

func TestRedisLPopSrcKeyGone(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	output := ts.ProcessCommand("rpush", "key1", "fox")
	if !output.isInt(1) {
		t.Fatal("rpush error")
	}

	output = ts.ProcessCommand("lpop", "key1")
	if !output.isString("fox") {
		t.Fatal("lpop fail")
	}

	output = ts.ProcessCommand("keys", "*")
	if !output.isArray() {
		t.Fatal("key remove fail")
	}
}

func TestRedisRPopSrcKeyGone(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	output := ts.ProcessCommand("rpush", "key1", "fox")
	if !output.isInt(1) {
		t.Fatal("rpush error")
	}

	output = ts.ProcessCommand("rpop", "key1")
	if !output.isString("fox") {
		t.Fatal("rpop fail")
	}

	output = ts.ProcessCommand("keys", "*")
	if !output.isArray() {
		t.Fatal("key remove fail")
	}
}

func TestRedisLMPop(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key testa
	output := ts.ProcessCommand("lmpop", "2", "key1", "key2", "left", "count", "10")
	if !output.isNull() {
		t.Fatal("lmpop step 1 fail")
	}

	output = ts.ProcessCommand("lmpop", "1", "key1", "left")
	if !output.isNull() {
		t.Fatal("lmpop step 2 fail")
	}

	// bad syntax tests
	output = ts.ProcessCommand("lmpop", "0", "left")
	if !output.isErrorType() {
		t.Fatal("lmpop step 3 fail")
	}

	output = ts.ProcessCommand("lmpop", "1", "left")
	if !output.isErrorType() {
		t.Fatal("lmpop step 4 fail")
	}

	output = ts.ProcessCommand("lmpop", "-1", "key", "left")
	if !output.isErrorType() {
		t.Fatal("lmpop step 5 fail")
	}

	output = ts.ProcessCommand("lmpop", "2", "key", "left")
	if !output.isErrorType() {
		t.Fatal("lmpop step 6 fail")
	}

	// first list has items test
	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("lmpop step 7 fail")
	}

	output = ts.ProcessCommand("lmpop", "2", "key1", "key2", "left", "count", "10")
	if !output.isValue([]any{"key1", []any{"cat", "dog", "pig"}}) {
		t.Fatal("lmpop step 8 fail")
	}

	// first list is empty test
	output = ts.ProcessCommand("lmpop", "2", "key1", "key2", "left")
	if !output.isNull() {
		t.Fatal("lmpop step 9 fail")
	}

	output = ts.ProcessCommand("lmpop", "1", "key1", "left")
	if !output.isNull() {
		t.Fatal("lmpop step 10 fail")
	}

	output = ts.ProcessCommand("keys", "*")
	if !output.isArray() {
		t.Fatal("key remove fail")
	}

	// second list has items test
	output = ts.ProcessCommand("rpush", "key2", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("lmpop step 11 fail")
	}

	output = ts.ProcessCommand("lmpop", "2", "key1", "key2", "left", "count", "10")
	if !output.isValue([]any{"key2", []any{"cat", "dog", "pig"}}) {
		t.Fatal("lmpop step 12 fail")
	}

	// count and left/right test
	output = ts.ProcessCommand("rpush", "key3", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("lmpop step 13 fail")
	}

	output = ts.ProcessCommand("lmpop", "1", "key3", "right", "count", "1")
	if !output.isValue([]any{"key3", []any{"pig"}}) {
		t.Fatal("lmpop step 14 fail")
	}

	output = ts.ProcessCommand("lmpop", "1", "key3", "left", "count", "1")
	if !output.isValue([]any{"key3", []any{"cat"}}) {
		t.Fatal("lmpop step 15 fail")
	}

	output = ts.ProcessCommand("lmpop", "1", "key3", "right", "count", "1")
	if !output.isValue([]any{"key3", []any{"dog"}}) {
		t.Fatal("lmpop step 16 fail")
	}

	output = ts.ProcessCommand("rpush", "key3", "fox", "duck")
	if !output.isInt(2) {
		t.Fatal("lmpop step 17 fail")
	}

	output = ts.ProcessCommand("lmpop", "1", "key3", "right", "count", "1")
	if !output.isValue([]any{"key3", []any{"duck"}}) {
		t.Fatal("lmpop step 18 fail")
	}

	output = ts.ProcessCommand("lmpop", "1", "key3", "left", "count", "1")
	if !output.isValue([]any{"key3", []any{"fox"}}) {
		t.Fatal("lmpop step 19 fail")
	}

	// bad count test
	output = ts.ProcessCommand("lmpop", "1", "key3", "left", "count", "0")
	if !output.isErrorType() {
		t.Fatal("lmpop step 20 fail")
	}

	output = ts.ProcessCommand("lmpop", "1", "key3", "left", "count", "-1")
	if !output.isErrorType() {
		t.Fatal("lmpop step 21 fail")
	}

	// wrong key type test
	output = ts.ProcessCommand("set", "str", "text")
	if !output.isString("OK") {
		t.Fatal("lmpop make string key fail")
	}

	output = ts.ProcessCommand("lmpop", "1", "str", "left")
	if !output.isErrorType() {
		t.Fatal("lmpop wrong key type fail")
	}
}

func TestRedisLPos(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key test
	output := ts.ProcessCommand("lpos", "missing", "foo")
	if !output.isNull() {
		t.Fatal("lpos step 1 fail")
	}

	output = ts.ProcessCommand("lpos", "missing", "foo", "rank", "1")
	if !output.isNull() {
		t.Fatal("lpos step 2 fail")
	}

	output = ts.ProcessCommand("lpos", "missing", "foo", "count", "0")
	if !output.isValue([]any{}) {
		t.Fatal("lpos step 3 fail")
	}

	output = ts.ProcessCommand("lpos", "missing", "foo", "count", "1")
	if !output.isValue([]any{}) {
		t.Fatal("lpos step 4 fail")
	}

	output = ts.ProcessCommand("lpos", "missing", "foo", "maxlen", "1")
	if !output.isNull() {
		t.Fatal("lpos step 5 fail")
	}

	// item not found test
	output = ts.ProcessCommand("rpush", "k1", "fox", "duck", "cow", "fox")
	if !output.isInt(4) {
		t.Fatal("lpos step 6 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "foo")
	if !output.isNull() {
		t.Fatal("lpos step 7 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "foo", "count", "0")
	if !output.isValue([]any{}) {
		t.Fatal("lpos step 8 fail")
	}

	// item found test
	output = ts.ProcessCommand("lpos", "k1", "duck")
	if !output.isInt(1) {
		t.Fatal("lpos step 9 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "duck", "count", "0")
	if !output.isValue([]any{1}) {
		t.Fatal("lpos step 10 fail")
	}

	// two items found test
	output = ts.ProcessCommand("lpos", "k1", "fox")
	if !output.isInt(0) {
		t.Fatal("lpos step 10 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "count", "0")
	if !output.isValue([]any{0, 3}) {
		t.Fatal("lpos step 11 fail")
	}

	// rank test
	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "0")
	if !output.isErrorType() {
		t.Fatal("lpos step 12 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "1")
	if !output.isInt(0) {
		t.Fatal("lpos step 13 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "2")
	if !output.isInt(3) {
		t.Fatal("lpos step 14 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "3")
	if !output.isNull() {
		t.Fatal("lpos step 15 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "-1")
	if !output.isInt(3) {
		t.Fatal("lpos step 16 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "-2")
	if !output.isInt(0) {
		t.Fatal("lpos step 17 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "-3")
	if !output.isNull() {
		t.Fatal("lpos step 18 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "0", "count", "0")
	if !output.isErrorType() {
		t.Fatal("lpos step 19 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "1", "count", "0")
	if !output.isValue([]any{0, 3}) {
		t.Fatal("lpos step 20 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "2", "count", "0")
	if !output.isValue([]any{3}) {
		t.Fatal("lpos step 21 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "3", "count", "0")
	if !output.isValue([]any{}) {
		t.Fatal("lpos step 22 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "-1", "count", "0")
	if !output.isValue([]any{3, 0}) {
		t.Fatal("lpos step 23 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "-2", "count", "0")
	if !output.isValue([]any{0}) {
		t.Fatal("lpos step 24 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "rank", "-3", "count", "0")
	if !output.isValue([]any{}) {
		t.Fatal("lpos step 25 fail")
	}

	// maxlen test
	output = ts.ProcessCommand("lpos", "k1", "fox", "maxlen", "-1")
	if !output.isErrorType() {
		t.Fatal("lpos step 26 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "maxlen", "0")
	if !output.isInt(0) {
		t.Fatal("lpos step 27 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "count", "0", "maxlen", "0")
	if !output.isValue([]any{0, 3}) {
		t.Fatal("lpos step 28 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "maxlen", "1")
	if !output.isInt(0) {
		t.Fatal("lpos step 29 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "maxlen", "1", "count", "0")
	if !output.isValue([]any{0}) {
		t.Fatal("lpos step 30 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "maxlen", "4")
	if !output.isInt(0) {
		t.Fatal("lpos step 31 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "maxlen", "4", "count", "0")
	if !output.isValue([]any{0, 3}) {
		t.Fatal("lpos step 32 fail")
	}

	// invalid count test
	output = ts.ProcessCommand("lpos", "k1", "fox", "maxlen", "4", "count", "-1")
	if !output.isErrorType() {
		t.Fatal("lpos step 33 fail")
	}

	output = ts.ProcessCommand("lpos", "k1", "fox", "maxlen", "4", "count")
	if !output.isErrorType() {
		t.Fatal("lpos step 34 fail")
	}

	// wrong key type test
	output = ts.ProcessCommand("set", "str", "foo")
	if !output.isString("OK") {
		t.Fatal("lpos step 35 fail")
	}

	output = ts.ProcessCommand("lpos", "str", "fox")
	if !output.isErrorType() {
		t.Fatal("lpos step 36 fail")
	}
}

func TestRedisLPushXPop(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("lpushx", "key1", "cat")
	if !output.isInt(0) {
		t.Fatal("lpushxpop missing key fail")
	}

	// empty list
	output = ts.ProcessCommand("lpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("lpushxpop start the list fail")
	}

	output = ts.ProcessCommand("lpop", "key1")
	if !output.isString("cat") {
		t.Fatal("lpushxpop empty the list fail")
	}

	output = ts.ProcessCommand("lpushx", "key1", "cat")
	if !output.isInt(0) {
		t.Fatal("lpushxpop push to empty list fail")
	}

	// push to a non empty list
	output = ts.ProcessCommand("lpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("lpushxpop put item back in the list fail")
	}

	output = ts.ProcessCommand("lpushx", "key1", "dog")
	if !output.isInt(2) {
		t.Fatal("lpushxpop push to non empty list fail")
	}

	// push two elements
	output = ts.ProcessCommand("lpushx", "key1", "fox", "duck")
	if !output.isInt(4) {
		t.Fatal("lpushxpop push two to non empty list fail")
	}

	output = ts.ProcessCommand("lpop", "key1", "10")
	if !output.isValue([]any{"duck", "fox", "dog", "cat"}) {
		t.Fatal("lpushxpop pop verify fail")
	}

	// wrong key type
	output = ts.ProcessCommand("set", "str", "foo")
	if !output.isString("OK") {
		t.Fatal("lpushxpop create string fail")
	}

	output = ts.ProcessCommand("lpushx", "str", "item")
	if !output.isErrorType() {
		t.Fatal("lpushxpop wrong key type fail")
	}
}

func TestRedisRPushXPop(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("rpushx", "key1", "cat")
	if !output.isInt(0) {
		t.Fatal("rpushxpop missing key fail")
	}

	// empty list
	output = ts.ProcessCommand("lpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("rpushxpop start the list fail")
	}

	output = ts.ProcessCommand("lpop", "key1")
	if !output.isString("cat") {
		t.Fatal("rpushxpop empty the list fail")
	}

	output = ts.ProcessCommand("rpushx", "key1", "cat")
	if !output.isInt(0) {
		t.Fatal("rpushxpop push to empty list fail")
	}

	// push to a non empty list
	output = ts.ProcessCommand("lpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("rpushxpop push item back in the list fail")
	}

	output = ts.ProcessCommand("rpushx", "key1", "dog")
	if !output.isInt(2) {
		t.Fatal("rpushxpop push to non empty list fail")
	}

	// push two elements
	output = ts.ProcessCommand("rpushx", "key1", "fox", "duck")
	if !output.isInt(4) {
		t.Fatal("rpushxpop push two to non empty list fail")
	}

	output = ts.ProcessCommand("rpop", "key1", "10")
	if !output.isValue([]any{"duck", "fox", "dog", "cat"}) {
		t.Fatal("rpushxpop pop verify fail")
	}

	// wrong key type
	output = ts.ProcessCommand("set", "str", "foo")
	if !output.isString("OK") {
		t.Fatal("rpushxpop create string fail")
	}

	output = ts.ProcessCommand("rpushx", "str", "item")
	if !output.isErrorType() {
		t.Fatal("rpushxpop wrong key type fail")
	}
}

func TestRedisLRem(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("lrem", "key1", "0", "cat")
	if !output.isInt(0) {
		t.Fatal("lrem missing key fail")
	}

	// single item list
	output = ts.ProcessCommand("lpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("lrem start the list fail")
	}

	output = ts.ProcessCommand("lrem", "key1", "0", "cat")
	if !output.isInt(1) {
		t.Fatal("lrem single item remove fail")
	}

	keycheck := ts.ProcessCommand("keys", "key1")
	if !keycheck.isValue([]any{}) {
		t.Fatal("lrem should have removed the key")
	}

	// empty list
	output = ts.ProcessCommand("lrem", "key1", "0", "cat")
	if !output.isInt(0) {
		t.Fatal("lrem empty list fail")
	}

	// two item remove
	output = ts.ProcessCommand("lpush", "key1", "cat", "dog", "cow", "cat")
	if !output.isInt(4) {
		t.Fatal("lrem start the list of four fail")
	}

	output = ts.ProcessCommand("lrem", "key1", "0", "cat")
	if !output.isInt(2) {
		t.Fatal("lrem two item remove fail")
	}

	// count arg tests
	output = ts.ProcessCommand("lpush", "key1", "cat", "fox", "cat")
	if !output.isInt(5) {
		t.Fatal("lrem start the positive count arg list fail")
	}

	output = ts.ProcessCommand("lrem", "key1", "1", "cat")
	if !output.isInt(1) {
		t.Fatal("lrem head item remove fail")
	}

	output = ts.ProcessCommand("lpop", "key1", "10")
	if !output.isValue([]any{"fox", "cat", "cow", "dog"}) {
		t.Fatal("lrem verify head removal fail")
	}

	output = ts.ProcessCommand("lpush", "key1", "fox", "cat", "cow", "dog", "cat")
	if !output.isInt(5) {
		t.Fatal("lrem start the negative count arg list fail")
	}

	output = ts.ProcessCommand("lrem", "key1", "-1", "cat")
	if !output.isInt(1) {
		t.Fatal("lrem head tail remove fail")
	}

	output = ts.ProcessCommand("lpop", "key1", "10")
	if !output.isValue([]any{"cat", "dog", "cow", "fox"}) {
		t.Fatal("lrem verify tail removal fail")
	}

	// wrong key type test
	output = ts.ProcessCommand("set", "str", "text")
	if !output.isString("OK") {
		t.Fatal("lrem make string key fail")
	}

	output = ts.ProcessCommand("lrem", "str", "0", "cat")
	if !output.isErrorType() {
		t.Fatal("lrem wrong key type fail")
	}
}

func TestRedisLRange(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{}) {
		t.Fatal("lrange missing key fail")
	}

	// single item list
	output = ts.ProcessCommand("lpush", "key1", "one")
	if !output.isInt(1) {
		t.Fatal("lrange start the list fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{"one"}) {
		t.Fatal("lrange single item list fail")
	}

	// empty list
	output = ts.ProcessCommand("lpop", "key1")
	if !output.isString("one") {
		t.Fatal("lrange pop one fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{}) {
		t.Fatal("lrange empty list fail")
	}

	// three item list with an empty string
	output = ts.ProcessCommand("rpush", "key1", "", "one", "two")
	if !output.isInt(3) {
		t.Fatal("lrange add three to the list fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{"", "one", "two"}) {
		t.Fatal("lrange three item list fail")
	}

	// two item list
	output = ts.ProcessCommand("lpop", "key1")
	if !output.isString("") {
		t.Fatal("lrange pop empty string fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{"one", "two"}) {
		t.Fatal("lrange three item list fail")
	}

	// range tests
	output = ts.ProcessCommand("rpush", "key1", "three", "four", "five")
	if !output.isInt(5) {
		t.Fatal("lrange add three to the list fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{"one", "two", "three", "four", "five"}) {
		t.Fatal("lrange five item list fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "-1", "-1")
	if !output.isValue([]any{"five"}) {
		t.Fatal("lrange last item fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "-1", "-2")
	if !output.isValue([]any{}) {
		t.Fatal("lrange negative start greater than end fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "2", "1")
	if !output.isValue([]any{}) {
		t.Fatal("lrange positive start greater than end fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "-200", "1")
	if !output.isValue([]any{"one", "two"}) {
		t.Fatal("lrange big negative start fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "200", "210")
	if !output.isValue([]any{}) {
		t.Fatal("lrange big positive start fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "1", "3")
	if !output.isValue([]any{"two", "three", "four"}) {
		t.Fatal("lrange middle range fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "1", "30")
	if !output.isValue([]any{"two", "three", "four", "five"}) {
		t.Fatal("lrange big positive end fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "1", "-30")
	if !output.isValue([]any{}) {
		t.Fatal("lrange big negative end fail")
	}

	// wrong key type test
	output = ts.ProcessCommand("set", "str", "text")
	if !output.isString("OK") {
		t.Fatal("lrange make string key fail")
	}

	output = ts.ProcessCommand("lrange", "str", "0", "-1")
	if !output.isErrorType() {
		t.Fatal("lrange wrong key type fail")
	}
}

func TestRedisLSet(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("lset", "key1", "0", "test")
	if !output.isErrorType() || !output.isString("ERR no such key") {
		t.Fatal("lset missing key fail")
	}

	// single item list
	output = ts.ProcessCommand("lpush", "key1", "one")
	if !output.isInt(1) {
		t.Fatal("lset start the list fail")
	}

	output = ts.ProcessCommand("lset", "key1", "0", "test")
	if !output.isString("OK") {
		t.Fatal("lset set value 0 fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "0")
	if !output.isValue([]any{"test"}) {
		t.Fatal("lset get value 0 fail")
	}

	// empty list
	output = ts.ProcessCommand("lpop", "key1")
	if !output.isString("test") {
		t.Fatal("lset empty the list fail")
	}

	output = ts.ProcessCommand("lset", "key1", "0", "test")
	if !output.isErrorType() || !output.isString("ERR no such key") {
		t.Fatal("lset set empty list fail")
	}

	// out of range
	output = ts.ProcessCommand("lpush", "key1", "one", "two", "three")
	if !output.isInt(3) {
		t.Fatal("lset start the multi item list fail")
	}

	output = ts.ProcessCommand("lset", "key1", "-10", "test")
	if !output.isErrorType() || !output.isString("ERR index out of range") {
		t.Fatal("lset set empty list fail")
	}

	// multi item list tests
	output = ts.ProcessCommand("lset", "key1", "1", "cat")
	if !output.isString("OK") {
		t.Fatal("lset set value 1 fail")
	}

	output = ts.ProcessCommand("lset", "key1", "2", "dog")
	if !output.isString("OK") {
		t.Fatal("lset set value 2 fail")
	}

	output = ts.ProcessCommand("rpush", "key1", "four")
	if !output.isInt(4) {
		t.Fatal("lset add fourth item fail")
	}

	output = ts.ProcessCommand("lset", "key1", "1", "fox")
	if !output.isString("OK") {
		t.Fatal("lset set value 1 again fail")
	}

	output = ts.ProcessCommand("lpop", "key1", "10")
	if !output.isValue([]any{"three", "fox", "dog", "four"}) {
		t.Fatal("lset verify multi set fail")
	}

	// negative index tests
	output = ts.ProcessCommand("rpush", "key1", "one", "two", "three")
	if !output.isInt(3) {
		t.Fatal("lset start the range test list fail")
	}

	output = ts.ProcessCommand("lset", "key1", "-1", "cat")
	if !output.isString("OK") {
		t.Fatal("lset set value -1 fail")
	}

	output = ts.ProcessCommand("lset", "key1", "-2", "dog")
	if !output.isString("OK") {
		t.Fatal("lset set value -2 fail")
	}

	output = ts.ProcessCommand("lset", "key1", "-3", "fox")
	if !output.isString("OK") {
		t.Fatal("lset set value -3 fail")
	}

	output = ts.ProcessCommand("lpop", "key1", "10")
	if !output.isValue([]any{"fox", "dog", "cat"}) {
		t.Fatal("lset verify negative index fail")
	}

	// invalid range tests
	output = ts.ProcessCommand("rpush", "key1", "one", "two", "three")
	if !output.isInt(3) {
		t.Fatal("lset start the invalid range test list fail")
	}

	output = ts.ProcessCommand("lset", "key1", "3", "test")
	if !output.isErrorType() || !output.isString("ERR index out of range") {
		t.Fatal("lset set positive bounds fail")
	}

	output = ts.ProcessCommand("lset", "key1", "-4", "test")
	if !output.isErrorType() || !output.isString("ERR index out of range") {
		t.Fatal("lset set negative bounds fail")
	}

	// wrong type test
	output = ts.ProcessCommand("set", "str", "foo")
	if !output.isString("OK") {
		t.Fatal("lset make string key fail")
	}

	output = ts.ProcessCommand("lset", "str", "0", "test")
	if !output.isErrorType() || !output.isString(string(wrongTypeError)) {
		t.Fatal("lset wrong type fail")
	}
}

func TestRedisLTrim(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("ltrim", "key1", "0", "-1")
	if !output.isString("OK") {
		t.Fatal("ltrim missing key fail")
	}

	// single item list
	output = ts.ProcessCommand("lpush", "key1", "one")
	if !output.isInt(1) {
		t.Fatal("ltrim start the list fail")
	}

	output = ts.ProcessCommand("ltrim", "key1", "1", "1")
	if !output.isString("OK") {
		t.Fatal("ltrim empty fail")
	}

	output = ts.ProcessCommand("keys", "*")
	if !output.isArray() {
		t.Fatal("key remove fail")
	}

	output = ts.ProcessCommand("lpush", "key1", "one")
	if !output.isInt(1) {
		t.Fatal("ltrim start the list fail")
	}

	output = ts.ProcessCommand("ltrim", "key1", "0", "1")
	if !output.isString("OK") {
		t.Fatal("ltrim no change fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{"one"}) {
		t.Fatal("ltrim get value after no change fail")
	}

	output = ts.ProcessCommand("ltrim", "key1", "0", "-1")
	if !output.isString("OK") {
		t.Fatal("ltrim no change negative fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{"one"}) {
		t.Fatal("ltrim get value after no change negative fail")
	}

	output = ts.ProcessCommand("ltrim", "key1", "-2", "-1")
	if !output.isString("OK") {
		t.Fatal("ltrim no change negative start fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{"one"}) {
		t.Fatal("ltrim get value after no change negative start fail")
	}

	// many item list
	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "fox", "mule")
	if !output.isInt(5) {
		t.Fatal("ltrim start the multi list fail")
	}

	output = ts.ProcessCommand("ltrim", "key1", "1", "-1")
	if !output.isString("OK") {
		t.Fatal("ltrim trim first item fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{"cat", "dog", "fox", "mule"}) {
		t.Fatal("ltrim get value after trim first item fail")
	}

	output = ts.ProcessCommand("ltrim", "key1", "-3", "3")
	if !output.isString("OK") {
		t.Fatal("ltrim trim first item negative start fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{"dog", "fox", "mule"}) {
		t.Fatal("ltrim get value after trim first item negative start fail")
	}

	// end less than start tests
	output = ts.ProcessCommand("ltrim", "key1", "1", "0")
	if !output.isString("OK") {
		t.Fatal("ltrim trim end less than start fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{}) {
		t.Fatal("ltrim get value after end less than start fail")
	}

	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "fox", "mule")
	if !output.isInt(4) {
		t.Fatal("ltrim start the empty list fail")
	}

	output = ts.ProcessCommand("ltrim", "key1", "-2", "-3")
	if !output.isString("OK") {
		t.Fatal("ltrim trim end less than start negative fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{}) {
		t.Fatal("ltrim get value after end less than start negative fail")
	}

	// bounds tests
	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "fox", "mule")
	if !output.isInt(4) {
		t.Fatal("ltrim start the first bounds list fail")
	}

	output = ts.ProcessCommand("ltrim", "key1", "-10", "10")
	if !output.isString("OK") {
		t.Fatal("ltrim wider than list fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{"cat", "dog", "fox", "mule"}) {
		t.Fatal("ltrim wider than list fail")
	}

	output = ts.ProcessCommand("ltrim", "key1", "10", "20")
	if !output.isString("OK") {
		t.Fatal("ltrim beyond list right fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{}) {
		t.Fatal("ltrim beyond list right fail")
	}

	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "fox", "mule")
	if !output.isInt(4) {
		t.Fatal("ltrim start the second bounds list fail")
	}

	output = ts.ProcessCommand("ltrim", "key1", "-20", "-10")
	if !output.isString("OK") {
		t.Fatal("ltrim beyond list left fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{}) {
		t.Fatal("ltrim beyond list left fail")
	}

	// single item middle of multi item list tests
	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "fox", "mule")
	if !output.isInt(4) {
		t.Fatal("ltrim start the trim to single item list fail")
	}

	output = ts.ProcessCommand("ltrim", "key1", "2", "2")
	if !output.isString("OK") {
		t.Fatal("ltrim to single item fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{"fox"}) {
		t.Fatal("ltrim to single item fail")
	}

	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "fox", "mule")
	if !output.isInt(5) {
		t.Fatal("ltrim start the trim to single item list negative fail")
	}

	output = ts.ProcessCommand("ltrim", "key1", "-3", "-3")
	if !output.isString("OK") {
		t.Fatal("ltrim to single item negative fail")
	}

	output = ts.ProcessCommand("lrange", "key1", "0", "-1")
	if !output.isValue([]any{"dog"}) {
		t.Fatal("ltrim to single item negative fail")
	}

	// wrong type test
	output = ts.ProcessCommand("set", "str", "text")
	if !output.isString("OK") {
		t.Fatal("ltrim make string key fail")
	}

	output = ts.ProcessCommand("ltrim", "str", "0", "-1")
	if !output.isErrorType() {
		t.Fatal("ltrim wrong source key type fail")
	}
}

func TestRedisRPopLPush(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing key test
	output := ts.ProcessCommand("rpoplpush", "key1", "key2")
	if !output.isNull() {
		t.Fatal("rpoplpush missing src key fail")
	}

	// missing dest test
	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("rpoplpush missing dest key fail")
	}

	output = ts.ProcessCommand("rpoplpush", "key1", "key2")
	if !output.isString("pig") {
		t.Fatal("rpoplpush missing dest key fail")
	}

	output = ts.ProcessCommand("lpop", "key2", "10")
	if !output.isValue([]any{"pig"}) {
		t.Fatal("rpoplpush missing dest key verify fail")
	}

	// empty list test
	output = ts.ProcessCommand("rpush", "key3", "cat")
	if !output.isInt(1) {
		t.Fatal("rpoplpush start empty list fail")
	}

	output = ts.ProcessCommand("rpop", "key3")
	if !output.isString("cat") {
		t.Fatal("rpoplpush clear empty list fail")
	}

	output = ts.ProcessCommand("rpoplpush", "key3", "key4")
	if !output.isNull() {
		t.Fatal("rpoplpush empty list op fail")
	}

	// two lists test
	output = ts.ProcessCommand("rpush", "key5", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("rpoplpush start two item list fail")
	}

	output = ts.ProcessCommand("rpush", "key6", "pig", "mouse")
	if !output.isInt(2) {
		t.Fatal("rpoplpush start second two item list fail")
	}

	output = ts.ProcessCommand("rpoplpush", "key5", "key6") // cat,*dog:pig,mouse -> cat:*dog,pig,mouse
	if !output.isString("dog") {
		t.Fatal("rpoplpush two item list dog fail")
	}

	output = ts.ProcessCommand("lrange", "key6", "0", "-1")
	if !output.isValue([]any{"dog", "pig", "mouse"}) {
		t.Fatal("rpoplpush two item list verify fail")
	}

	// invalid key type test
	output = ts.ProcessCommand("set", "str", "abc")
	if !output.isString("OK") {
		t.Fatal("rpoplpush make string key fail")
	}

	output = ts.ProcessCommand("rpoplpush", "str", "str2")
	if !output.isErrorType() {
		t.Fatal("rpoplpush source string key fail")
	}

	// rotation test
	output = ts.ProcessCommand("rpush", "rot", "cat", "dog", "mouse", "lamb", "fox")
	if !output.isInt(5) {
		t.Fatal("rpoplpush make rotation list fail")
	}

	output = ts.ProcessCommand("rpoplpush", "rot", "rot")
	if !output.isString("fox") {
		t.Fatal("rpoplpush rotation fail")
	}

	output = ts.ProcessCommand("lrange", "rot", "0", "-1")
	if !output.isValue([]any{"fox", "cat", "dog", "mouse", "lamb"}) {
		t.Fatal("rpoplpush rotation verify fail")
	}

	// wrong key type test
	output = ts.ProcessCommand("set", "str", "text")
	if !output.isString("OK") {
		t.Fatal("rpoplpush make string key fail")
	}

	output = ts.ProcessCommand("rpoplpush", "str", "rot")
	if !output.isErrorType() {
		t.Fatal("rpoplpush wrong source key type fail")
	}

	output = ts.ProcessCommand("rpoplpush", "rot", "str")
	if !output.isErrorType() {
		t.Fatal("rpoplpush wrong dest key type fail")
	}
}

func TestRedisBLMove(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()
	ts3 := ts.AdditionalClient()
	defer ts3.Close()

	// missing key test
	output := ts.ProcessCommand("blmove", "key1", "key2", "left", "left", "0.010")
	if !output.isNull() {
		t.Fatal("blmove missing keys left left fail")
	}

	output = ts.ProcessCommand("blmove", "key1", "key2", "left", "right", "0.010")
	if !output.isNull() {
		t.Fatal("blmove missing keys left right fail")
	}

	output = ts.ProcessCommand("blmove", "key1", "key2", "right", "left", "0.010")
	if !output.isNull() {
		t.Fatal("blmove missing keys right left fail")
	}

	output = ts.ProcessCommand("blmove", "key1", "key2", "right", "right", "0.010")
	if !output.isNull() {
		t.Fatal("blmove missing keys right right fail")
	}

	// missing dest test
	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("blmove set up dest list fail")
	}

	output = ts.ProcessCommand("blmove", "key1", "key2", "left", "left", "0.010")
	if !output.isString("cat") {
		t.Fatal("blmove missing dest fail")
	}

	output = ts.ProcessCommand("lpop", "key2", "10")
	if !output.isValue([]any{"cat"}) {
		t.Fatal("blmove missing dest verify fail")
	}

	// empty list test
	output = ts.ProcessCommand("lpop", "key1", "10")
	if !output.isValue([]any{"dog", "pig"}) {
		t.Fatal("blmove missing dest verify fail")
	}

	output = ts.ProcessCommand("blmove", "key1", "key2", "left", "left", "0.010")
	if !output.isNull() {
		t.Fatal("blmove empty list fail")
	}

	// empty then non empty after 10ms rpush test
	now := ts.ServerNow()
	var complete time.Time
	var output2 respValue
	go func() {
		ts.Lane().Tracef("blmove starting at %s", ts.ServerNow().Format(time.StampMilli))
		output = ts.ProcessCommand("blmove", "key1", "key2", "left", "left", ".080")
		complete = ts.ServerNow()
		ts.Lane().Tracef("blmove completed at %s", complete.Format(time.StampMilli))
	}()
	go func() {
		time.Sleep(40 * time.Millisecond)
		output2 = ts2.ProcessCommand("rpush", "key1", "cat")
		ts2.Lane().Tracef("rpush completed at %s", ts2.ServerNow().Format(time.StampMilli))
	}()

	time.Sleep(120 * time.Millisecond)
	if !output.isString("cat") {
		t.Fatal("blmove empty then non-empty rpush list fail")
	}
	if !output2.isInt(1) {
		t.Fatal("blmove rpush fail")
	}
	delta := complete.Sub(now)
	if delta.Milliseconds() < 40 || delta.Milliseconds() > 70 {
		ts.Lane().Errorf("delta is %d ms", delta.Milliseconds())
		t.Fatal("blmove empty then non-empty rpush delta fail")
	}

	// empty then non empty after 10ms rpush test
	now = ts.ServerNow()
	go func() {
		ts.Lane().Tracef("blmove starting at %s", ts.ServerNow().Format(time.StampMilli))
		output = ts.ProcessCommand("blmove", "key1", "key2", "left", "left", ".080")
		complete = ts.ServerNow()
		ts.Lane().Tracef("blmove completed at %s", complete.Format(time.StampMilli))
	}()
	go func() {
		time.Sleep(40 * time.Millisecond)
		output2 = ts2.ProcessCommand("lpush", "key1", "goat")
		ts2.Lane().Tracef("rpush completed at %s", ts2.ServerNow().Format(time.StampMilli))
	}()

	time.Sleep(120 * time.Millisecond)
	if !output.isString("goat") {
		t.Fatal("blmove empty then non-empty lpush list fail")
	}
	if !output2.isInt(1) {
		t.Fatal("blmove rpush fail")
	}
	delta = complete.Sub(now)
	if delta.Milliseconds() < 40 || delta.Milliseconds() > 70 {
		ts.Lane().Errorf("delta is %d ms", delta.Milliseconds())
		t.Fatal("blmove empty then non-empty lpush delta fail")
	}

	// negative timeout blocks
	now = ts.ServerNow()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		ts.Lane().Tracef("blmove starting at %s", ts.ServerNow().Format(time.StampMilli))
		output = ts.ProcessCommand("blmove", "z1", "z2", "left", "left", "-.001")
		complete = ts.ServerNow()
		ts.Lane().Tracef("blmove completed at %s", complete.Format(time.StampMilli))
		wg.Done()
	}()
	time.Sleep(60 * time.Millisecond)
	output = ts2.ProcessCommand("client", "unblock", fmt.Sprintf("%d", ts.ClientID()))
	if !output.isInt(1) {
		t.Fatal("client unblock fail")
	}

	wg.Wait()
	delta = complete.Sub(now)

	// redis bug - it should not block with a negative timeout
	_, isTestClient := ts.(*testClient)
	if isTestClient {
		if delta.Milliseconds() > 10 {
			ts.Lane().Errorf("delta is %d ms", delta.Milliseconds())
			t.Fatal("blmove negative timeout does not block fail")
		}
	} else {
		if delta.Milliseconds() < 60 {
			ts.Lane().Errorf("delta is %d ms", delta.Milliseconds())
			t.Fatal("blmove negative timeout no faster than 60ms fail")
		}
	}

	// cancel the connection while blocked
	now = ts.ServerNow()
	go func() {
		ts.Lane().Tracef("blmove starting at %s", ts.ServerNow().Format(time.StampMilli))
		output = ts.ProcessCommand("blmove", "key1", "key2", "left", "left", ".080")
		complete = ts.ServerNow()
		ts.Lane().Tracef("blmove completed at %s", complete.Format(time.StampMilli))
	}()
	go func() {
		time.Sleep(40 * time.Millisecond)
		ts.Terminate()
		ts.Lane().Tracef("client terminate queued at %s", ts.ServerNow().Format(time.StampMilli))
	}()

	time.Sleep(120 * time.Millisecond)
	if !output.isErrorType() {
		t.Fatal("blmove blocked then terminated list fail")
	}
	delta = complete.Sub(now)
	if delta.Milliseconds() < 40 || delta.Milliseconds() > 70 {
		ts.Lane().Errorf("delta is %d ms", delta.Milliseconds())
		t.Fatal("blmove blocked then terminated delta fail")
	}
}

func TestRedisBLMove2(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()
	ts3 := ts.AdditionalClient()
	defer ts3.Close()

	// two block indefinitely test
	now := ts.ServerNow()
	var output respValue
	var output2 respValue
	var output3 respValue
	var output4 respValue
	var complete time.Time
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		ts.Lane().Tracef("blmove two block 1 starting at %s", ts.ServerNow().Format(time.StampMilli))
		output = ts.ProcessCommand("blmove", "key1", "key2", "left", "left", "0")
		complete = ts.ServerNow()
		ts.Lane().Tracef("blmove two block 1 completed at %s", complete.Format(time.StampMilli))
		wg.Done()
	}()
	go func() {
		time.Sleep(4 * time.Millisecond)
		ts2.Lane().Tracef("blmove two block 2 starting at %s", ts2.ServerNow().Format(time.StampMilli))
		output2 = ts2.ProcessCommand("blmove", "key1", "key2", "left", "left", "0")
		complete = ts2.ServerNow()
		ts2.Lane().Tracef("blmove two block 2 completed at %s", complete.Format(time.StampMilli))
		wg.Done()
	}()
	go func() {
		time.Sleep(20 * time.Millisecond)
		output3 = ts3.ProcessCommand("lpush", "key1", "mule")
		ts3.Lane().Tracef("lpush completed at %s", ts3.ServerNow().Format(time.StampMilli))
		time.Sleep(20 * time.Millisecond)
		output4 = ts3.ProcessCommand("lpush", "key1", "chicken")
		ts3.Lane().Tracef("lpush completed again at %s", ts3.ServerNow().Format(time.StampMilli))
		wg.Done()
	}()

	wg.Wait()
	if !output.isString("mule") {
		t.Fatal("blmove two item blocking first fail")
	}
	if !output3.isInt(1) {
		t.Fatal("blmove two block lpush 1 fail")
	}
	if !output4.isInt(1) {
		t.Fatal("blmove two block lpush 2 fail")
	}
	if !output2.isString("chicken") {
		t.Fatal("blmove two item blocking second fail")
	}
	delta := complete.Sub(now)
	if delta.Milliseconds() < 40 || delta.Milliseconds() > 70 {
		ts.Lane().Errorf("delta is %d ms", delta.Milliseconds())
		t.Fatal("blmove two item blocking delta fail")
	}

	// wrong key type test
	output = ts.ProcessCommand("rpush", "wrongkey", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("blmove set up wrong key list fail")
	}

	output = ts.ProcessCommand("set", "str", "text")
	if !output.isString("OK") {
		t.Fatal("blmove make string key fail")
	}

	output = ts.ProcessCommand("blmove", "str", "wrongkey", "right", "left", "0")
	if !output.isErrorType() {
		t.Fatal("blmove wrong source key type fail")
	}

	output = ts.ProcessCommand("blmove", "wrongkey", "str", "right", "left", "0")
	if !output.isErrorType() {
		t.Fatal("blmove wrong dest key type fail")
	}
}

func onStressCommandComplete(ch chan RedisTestClient, client RedisTestClient, waiters *int32, f int) {
	if f < 12 {
		atomic.AddInt32(waiters, -1)
	}
	ch <- client
}

func TestRedisBLMoveStress(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	ts.Lane().SetLogLevel(lane.LogLevelDebug)

	r := rand.New(rand.NewSource(0))

	output := ts.ProcessCommand("rpush", "rot", "hen", "pig", "cow", "mouse", "hawk")
	if !output.isInt(5) {
		t.Fatal("blmove stress list init fail")
	}

	pushClients := make(chan RedisTestClient, 100)
	rotateClients := make(chan RedisTestClient, 100)
	terminationClients := make([]RedisTestClient, 0, 100)

	for i := 0; i < 100; i++ {
		c := ts.AdditionalClient()
		pushClients <- c
		terminationClients = append(terminationClients, c)

		c = ts.AdditionalClient()
		rotateClients <- c
		terminationClients = append(terminationClients, c)
	}

	stopped := false
	fail := false
	pushes := []string{"cat", "dog", "chicken", "rooster", "horse", "lamb", "goat"}

	var wg sync.WaitGroup

	waiters := int32(0)

	for i := 0; i < 200000; i++ {
		// pick a random function and wait for an available client
		var ch chan RedisTestClient
		f := r.Intn(19)
		if f < 12 {
			if atomic.AddInt32(&waiters, 1) > 75 {
				atomic.AddInt32(&waiters, -1)
				f = 12 + (f % 6)
			}
		}

		if f >= 12 {
			ch = pushClients
		} else {
			ch = rotateClients
		}
		client := <-ch

		// do the random operation
		func() {
			wg.Add(1)
			defer wg.Done()

			switch f {
			case 0, 1:
				go func() {
					o := client.ProcessCommand("blmove", "key1", "key2", "left", "right", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 2, 3:
				go func() {
					o := client.ProcessCommand("blmove", "key2", "key1", "right", "left", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 4, 5:
				go func() {
					o := client.ProcessCommand("blmove", "key2", "key1", "left", "right", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 6, 7:
				go func() {
					o := client.ProcessCommand("blmove", "key1", "key2", "right", "left", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 8:
				go func() {
					o := client.ProcessCommand("blmove", "key1", "key1", "right", "left", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 9:
				go func() {
					o := client.ProcessCommand("blmove", "key1", "key1", "left", "right", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 10:
				go func() {
					o := client.ProcessCommand("blmove", "key2", "key2", "right", "left", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 11:
				go func() {
					o := client.ProcessCommand("blmove", "key2", "key2", "left", "right", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 12:
				go func() {
					client.ProcessCommand("lpop", "key1", "5")
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 13:
				go func() {
					client.ProcessCommand("lpop", "key2", "5")
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 14:
				go func() {
					m := r.Intn(len(pushes))
					output = client.ProcessCommand("lpush", "key1", pushes[m])
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 15:
				go func() {
					m := r.Intn(len(pushes))
					output = client.ProcessCommand("lpush", "key2", pushes[m])
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 16:
				go func() {
					m := r.Intn(len(pushes))
					output = client.ProcessCommand("rpush", "key1", pushes[m])
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 17:
				go func() {
					m := r.Intn(len(pushes))
					output = client.ProcessCommand("rpush", "key2", pushes[m])
					time.Sleep(time.Millisecond)
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 18:
				go func() {
					m := r.Intn(len(terminationClients))
					output = client.ProcessCommand("client", "unblock", fmt.Sprintf("%d", m), "error")
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			}
		}()
	}

	stopped = true
	for _, tc := range terminationClients {
		tc.Terminate()
	}

	wg.Wait()

	if fail {
		t.Fatal("blmove stress fail somewhere")
	}
}

func TestRedisBLMPop(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	// missing key tests
	output := ts.ProcessCommand("blmpop", "0.010", "2", "key1", "key2", "left", "count", "10")
	if !output.isNull() {
		t.Fatal("blmpop wait for missing key fail")
	}

	output = ts.ProcessCommand("blmpop", "0.010", "1", "key1", "left")
	if !output.isNull() {
		t.Fatal("blmpop mi fail")
	}

	// bad syntax tests
	output = ts.ProcessCommand("blmpop", "0", "0", "left")
	if !output.isErrorType() {
		t.Fatal("blmpop invalid key count fail")
	}

	output = ts.ProcessCommand("blmpop", "0", "1", "left")
	if !output.isErrorType() {
		t.Fatal("blmpop missing key fail")
	}

	output = ts.ProcessCommand("blmpop", "0", "-1", "key", "left")
	if !output.isErrorType() {
		t.Fatal("blmpop negative key count fail")
	}

	output = ts.ProcessCommand("blmpop", "0", "2", "key", "left")
	if !output.isErrorType() {
		t.Fatal("blmpop missing second key fail")
	}

	output = ts.ProcessCommand("blmpop", "1", "key", "left")
	if !output.isErrorType() {
		t.Fatal("blmpop missing timeout fail")
	}

	// first list has items test
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		output = ts.ProcessCommand("blmpop", "0", "2", "key1", "key2", "left", "count", "10")
		wg.Done()
	}()

	time.Sleep(time.Millisecond)

	output2 := ts2.ProcessCommand("rpush", "key1", "cat", "dog", "pig")
	if !output2.isInt(3) {
		t.Fatal("blmpop push first list fail")
	}

	wg.Wait()

	if !output.isValue([]any{"key1", []any{"cat", "dog", "pig"}}) {
		t.Fatal("blmpop pop first list fail")
	}

	// count and left/right test
	invalid := 0
	wg.Add(1)
	go func() {
		output = ts.ProcessCommand("blmpop", "1", "1", "key3", "right", "count", "1")
		if !output.isValue([]any{"key3", []any{"pig"}}) {
			invalid++
		}

		output = ts.ProcessCommand("blmpop", "0.5", "1", "key3", "left", "count", "1")
		if !output.isValue([]any{"key3", []any{"cat"}}) {
			invalid++
		}

		output = ts.ProcessCommand("blmpop", ".5", "1", "key3", "right", "count", "1")
		if !output.isValue([]any{"key3", []any{"dog"}}) {
			invalid++
		}

		wg.Done()
	}()

	output2 = ts2.ProcessCommand("rpush", "key3", "cat", "dog", "pig")
	if !output2.isInt(3) {
		t.Fatal("blmpop push second list fail")
	}

	wg.Wait()

	if invalid != 0 {
		t.Fatal("blmpop pop second list fail")
	}

	// wrong key type test
	output = ts.ProcessCommand("set", "str", "text")
	if !output.isString("OK") {
		t.Fatal("blmpop make string key fail")
	}

	output = ts.ProcessCommand("blmpop", "0", "1", "str", "left")
	if !output.isErrorType() {
		t.Fatal("blmpop wrong key type fail")
	}
}

func TestRedisBLMPopStress(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	ts.Lane().SetLogLevel(lane.LogLevelDebug)

	r := rand.New(rand.NewSource(0))

	output := ts.ProcessCommand("rpush", "rot", "hen", "pig", "cow", "mouse", "hawk")
	if !output.isInt(5) {
		t.Fatal("blmove stress list init fail")
	}

	pushClients := make(chan RedisTestClient, 100)
	rotateClients := make(chan RedisTestClient, 100)
	terminationClients := make([]RedisTestClient, 0, 100)

	for i := 0; i < 100; i++ {
		c := ts.AdditionalClient()
		pushClients <- c
		terminationClients = append(terminationClients, c)

		c = ts.AdditionalClient()
		rotateClients <- c
		terminationClients = append(terminationClients, c)
	}

	stopped := false
	fail := false
	pushes := []string{"cat", "dog", "chicken", "rooster", "horse", "lamb", "goat"}

	var wg sync.WaitGroup

	waiters := int32(0)

	for i := 0; i < 50000; i++ {
		// pick a random function and wait for an available client
		var ch chan RedisTestClient
		f := r.Intn(18)
		if f < 12 {
			if atomic.AddInt32(&waiters, 1) > 75 {
				atomic.AddInt32(&waiters, -1)
				f = 12 + (f % 6)
			}
		}

		if f >= 12 {
			ch = pushClients
		} else {
			ch = rotateClients
		}
		client := <-ch

		// do the random operation
		func() {
			wg.Add(1)
			defer wg.Done()

			switch f {
			case 0:
				go func() {
					o := client.ProcessCommand("blmpop", "0", "1", "key1", "left")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 1:
				go func() {
					o := client.ProcessCommand("blmpop", "0", "1", "key1", "right")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 2:
				go func() {
					o := client.ProcessCommand("blmpop", "0", "2", "key1", "key2", "left", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 3:
				go func() {
					o := client.ProcessCommand("blmpop", "0", "2", "key1", "key2", "right", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 4:
				go func() {
					o := client.ProcessCommand("blmpop", "0", "1", "key2", "left", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 5:
				go func() {
					o := client.ProcessCommand("blmpop", "0", "1", "key2", "right", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 6:
				go func() {
					o := client.ProcessCommand("blmove", "key1", "key2", "right", "left", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 7:
				go func() {
					o := client.ProcessCommand("blmove", "key2", "key1", "right", "left", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 8:
				go func() {
					o := client.ProcessCommand("blmove", "key1", "key1", "right", "left", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 9:
				go func() {
					o := client.ProcessCommand("blmove", "key1", "key1", "left", "right", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 10:
				go func() {
					o := client.ProcessCommand("blmove", "key2", "key2", "right", "left", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 11:
				go func() {
					o := client.ProcessCommand("blmove", "key2", "key2", "left", "right", "0")
					if o.data == nil && !stopped {
						fail = true
					}
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 12:
				go func() {
					client.ProcessCommand("lpop", "key1", "5")
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 13:
				go func() {
					client.ProcessCommand("lpop", "key2", "5")
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 14:
				go func() {
					m := r.Intn(len(pushes))
					output = client.ProcessCommand("lpush", "key1", pushes[m])
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 15:
				go func() {
					m := r.Intn(len(pushes))
					output = client.ProcessCommand("lpush", "key2", pushes[m])
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 16:
				go func() {
					m := r.Intn(len(pushes))
					output = client.ProcessCommand("rpush", "key1", pushes[m])
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			case 17:
				go func() {
					m := r.Intn(len(pushes))
					output = client.ProcessCommand("rpush", "key2", pushes[m])
					onStressCommandComplete(ch, client, &waiters, f)
				}()
			}
		}()
	}

	stopped = true
	for _, tc := range terminationClients {
		tc.Terminate()
	}

	wg.Wait()

	if fail {
		t.Fatal("blmpop stress fail somewhere")
	}
}

func TestRedisBLPushPop(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	// pop missing key tests
	output := ts.ProcessCommand("blpop", "key1", "0.1")
	if !output.isNull() {
		t.Fatal("blpushpop missing key timeout fail")
	}

	output = ts.ProcessCommand("blpop", "key1", "1", "0.1") // second key name is not a count, unlike lpop
	if !output.isNull() {
		t.Fatal("blpushpop two missing keys timeout fail")
	}

	// basic push pop
	output = ts.ProcessCommand("lpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("blpushpop basic push fail")
	}

	output = ts.ProcessCommand("blpop", "key1", "0")
	if !output.isValue([]any{"key1", "cat"}) {
		t.Fatal("blpushpop basic pop fail")
	}

	// wait for multi-key push to complete test
	var wg sync.WaitGroup
	fail := false
	wg.Add(1)
	go func() {
		output = ts.ProcessCommand("blpop", "multikey", "0")
		if !output.isValue([]any{"multikey", "cow"}) {
			fail = true
		}
		wg.Done()
	}()

	time.Sleep(time.Millisecond)

	output = ts2.ProcessCommand("lpush", "multikey", "cat", "dog", "cow")
	if !output.isInt(3) {
		t.Fatal("blpushpop push three items fail")
	}

	wg.Wait()

	if fail {
		t.Fatal("blpushpop multi-key pop fail")
	}

	// pop to string test
	output = ts.ProcessCommand("set", "key2", "dog")
	if !output.isString("OK") {
		t.Fatal("blpushpop can't make string key fail")
	}

	output = ts.ProcessCommand("blpop", "key2", "0")
	if !output.isErrorType() {
		t.Fatal("blpushpop pop from string fail")
	}

	// expired key test
	output = ts.ProcessCommand("lpush", "key1", "dog", "cow", "duck")
	if !output.isInt(3) {
		t.Fatal("blpushpop push expired list fail")
	}

	output = ts.ProcessCommand("pexpire", "key1", "10")
	if !output.isInt(1) {
		t.Fatal("blpushpop expire the list fail")
	}

	time.Sleep(11 * time.Millisecond)

	output = ts.ProcessCommand("blpop", "key1", ".01")
	if !output.isNull() {
		t.Fatal("blpushpop pop expired fail")
	}
}

func TestRedisBRPushPop(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	// pop missing key tests
	output := ts.ProcessCommand("brpop", "key1", "0.1")
	if !output.isNull() {
		t.Fatal("brpushpop missing key timeout fail")
	}

	output = ts.ProcessCommand("brpop", "key1", "1", "0.1") // second key name is not a count, unlike lpop
	if !output.isNull() {
		t.Fatal("brpushpop two missing keys timeout fail")
	}

	// basic push pop
	output = ts.ProcessCommand("rpush", "key1", "cat")
	if !output.isInt(1) {
		t.Fatal("brpushpop basic push fail")
	}

	output = ts.ProcessCommand("brpop", "key1", "0")
	if !output.isValue([]any{"key1", "cat"}) {
		t.Fatal("brpushpop basic pop fail")
	}

	// wait for multi-key push to complete test
	var wg sync.WaitGroup
	fail := false
	wg.Add(1)
	go func() {
		output = ts.ProcessCommand("brpop", "multikey", "0")
		if !output.isValue([]any{"multikey", "cow"}) {
			fail = true
		}
		wg.Done()
	}()

	time.Sleep(time.Millisecond)

	output = ts2.ProcessCommand("rpush", "multikey", "cat", "dog", "cow")
	if !output.isInt(3) {
		t.Fatal("brpushpop push three items fail")
	}

	wg.Wait()

	if fail {
		t.Fatal("brpushpop multi-key pop fail")
	}

	// pop to string test
	output = ts.ProcessCommand("set", "key2", "dog")
	if !output.isString("OK") {
		t.Fatal("brpushpop can't make string key fail")
	}

	output = ts.ProcessCommand("brpop", "key2", "0")
	if !output.isErrorType() {
		t.Fatal("brpushpop pop from string fail")
	}

	// expired key test
	output = ts.ProcessCommand("rpush", "key1", "dog", "cow", "duck")
	if !output.isInt(3) {
		t.Fatal("brpushpop push expired list fail")
	}

	output = ts.ProcessCommand("pexpire", "key1", "10")
	if !output.isInt(1) {
		t.Fatal("brpushpop expire the list fail")
	}

	time.Sleep(11 * time.Millisecond)

	output = ts.ProcessCommand("brpop", "key1", ".01")
	if !output.isNull() {
		t.Fatal("brpushpop pop expired fail")
	}
}

func TestRedisBRPopLPush(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()
	ts2 := ts.AdditionalClient()
	defer ts2.Close()

	// missing key test
	output := ts.ProcessCommand("brpoplpush", "key1", "key2", "0.01")
	if !output.isNull() {
		t.Fatal("brpoplpush missing src key fail")
	}

	// missing dest test
	output = ts.ProcessCommand("rpush", "key1", "cat", "dog", "pig")
	if !output.isInt(3) {
		t.Fatal("brpoplpush missing dest key fail")
	}

	output = ts.ProcessCommand("brpoplpush", "key1", "key2", "0")
	if !output.isString("pig") {
		t.Fatal("brpoplpush missing dest key fail")
	}

	output = ts.ProcessCommand("lpop", "key2", "10")
	if !output.isValue([]any{"pig"}) {
		t.Fatal("brpoplpush missing dest key verify fail")
	}

	// two lists test
	fail := false
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		output = ts.ProcessCommand("brpoplpush", "key5", "key6", "10") // cat,*dog:pig,mouse -> cat:*dog,pig,mouse
		if !output.isString("dog") {
			fail = true
		}
		wg.Done()
	}()

	time.Sleep(time.Millisecond)

	output = ts2.ProcessCommand("rpush", "key6", "pig", "mouse")
	if !output.isInt(2) {
		t.Fatal("brpoplpush start second two item list fail")
	}

	output = ts2.ProcessCommand("rpush", "key5", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("brpoplpush start two item list fail")
	}

	wg.Wait()

	if fail {
		t.Fatal("brpoplpush two item list dog fail")
	}

	output = ts.ProcessCommand("lrange", "key6", "0", "-1")
	if !output.isValue([]any{"dog", "pig", "mouse"}) {
		t.Fatal("brpoplpush two item list verify fail")
	}

	// wrong key type tests
	output = ts.ProcessCommand("set", "str", "abc")
	if !output.isString("OK") {
		t.Fatal("brpoplpush make string key fail")
	}

	output = ts.ProcessCommand("brpoplpush", "str", "str2", "0")
	if !output.isErrorType() {
		t.Fatal("brpoplpush source string key fail")
	}

	output = ts.ProcessCommand("brpoplpush", "str", "dest", "0")
	if !output.isErrorType() {
		t.Fatal("brpoplpush wrong source key type fail")
	}

	output = ts.ProcessCommand("rpush", "src", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("brpoplpush make source list fail")
	}

	output = ts.ProcessCommand("brpoplpush", "src", "str", "0")
	if !output.isErrorType() {
		t.Fatal("brpoplpush wrong dest key type fail")
	}
}

func TestRedisBPushPopExecNils(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// start multi
	output := ts.ProcessCommand("multi")
	if !output.isString("OK") {
		t.Fatal("bpushpopexec start multi fail")
	}

	// blocking list move tests
	output = ts.ProcessCommand("blmove", "src", "dest", "left", "left", "0")
	if !output.isString(strQueued) {
		t.Fatal("bpushpopexec queue blmove missing key fail")
	}

	output = ts.ProcessCommand("blmpop", "0", "1", "missing", "left")
	if !output.isString(strQueued) {
		t.Fatal("bpushpopexec queue blmpop missing key fail")
	}

	output = ts.ProcessCommand("blpop", "missing", "0")
	if !output.isString(strQueued) {
		t.Fatal("bpushpopexec queue blpop missing key fail")
	}

	output = ts.ProcessCommand("brpop", "missing", "0")
	if !output.isString(strQueued) {
		t.Fatal("bpushpopexec queue brpop missing key fail")
	}

	output = ts.ProcessCommand("brpoplpush", "src", "dest", "0")
	if !output.isString(strQueued) {
		t.Fatal("bpushpopexec queue brpoplpush missing key fail")
	}

	output = ts.ProcessCommand("exec")
	if !output.isValue([]any{nil, nil, nil, nil, nil}) {
		t.Fatal("bpushpopexec exec nils fail")
	}
}

func TestRedisBPushPopExecNonNils(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// make a test list
	output := ts.ProcessCommand("rpush", "test", "cat", "dog", "fox", "turkey", "hen", "goat", "horse", "mule", "donkey", "bird", "cow", "lamb")
	if !output.isInt(12) {
		t.Fatal("bpushpopexec non-nil start multi fail")
	}

	// start multi
	output = ts.ProcessCommand("multi")
	if !output.isString("OK") {
		t.Fatal("bpushpopexec non-nil start multi fail")
	}

	// blocking list move tests
	output = ts.ProcessCommand("blmove", "test", "dest", "left", "left", "0")
	if !output.isString(strQueued) {
		t.Fatal("bpushpopexec non-nil queue blmove test key fail")
	}

	output = ts.ProcessCommand("blmpop", "0", "1", "test", "left")
	if !output.isString(strQueued) {
		t.Fatal("bpushpopexec non-nil queue blmpop test key fail")
	}

	output = ts.ProcessCommand("blpop", "test", "0")
	if !output.isString(strQueued) {
		t.Fatal("bpushpopexec non-nil queue blpop test key fail")
	}

	output = ts.ProcessCommand("brpop", "test", "0")
	if !output.isString(strQueued) {
		t.Fatal("bpushpopexec non-nil queue brpop test key fail")
	}

	output = ts.ProcessCommand("brpoplpush", "test", "dest", "0")
	if !output.isString(strQueued) {
		t.Fatal("bpushpopexec non-nil queue brpoplpush test key fail")
	}

	output = ts.ProcessCommand("exec")
	if !output.isValue([]any{"cat", []any{"test", []any{"dog"}}, []any{"test", "fox"}, []any{"test", "lamb"}, "cow"}) {
		t.Fatal("bpushpopexec non-nil exec nils fail")
	}
}
