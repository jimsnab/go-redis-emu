package goredisemu

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestHSet(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing pair test
	output := ts.ProcessCommand("hset", "k1")
	if !output.isErrorType() {
		t.Fatal("hset missing pair fail")
	}

	output = ts.ProcessCommand("hset", "k1", "x")
	if !output.isErrorType() {
		t.Fatal("hset missing pair value fail")
	}

	// basic pair
	output = ts.ProcessCommand("hset", "k1", "field1", "Hello")
	if !output.isInt(1) {
		t.Fatal("hset basic set fail")
	}

	output = ts.ProcessCommand("hget", "k1", "field1")
	if !output.isString("Hello") {
		t.Fatal("hset basic get fail")
	}

	// missing second pair value
	output = ts.ProcessCommand("hset", "k1", "field1", "Hello", "field2")
	if !output.isErrorType() {
		t.Fatal("hset missing second pair value fail")
	}

	// two pairs - to make three
	output = ts.ProcessCommand("hset", "k1", "field2", "Hi", "field3", "World")
	if !output.isInt(2) {
		t.Fatal("hset add two pairs fail")
	}

	output = ts.ProcessCommand("hget", "k1", "field2")
	if !output.isString("Hi") {
		t.Fatal("hset get pair 2 fail")
	}

	output = ts.ProcessCommand("hget", "k1", "field3")
	if !output.isString("World") {
		t.Fatal("hset get pair 2 fail")
	}

	// two pairs - new key
	output = ts.ProcessCommand("hset", "k2", "data1", "Hi", "data2", "World")
	if !output.isInt(2) {
		t.Fatal("hset two pairs only fail")
	}

	output = ts.ProcessCommand("hget", "k2", "data1")
	if !output.isString("Hi") {
		t.Fatal("hset get data1 fail")
	}

	output = ts.ProcessCommand("hget", "k2", "data2")
	if !output.isString("World") {
		t.Fatal("hset get data2 fail")
	}

	output = ts.ProcessCommand("hget", "k2", "data3")
	if !output.isNull() {
		t.Fatal("hset get data3 fail")
	}

	// replace value
	output = ts.ProcessCommand("hset", "k2", "data1", "New")
	if !output.isInt(0) {
		t.Fatal("hset replace data1 fail")
	}

	output = ts.ProcessCommand("hget", "k2", "data1")
	if !output.isString("New") {
		t.Fatal("hset data1 replaced fail")
	}

	output = ts.ProcessCommand("hset", "k2", "data1", "Different", "data3", "Additional")
	if !output.isInt(1) {
		t.Fatal("hset replace data1 and add data3 fail")
	}

	output = ts.ProcessCommand("hget", "k2", "data1")
	if !output.isString("Different") {
		t.Fatal("hset data1 replaced and data3 added fail")
	}

	output = ts.ProcessCommand("hget", "k2", "data3")
	if !output.isString("Additional") {
		t.Fatal("hset data3 added fail")
	}

	// empty key/value
	output = ts.ProcessCommand("hset", "k3", "", "")
	if !output.isInt(1) {
		t.Fatal("hset add empty string pair fail")
	}

	output = ts.ProcessCommand("hget", "k3", "")
	if !output.isString("") {
		t.Fatal("hset get empty fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hset prepare list fail")
	}

	output = ts.ProcessCommand("hset", "list", "cat", "meow")
	if !output.isErrorType() {
		t.Fatal("hset set pair into a list fail")
	}
}

func TestHGet(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// argument test
	output := ts.ProcessCommand("hget", "missing")
	if !output.isErrorType() {
		t.Fatal("hget missing field arg fail")
	}

	// missing key test
	output = ts.ProcessCommand("hget", "missing", "cat")
	if !output.isNull() {
		t.Fatal("hget missing key fail")
	}

	// missing pair test
	output = ts.ProcessCommand("hset", "k1", "cat", "meow")
	if !output.isInt(1) {
		t.Fatal("hget create first table fail")
	}

	output = ts.ProcessCommand("hget", "k1", "dog")
	if !output.isNull() {
		t.Fatal("hget get missing field fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hget prepare list fail")
	}

	output = ts.ProcessCommand("hget", "list", "cat")
	if !output.isErrorType() {
		t.Fatal("hget get pair from a list fail")
	}
}

func TestHGetAll(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key test
	output := ts.ProcessCommand("hgetall", "missing")
	if !output.isArraySet() {
		t.Fatal("hgetall missing key fail")
	}

	// basic pair
	output = ts.ProcessCommand("hset", "k1", "field1", "Hello")
	if !output.isInt(1) {
		t.Fatal("hgetall basic set fail")
	}

	output = ts.ProcessCommand("hgetall", "k1")
	if !output.isArrayMap(map[any]any{"field1": "Hello"}) {
		t.Fatal("hgetall basic getall fail")
	}

	// more pairs
	output = ts.ProcessCommand("hset", "k1", "field2", "Hi")
	if !output.isInt(1) {
		t.Fatal("hgetall set field2 fail")
	}

	output = ts.ProcessCommand("hgetall", "k1")
	if !output.isArrayMap(map[any]any{"field1": "Hello", "field2": "Hi"}) {
		t.Fatal("hgetall getall 2 fail")
	}

	output = ts.ProcessCommand("hset", "k1", "field3", "World")
	if !output.isInt(1) {
		t.Fatal("hgetall set field3 fail")
	}

	output = ts.ProcessCommand("hgetall", "k1")
	if !output.isArrayMap(map[any]any{"field1": "Hello", "field2": "Hi", "field3": "World"}) {
		t.Fatal("hgetall getall 2 fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hgetall prepare list fail")
	}

	output = ts.ProcessCommand("hgetall", "list")
	if !output.isErrorType() {
		t.Fatal("hgetall get pairs from a list fail")
	}
}

func TestHDel(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing arg
	output := ts.ProcessCommand("hdel", "missing")
	if !output.isErrorType() {
		t.Fatal("hdel missing arg fail")
	}

	// missing key
	output = ts.ProcessCommand("hdel", "missing", "field1")
	if !output.isInt(0) {
		t.Fatal("hdel missing field fail")
	}

	// last field
	output = ts.ProcessCommand("hset", "k1", "field1", "first")
	if !output.isInt(1) {
		t.Fatal("hdel set field1 fail")
	}

	output = ts.ProcessCommand("hdel", "k1", "field1")
	if !output.isInt(1) {
		t.Fatal("hdel remove field1 to empty fail")
	}

	output = ts.ProcessCommand("lpush", "k1", "field1")
	if !output.isInt(1) {
		t.Fatal("hdel make k1 a list fail")
	}

	// one field in table of two
	output = ts.ProcessCommand("hset", "k2", "field1", "first", "field2", "second")
	if !output.isInt(2) {
		t.Fatal("hdel set field1 & field2 fail")
	}

	output = ts.ProcessCommand("hdel", "k2", "field1")
	if !output.isInt(1) {
		t.Fatal("hdel remove field1 once fail")
	}

	output = ts.ProcessCommand("hdel", "k2", "field1")
	if !output.isInt(0) {
		t.Fatal("hdel remove field1 twice fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hdel prepare list fail")
	}

	output = ts.ProcessCommand("hdel", "list", "field1")
	if !output.isErrorType() {
		t.Fatal("hdel del pairs from a list fail")
	}
}

func TestHExists(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing arg
	output := ts.ProcessCommand("hexists", "missing")
	if !output.isErrorType() {
		t.Fatal("hexists missing arg fail")
	}

	// missing key
	output = ts.ProcessCommand("hexists", "missing", "field1")
	if !output.isInt(0) {
		t.Fatal("hexists missing field fail")
	}

	// one field
	output = ts.ProcessCommand("hset", "k1", "field1", "first")
	if !output.isInt(1) {
		t.Fatal("hexists set field1 fail")
	}

	output = ts.ProcessCommand("hexists", "k1", "field1")
	if !output.isInt(1) {
		t.Fatal("hexists field1 exists fail")
	}

	output = ts.ProcessCommand("hexists", "k1", "field2")
	if !output.isInt(0) {
		t.Fatal("hexists field2 exists fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hexists prepare list fail")
	}

	output = ts.ProcessCommand("hexists", "list", "field1")
	if !output.isErrorType() {
		t.Fatal("hexists exists from a list fail")
	}
}

func TestHIncrBy(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing arg
	output := ts.ProcessCommand("hincrby", "missing")
	if !output.isErrorType() {
		t.Fatal("hincrby missing arg fail")
	}

	output = ts.ProcessCommand("hincrby", "missing", "field1")
	if !output.isErrorType() {
		t.Fatal("hincrby missing increment fail")
	}

	// missing key is created
	output = ts.ProcessCommand("hincrby", "newkey", "field1", "1")
	if !output.isInt(1) {
		t.Fatal("hincrby missing key increment fail")
	}

	output = ts.ProcessCommand("hget", "newkey", "field1")
	if !output.isString("1") {
		t.Fatal("hincrby missing key created verify fail")
	}

	// max positive
	output = ts.ProcessCommand("hincrby", "test", "positive", fmt.Sprintf("%d", math.MaxInt64))
	if !output.isInt64(math.MaxInt64) {
		t.Fatal("hincrby create max int fail")
	}

	output = ts.ProcessCommand("hincrby", "test", "positive", "1")
	if !output.isErrorType() { // "ERR increment or decrement would overflow"
		t.Fatal("hincrby positive max increment fail")
	}

	// max negatve
	highBit := uint64(0x8000000000000000)
	signed := int64(highBit)
	output = ts.ProcessCommand("hincrby", "test", "negative", fmt.Sprintf("%d", signed))
	if !output.isInt64(signed) {
		t.Fatal("hincrby create -max int fail")
	}

	output = ts.ProcessCommand("hincrby", "test", "negative", "-1")
	if !output.isErrorType() { // "ERR increment or decrement would overflow"
		t.Fatal("hincrby negative max increment fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hincrby prepare list fail")
	}

	output = ts.ProcessCommand("hincrby", "list", "field1", "1")
	if !output.isErrorType() {
		t.Fatal("hincrby increment a list fail")
	}
}

func TestHIncrByFloat(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing arg
	output := ts.ProcessCommand("hincrbyfloat", "missing")
	if !output.isErrorType() {
		t.Fatal("hincrbyfloat missing arg fail")
	}

	output = ts.ProcessCommand("hincrbyfloat", "missing", "field1")
	if !output.isErrorType() {
		t.Fatal("hincrbyfloat missing increment fail")
	}

	// missing key is created
	output = ts.ProcessCommand("hincrbyfloat", "newkey", "field1", "1.0")
	if !output.isFloat(1.0, -1) {
		t.Fatal("hincrbyfloat missing key increment fail")
	}

	output = ts.ProcessCommand("hget", "newkey", "field1")
	if !output.isString("1") {
		t.Fatal("hincrbyfloat missing key created verify fail")
	}

	// max positive
	str := strconv.FormatFloat(math.MaxFloat64, 'f', -1, 64)
	output = ts.ProcessCommand("hincrbyfloat", "test", "positive", str)
	if !output.isFloat(math.MaxFloat64, -1) {
		t.Fatal("hincrbyfloat create max float fail")
	}

	output = ts.ProcessCommand("hincrbyfloat", "test", "positive", str)

	_, isTestClient := ts.(*testClient)
	if isTestClient {
		if !output.isErrorType() { // "ERR increment would produce NaN or Infinity"
			t.Fatal("hincrbyfloat create max float fail")
		}
	} else {
		// real redis range goes beyond the documented 64-bit float
		str, ok := output.toString()
		if !ok || !strings.HasPrefix(str, "3595386269724631") {
			t.Fatal("hincrbyfloat create max float fail")
		}
	}

	output = ts.ProcessCommand("hincrbyfloat", "test", "positive", "INFINITY")
	if !output.isErrorType() { // "ERR increment would produce NaN or Infinity"
		t.Fatal("hincrbyfloat positive max increment fail")
	}

	// max negatve
	str = strconv.FormatFloat(-math.MaxFloat64, 'f', -1, 64)
	output = ts.ProcessCommand("hincrbyfloat", "test", "negative", str)
	if !output.isFloat(-math.MaxFloat64, -1) {
		t.Fatal("hincrbyfloat create -max float fail")
	}

	output = ts.ProcessCommand("hincrbyfloat", "test", "negative", str)
	if isTestClient {
		if !output.isErrorType() { // "ERR increment would produce NaN or Infinity"
			t.Fatal("hincrbyfloat create max float fail")
		}
	} else {
		// real redis range goes beyond the documented 64-bit float
		str, ok := output.toString()
		if !ok || !strings.HasPrefix(str, "-3595386269724631") {
			t.Fatal("hincrbyfloat negative max increment fail")
		}
	}

	output = ts.ProcessCommand("hincrbyfloat", "test", "negative", "-INFINITY")
	if !output.isErrorType() { // "ERR increment would produce NaN or Infinity"
		t.Fatal("hincrbyfloat negative max increment fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hincrbyfloat prepare list fail")
	}

	output = ts.ProcessCommand("hincrbyfloat", "list", "field1", "1")
	if !output.isErrorType() {
		t.Fatal("hincrbyfloat increment a list fail")
	}
}

func TestHKeys(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("hkeys", "missing")
	if !output.isArray() {
		t.Fatal("hkeys missing key fail")
	}

	// single key
	ts.ProcessCommand("hset", "single", "field1", "value1")
	output = ts.ProcessCommand("hkeys", "single")
	if !output.isArray("field1") {
		t.Fatal("hkeys single key fail")
	}

	// multiple keys
	ts.ProcessCommand("hset", "multiple", "field1", "value1")
	ts.ProcessCommand("hset", "multiple", "field2", "value2")
	ts.ProcessCommand("hset", "multiple", "field3", "value3")
	output = ts.ProcessCommand("hkeys", "multiple")
	if !output.isArrayInSet(3, "field1", "field2", "field3") {
		t.Fatal("hkeys multiple key fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hkeys prepare list fail")
	}

	output = ts.ProcessCommand("hkeys", "list")
	if !output.isErrorType() {
		t.Fatal("hkeys get from a list fail")
	}
}

func TestHLen(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("hlen", "missing")
	if !output.isInt(0) {
		t.Fatal("hlen missing key fail")
	}

	// empty key
	ts.ProcessCommand("hset", "empty", "field1", "value1")
	output = ts.ProcessCommand("hlen", "empty")
	if !output.isInt(1) {
		t.Fatal("hlen empty key fail")
	}

	// multiple keys
	ts.ProcessCommand("hset", "multiple", "field1", "value1")
	ts.ProcessCommand("hset", "multiple", "field2", "value2")
	ts.ProcessCommand("hset", "multiple", "field3", "value3")
	output = ts.ProcessCommand("hlen", "multiple")
	if !output.isInt(3) {
		t.Fatal("hlen multiple key fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hlen prepare list fail")
	}

	output = ts.ProcessCommand("hlen", "list")
	if !output.isErrorType() {
		t.Fatal("hlen length of a list fail")
	}
}

func TestHMGet(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("hmget", "missing", "field1", "field2")
	if !output.isArray(nil, nil) {
		t.Fatal("hmget missing key fail")
	}

	// empty key
	ts.ProcessCommand("hset", "empty", "field1", "value1")
	output = ts.ProcessCommand("hmget", "empty", "field1")
	if !output.isArray("value1") {
		t.Fatal("hmget empty key fail")
	}

	// multiple keys
	ts.ProcessCommand("hset", "multiple", "field1", "value1")
	ts.ProcessCommand("hset", "multiple", "field2", "value2")
	ts.ProcessCommand("hset", "multiple", "field3", "value3")
	output = ts.ProcessCommand("hmget", "multiple", "field1", "field2", "field3")
	if !output.isArray("value1", "value2", "value3") {
		t.Fatal("hmget multiple key fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hmget prepare list fail")
	}

	output = ts.ProcessCommand("hmget", "list", "field1", "field2")
	if !output.isErrorType() {
		t.Fatal("hmget get from a list fail")
	}
}

func TestHMSet(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// new key
	output := ts.ProcessCommand("hmset", "newkey", "field1", "value1", "field2", "value2")
	if !output.isString("OK") {
		t.Fatal("hmset new key fail")
	}

	// check set fields
	output = ts.ProcessCommand("hmget", "newkey", "field1", "field2")
	if !output.isArray("value1", "value2") {
		t.Fatal("hmset check fields fail")
	}

	// update key
	output = ts.ProcessCommand("hmset", "newkey", "field1", "newvalue1", "field2", "newvalue2")
	if !output.isString("OK") {
		t.Fatal("hmset update key fail")
	}

	// check updated fields
	output = ts.ProcessCommand("hmget", "newkey", "field1", "field2")
	if !output.isArray("newvalue1", "newvalue2") {
		t.Fatal("hmset check updated fields fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("mset prepare list fail")
	}

	output = ts.ProcessCommand("mset", "list", "field1", "1", "field2", "x")
	if !output.isErrorType() {
		t.Fatal("mset set in a list fail")
	}
}

func TestHRandField(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("hrandfield", "missing")
	if !output.isNull() {
		t.Fatal("hrandfield missing key fail")
	}

	// empty key
	ts.ProcessCommand("hset", "empty", "field1", "value1")
	output = ts.ProcessCommand("hrandfield", "empty")
	if !output.isString("field1") {
		t.Fatal("hrandfield empty key fail")
	}

	// multiple keys
	ts.ProcessCommand("hset", "multiple", "field1", "value1")
	ts.ProcessCommand("hset", "multiple", "field2", "value2")
	ts.ProcessCommand("hset", "multiple", "field3", "value3")
	output = ts.ProcessCommand("hrandfield", "multiple")
	if !output.isOneOf("field1", "field2", "field3") {
		t.Fatal("hrandfield multiple key fail")
	}

	// positive count
	output = ts.ProcessCommand("hrandfield", "multiple", "0")
	if !output.isArrayInSet(0) {
		t.Fatal("hrandfield positive count 0 fail")
	}

	output = ts.ProcessCommand("hrandfield", "multiple", "1")
	if !output.isArrayInSet(1, "field1", "field2", "field3") {
		t.Fatal("hrandfield positive count 1 fail")
	}

	output = ts.ProcessCommand("hrandfield", "multiple", "2")
	if !output.isArrayInSet(2, "field1", "field2", "field3") {
		t.Fatal("hrandfield positive count 2 fail")
	}

	output = ts.ProcessCommand("hrandfield", "multiple", "4")
	if !output.isArrayInSet(3, "field1", "field2", "field3") {
		t.Fatal("hrandfield positive count 4 fail")
	}

	// negative count
	output = ts.ProcessCommand("hrandfield", "multiple", "-1")
	if !output.isArrayInSet(1, "field1", "field2", "field3") {
		t.Fatal("hrandfield negative count -1 fail")
	}

	output = ts.ProcessCommand("hrandfield", "multiple", "-2")
	if !output.isArrayInSet(2, "field1", "field2", "field3") {
		t.Fatal("hrandfield negative count -2 fail")
	}

	output = ts.ProcessCommand("hrandfield", "multiple", "-4")
	if !output.isArrayInSet(4, "field1", "field2", "field3") {
		t.Fatal("hrandfield negative count -4 fail")
	}

	// values
	output = ts.ProcessCommand("hrandfield", "multiple", "3", "withvalues")
	if !output.isArrayMap(map[any]any{"field1": "value1", "field2": "value2", "field3": "value3"}) {
		t.Fatal("hrandfield withvalues full table fail")
	}

	output = ts.ProcessCommand("hrandfield", "multiple", "-3", "withvalues")
	if !output.isArrayInMap(3, map[any]any{"field1": "value1", "field2": "value2", "field3": "value3"}) {
		t.Fatal("hrandfield withvalues full table fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hrandfield prepare list fail")
	}

	output = ts.ProcessCommand("hrandfield", "list", "field1", "field2")
	if !output.isErrorType() {
		t.Fatal("hrandfield from a list fail")
	}
}

func TestHScan(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("hscan", "missing", "0")
	if !output.isArray("0", []any{}) {
		t.Fatal("hscan missing key fail")
	}

	// single key
	ts.ProcessCommand("hset", "single", "field1", "value1")
	output = ts.ProcessCommand("hscan", "single", "0")
	if !output.isArray("0", []any{"field1", "value1"}) {
		t.Fatal("hscan single key fail")
	}

	// multiple keys
	ts.ProcessCommand("hset", "multiple", "field1", "value1")
	ts.ProcessCommand("hset", "multiple", "field2", "value2")
	ts.ProcessCommand("hset", "multiple", "field3", "value3")
	output = ts.ProcessCommand("hscan", "multiple", "0")
	a, ok := output.toArray()
	if !ok || len(a) != 2 {
		t.Fatal("hscan multiple keys return array fail")
	}
	str, ok := a[0].toString()
	if !ok || str != "0" {
		t.Fatal("hscan multiple keys cursor fail")
	}
	if !a[1].isArrayInMap(3, map[any]any{"field1": "value1", "field2": "value2", "field3": "value3"}) {
		t.Fatal("hscan multiple key fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hscan prepare list fail")
	}

	output = ts.ProcessCommand("hscan", "list", "0")
	if !output.isErrorType() {
		t.Fatal("hscan scan a list fail")
	}
}

func TestHSetNX(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("hsetnx", "missing", "field1", "value1")
	if !output.isInt(1) {
		t.Fatal("hsetnx missing key fail")
	}

	// existing key
	ts.ProcessCommand("hset", "existing", "field1", "value1")
	output = ts.ProcessCommand("hsetnx", "existing", "field1", "value2")
	if !output.isInt(0) {
		t.Fatal("hsetnx existing key fail")
	}

	// existing key, new field
	output = ts.ProcessCommand("hsetnx", "existing", "field2", "value2")
	if !output.isInt(1) {
		t.Fatal("hsetnx existing key new field fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hsetnx prepare list fail")
	}

	output = ts.ProcessCommand("hsetnx", "list", "field1", "1")
	if !output.isErrorType() {
		t.Fatal("hsetnx set into a list fail")
	}
}

func TestHStrLen(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("hstrlen", "missing", "field1")
	if !output.isInt(0) {
		t.Fatal("hstrlen missing key fail")
	}

	// existing key
	ts.ProcessCommand("hset", "existing", "field1", "value1")
	output = ts.ProcessCommand("hstrlen", "existing", "field1")
	if !output.isInt(6) {
		t.Fatal("hstrlen existing key fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hstrlen prepare list fail")
	}

	output = ts.ProcessCommand("hstrlen", "list", "field1")
	if !output.isErrorType() {
		t.Fatal("hstrlen length of an item in a list fail")
	}
}

func TestHVals(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("hvals", "missing")
	if !output.isArray() {
		t.Fatal("hvals missing key fail")
	}

	// empty key
	ts.ProcessCommand("hset", "empty", "field1", "value1")
	output = ts.ProcessCommand("hvals", "empty")
	if !output.isArray("value1") {
		t.Fatal("hvals empty key fail")
	}

	// multiple keys
	ts.ProcessCommand("hset", "multiple", "field1", "value1")
	ts.ProcessCommand("hset", "multiple", "field2", "value2")
	ts.ProcessCommand("hset", "multiple", "field3", "value3")
	output = ts.ProcessCommand("hvals", "multiple")
	if !output.isArrayInSet(3, "value1", "value2", "value3") {
		t.Fatal("hvals multiple key fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("hvals prepare list fail")
	}

	output = ts.ProcessCommand("hvals", "list")
	if !output.isErrorType() {
		t.Fatal("hvals get from a list fail")
	}
}
