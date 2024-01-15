package redisemu

import (
	"context"
	"math"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestRespBlobString(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "$11\r\nhello world\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 18 {
		t.Error("byte count is wrong")
	}
	str, valid := v.toString()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if str != "hello world" {
		t.Error("string is wrong")
	}
	if v.isErrorType() {
		t.Error("string is error")
	}

	content = "$11\r\nhelloworld\r\n"

	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("deserialize valid is true")
	}
}

func TestRespSimpleString(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "+hello world\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 14 {
		t.Error("byte count is wrong")
	}
	str, valid := v.toString()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if str != "hello world" {
		t.Error("string is wrong")
	}
}

func TestRespSimpleErrorString(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "-hello world\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 14 {
		t.Error("byte count is wrong")
	}
	str, valid := v.toString()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if str != "hello world" {
		t.Error("string is wrong")
	}

	if !v.isErrorType() {
		t.Error("string is not an error")
	}
}

func TestRespNumber(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := ":1234\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 7 {
		t.Error("byte count is wrong")
	}
	val, valid := v.toInt()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if val != 1234 {
		t.Error("number is wrong")
	}

	content = ":-1234\r\n"

	d = newRespDeserializer(l, []byte(content))
	v, n, valid = d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 8 {
		t.Error("byte count is wrong")
	}
	val, valid = v.toInt()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if val != -1234 {
		t.Error("number is wrong")
	}
}

func TestRespNull(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "_\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 3 {
		t.Error("byte count is wrong")
	}
	if !v.isNull() {
		t.Error("value is not null")
	}
}

func TestRespDouble(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := ",123.4\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 8 {
		t.Error("byte count is wrong")
	}
	val, valid := v.toFloat()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if val != 123.4 {
		t.Error("number is wrong")
	}

	content = ",-123.4\r\n"

	d = newRespDeserializer(l, []byte(content))
	v, n, valid = d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 9 {
		t.Error("byte count is wrong")
	}
	val, valid = v.toFloat()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if val != -123.4 {
		t.Error("number is wrong")
	}

	content = ",inf\r\n"

	d = newRespDeserializer(l, []byte(content))
	v, n, valid = d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 6 {
		t.Error("byte count is wrong")
	}
	val, valid = v.toFloat()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if val != math.Inf(1) {
		t.Error("number is wrong")
	}

	content = ",-inf\r\n"

	d = newRespDeserializer(l, []byte(content))
	v, n, valid = d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 7 {
		t.Error("byte count is wrong")
	}
	val, valid = v.toFloat()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if val != math.Inf(-1) {
		t.Error("number is wrong")
	}
}

func TestRespBoolean(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "#t\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 4 {
		t.Error("byte count is wrong")
	}
	val, valid := v.toBool()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if !val {
		t.Error("true is wrong")
	}

	content = "#f\r\n"

	d = newRespDeserializer(l, []byte(content))
	v, n, valid = d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 4 {
		t.Error("byte count is wrong")
	}
	val, valid = v.toBool()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if val {
		t.Error("false is wrong")
	}

	content = "#z\r\n"

	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("deserialize valid is true")
	}
}

func TestRespBlobError(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "!21\r\nSYNTAX invalid syntax\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 28 {
		t.Error("byte count is wrong")
	}
	str, valid := v.toString()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if str != "SYNTAX invalid syntax" {
		t.Error("string is wrong")
	}
	if !v.isErrorType() {
		t.Error("string is not error")
	}

	content = "!11\r\nSYNTAX invalid syntax\r\n"

	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("deserialize valid is true")
	}
}

func TestRespVerbatimString(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "=15\r\ntxt:Some string\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 22 {
		t.Error("byte count is wrong")
	}
	str, valid := v.toString()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if str != "Some string" {
		t.Error("string is wrong")
	}
	ver, valid := v.data.(respVerbatimString)
	if !valid {
		t.Error("string is not verbatim")
	}
	if ver.format != "txt" {
		t.Error("format is not txt")
	}
	if ver.text != "Some string" {
		t.Error("string is wrong")
	}
}

func TestRespBigNumber(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "(3492890328409238509324850943850943825024385\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 46 {
		t.Error("byte count is wrong")
	}
	bn, valid := v.toBigNumber()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if bn.String() != "3492890328409238509324850943850943825024385" {
		t.Error("string is wrong")
	}

	content = "(-3492890328409238509324850943850943825024385\r\n"

	d = newRespDeserializer(l, []byte(content))
	v, n, valid = d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != 47 {
		t.Error("byte count is wrong")
	}
	bn, valid = v.toBigNumber()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if bn.String() != "-3492890328409238509324850943850943825024385" {
		t.Error("string is wrong")
	}
}

func TestRespArray(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "*3\r\n:1\r\n:2\r\n:3\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	a, valid := v.data.(respArray)
	if !valid {
		t.Error("data is not an array")
	}
	if len(a) != 3 {
		t.Error("array length is not 3")
	}
	if !v.isValue([]any{1, 2, 3}) {
		t.Error("array is wrong")
	}

	content = "*3\r\n:1\r\n*1\r\n:10\r\n:3\r\n"

	d = newRespDeserializer(l, []byte(content))
	v, n, valid = d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	a, valid = v.data.(respArray)
	if !valid {
		t.Error("data is not an array")
	}
	if len(a) != 3 {
		t.Error("array length is not 3")
	}
	if !v.isValue([]any{1, []any{10}, 3}) {
		t.Error("array is wrong")
	}
}

func TestRespMap(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	m, valid := v.data.(respMap)
	if !valid {
		t.Error("data is not a map")
	}
	if len(m.m) != 2 {
		t.Error("map length is not 2")
	}
	if !v.isValue(map[any]any{"first": 1, "second": 2}) {
		t.Error("map is wrong")
	}
}

func TestRespSet(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "~5\r\n+orange\r\n+apple\r\n#t\r\n:100\r\n:999\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	s, valid := v.data.(respSet)
	if !valid {
		t.Error("data is not a set")
	}
	if len(s) != 5 {
		t.Error("set length is not 5")
	}
	if !v.isValue([]any{"orange", "apple", true, 100, 999}) {
		t.Error("set is wrong")
	}
}

func TestRespAttribute(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	am, valid := v.data.(respAttributeMap)
	if !valid {
		t.Error("data is not an attribute map")
	}
	if len(am) != 1 {
		t.Error("map length is not 1")
	}
	if !v.isValue(map[any]any{"key-popularity": map[any]any{"a": 0.1923, "b": 0.0012}}) {
		t.Error("map is wrong")
	}
}

func TestRespPush(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := ">4\r\n+pubsub\r\n+message\r\n+somechannel\r\n+this is the message\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	p, valid := v.data.(respPush)
	if !valid {
		t.Error("data is not a push struct")
	}
	if p.kind != "pubsub" {
		t.Error("push struct is not pubsub")
	}

	expected := []any{"message", "somechannel", "this is the message"}
	for idx, e := range expected {
		if !p.data[idx].isValue(e) {
			t.Error("push args are wrong")
		}
	}

	content = ">4\r\n$6\r\npubsub\r\n+message\r\n+somechannel\r\n+this is the message\r\n"

	d = newRespDeserializer(l, []byte(content))
	v, n, valid = d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	p, valid = v.data.(respPush)
	if !valid {
		t.Error("data is not a push struct")
	}
	if p.kind != "pubsub" {
		t.Error("push struct is not pubsub")
	}

	for idx, e := range expected {
		if !p.data[idx].isValue(e) {
			t.Error("push args are wrong")
		}
	}
}

func TestRespStreamedString(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "$?\r\n;4\r\nHell\r\n;5\r\no wor\r\n;2\r\nld\r\n;0\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	str, valid := v.data.(respBulkString)
	if !valid {
		t.Error("data is not a string")
	}
	if str != "Hello world" {
		t.Error("string is not correct")
	}
}

func TestRespStreamedArray(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "*?\r\n:1\r\n:2\r\n:3\r\n.\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	a, valid := v.data.(respArray)
	if !valid {
		t.Error("data is not an array")
	}
	if len(a) != 3 {
		t.Error("data is not the right length")
	}
	if !v.isValue([]any{1, 2, 3}) {
		t.Error("array is not correct")
	}
}

func TestRespStreamedMap(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "%?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	m, valid := v.data.(respMap)
	if !valid {
		t.Error("data is not a map")
	}
	if len(m.m) != 2 {
		t.Error("data is not the right length")
	}
	if !v.isValue(map[any]any{"a": 1, "b": 2}) {
		t.Error("map is not correct")
	}
}

func TestRespStreamedSet(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "~?\r\n:1\r\n:2\r\n:3\r\n.\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	s, valid := v.data.(respSet)
	if !valid {
		t.Error("data is not a set")
	}
	if len(s) != 3 {
		t.Error("data is not the right length")
	}
	if !v.isValue([]any{1, 2, 3}) {
		t.Error("set is not correct")
	}
}

func TestRespStreamedAttributes(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "|?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	m, valid := v.data.(respAttributeMap)
	if !valid {
		t.Error("data is not a map")
	}
	if len(m) != 2 {
		t.Error("data is not the right length")
	}
	if !v.isValue(map[any]any{"a": 1, "b": 2}) {
		t.Error("map is not correct")
	}
}

func TestRespStreamedErrorString(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "!?\r\n;4\r\nHell\r\n;5\r\no wor\r\n;2\r\nld\r\n;0\r\n"

	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("deserialize valid is false")
	}
	if n != len(content) {
		t.Error("byte count is wrong")
	}
	str, valid := v.data.(respBlobError)
	if !valid {
		t.Error("data is not an error string")
	}
	if str != "Hello world" {
		t.Error("error string is not correct")
	}
}

func TestResp2Null(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "$-1\r\n"
	d := newRespDeserializer(l, []byte(content))
	v, n, valid := d.deserializeNext()
	if !valid {
		t.Error("null string is invalid")
	}
	if n != len(content) {
		t.Error("content length is invalid")
	}
	if v.data != nil {
		t.Error("value is not nil")
	}

	content = "*-1\r\n"
	d = newRespDeserializer(l, []byte(content))
	v, n, valid = d.deserializeNext()
	if !valid {
		t.Error("null string is invalid")
	}
	if n != len(content) {
		t.Error("content length is invalid")
	}
	if v.data != nil {
		t.Error("value is not nil")
	}
}

func TestRespInvalid(t *testing.T) {
	l := lane.NewLogLane(context.Background())

	content := "$11\r\nhello world\n"
	d := newRespDeserializer(l, []byte(content))
	_, _, valid := d.deserializeNext()
	if valid {
		t.Error("missing cr is valid")
	}

	content = "$10\r\nhello world\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("wrong string length is valid")
	}

	content = "$x\r\n\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid string length is valid")
	}

	content = "$10\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("string missing data valid")
	}

	content = "+"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("missing line ending is valid")
	}

	content = "-"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("missing error line ending is valid")
	}

	content = "$?\r\n;4\r\nsome\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("incomplete string chunks valid")
	}

	content = "$?\r\ninvalid\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid chunk valid")
	}

	content = "$?\r\n;x\r\nsome\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid chunk count valid")
	}

	content = "$?\r\n;-1\r\nsome\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("negative chunk count valid")
	}

	content = "$?\r\n+some\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("bad chunk type valid")
	}

	content = ":z\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid int valid")
	}

	content = "*?\r\n;4\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid dynamic array item valid")
	}

	content = "*2\r\n+one\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("missing array item valid")
	}

	content = "*1\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("end used in fixed array valid")
	}

	content = "*x\r\n\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid array item count valid")
	}

	content = "%?\r\n+one\r\n:10\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("missing map terminator valid")
	}

	content = "%?\r\n+one\r\ninvalid\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("streamed map invalid value valid")
	}

	content = "%?\r\n+one\r\n.\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("streamed map end as value valid")
	}

	content = "%2\r\n+one\r\n:10\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("missing map pair valid")
	}

	content = "%1\r\n.\r\n:10\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("end used as map key valid")
	}

	content = "%1\r\n:10\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("end used as map value valid")
	}

	content = "%x\r\n\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid map pair count valid")
	}

	content = "%-1\r\n+one\r\n:10\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid map pair count valid")
	}

	content = "%1\r\n+one\r\ninvalid\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("map pair invalid value valid")
	}

	content = "%1\r\ninvalid\r\n+value\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("map pair invalid key valid")
	}

	content = ",x\r\n\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid float valid")
	}

	content = "#x\r\n\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid bool valid")
	}

	content = "~x\r\n\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid set count valid")
	}

	content = "~-1\r\n+a\r\n+b\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("negative set count valid")
	}

	content = "~?\r\n+a\r\n+b\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("unterminated set valid")
	}

	content = "~1\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("end used as set value valid")
	}

	content = "~3\r\n+a\r\n+b\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("set missing data valid")
	}

	content = "!x\r\n\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid binary string count valid")
	}

	content = "!-1\r\none\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("negative binary string count valid")
	}

	content = "!1\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("binary string missing data valid")
	}

	content = "!?\r\n;3\r\none\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("bad binary string count valid")
	}

	content = "=-1\r\nexample\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("bad verbatim string count -1 valid")
	}

	content = "=x\r\n\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid verbatim string count valid")
	}

	content = "=1\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("verbatim missing string valid")
	}

	content = "=7\r\nexample\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("missing format type valid")
	}

	content = "=7\r\nex:mple\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("missing format type valid")
	}

	content = "=3\r\nex:\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("missing format type valid")
	}

	content = "|?\r\n+one\r\n:10\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("missing attribute map terminator valid")
	}

	content = "|2\r\n+one\r\n:10\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("missing attribute map pair valid")
	}

	content = "|1\r\n.\r\n:10\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("end used as attribute key valid")
	}

	content = "|1\r\n:10\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("end used as attribute value valid")
	}

	content = "|x\r\n\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid attribute map pair count valid")
	}

	content = "|-1\r\n+one\r\n:10\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid attribute map pair count valid")
	}

	content = "|1\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("attribute map pair missing data valid")
	}

	content = "|1\r\n:10\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("attribute map pair missing value valid")
	}

	content = "|1\r\n+one\r\ninvalid\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("attribute map pair invalid value valid")
	}

	content = "|1\r\ninvalid\r\n+value\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("attribute map pair invalid key valid")
	}

	content = "|?\r\n+one\r\ninvalid\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("attribute map invalid value valid")
	}

	content = "|?\r\n+one\r\n.\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("attribute map end as value valid")
	}

	content = ">4\r\n+pubsub\r\n+message\r\n+somechannel\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("push missing data valid")
	}

	content = ">-1\r\n+pubsub\r\n+message\r\n+somechannel\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("push negative count valid")
	}

	content = ">x\r\n\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("push invalid count valid")
	}

	content = ">0\r\n\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("push zero count valid")
	}

	content = ">1\r\ninvalid\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("invalid push type valid")
	}

	content = ">2\r\n+pubsub\r\ninvalid\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("push invalid data valid")
	}

	content = ">2\r\n:10\r\n+message\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("push invalid kind valid")
	}

	content = ">2\r\n.\r\n+message\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("push end used as kind valid")
	}

	content = ">2\r\n+pubsub\r\n.\r\n"
	d = newRespDeserializer(l, []byte(content))
	_, _, valid = d.deserializeNext()
	if valid {
		t.Error("push end used as value valid")
	}
}
