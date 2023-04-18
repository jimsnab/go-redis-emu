package redisemu

import (
	"math"
	"math/big"
	"reflect"
	"testing"
)

func TestRespSerializeValues(t *testing.T) {
	rv := respValue{data: respSimpleString("hello")}
	if string(rv.serialize()) != "+hello\r\n" {
		t.Error("simple string serialize fail")
	}

	rv = respValue{data: respBulkString("hello")}
	if string(rv.serialize()) != "$5\r\nhello\r\n" {
		t.Error("bulk string serialize fail")
	}

	rv = respValue{data: respBulkString("")}
	if string(rv.serialize()) != "$0\r\n\r\n" {
		t.Error("bulk string serialize fail")
	}

	rv = respValue{data: respBlobError("error")}
	if string(rv.serialize()) != "!5\r\nerror\r\n" {
		t.Error("blob error string serialize fail")
	}

	rv = respValue{data: respBlobError("")}
	if string(rv.serialize()) != "!0\r\n\r\n" {
		t.Error("blob error string serialize fail")
	}

	rv = respValue{data: respErrorString("error")}
	if string(rv.serialize()) != "-error\r\n" {
		t.Error("error string serialize fail")
	}

	rv = respValue{data: respInt(10)}
	if string(rv.serialize()) != ":10\r\n" {
		t.Error("error int serialize fail")
	}

	rv = respValue{data: respInt(-10)}
	if string(rv.serialize()) != ":-10\r\n" {
		t.Error("error int negative serialize fail")
	}

	rv = respValue{data: respDouble(10.5)}
	if string(rv.serialize()) != ",10.5\r\n" {
		t.Error("error float serialize fail")
	}

	rv = respValue{data: respDouble(-10.5)}
	if string(rv.serialize()) != ",-10.5\r\n" {
		t.Error("error float negative serialize fail")
	}

	rv = respValue{data: respDouble(math.Inf(-1))}
	if string(rv.serialize()) != ",-inf\r\n" {
		t.Error("error float -inf serialize fail")
	}

	rv = respValue{data: respDouble(math.Inf(1))}
	if string(rv.serialize()) != ",inf\r\n" {
		t.Error("error float +inf serialize fail")
	}

	rv = respValue{data: respBool(true)}
	if string(rv.serialize()) != "#t\r\n" {
		t.Error("error float serialize fail")
	}

	rv = respValue{data: respBool(false)}
	if string(rv.serialize()) != "#f\r\n" {
		t.Error("error float negative serialize fail")
	}

	rv = respValue{data: respBigNumber{bn: big.NewInt(20)}}
	if string(rv.serialize()) != "(20\r\n" {
		t.Error("big number fail")
	}

	rv = respValue{data: respVerbatimString{format: "txt", text: "example"}}
	if string(rv.serialize()) != "=11\r\ntxt:example\r\n" {
		t.Error("verbatim string fail")
	}

	rv = respValue{data: respVerbatimString{format: "tx", text: "example"}}
	if string(rv.serialize()) != "=11\r\ntx :example\r\n" {
		t.Error("verbatim string tx fail")
	}

	rv = respValue{data: respVerbatimString{format: "text", text: "example"}}
	if string(rv.serialize()) != "=11\r\ntex:example\r\n" {
		t.Error("verbatim string text fail")
	}

	first := respValue{data: respSimpleString("first")}
	second := respValue{data: respSimpleString("second")}
	ords := []respValue{first, second}

	rv = respValue{data: respPush{kind: "pubsub", data: ords}}
	if string(rv.serialize()) != ">3\r\n+pubsub\r\n+first\r\n+second\r\n" {
		t.Error("push test fail")
	}

	a := respArray{first, second}
	rv = respValue{data: a}
	if string(rv.serialize()) != "*2\r\n+first\r\n+second\r\n" {
		t.Error("array test fail")
	}

	m := respMap{first: second}
	rv = respValue{data: m}
	if string(rv.serialize()) != "%1\r\n+first\r\n+second\r\n" {
		t.Error("map test fail")
	}

	s := respSet{first: struct{}{}, second: struct{}{}}
	rv = respValue{data: s}
	str := string(rv.serialize())
	if str != "~2\r\n+first\r\n+second\r\n" && str != "~2\r\n+second\r\n+first\r\n" {
		t.Error("set test fail")
	}

	am := respAttributeMap{first: second}
	rv = respValue{data: am}
	if string(rv.serialize()) != "|1\r\n+first\r\n+second\r\n" {
		t.Error("attribute map test fail")
	}

	rv = respValue{} // native nil goes to RESP2 null
	if string(rv.serialize()) != "$-1\r\n" {
		t.Error("RESP2 null test fail")
	}

	rv = respValue{data: respNull{}} // RESP3 null
	if string(rv.serialize()) != "_\r\n" {
		t.Error("RESP3 null test fail")
	}
}

func TestRespConversions(t *testing.T) {
	rv := nativeValueToResp("test")
	if !rv.isString("test") {
		t.Error("string conversion fail")
	}

	rv = nativeValueToResp("10.5")
	if !rv.isFloat(10.5, -1) {
		t.Error("string-float conversion fail")
	}

	rv = nativeValueToResp("10.5!")
	if rv.isFloat(10.5, -1) {
		t.Error("string-float invalid conversion fail")
	}

	rv = nativeValueToResp(true)
	if !rv.isBool(true) {
		t.Error("bool conversion fail")
	}

	rv = nativeValueToResp(false)
	if !rv.isBool(false) {
		t.Error("bool conversion fail")
	}

	rv = nativeValueToResp(10)
	if !rv.isInt(10) {
		t.Error("int conversion fail")
	}

	rv = nativeValueToResp(10.5)
	if !rv.isFloat(10.5, -1) {
		t.Error("float conversion fail")
	}

	rv = nativeValueToResp([]any{1, 2})
	if !rv.isArray(1, 2) {
		t.Error("array conversion fail")
	}
	if rv.isFloat(10.5, -1) {
		t.Error("array-float invalid conversion fail")
	}

	rv = nativeValueToResp(map[any]any{1: 2})
	if !rv.isMap(map[any]any{1: 2}) {
		t.Error("map conversion fail")
	}

	rv = nativeValueToResp(map[any]struct{}{1: {}})
	if !rv.isSet(1) {
		t.Error("set conversion fail")
	}

	rv = nativeValueToResp(map[string]string{"test": "val"})
	// targets RESP2, so returns string table as an array
	if !rv.isArray("test", "val") {
		t.Error("string table conversion fail")
	}

	bn := big.NewInt(30)
	rv = nativeValueToResp(bn)
	if !rv.isBigInt(big.NewInt(30)) {
		t.Error("big number conversion fail")
	}
}

func TestRespBadConversions(t *testing.T) {
	rv := respValue{data: respSimpleString("1234")}
	bn, valid := rv.toBigNumber()
	if !valid {
		t.Error("string to big number conversion fail")
	}
	if bn.Cmp(big.NewInt(1234)) != 0 {
		t.Error("big number wrong conversion")
	}

	rv = respValue{data: respBulkString("1234")}
	bn, valid = rv.toBigNumber()
	if !valid {
		t.Error("string to big number conversion fail")
	}
	if bn.Cmp(big.NewInt(1234)) != 0 {
		t.Error("big number wrong conversion")
	}

	rv = respValue{data: respBulkString("x")}
	_, valid = rv.toBigNumber()
	if valid {
		t.Error("bad string to big number conversion fail")
	}

	rv = respValue{data: respBool(true)}
	_, valid = rv.toBigNumber()
	if valid {
		t.Error("bool to big number conversion fail")
	}

	rv = respValue{data: respSimpleString("1234x")}
	_, valid = rv.toInt()
	if valid {
		t.Error("bad string to int64 conversion fail")
	}

	rv = respValue{data: respBool(false)}
	_, valid = rv.toInt()
	if valid {
		t.Error("bool to int64 conversion fail")
	}

	rv = respValue{data: respSimpleString("truee")}
	_, valid = rv.toBool()
	if valid {
		t.Error("bad string to bool conversion fail")
	}

	rv = respValue{data: respSimpleString("12.34x")}
	_, valid = rv.toFloat()
	if valid {
		t.Error("bad string to float64 conversion fail")
	}

	rv = respValue{data: respBool(false)}
	_, valid = rv.toFloat()
	if valid {
		t.Error("bool to float64 conversion fail")
	}
}

func TestRespStringify(t *testing.T) {
	rv := respValue{data: respSimpleString("hello")}
	if rv.String() != "hello" {
		t.Error("simple string stringify fail")
	}

	rv = respValue{data: respBulkString("hello")}
	if rv.String() != "hello" {
		t.Error("bulk string stringify fail")
	}

	rv = respValue{data: respErrorString("error")}
	if rv.String() != "error" {
		t.Error("error string stringify fail")
	}

	rv = respValue{data: respBlobError("error")}
	if rv.String() != "error" {
		t.Error("blob error string stringify fail")
	}

	rv = respValue{data: respInt(-123)}
	if rv.String() != "-123" {
		t.Error("int stringify fail")
	}

	rv = respValue{data: respDouble(12.34)}
	if rv.String() != "12.34" {
		t.Error("double stringify fail")
	}

	rv = respValue{data: respDouble(math.Inf(1))}
	if rv.String() != "inf" {
		t.Error("double inf stringify fail")
	}

	rv = respValue{data: respDouble(math.Inf(-1))}
	if rv.String() != "-inf" {
		t.Error("double -inf stringify fail")
	}

	rv = respValue{data: respBigNumber{bn: big.NewInt(123)}}
	if rv.String() != "123" {
		t.Error("big number stringify fail")
	}

	rv = respValue{}
	if rv.String() != "" {
		t.Error("nil stringify fail")
	}

	rv = respValue{data: respNull{}}
	if rv.String() != "" {
		t.Error("null stringify fail")
	}

	first := respValue{data: respInt(1)}
	second := respValue{data: respSimpleString("2")}

	rv = respValue{data: respArray{first, second}}
	if rv.String() != `[1,"2"]` {
		t.Error("array stringify fail")
	}

	rv = respValue{data: respSet{first: {}, second: {}}}
	str := rv.String()
	if str != `(1,"2")` {
		t.Error("set stringify fail")
	}

	rv = respValue{data: respMap{first: second}}
	str = rv.String()
	if str != `{1:"2"}` {
		t.Error("map stringify fail")
	}

	rv = respValue{data: respMap{first: second, second: first}}
	str = rv.String()
	if str != `{1:"2","2":1}` {
		t.Error("map stringify fail")
	}

	rv = respValue{data: respAttributeMap{first: second}}
	str = rv.String()
	if str != `|1:"2"|` {
		t.Error("attribute map stringify fail")
	}

	rv = respValue{data: respAttributeMap{first: second, second: first}}
	str = rv.String()
	if str != `|1:"2","2":1|` {
		t.Error("attribute map stringify fail")
	}

	rv = respValue{data: respBool(true)}
	if rv.String() != "true" {
		t.Error("true stringify fail")
	}

	rv = respValue{data: respBool(false)}
	if rv.String() != "false" {
		t.Error("false stringify fail")
	}

	rv = respValue{data: respVerbatimString{format: "txt", text: "example"}}
	if rv.String() != "txt:example" {
		t.Error("verbatim stringify fail")
	}

	objs := []respValue{first, second}
	rv = respValue{data: respPush{kind: "pubsub", data: objs}}
	str = rv.String()
	if str != `pubsub->1,"2"` {
		t.Error("push stringify fail")
	}
}

func TestRespTable(t *testing.T) {
	first := respValue{data: respSimpleString("first")}
	second := respValue{data: respInt(2)}

	a := respArray{first}
	_, valid := a.toTable()
	if valid {
		t.Error("odd array count fail")
	}

	a = respArray{second}
	_, valid = a.toTable()
	if valid {
		t.Error("odd array count, wrong type fail")
	}

	a = respArray{respValue{}, second}
	_, valid = a.toTable()
	if valid {
		t.Error("wrong key type fail")
	}

	a = respArray{first, second}
	table, valid := a.toTable()
	if !valid {
		t.Error("valid table fail")
	}
	expected := respValue{data: respInt(2)}
	if table["first"] != expected {
		t.Error("valid table data wrong")
	}

	a = respArray{first, first}
	table, valid = a.toTable()
	if !valid {
		t.Error("valid string table fail")
	}
	_, valid = getTableInt(table, first.String())
	if valid {
		t.Error("get int of a string fail")
	}

	rv := nativeValueToResp("test")
	_, valid = rv.toTable()
	if valid {
		t.Error("string as table fail")
	}
}

func TestRespIsValue(t *testing.T) {
	rv := nativeValueToResp("test")
	if !rv.isValue("test") {
		t.Error("string comparison fail")
	}
	if rv.isValue("testing") {
		t.Error("string false comparison fail")
	}
	if rv.isValue(1) {
		t.Error("string type comparison fail")
	}
	if rv.isValue(map[any]any{"key": "value"}) {
		t.Error("string compare to map fail")
	}
	if rv.isValue(map[any]struct{}{1: {}}) {
		t.Error("string compare to set fail")
	}
	if rv.isValue(1) {
		t.Error("string compare to int fail")
	}
	if rv.isValue(int64(1)) {
		t.Error("string compare to int64 fail")
	}
	if rv.isValue(big.NewInt(1)) {
		t.Error("string compare to big int fail")
	}

	rv = nativeValueToResp("1")
	if !rv.isValue(true) {
		t.Error("string type bool comparison fail")
	}
	if rv.isValue(false) {
		t.Error("string type bool false comparison fail")
	}

	rv = nativeValueToResp(true)
	if !rv.isValue(true) {
		t.Error("bool comparison fail")
	}
	if rv.isValue(false) {
		t.Error("bool false comparison fail")
	}

	rv = nativeValueToResp(false)
	if !rv.isValue(false) {
		t.Error("bool comparison fail")
	}
	if rv.isValue(true) {
		t.Error("bool false comparison fail")
	}
	if rv.isValue([]any{}) {
		t.Error("bool type comparison fail")
	}

	rv = nativeValueToResp(10)
	if !rv.isValue(10) {
		t.Error("int comparison fail")
	}
	if rv.isValue(11) {
		t.Error("int false comparison fail")
	}
	if rv.isValue("test") {
		t.Error("int type comparison fail")
	}
	if rv.isValue(false) {
		t.Error("int type bool false comparison fail")
	}
	if !rv.isValue(true) {
		t.Error("int type bool true comparison fail")
	}

	rv = nativeValueToResp(10.5)
	if !rv.isValue(10.5) {
		t.Error("float comparison fail")
	}
	if rv.isValue(50.5) {
		t.Error("float false comparison fail")
	}
	if rv.isValue("test") {
		t.Error("type false comparison fail")
	}

	rv = nativeValueToResp([]any{1, 2})
	if !rv.isValue([]any{1, 2}) {
		t.Error("array comparison fail")
	}
	if rv.isValue([]any{1}) {
		t.Error("array false comparison fail")
	}
	if rv.isValue([]any{1, 3}) {
		t.Error("array false comparison fail")
	}
	if rv.isValue("test") {
		t.Error("array type comparison fail")
	}
	if rv.isValue(true) {
		t.Error("array type bool comparison fail")
	}
	if rv.isValue(false) {
		t.Error("array type bool false comparison fail")
	}

	rv = nativeValueToResp(map[any]any{1: 2})
	if !rv.isValue(map[any]any{1: 2}) {
		t.Error("map comparison fail")
	}
	if rv.isValue(map[any]any{2: 2}) {
		t.Error("map false comparison fail")
	}
	if rv.isValue(map[any]any{1: 1}) {
		t.Error("map false value comparison fail")
	}
	if rv.isValue(map[any]any{1: 2, 2: 0}) {
		t.Error("map false length comparison fail")
	}
	if rv.isValue("test") {
		t.Error("map type comparison fail")
	}

	rv = nativeValueToResp(map[any]struct{}{1: {}})
	if !rv.isValue(map[any]struct{}{1: {}}) {
		t.Error("set comparison fail")
	}
	if rv.isValue(map[any]struct{}{2: {}}) {
		t.Error("set false comparison fail")
	}
	if rv.isValue(map[any]struct{}{2: {}, 3: {}}) {
		t.Error("set length comparison fail")
	}
	if rv.isValue("test") {
		t.Error("set type comparison fail")
	}

	rv = nativeValueToResp(map[any]struct{}{1: {}, 2: {}})
	if rv.isValue([]any{1}) {
		t.Error("set comparison by array fail")
	}
	if !rv.isValue([]any{1, 2}) {
		t.Error("set comparison by correct array fail")
	}
	if !rv.isValue([]any{2, 1}) {
		t.Error("set comparison by reversed array fail")
	}
	if rv.isValue([]any{2, 2}) {
		t.Error("set comparison by dup array fail")
	}
	if rv.isValue([]any{2, 3}) {
		t.Error("set comparison by mismatched array fail")
	}

	rv = nativeValueToResp(map[string]string{"test": "val"})
	// targets RESP2, so returns string table as an array
	if !rv.isValue([]any{"test", "val"}) {
		t.Error("string table comparison fail")
	}
	if rv.isValue([]any{"val", "test"}) {
		t.Error("string table false comparison fail")
	}
	if rv.isValue("test") {
		t.Error("string table type comparison fail")
	}

	bn := big.NewInt(30)
	rv = nativeValueToResp(bn)
	if !rv.isValue(big.NewInt(30)) {
		t.Error("big number comparison fail")
	}
	if rv.isValue(big.NewInt(60)) {
		t.Error("big number false comparison fail")
	}
	if rv.isValue(60) {
		t.Error("big number type comparison fail")
	}
}

func TestRespToNative(t *testing.T) {
	rv := nativeValueToResp(nil)
	if rv.toNative() != nil {
		t.Error("native nil fail")
	}

	rv = nativeValueToResp([]any{1, 2, 3})
	a := rv.toNative().([]any)
	ea := []any{int64(1), int64(2), int64(3)}
	if !reflect.DeepEqual(a, ea) {
		t.Error("native array fail")
	}

	first := nativeValueToResp("first")
	second := nativeValueToResp("second")

	rv = respValue{data: respAttributeMap{first: second}}
	am := rv.toNative().(map[any]any)
	eam := map[any]any{"first": "second"}
	if !reflect.DeepEqual(am, eam) {
		t.Error("native attribute map fail")
	}

	rv = nativeValueToResp(big.NewInt(123))
	bn := rv.toNative().(*big.Int)
	ebn := big.NewInt(123)
	if bn.Cmp(ebn) != 0 {
		t.Error("native big int fail")
	}

	rv = respValue{data: respBlobError("error")}
	be := rv.toNative().(string)
	ebe := "error"
	if be != ebe {
		t.Error("native blob error fail")
	}

	rv = nativeValueToResp(true)
	b := rv.toNative().(bool)
	eb := true
	if b != eb {
		t.Error("native bool fail")
	}

	rv = respValue{data: respBulkString("mystr")}
	bs := rv.toNative().(string)
	ebs := "mystr"
	if bs != ebs {
		t.Error("bulk string error fail")
	}

	rv = nativeValueToResp(1.2)
	f := rv.toNative().(float64)
	ef := float64(1.2)
	if f != ef {
		t.Error("native float64 fail")
	}

	rv = respValue{data: respErrorString("error")}
	errs := rv.toNative().(string)
	eerrs := "error"
	if errs != eerrs {
		t.Error("error string error fail")
	}

	rv = nativeValueToResp(1)
	n := rv.toNative().(int64)
	en := int64(1)
	if n != en {
		t.Error("native int64 fail")
	}

	rv = respValue{data: respMap{first: second}}
	m := rv.toNative().(map[any]any)
	em := map[any]any{"first": "second"}
	if !reflect.DeepEqual(m, em) {
		t.Error("native map fail")
	}

	rv = respValue{data: respNull{}}
	null := rv.toNative()
	var enull any
	if !reflect.DeepEqual(null, enull) {
		t.Error("native null fail")
	}

	objs := []respValue{first, second}
	rv = respValue{data: respPush{kind: "pubsub", data: objs}}
	p := rv.toNative().([]any)
	ep := []any{"pubsub", "first", "second"}
	if !reflect.DeepEqual(p, ep) {
		t.Error("native push fail")
	}

	rv = nativeValueToResp(map[any]struct{}{1: {}, 2: {}, 3: {}})
	s := rv.toNative().(map[any]struct{})
	es := map[any]struct{}{int64(1): {}, int64(2): {}, int64(3): {}}
	if !reflect.DeepEqual(s, es) {
		t.Error("native set fail")
	}

	rv = respValue{data: respSimpleString("myss")}
	ss := rv.toNative().(string)
	ess := "myss"
	if ss != ess {
		t.Error("native simple string fail")
	}

	rv = respValue{data: respVerbatimString{format: "txt", text: "example"}}
	vs := rv.toNative().(string)
	evs := "example"
	if vs != evs {
		t.Error("native verbatim string fail")
	}
}
