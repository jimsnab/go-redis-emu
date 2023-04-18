package goredisemu

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type (
	respSimpleString string
	respErrorString  string
	respArray        []respValue
	respBulkString   string
	respInt          int64
	respDouble       float64
	respBool         bool
	respBlobError    string
	respMap          map[respValue]respValue
	respSet          map[respValue]struct{}
	respAttributeMap map[respValue]respValue
	respEnd          struct{}
	respNull         struct{}

	respBigNumber struct {
		bn *big.Int
	}

	respVerbatimString struct {
		format string
		text   string
	}

	respPush struct {
		kind string
		data []respValue
	}

	respValue struct {
		data any
	}
)

// implement Stringer interface on resp types
func (rss respSimpleString) String() string {
	return string(rss)
}

func (res respErrorString) String() string {
	return string(res)
}

func (rbs respBulkString) String() string {
	return string(rbs)
}

func (rbe respBlobError) String() string {
	return string(rbe)
}

func (ri respInt) String() string {
	return strconv.FormatInt(int64(ri), 10)
}

func (rd respDouble) String() string {
	f := float64(rd)
	if f == math.Inf(1) {
		return "inf" // not +inf
	} else if f == math.Inf(-1) {
		return "-inf"
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func (rbn respBigNumber) String() string {
	return rbn.bn.String()
}

func stringifyCollectionValue(v respValue) string {
	if v.isStringType() {
		return `"` + v.String() + `"`
	} else {
		return v.String()
	}
}

func (ra respArray) String() string {
	var sb strings.Builder
	sb.WriteRune('[')
	for _, v := range ra {
		if sb.Len() > 1 {
			sb.WriteRune(',')
		}
		sb.WriteString(stringifyCollectionValue(v))
	}
	sb.WriteRune(']')

	return sb.String()
}

func (rs respSet) String() string {
	var sb strings.Builder
	sb.WriteRune('(')

	vals := make([]respValue, 0, len(rs))
	for v := range rs {
		vals = append(vals, v)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i].String() < vals[j].String() })

	for _, v := range vals {
		if sb.Len() > 1 {
			sb.WriteRune(',')
		}
		sb.WriteString(stringifyCollectionValue(v))
	}
	sb.WriteRune(')')

	return sb.String()
}

func (rm respMap) String() string {
	var sb strings.Builder

	keys := make([]respValue, 0, len(rm))
	for k := range rm {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

	sb.WriteRune('{')
	for _, k := range keys {
		v := rm[k]
		if sb.Len() > 1 {
			sb.WriteRune(',')
		}
		sb.WriteString(stringifyCollectionValue(k))
		sb.WriteRune(':')
		sb.WriteString(stringifyCollectionValue(v))
	}
	sb.WriteRune('}')

	return sb.String()
}

func (ram respAttributeMap) String() string {
	var sb strings.Builder
	keys := make([]respValue, 0, len(ram))
	for k := range ram {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

	sb.WriteRune('|')
	for _, k := range keys {
		v := ram[k]
		if sb.Len() > 1 {
			sb.WriteRune(',')
		}
		sb.WriteString(stringifyCollectionValue(k))
		sb.WriteRune(':')
		sb.WriteString(stringifyCollectionValue(v))
	}
	sb.WriteRune('|')

	return sb.String()
}

func (rb respBool) String() string {
	if rb {
		return "true"
	} else {
		return "false"
	}
}

func (rvs respVerbatimString) String() string {
	return fmt.Sprintf("%-3.3s:%s", rvs.format, rvs.text)
}

func (rp respPush) String() string {
	var sb strings.Builder
	sb.WriteString(rp.kind)
	sb.WriteString("->")
	first := true
	for _, v := range rp.data {
		if first {
			first = false
		} else {
			sb.WriteRune(',')
		}
		sb.WriteString(stringifyCollectionValue(v))
	}

	return sb.String()
}

func (rv respValue) String() string {
	switch v := rv.data.(type) {
	case nil, respNull:
		return ""
	case respSimpleString, respErrorString, respBulkString, respBlobError, respBool, respInt, respPush,
		respDouble, respBigNumber, respArray, respMap, respSet, respAttributeMap, respVerbatimString:
		return fmt.Sprintf("%v", v)
	default:
		panic(fmt.Sprintf("unsupported data member type %T in respValue", rv.data))
	}
}

func (a respArray) toTable() (m map[string]respValue, valid bool) {
	if len(a)%2 != 0 {
		valid = false
		return
	}

	m = map[string]respValue{}
	for pos := 0; pos < len(a); pos += 2 {
		var key string
		if key, valid = a[pos].toString(); !valid {
			return
		}
		m[key] = a[pos+1]
	}

	return
}

func (a respArray) toValues() (values []respValue) {
	values = make([]respValue, 0, len(a))
	values = append(values, a...)
	return
}

func getTableString(m map[string]respValue, key string) (value string, valid bool) {
	val, valid := m[key]
	if !valid {
		return
	}
	return val.toString()
}

func getTableInt(m map[string]respValue, key string) (value int64, valid bool) {
	val, valid := m[key]
	if !valid {
		return
	}
	switch v := val.data.(type) {
	case respInt:
		value = int64(v)
	default:
		valid = false
	}
	return
}

func nativeValueToResp(val any) (value respValue) {
	switch v := val.(type) {
	case int:
		value.data = respInt(v)
	case int64:
		value.data = respInt(v)
	case string:
		value.data = respBulkString(v)
	case float64:
		value.data = respDouble(v)
	case []any:
		value.data = nativeArrayToResp(v)
	case []int:
		value.data = nativeIntArrayToResp(v)
	case []string:
		value.data = nativeStringArrayToResp(v)
	case map[string]any:
		value.data = nativeTableToResp(v)
	case map[string]string:
		value.data = nativeStringTableToResp(v)
	case map[any]any:
		value.data = nativeMapToResp(v)
	case map[any]struct{}:
		value.data = nativeSetToResp(v)
	case bool:
		value.data = respBool(v)
	case *big.Int:
		value.data = respBigNumber{bn: v}
	case respArray, respBulkString, respErrorString, respInt, respSimpleString:
		value.data = v
	case respValue:
		value.data = v.data
	case []respValue:
		a := make(respArray, 0, len(v))
		a = append(a, v...)
		value.data = a
	case nil:
		value.data = nil
	default:
		panic(fmt.Sprintf("nativeValueToResp can't convert type %T", val))
	}

	return
}

func nativeArrayToResp(val []any) (a respArray) {
	a = []respValue{}
	for _, v := range val {
		a = append(a, nativeValueToResp(v))
	}
	return
}

func nativeIntArrayToResp(val []int) (a respArray) {
	a = []respValue{}
	for _, v := range val {
		a = append(a, nativeValueToResp(v))
	}
	return
}

func nativeStringArrayToResp(val []string) (a respArray) {
	a = []respValue{}
	for _, v := range val {
		a = append(a, nativeValueToResp(v))
	}
	return
}

func nativeTableToResp(val map[string]any) (a respArray) {
	names := make([]string, 0, len(val))
	for name := range val {
		names = append(names, name)
	}
	sort.Strings(names)

	a = []respValue{}
	for _, name := range names {
		a = append(a, nativeValueToResp(name))
		a = append(a, nativeValueToResp(val[name]))
	}
	return
}

func nativeStringTableToResp(val map[string]string) (a respArray) {
	names := make([]string, 0, len(val))
	for name := range val {
		names = append(names, name)
	}
	sort.Strings(names)

	a = []respValue{}
	for _, name := range names {
		a = append(a, nativeValueToResp(name))
		a = append(a, nativeValueToResp(val[name]))
	}
	return
}

func nativeMapToResp(val map[any]any) (m respMap) {
	m = respMap{}
	for k, v := range val {
		m[nativeValueToResp(k)] = nativeValueToResp(v)
	}
	return
}

func nativeSetToResp(val map[any]struct{}) (s respSet) {
	s = respSet{}
	for v := range val {
		s[nativeValueToResp(v)] = struct{}{}
	}
	return
}

func (rv *respValue) isValue(other any) bool {
	switch o := other.(type) {
	case nil:
		return rv.data == nil
	case []any:
		return rv.isArray(o...)
	case bool:
		return rv.isBool(o)
	case map[any]any:
		return rv.isMap(o)
	case map[any]struct{}:
		keys := make([]any, 0, len(o))
		for k := range o {
			keys = append(keys, k)
		}
		return rv.isSet(keys...)
	case int:
		return rv.isInt(o)
	case int64:
		return rv.isInt64(o)
	case float64:
		return rv.isFloat(o, -1)
	case string:
		return rv.isString(o)
	case *big.Int:
		return rv.isBigInt(o)
	default:
		panic(fmt.Sprintf("unsupported native value type %T in isValue", other))
	}
}

func (rv *respValue) isEnd() bool {
	_, valid := rv.data.(respEnd)
	return valid
}

func (rv *respValue) isBool(other bool) bool {
	rvb, valid := rv.toBool()
	if !valid {
		return false
	}
	return other == rvb
}

func (rv *respValue) isArray(other ...any) bool {
	a, valid := rv.toArray()
	if !valid {
		// special case for a set
		s, isSet := rv.data.(respSet)
		if isSet {
			// convert other array to a set
			otherSet := map[respValue]struct{}{}
			for _, v := range other {
				nv := nativeValueToResp(v)
				otherSet[nv] = struct{}{}
			}

			// compare sets
			if len(s) != len(otherSet) {
				return false
			}
			for v := range otherSet {
				_, found := s[v]
				if !found {
					return false
				}
			}
			return true
		}
		return false
	}

	if len(a) != len(other) {
		return false
	}

	for idx, otherVal := range other {
		rv2 := respValue{data: a[idx].data}
		if !rv2.isValue(otherVal) {
			return false
		}
	}
	return true
}

func (rv *respValue) isArraySet(other ...any) bool {
	s, valid := rv.arrayToSet()
	if !valid {
		return false
	}

	if len(s) != len(other) {
		return false
	}

	for _, v := range other {
		nv := nativeValueToResp(v)
		_, exists := s[nv]
		if !exists {
			return false
		}
	}

	return true
}

func (rv *respValue) isStringInSet(other ...string) bool {
	s, valid := rv.toString()
	if !valid {
		return false
	}

	for _, o := range other {
		if s == o {
			return true
		}
	}
	return false
}

func (rv *respValue) isArrayASet() bool {
	a, exists := rv.toArray()
	if !exists {
		return false
	}

	vals := make(map[respValue]struct{}, len(a))
	for _, item := range a {
		_, exists := vals[item]
		if exists {
			return false
		}
		vals[item] = struct{}{}
	}

	return true
}

func (rv *respValue) isArrayInSet(length int, other ...any) bool {
	possibleVals := make(map[respValue]struct{}, len(other))
	for _, v := range other {
		nv := nativeValueToResp(v)
		possibleVals[nv] = struct{}{}
	}

	a, valid := rv.toArray()
	if !valid {
		return false
	}

	if len(a) != length {
		return false
	}

	for _, v := range a {
		_, exists := possibleVals[v]
		if !exists {
			return false
		}
	}

	return true
}

func (rv *respValue) isMap(other map[any]any) bool {
	m, valid := rv.toMap()
	if !valid {
		return false
	}

	if len(m) != len(other) {
		return false
	}

	for k, v := range other {
		rk := nativeValueToResp(k)
		val, exists := m[rk]
		if !exists {
			return false
		}
		if !val.isValue(v) {
			return false
		}
	}
	return true
}

func (rv *respValue) isArrayMap(other map[any]any) bool {
	a, valid := rv.toArray()
	if !valid || (len(a)%2 != 0) {
		return false
	}

	table := make(map[respValue]respValue, len(a)/2)
	for i := 0; i < len(a); i += 2 {
		table[a[i]] = a[i+1]
	}

	if len(table) != len(other) {
		return false
	}

	for k, v := range other {
		nk := nativeValueToResp(k)
		tableVal, exists := table[nk]
		if !exists {
			return false
		}
		if !tableVal.isValue(v) {
			return false
		}
	}

	return true
}

// tests if this respValue is an array, and if it is, checks the length (the
// number of key-value pairs), and then ensures the pairs specified in other
// exist in the array (in the key/value pair order)
func (rv *respValue) isArrayInMap(length int, other map[any]any) bool {
	a, valid := rv.toArray()
	if !valid || (len(a)%2 != 0) {
		return false
	}

	if len(a) != length*2 {
		return false
	}

	table := make(map[respValue]respValue, len(a)/2)
	for i := 0; i < len(a); i += 2 {
		table[a[i]] = a[i+1]
	}

	possibleVals := make(map[respValue]respValue, len(other))
	for k, v := range other {
		nk := nativeValueToResp(k)
		nv := nativeValueToResp(v)
		possibleVals[nk] = nv
	}

	for k, v := range table {
		otherVal, exists := possibleVals[k]
		if !exists {
			return false
		}
		if !reflect.DeepEqual(v.data, otherVal.data) {
			return false
		}
	}

	return true
}

func (rv *respValue) isSet(other ...any) bool {
	s, valid := rv.toSet()
	if !valid {
		return false
	}

	// convert other array to a normalized set
	otherSet := make(map[respValue]struct{}, len(other))
	for _, v := range other {
		nv := nativeValueToResp(v)
		otherSet[nv] = struct{}{}
	}

	if len(s) != len(otherSet) {
		return false
	}

	for v := range otherSet {
		_, exists := s[v]
		if !exists {
			return false
		}
	}
	return true
}

func (rv *respValue) isInt(other int) bool {
	switch v := rv.data.(type) {
	case respInt:
		return int(v) == other
	default:
		return false
	}
}

func (rv *respValue) isAtLeast(other int) bool {
	switch v := rv.data.(type) {
	case respInt:
		return int(v) >= other
	default:
		return false
	}
}

func (rv *respValue) isBigInt(other *big.Int) bool {
	switch v := rv.data.(type) {
	case respBigNumber:
		return v.bn.Cmp(other) == 0
	default:
		return false
	}
}

func (rv *respValue) isInt64(other int64) bool {
	switch v := rv.data.(type) {
	case respInt:
		return int64(v) == other
	default:
		return false
	}
}

func (rv *respValue) isString(other string) bool {
	str, valid := rv.toString()
	return valid && str == other
}

func (rv *respValue) isOneOf(values ...any) bool {
	for _, value := range values {
		if rv.isValue(value) {
			return true
		}
	}
	return false
}

func (rv *respValue) isErrorType() bool {
	switch rv.data.(type) {
	case respErrorString, respBlobError:
		return true
	default:
		return false
	}
}

func (rv *respValue) isErrorString(text string) bool {
	switch rv.data.(type) {
	case respErrorString, respBlobError:
		if rv.isString(text) {
			return true
		}
	}
	return false
}

func (rv *respValue) isStringType() bool {
	switch rv.data.(type) {
	case respSimpleString, respBulkString, respErrorString, respBlobError, respVerbatimString:
		return true
	default:
		return false
	}
}

// compares other float to the stored value; precision is the number
// of decimal places to compare, or -1 to compare exact
func (rv *respValue) isFloat(other float64, precision int) bool {
	var val float64
	dbl, ok := rv.data.(respDouble)
	if ok {
		val = float64(dbl)
	} else {
		str, valid := rv.toString()
		if !valid {
			return false
		}
		var err error
		val, err = strconv.ParseFloat(str, 64)
		if err != nil {
			return false
		}
	}

	f1 := strconv.FormatFloat(val, 'f', precision, 64)
	f2 := strconv.FormatFloat(other, 'f', precision, 64)

	return f1 == f2
}

func (rv *respValue) isNull() bool {
	if rv.data == nil {
		return true
	}
	_, isType := rv.data.(respNull)
	return isType
}

func (rv *respValue) toString() (value string, valid bool) {
	switch data := rv.data.(type) {
	case respBulkString:
		return string(data), true

	case respSimpleString:
		return string(data), true

	case respErrorString:
		return string(data), true

	case respBlobError:
		return string(data), true

	case respVerbatimString:
		return string(data.text), true

	default:
		return "", false
	}
}

func (rv *respValue) toBigNumber() (value *big.Int, valid bool) {
	value = big.NewInt(0)

	switch data := rv.data.(type) {
	case respBulkString:
		_, valid = value.SetString(string(data), 10)
		return

	case respSimpleString:
		_, valid = value.SetString(string(data), 10)
		return

	case respBigNumber:
		value.Set(data.bn)
		valid = true
		return

	default:
		value = nil
		valid = false
		return
	}
}

func (rv *respValue) toInt() (value int64, valid bool) {
	switch data := rv.data.(type) {
	case respInt:
		return int64(data), true

	case respBulkString, respSimpleString:
		var err error
		value, err = strconv.ParseInt(fmt.Sprintf("%v", data), 10, 64)
		if err == nil {
			valid = true
		}
		return

	default:
		return 0, false
	}
}

func (rv *respValue) toBool() (value bool, valid bool) {
	switch data := rv.data.(type) {
	case respInt:
		return int64(data) != 0, true

	case respBulkString, respSimpleString:
		n, err := strconv.ParseInt(fmt.Sprintf("%v", data), 10, 64)
		if err == nil {
			return (n != 0), true
		}
		return false, false

	case respBool:
		return bool(data), true

	default:
		return false, false
	}
}

func (rv *respValue) toFloat() (value float64, valid bool) {
	switch data := rv.data.(type) {
	case respBulkString, respSimpleString:
		var err error
		str, _ := rv.toString()
		if strings.EqualFold(str, "INFINITE") {
			value = math.Inf(1)
		} else if strings.EqualFold(str, "-INFINITE") {
			value = math.Inf(-1)
		} else if strings.EqualFold(str, "NAN") {
			value = math.NaN()
		} else {
			value, err = strconv.ParseFloat(str, 64)
			if err == nil {
				valid = true
			}
		}
		return

	case respDouble:
		value = float64(data)
		valid = true
		return

	default:
		return 0, false
	}
}

func (rv *respValue) toArray() (a []respValue, valid bool) {
	a, valid = rv.data.(respArray)
	return
}

func (rv *respValue) toMap() (m map[respValue]respValue, valid bool) {
	switch v := rv.data.(type) {
	case respMap:
		m = v
		valid = true
	case respAttributeMap:
		m = v
		valid = true
	}
	return
}

func (rv *respValue) toSet() (s map[respValue]struct{}, valid bool) {
	s, valid = rv.data.(respSet)
	return
}

func (rv *respValue) arrayToSet() (s respSet, valid bool) {
	a, valid := rv.data.(respArray)
	if !valid {
		return
	}

	s = make(respSet, len(a))
	for _, v := range a {
		_, exists := s[v]
		if exists {
			valid = false
			return
		}
		s[v] = struct{}{}
	}
	return
}

func (rv *respValue) toTable() (m map[string]respValue, valid bool) {
	var a respArray
	if a, valid = rv.data.(respArray); !valid {
		return
	}
	return a.toTable()
}

func (rv *respValue) toNative() any {
	switch v := rv.data.(type) {
	case nil:
		return nil
	case respArray:
		return respToNativeArray(v)
	case respAttributeMap:
		return respToNativeAttributeMap(v)
	case respBigNumber:
		return v.bn
	case respBlobError:
		return string(v)
	case respBool:
		return bool(v)
	case respBulkString:
		return string(v)
	case respDouble:
		return float64(v)
	case respErrorString:
		return string(v)
	case respInt:
		return int64(v)
	case respMap:
		return respToNativeMap(v)
	case respNull:
		return nil
	case respPush:
		return respPushToNative(v)
	case respSet:
		return respToNativeSet(v)
	case respSimpleString:
		return string(v)
	case respVerbatimString:
		return v.text
	default:
		panic(fmt.Sprintf("unexpected resp type %T", rv.data))
	}
}

func respToNativeArray(a respArray) []any {
	out := make([]any, 0, len(a))

	for _, item := range a {
		out = append(out, item.toNative())
	}

	return out
}

func respToNativeSet(s respSet) map[any]struct{} {
	out := make(map[any]struct{}, len(s))

	for v := range s {
		out[v.toNative()] = struct{}{}
	}

	return out
}

func respToNativeMap(m respMap) map[any]any {
	out := make(map[any]any, len(m))

	for k, v := range m {
		out[k.toNative()] = v.toNative()
	}

	return out
}

func respToNativeAttributeMap(m respAttributeMap) map[any]any {
	out := make(map[any]any, len(m))

	for k, v := range m {
		out[k.toNative()] = v.toNative()
	}

	return out
}

func respPushToNative(p respPush) []any {
	out := make([]any, 0, len(p.data)+1)

	out = append(out, p.kind)

	for _, v := range p.data {
		out = append(out, v.toNative())
	}

	return out
}

func respNormalizeKey(k respValue) (output respValue) {
	str, valid := k.toString()
	if valid {
		output.data = respBulkString(str)
	} else {
		output.data = k.data
	}
	return
}
