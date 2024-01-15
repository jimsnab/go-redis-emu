package redisemu

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

func (rv *respValue) serialize() []byte {
	var sb strings.Builder
	rv.serializeValue(&sb)
	return []byte(sb.String())
}

func (rv *respValue) serializeValue(sb *strings.Builder) {
	switch o := rv.data.(type) {
	case respInt:
		rv.serializeInt(sb, o)
	case respBulkString:
		rv.serializeString(sb, o)
	case respBlobError:
		rv.serializeBlobErrorString(sb, o)
	case respSimpleString:
		rv.serializeSimpleString(sb, "+"+string(o))
	case respErrorString:
		rv.serializeSimpleString(sb, "-"+string(o))
	case respDouble:
		rv.serializeDouble(sb, o)
	case respBool:
		rv.serializeBool(sb, o)
	case respBigNumber:
		rv.serializeBigInt(sb, o)
	case respVerbatimString:
		rv.serializeVerbatim(sb, o)
	case respArray:
		rv.serializeArray(sb, o)
	case respMap:
		rv.serializeMap(sb, o)
	case respPairs:
		rv.serializePairs(sb, o)
	case respSet:
		rv.serializeSet(sb, o)
	case respAttributeMap:
		rv.serializeAttributeMap(sb, o)
	case respPush:
		rv.serializePush(sb, o)
	case respValue:
		o.serializeValue(sb)
	case nil:
		sb.WriteString("$-1\r\n")
	case respNull:
		sb.WriteString("_\r\n")
	default:
		panic(fmt.Sprintf("unsupported value type %T", rv.data))
	}
}

func (rv *respValue) serializeString(sb *strings.Builder, data respBulkString) {
	sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(data), data))
}

func (rv *respValue) serializeBlobErrorString(sb *strings.Builder, data respBlobError) {
	sb.WriteString(fmt.Sprintf("!%d\r\n%s\r\n", len(data), data))
}

func (rv *respValue) serializeSimpleString(sb *strings.Builder, data string) {
	sb.WriteString(fmt.Sprintf("%s\r\n", data))
}

func (rv *respValue) serializeInt(sb *strings.Builder, data respInt) {
	sb.WriteString(fmt.Sprintf(":%d\r\n", data))
}

func (rv *respValue) serializeDouble(sb *strings.Builder, data respDouble) {
	f := float64(data)
	if f == math.Inf(-1) {
		sb.WriteString(",-inf\r\n")
	} else if f == math.Inf(1) {
		sb.WriteString(",inf\r\n")
	} else {
		text := strconv.FormatFloat(f, 'f', -1, 64)
		sb.WriteString(fmt.Sprintf(",%s\r\n", text))
	}
}

func (rv *respValue) serializeBool(sb *strings.Builder, data respBool) {
	b := bool(data)
	if b {
		sb.WriteString("#t\r\n")
	} else {
		sb.WriteString("#f\r\n")
	}
}

func (rv *respValue) serializeBigInt(sb *strings.Builder, data respBigNumber) {
	sb.WriteString(fmt.Sprintf("(%s\r\n", data.bn.String()))
}

func (rv *respValue) serializeVerbatim(sb *strings.Builder, data respVerbatimString) {
	text := fmt.Sprintf("%-3.3s:%s", data.format, data.text)
	sb.WriteString(fmt.Sprintf("=%d\r\n%s\r\n", len(text), text))
}

func (rv *respValue) serializeArray(sb *strings.Builder, data respArray) {
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(data)))
	for _, item := range data {
		item.serializeValue(sb)
	}
}

func (rv *respValue) serializeMap(sb *strings.Builder, data respMap) {
	sb.WriteString(fmt.Sprintf("%%%d\r\n", len(data.m)))
	for _, k := range data.order {
		k.serializeValue(sb)
		v := data.mustGet(k)
		v.serializeValue(sb)
	}
}

func (rv *respValue) serializePairs(sb *strings.Builder, data respPairs) {
	// non-standard, serialize only (no deserialize)
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(data)))
	for _, pair := range data {
		sb.WriteString("*2\r\n")
		pair.key.serializeValue(sb)
		pair.value.serializeValue(sb)
	}
}

func (rv *respValue) serializeAttributeMap(sb *strings.Builder, data respAttributeMap) {
	sb.WriteString(fmt.Sprintf("|%d\r\n", len(data)))
	for k, v := range data {
		k.serializeValue(sb)
		v.serializeValue(sb)
	}
}

func (rv *respValue) serializeSet(sb *strings.Builder, data respSet) {
	sb.WriteString(fmt.Sprintf("~%d\r\n", len(data)))
	for v := range data {
		v.serializeValue(sb)
	}
}

func (rv *respValue) serializePush(sb *strings.Builder, data respPush) {
	line := fmt.Sprintf(">%d\r\n+%s\r\n", 1+len(data.data), data.kind)
	sb.WriteString(line)
	for _, item := range data.data {
		item.serializeValue(sb)
	}
}
