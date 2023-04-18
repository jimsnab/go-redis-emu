package redisemu

import (
	"math/big"
	"strconv"
	"strings"

	"github.com/jimsnab/go-lane"
)

type respDeserializer struct {
	l          lane.Lane
	content    []byte
	pos        int
	nextPos    int
	lineNumber int
}

// parses redis-formatted content into an object structure, returning nil
// if the content is invalid
func newRespDeserializer(l lane.Lane, content []byte) *respDeserializer {
	return &respDeserializer{l: l, content: content, pos: 0, lineNumber: 1, nextPos: -1}
}

// this is the main deserialization entry point
func (rl *respDeserializer) deserializeNext() (value respValue, length int, valid bool) {
	start := rl.pos
	rl.lineNumber = 1
	rl.nextPos = -1

	if value, valid = rl.getNextValue(); !valid {
		return
	}

	length = rl.pos - start
	return
}

func (rl *respDeserializer) getNextValue() (value respValue, valid bool) {
	return rl.getNextValueEx(false)
}

func (rl *respDeserializer) getNextValueEx(endAllowed bool) (value respValue, valid bool) {
	var line string
	if line, valid = rl.peekNextLine(); !valid {
		return
	}

	if line[0] == '+' {
		// simple string
		rl.moveToNextLine()
		value.data = respSimpleString(line[1:])
		return
	}

	if line[0] == '-' {
		// error string
		rl.moveToNextLine()
		value.data = respErrorString(line[1:])
		return
	}

	if line[0] == '$' {
		if line == "$?" {
			// string of unknown length
			rl.moveToNextLine()
			value.data, valid = rl.getChunkedString()
			return
		} else {
			// standard bulk string
			var count int
			if count, valid = rl.getCount(line); !valid {
				return
			}
			rl.moveToNextLine()

			if count < 0 {
				value.data = nil
				return
			}

			if value.data, valid = rl.peekBulkLine(count); !valid {
				return
			}
			rl.moveToNextLine()
			return
		}
	}

	if line[0] == ':' {
		// 64-bit integer
		if value.data, valid = rl.getCount64(line); !valid {
			return
		}
		rl.moveToNextLine()
		return
	}

	if line[0] == '*' {
		if line == "*?" {
			// variable-length array
			rl.moveToNextLine()
			value.data, valid = rl.getNextDynamicArray()
			return
		} else {
			// fixed-length array
			var count int
			if count, valid = rl.getCount(line); !valid {
				return
			}
			rl.moveToNextLine()

			if count < 0 {
				value.data = nil
				valid = true
				return
			}
			value.data, valid = rl.getNextArray(count)
			return
		}
	}

	if line[0] == '%' {
		if line == "%?" {
			// variable-length map
			rl.moveToNextLine()
			value.data, valid = rl.getNextDynamicMap()
			return
		} else {
			// fixed-length map
			var pairs int
			if pairs, valid = rl.getCount(line); !valid {
				return
			}
			rl.moveToNextLine()

			if pairs < 0 {
				valid = false
				return
			}
			value.data, valid = rl.getNextMap(pairs)
			return
		}
	}

	if line[0] == ',' {
		// double
		if value.data, valid = rl.getDouble(line); !valid {
			return
		}
		rl.moveToNextLine()
		return
	}

	if line == "#t" {
		valid = true
		value.data = respBool(true)
		rl.moveToNextLine()
		return
	}

	if line == "#f" {
		valid = true
		value.data = respBool(false)
		rl.moveToNextLine()
		return
	}

	if line[0] == '~' {
		if line == "~?" {
			// variable-length set
			rl.moveToNextLine()
			value.data, valid = rl.getNextDynamicSet()
			return
		} else {
			// fixed-length set
			var count int
			if count, valid = rl.getCount(line); !valid {
				return
			}
			rl.moveToNextLine()

			if count < 0 {
				valid = false
				return
			}
			value.data, valid = rl.getNextSet(count)
			return
		}
	}

	if line == "." && endAllowed {
		rl.moveToNextLine()
		value.data = respEnd{}
		valid = true
		return
	}

	if line == "_" {
		// null
		rl.moveToNextLine()
		value.data = respNull{}
		valid = true
		return
	}

	if line[0] == '!' {
		if line == "!?" {
			// error string of unknown length
			rl.moveToNextLine()
			var str respBulkString
			str, valid = rl.getChunkedString()
			if valid {
				value.data = respBlobError(str)
			}
			return
		} else {
			// fixed-length blob error string
			var count int
			if count, valid = rl.getCount(line); !valid {
				return
			}
			rl.moveToNextLine()

			if count < 0 {
				valid = false
				return
			}

			var str respBulkString
			if str, valid = rl.peekBulkLine(count); !valid {
				return
			}
			value.data = respBlobError(str)
			rl.moveToNextLine()
			return
		}
	}

	if line[0] == '=' {
		// verbatim string
		var count int
		if count, valid = rl.getCount(line); !valid {
			return
		}
		rl.moveToNextLine()

		if count < 0 {
			value.data = nil
			valid = false
			return
		}

		var str respBulkString
		if str, valid = rl.peekBulkLine(count); !valid {
			return
		}
		if len(str) < 4 || str[3] != ':' {
			valid = false
			return
		}
		value.data = respVerbatimString{
			format: string(str[0:3]),
			text:   string(str[4:]),
		}
		rl.moveToNextLine()
		return
	}

	if line[0] == '|' {
		if line == "|?" {
			// variable-length attribute map
			rl.moveToNextLine()
			value.data, valid = rl.getNextDynamicAttributeMap()
			return
		} else {
			// fixed-length attribute map
			var pairs int
			if pairs, valid = rl.getCount(line); !valid {
				return
			}
			rl.moveToNextLine()

			if pairs < 0 {
				valid = false
				return
			}
			value.data, valid = rl.getNextAttributeMap(pairs)
			return
		}
	}

	if line[0] == '>' {
		var count int
		if count, valid = rl.getCount(line); !valid {
			return
		}
		rl.moveToNextLine()

		if count < 1 {
			value.data = nil
			valid = false
			return
		}
		value.data, valid = rl.getNextPush(count - 1)
		return
	}

	if line[0] == '(' {
		bn := big.NewInt(0)
		bn.SetString(line[1:], 10)
		value.data = respBigNumber{bn: bn}
		rl.moveToNextLine()
		return
	}

	rl.l.Errorf("unexpected line %d: %s", rl.lineNumber, line)
	valid = false
	return
}

func (rl *respDeserializer) findNextLine() (valid bool) {
	if rl.nextPos >= 0 {
		panic("already determined the next line")
	}

	pos := rl.pos
	end := len(rl.content) - 1
	for {
		if pos >= end {
			return false
		}
		if rl.content[pos] == '\r' && rl.content[pos+1] == '\n' {
			rl.nextPos = pos + 2
			return true
		}
		pos++
	}
}

func (rl *respDeserializer) moveToNextLine() {
	if rl.nextPos < 0 { // sanity check
		panic("move to next line without prior findNextLine, or duplicate moveToNextLine call")
	}
	rl.pos = rl.nextPos
	rl.nextPos = -1
	rl.lineNumber++
}

func (rl *respDeserializer) peekNextLine() (line string, valid bool) {
	if valid = rl.findNextLine(); !valid {
		return
	}

	line = string(rl.content[rl.pos : rl.nextPos-2])
	return
}

func (rl *respDeserializer) peekBulkLine(length int) (line respBulkString, valid bool) {
	if rl.nextPos >= 0 {
		panic("already determined the next line")
	}

	rl.nextPos = rl.pos + length + 2
	if rl.nextPos > len(rl.content) {
		valid = false
		return
	}

	if rl.content[rl.nextPos-2] != '\r' || rl.content[rl.nextPos-1] != '\n' {
		rl.l.Errorf("bulk line does not have expected ending on line %d", rl.lineNumber)
		valid = false
		return
	}

	line = respBulkString(rl.content[rl.pos : rl.pos+length])
	valid = true
	return
}

func (rl *respDeserializer) getCount(line string) (value int, valid bool) {
	var count64 respInt
	if count64, valid = rl.getCount64(line); !valid {
		return
	}
	value = int(count64)
	return
}

func (rl *respDeserializer) getCount64(line string) (value respInt, valid bool) {
	count64, err := strconv.ParseInt(line[1:], 10, 64)
	if err != nil {
		rl.l.Errorf("invalid integer %s on line %d", line[1:], rl.lineNumber)
		return 0, false
	}
	return respInt(count64), true
}

func (rl *respDeserializer) getDouble(line string) (value respDouble, valid bool) {
	value64, err := strconv.ParseFloat(line[1:], 64)
	if err != nil {
		rl.l.Errorf("invalid float %s on line %d", line[1:], rl.lineNumber)
		return 0, false
	}
	return respDouble(value64), true
}

func (rl *respDeserializer) getNextArray(count int) (value respArray, valid bool) {
	a := make(respArray, 0, count)

	for i := 0; i < count; i++ {
		var v respValue
		if v, valid = rl.getNextValue(); !valid {
			return
		}
		a = append(a, v)
	}

	return a, true
}

func (rl *respDeserializer) getNextMap(pairs int) (value respMap, valid bool) {
	m := make(respMap, pairs)

	for i := 0; i < pairs; i++ {
		var k, v respValue
		if k, valid = rl.getNextValue(); !valid {
			return
		}
		k = respNormalizeKey(k)
		if v, valid = rl.getNextValue(); !valid {
			return
		}

		m[k] = v
	}

	return m, true
}

func (rl *respDeserializer) getNextAttributeMap(pairs int) (value respAttributeMap, valid bool) {
	m := make(respAttributeMap, pairs)

	for i := 0; i < pairs; i++ {
		var k, v respValue
		if k, valid = rl.getNextValue(); !valid {
			return
		}
		k = respNormalizeKey(k)
		if v, valid = rl.getNextValue(); !valid {
			return
		}

		m[k] = v
	}

	return m, true
}

func (rl *respDeserializer) getNextSet(count int) (value respSet, valid bool) {
	s := make(respSet, count)

	for i := 0; i < count; i++ {
		var v respValue
		if v, valid = rl.getNextValue(); !valid {
			return
		}
		v = respNormalizeKey(v)
		s[v] = struct{}{}
	}

	return s, true
}

func (rl *respDeserializer) getNextPush(count int) (value respPush, valid bool) {
	a := make([]respValue, 0, count)
	p := respPush{}

	var v respValue
	if v, valid = rl.getNextValue(); !valid {
		return
	}
	var str string
	if str, valid = v.toString(); !valid {
		return
	}
	p.kind = str

	for i := 0; i < count; i++ {
		var v respValue
		if v, valid = rl.getNextValue(); !valid {
			return
		}
		a = append(a, v)
	}

	p.data = a
	return p, true
}

func (rl *respDeserializer) getChunkedString() (value respBulkString, valid bool) {
	var sb strings.Builder

	for {
		var line string
		if line, valid = rl.peekNextLine(); !valid {
			return
		}

		if len(line) < 1 || line[0] != ';' {
			valid = false
			return
		}

		var count int
		if count, valid = rl.getCount(line); !valid {
			return
		}
		rl.moveToNextLine()

		if count == 0 {
			return respBulkString(sb.String()), true
		}

		var str respBulkString
		if str, valid = rl.peekBulkLine(count); !valid {
			return
		}

		sb.WriteString(string(str))
		rl.moveToNextLine()
	}
}

func (rl *respDeserializer) getNextDynamicArray() (value respArray, valid bool) {
	a := respArray{}

	for {
		var v respValue
		if v, valid = rl.getNextValueEx(true); !valid {
			return
		}
		if v.isEnd() {
			return a, true
		}
		a = append(a, v)
	}
}

func (rl *respDeserializer) getNextDynamicMap() (value respMap, valid bool) {
	m := respMap{}

	for {
		var k, v respValue
		if k, valid = rl.getNextValueEx(true); !valid {
			return
		}
		if k.isEnd() {
			return m, true
		}
		k = respNormalizeKey(k)
		if v, valid = rl.getNextValue(); !valid {
			return
		}

		m[k] = v
	}
}

func (rl *respDeserializer) getNextDynamicAttributeMap() (value respAttributeMap, valid bool) {
	m := respAttributeMap{}

	for {
		var k, v respValue
		if k, valid = rl.getNextValueEx(true); !valid {
			return
		}
		if k.isEnd() {
			return m, true
		}

		k = respNormalizeKey(k)
		if v, valid = rl.getNextValue(); !valid {
			return
		}

		m[k] = v
	}
}

func (rl *respDeserializer) getNextDynamicSet() (value respSet, valid bool) {
	s := respSet{}

	for {
		var v respValue
		if v, valid = rl.getNextValueEx(true); !valid {
			return
		}
		if v.isEnd() {
			return s, true
		}
		v = respNormalizeKey(v)
		s[v] = struct{}{}
	}
}
