package redisemu

import (
	"math/bits"
	"sort"
	"strconv"
	"strings"
)

func fnBitCount(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	strBytes, valid := ctx.dsc.getKeyBytes(keyName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
		return
	}
	if valid != VALUE_EXISTS {
		output.data = respInt(0)
		return
	}

	// get the optional range args
	start := 0
	length := len(strBytes)
	end := length - 1
	bitMode := false
	rangeArg, exists := args["range"].(map[string]any)
	if exists {
		if start64, exists := rangeArg["start"].(int64); exists {
			start = int(start64)
		}
		if end64, exists := rangeArg["end"].(int64); exists {
			end = int(end64)
		}
		_, bitMode = rangeArg["unit.bit"]
	}

	if bitMode {
		length *= 8
	}

	// right side indexing
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	// bounds checking
	if start < 0 {
		start = 0
	} else if start >= length {
		start = length - 1
	}

	if end < start {
		output.data = respInt(0)
		return
	} else if end >= length {
		end = length - 1
	}

	if bitMode {
		output.data = respInt(countSetBitRange(strBytes, start, end))
	} else {
		count := 0
		for _, b := range strBytes[start : end+1] {
			count += bits.OnesCount8(b)
		}
		output.data = respInt(count)
	}

	return
}

func parseBitfieldEncodingType(encoding string) (signed bool, width int) {
	if strings.HasPrefix(encoding, "i") {
		signed = true
	} else {
		if !strings.HasPrefix(encoding, "u") {
			return
		}
	}
	width64, err := strconv.ParseInt(encoding[1:], 10, 32)
	if err != nil {
		return
	}
	width = int(width64)

	// enforce min and max bit widths
	if signed {
		if width < 1 || width > 64 {
			width = 0
			return
		}
	} else {
		if width < 1 || width > 63 {
			width = 0
			return
		}
	}
	return
}

func parseBitfieldOffset(spec string, width int) (offset int, valid bool) {
	if strings.HasPrefix(spec, "#") {
		n, err := strconv.ParseInt(spec[1:], 10, 32)
		if err != nil {
			valid = false
			return
		}
		if n < 0 {
			valid = false
			return
		}
		offset = int(n) * width
	} else {
		n, err := strconv.ParseInt(spec, 10, 32)
		if err != nil {
			valid = false
			return
		}
		offset = int(n)
	}
	valid = true
	return
}

func organizeBitfieldOp(tableObj map[string]any, tableKey, valueKey string, opType bitfieldOperation, oflowChanges map[int]overflowType) (op *bitfieldOp, errorText string, valid bool) {
	var opTable map[string]any
	if tableKey != "" {
		var exists bool
		opTable, exists = tableObj[tableKey].(map[string]any)
		if !exists {
			// valid but empty
			valid = true
			return
		}
	} else {
		opTable = tableObj
	}

	signed, width := parseBitfieldEncodingType(opTable["encoding"].(string))
	if width <= 0 {
		errorText = "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
		valid = false
		return
	}
	offset, valid := parseBitfieldOffset(opTable["offset"].(string), width)
	if !valid {
		errorText = "ERR bit offset is not an integer or out of range"
		return
	}

	var value int64
	if valueKey != "" {
		// parse the value arg string to an int64
		var pe error
		value, pe = strconv.ParseInt(opTable[valueKey].(string), 10, 64)
		if pe != nil {
			valid = false
			return
		}
	} else {
		value = 0
	}

	argIndex := opTable["arg-index"].(int)

	if _, exists := tableObj["overflow-block.wrap"]; exists {
		oflowChanges[argIndex] = OFLOW_WRAP
	} else if _, exists := tableObj["overflow-block.sat"]; exists {
		oflowChanges[argIndex] = OFLOW_SAT
	} else if _, exists := tableObj["overflow-block.fail"]; exists {
		oflowChanges[argIndex] = OFLOW_FAIL
	}

	op = &bitfieldOp{
		argIndex:  argIndex,
		op:        opType,
		value:     value,
		signed:    signed,
		bitOffset: offset,
		endOffset: offset + width - 1,
		width:     width,
		oflow:     OFLOW_WRAP,
	}
	return
}

func fnBitfield(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	ops := []*bitfieldOp{}

	keyName := args["key"].(string)

	oflowChanges := map[int]overflowType{}

	writes, valid := args["operation.write"].([]any)
	if valid {
		for _, w := range writes {
			opTable := w.(map[string]any)

			// INCRBY
			op, errText, valid := organizeBitfieldOp(opTable, "write-operation.incrby-block", "increment", BF_INCRBY, oflowChanges)
			if !valid {
				output.data = respErrorString(errText)
				return
			}
			if op != nil {
				ops = append(ops, op)
				continue
			} else {
				// SET
				op, errText, valid = organizeBitfieldOp(opTable, "write-operation.set-block", "value", BF_SET, oflowChanges)
				if !valid {
					output.data = respErrorString(errText)
					return
				}
				if op != nil {
					ops = append(ops, op)
					continue
				}
			}
		}
	}

	reads, valid := args["operation.get-block"].([]any) // argument key defined by bitfield
	if !valid {
		reads, valid = args["get-block"].([]any) // argument key defined by bitfield_ro
	}
	if valid {
		for _, eo := range reads {
			// GET
			op, errText, valid := organizeBitfieldOp(eo.(map[string]any), "", "", BF_GET, oflowChanges)
			if !valid {
				output.data = respErrorString(errText)
				return
			}
			ops = append(ops, op)
		}
	}

	// sort
	sort.Slice(ops, func(ia, ib int) bool { // less fn in closure
		return ops[ia].argIndex < ops[ib].argIndex
	})

	// update overflow
	oflow := OFLOW_WRAP
	for _, op := range ops {
		next, exists := oflowChanges[op.argIndex]
		if exists {
			oflow = next
		}
		op.oflow = oflow
	}

	// now do the command
	output = ctx.dsc.bitfieldWrite(keyName, ops)
	return
}

func fnBitOp(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keys := args["key"].([]any)
	destKeyName := keys[0].(string)
	srcKeys := keys[1:]
	_, op_not := args["operation.not"]
	_, op_and := args["operation.and"]
	_, op_or := args["operation.or"]
	_, op_xor := args["operation.xor"]

	srcKeyNames := make([]string, 0, len(srcKeys))
	for _, key := range srcKeys {
		srcKeyNames = append(srcKeyNames, key.(string))
	}

	if op_not {
		if len(srcKeys) != 1 {
			output.data = respErrorString("ERR BITOP NOT must be called with a single source key.")
			return
		}
		output = ctx.dsc.invertBits(srcKeys[0].(string), destKeyName)
	} else if op_and {
		output = ctx.dsc.changeBits(destKeyName, srcKeyNames, func(a, b byte) byte { return a & b })
	} else if op_or {
		output = ctx.dsc.changeBits(destKeyName, srcKeyNames, func(a, b byte) byte { return a | b })
	} else if op_xor {
		output = ctx.dsc.changeBits(destKeyName, srcKeyNames, func(a, b byte) byte { return a ^ b })
	} else {
		output.data = rstrSyntaxError
		return
	}

	return
}

func fnBitPos(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	// text, _ := json.MarshalIndent(args, "", "  ")
	// ctx.l.Trace(string(text))

	keyName := args["key"].(string)
	bit64, _ := args["bit"].(int64)

	if bit64 != 1 && bit64 != 0 {
		output.data = respErrorString("ERR The bit argument must be 1 or 0.")
		return
	}

	width := 8
	noEnd := true
	start64 := int64(0)
	end64 := int64(-1)
	rangeArg, hasRange := args["range"].(map[string]any)
	if hasRange {
		bitToken := false
		start64 = rangeArg["start"].(int64)

		endIndex, exists := rangeArg["end-unit-block"].(map[string]any)
		if exists {
			noEnd = false
			end64 = endIndex["end"].(int64)
			_, bitToken = endIndex["unit.bit"]
		}

		if bitToken {
			width = 1
		}
	}

	sk, exists := ctx.dsc.getKeyObject(keyName)
	if !exists {
		if bit64 != 0 {
			// N.B., redis is inconsistent here, it should return
			// 0 if noEnd is false
			output.data = respInt(-1)
		} else {
			output.data = respInt(0)
		}
		return
	}

	strBytes := sk.getStringBytes()
	if strBytes == nil {
		output.data = wrongTypeError
		return
	}

	// width must be sent to convert index args to bit offsets, and noEnd must be
	// sent to handle a special case of searching for 0 bit
	pos := findBit(strBytes, int(start64), int(end64), width, bit64 != 0, noEnd)
	output.data = respInt(pos)
	return
}

func fnGetBit(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	bit64 := args["offset"].(int64)

	if bit64 < 0 {
		output.data = respErrorString("ERR bit offset is not an integer or out of range")
		return
	}

	sk, exists := ctx.dsc.getKeyObject(keyName)
	if !exists {
		output.data = respInt(0)
		return
	}

	strBytes := sk.getStringBytes()
	if strBytes == nil {
		output.data = wrongTypeError
		return
	}

	byteOffset := int(bit64) / 8
	if byteOffset >= len(strBytes) {
		output.data = respInt(0)
		return
	}

	b := strBytes[byteOffset]
	output.data = respInt((b >> (7 - (bit64 % 8))) & 1)
	return
}

func fnSetBit(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	offset64 := args["offset"].(int64)
	value64 := args["value"].(int64)

	if offset64 < 0 {
		output.data = respErrorString("ERR bit offset is not an integer or out of range")
		return
	}

	if value64 != 0 && value64 != 1 {
		output.data = respErrorString("ERR bit is not an integer or out of range")
		return
	}

	op := &bitfieldOp{
		op:        BF_SET,
		value:     value64,
		signed:    false,
		bitOffset: int(offset64),
		endOffset: int(offset64),
		width:     1,
	}

	result := ctx.dsc.bitfieldWrite(keyName, []*bitfieldOp{op})

	// result is an array of 1; convert it to a single output value
	ra := result.toNative().([]any)
	output.data = respInt(ra[0].(int64))
	return
}
