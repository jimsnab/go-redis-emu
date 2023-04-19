package redisemu

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

func setWorker(cmdName string, args map[string]any, dsc *dataStoreCommand, get, append bool) (output respValue, hasError bool) {
	var key, value string
	options := bitflags(0)

	expiration, valid := parseArgsWithExpiration(args, func(name string, arg any) {
		switch name {
		case "key":
			key = arg.(string)
		case "value":
			value = arg.(string)
		case "condition.nx":
			options |= SET_NOT_EXIST
		case "condition.xx":
			options |= SET_EXISTS
		case "get":
			options |= SET_GET
		case "expiration.keepttl":
			options |= SET_KEEP_TTL
		}
	})

	if !valid {
		output.data = respErrorString(fmt.Sprintf("ERR invalid expire time in '%s' command", cmdName))
		return
	}

	if cmdName == "setnx" {
		options |= SET_NOT_EXIST
	}
	if get {
		options |= SET_GET
	}
	if append {
		options |= SET_APPEND
	}

	val, valueExists := dsc.setKey(key, value, options, expiration)
	if valueExists == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
		hasError = true
	} else if cmdName == "setnx" {
		if val.data == nil {
			output.data = respInt(0)
		} else {
			output.data = respInt(1)
		}
	} else {
		output.data = val.data
	}
	return
}

func keyAdd(ctx *cmdContext, key string, delta int64) (output respValue, err error) {
	result, valid := ctx.dsc.addInt(key, delta)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid != VALUE_EXISTS {
		output.data = respErrorString("ERR value is not an integer or out of range")
	} else {
		output.data = respInt(result)
	}
	return
}

func fnAppend(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	value := args["value"].(string)

	result, hasError := setWorker(ctx.cmdName, args, ctx.dsc, true, true)
	if hasError {
		output = result
		return
	}

	str, _ := result.toString()
	output.data = respInt(len(str) + len(value))
	return
}

func fnDecr(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return keyAdd(ctx, args["key"].(string), -1)
}

func fnDecrBy(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return keyAdd(ctx, args["key"].(string), -args["decrement"].(int64))
}

func fnGet(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	str, valid := ctx.dsc.getKey(keyName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_EXISTS {
		output.data = respBulkString(str)
	}
	return
}

func fnGetDel(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	str, valid := ctx.dsc.getDeleteKey(keyName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_EXISTS {
		output.data = respBulkString(str)
	}
	return
}

func parseArgsWithExpiration(args map[string]any, defaultHandler func(name string, arg any)) (expiration time.Time, valid bool) {
	now := time.Now()
	expiration = maxTime

	for name, arg := range args {
		switch name {
		case "expiration.seconds", "seconds", "expiration.milliseconds", "milliseconds", "expiration.unix-time-seconds", "expiration.unix-time-milliseconds":
			n := arg.(int64)
			if n <= 0 {
				return
			}
		}
	}

	for name, arg := range args {
		switch name {
		case "expiration.seconds", "seconds":
			expiration = now.Add(time.Second*time.Duration(arg.(int64)) - time.Nanosecond)
		case "expiration.milliseconds", "milliseconds":
			expiration = now.Add(time.Millisecond*time.Duration(arg.(int64)) - time.Nanosecond)
		case "expiration.unix-time-seconds":
			expiration = time.Unix(arg.(int64), 0).Add(time.Duration(now.Nanosecond()) - time.Nanosecond)
		case "expiration.unix-time-milliseconds":
			n := arg.(int64)
			expiration = time.Unix(n/1000, (n%1000)*(1000*1000))
		case "expiration.persist":
			expiration = maxTime
		default:
			if defaultHandler != nil {
				defaultHandler(name, arg)
			}
		}
	}

	valid = true
	return
}

func fnGetEx(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	expiration, valid := parseArgsWithExpiration(args, nil)
	if !valid {
		output.data = respErrorString(fmt.Sprintf("ERR invalid expire time in '%s' command", ctx.cmdName))
		return
	}

	str, valueExists := ctx.dsc.getKeySetExpiration(keyName, expiration)
	if valueExists == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valueExists == VALUE_EXISTS {
		output.data = respBulkString(str)
	}
	return
}

func fnGetRange(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	start64 := args["start"].(int64)
	end64 := args["end"].(int64)

	start := int(start64)
	end := int(end64)

	str, valid := ctx.dsc.getKey(keyName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_EXISTS {
		// convert negative indexes to positive
		n := len(str)
		if start < 0 {
			start = n + start
		}
		if end < 0 {
			end = n + end
		}

		// enforce boundaries
		if start < 0 {
			start = 0
		} else if start > n {
			start = n
		}

		if end < start {
			end = start - 1
		} else if end >= n {
			end = n - 1
		}

		output.data = respBulkString(str[start : end+1])
	}

	return
}

func fnGetSet(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output, _ = setWorker(ctx.cmdName, args, ctx.dsc, true, false)
	return
}

func fnLcs(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName1 := args["key1"].(string)
	keyName2 := args["key2"].(string)
	_, hasLength := args["len"]
	_, hasIdx := args["idx"]
	minMatchLen, _ := args["len"].(int64)
	_, hasWithMatchLen := args["withmatchlen"]

	vals, wrongType := ctx.dsc.getKeys(keyName1, keyName2)
	if wrongType {
		output.data = wrongTypeError
		return
	}

	if vals[0] == nil || vals[1] == nil {
		output.data = respBulkString("")
		return
	}

	ls := newLongestSeq(*vals[0], *vals[1])

	matchLen, matches, matchText := ls.longestSequence(int(minMatchLen), hasWithMatchLen)

	if hasIdx {
		result := map[any]any{"matches": matches, "len": matchLen}
		output = nativeValueToResp(result)
	} else if hasLength {
		output = nativeValueToResp(matchLen)
	} else {
		output = nativeValueToResp(matchText)
	}

	return
}

func fnIncr(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return keyAdd(ctx, args["key"].(string), 1)
}

func fnIncrBy(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return keyAdd(ctx, args["key"].(string), args["increment"].(int64))
}

func fnIncrByFloat(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	key := args["key"].(string)
	delta := args["increment"].(float64)

	if math.IsInf(delta, 0) || math.IsNaN(delta) {
		output.data = respErrorString("ERR increment would produce NaN or Infinity")
		return
	}

	result, valid := ctx.dsc.addFloat(key, delta)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_WRONG_FORMAT {
		output.data = respErrorString("ERR value is not a valid float")
	} else {
		output.data = respBulkString(strconv.FormatFloat(result, 'f', -1, 64))
	}
	return
}

func fnKeys(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	pattern := args["pattern"].(string)

	output = nativeValueToResp(ctx.dsc.keys(pattern))
	return
}

func fnMget(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keys, _ := args["key"].([]any)

	keyNames := make([]string, 0, len(keys))
	for _, keyObj := range keys {
		keyNames = append(keyNames, keyObj.(string))
	}

	vals, _ := ctx.dsc.getKeys(keyNames...)

	valArray := make([]any, 0, len(vals))
	for _, val := range vals {
		if val == nil {
			valArray = append(valArray, nil)
		} else {
			valArray = append(valArray, *val)
		}
	}

	output = nativeValueToResp(valArray)
	return
}

func fnMset(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyValuePairs, _ := args["key_value"].([]any)

	options := bitflags(0)
	if ctx.cmdName == "msetnx" {
		options = SET_NOT_EXIST
	}

	strKeys := make([]string, 0, len(keyValuePairs))
	strVals := make([]string, 0, len(keyValuePairs))

	for _, t := range keyValuePairs {
		table := t.(map[string]any)
		strKeys = append(strKeys, table["key"].(string))
		strVals = append(strVals, table["value"].(string))
	}

	output = ctx.dsc.setKeys(strKeys, strVals, options)
	return
}

func fnSet(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output, _ = setWorker(ctx.cmdName, args, ctx.dsc, false, false)
	return
}

func fnSetRange(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	key := args["key"].(string)
	offset := args["offset"].(int64)
	value := args["value"].(string)

	result := ctx.dsc.setRange(key, int(offset), value)
	output.data = result.data
	return
}

func fnStrLen(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	str, valid := ctx.dsc.getKey(keyName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_EXISTS {
		output.data = respInt(len(str))
	} else {
		output.data = respInt(0)
	}
	return
}
