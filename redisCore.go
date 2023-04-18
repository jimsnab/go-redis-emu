package goredisemu

import (
	"strings"
	"time"
)

func fnCopy(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	sourceKeyName := args["source"].(string)
	destKeyName := args["destination"].(string)
	_, replace := args["replace"]

	_, useOtherDb := args["destination-db"].(int64)
	dds := ctx.dsc.ds
	if useOtherDb {
		// in the future, allow a destination database (dds) to be specified
		output.data = respErrorString("ERR database copy not supported")
		return
	}

	result := ctx.dsc.copy(sourceKeyName, destKeyName, dds, replace)
	if result == RESULT_COMPLETED {
		output.data = respInt(1)
	} else {
		output.data = respInt(0)
	}
	return
}

func doDelete(ctx *cmdContext, args map[string]any, reclaim bool) (output respValue, err error) {
	keyArray := args["key"].([]any)

	keyStrs := make([]string, 0, len(keyArray))
	for _, key := range keyArray {
		keyStrs = append(keyStrs, key.(string))
	}

	output = ctx.dsc.del(keyStrs, reclaim)
	return
}

func fnDel(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return doDelete(ctx, args, true)
}

func fnUnlink(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return doDelete(ctx, args, false)
}

func fnExists(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyArray := args["key"].([]any)

	keyStrs := make([]string, 0, len(keyArray))
	for _, key := range keyArray {
		keyStrs = append(keyStrs, key.(string))
	}

	output = ctx.dsc.exists(keyStrs)
	return
}

func fnDump(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	output = ctx.dsc.dump(keyName)
	return
}

func fnRestore(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	value := args["serialized-value"].(string)
	ttl := args["ttl"].(int64)
	_, absttl := args["absttl"]
	_, replace := args["replace"]

	output = ctx.dsc.restore(keyName, value, ttl, absttl, replace)
	return
}

func fnExpire(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	ttl := args["seconds"].(int64)
	_, nx := args["condition.nx"]
	_, xx := args["condition.xx"]
	_, gt := args["condition.gt"]
	_, lt := args["condition.lt"]

	expiration := time.Now().Add(time.Duration(ttl) * time.Second)

	output = ctx.dsc.expire(keyName, expiration, nx, xx, gt, lt)
	return
}

func fnExpireAt(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	ttl := args["unix-time-seconds"].(int64)
	_, nx := args["condition.nx"]
	_, xx := args["condition.xx"]
	_, gt := args["condition.gt"]
	_, lt := args["condition.lt"]

	expiration := time.Unix(ttl, 0)

	output = ctx.dsc.expire(keyName, expiration, nx, xx, gt, lt)
	return
}

func fnExpireTime(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	expiration, valid := ctx.dsc.expireTime(keyName)
	if valid == 0 {
		output.data = respInt(expiration.Unix())
	} else {
		output.data = respInt(valid)
	}
	return
}

func fnPExpire(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	ttl := args["milliseconds"].(int64)
	_, nx := args["condition.nx"]
	_, xx := args["condition.xx"]
	_, gt := args["condition.gt"]
	_, lt := args["condition.lt"]

	expiration := time.Now().Add(time.Duration(ttl) * time.Millisecond)

	output = ctx.dsc.expire(keyName, expiration, nx, xx, gt, lt)
	return
}

func fnPExpireAt(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	ttl := args["unix-time-milliseconds"].(int64)
	_, nx := args["condition.nx"]
	_, xx := args["condition.xx"]
	_, gt := args["condition.gt"]
	_, lt := args["condition.lt"]

	expiration := time.UnixMilli(ttl)

	output = ctx.dsc.expire(keyName, expiration, nx, xx, gt, lt)
	return
}

func fnPExpireTime(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	expiration, valid := ctx.dsc.expireTime(keyName)
	if valid == 0 {
		output.data = respInt(expiration.UnixMilli())
	} else {
		output.data = respInt(valid)
	}
	return
}

func fnPersist(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	output = ctx.dsc.persist(keyName)
	return
}

func fnTtl(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	expiration, valid := ctx.dsc.expireTime(keyName)

	if valid == 0 {
		output.data = respInt(expiration.Unix() - time.Now().Unix())
	} else {
		output.data = respInt(valid)
	}
	return
}

func fnPTtl(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	expiration, valid := ctx.dsc.expireTime(keyName)

	if valid == 0 {
		output.data = respInt(expiration.UnixMilli() - time.Now().UnixMilli())
	} else {
		output.data = respInt(valid) // -2 key doesn't exist, -1 no expiration
	}
	return
}

func fnRandomKey(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output = ctx.dsc.randomKey()
	return
}

func fnRename(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	srcKeyName := args["key"].(string)
	destKeyName := args["newkey"].(string)

	result := ctx.dsc.move(srcKeyName, destKeyName, ctx.dsc.ds, true)
	if result == RESULT_COMPLETED {
		output.data = rstrOK
	} else {
		output.data = respErrorString("ERR no such key")
	}
	return
}

func fnRenameNx(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	srcKeyName := args["key"].(string)
	destKeyName := args["newkey"].(string)

	result := ctx.dsc.move(srcKeyName, destKeyName, ctx.dsc.ds, false)
	if result == RESULT_COMPLETED {
		output.data = respInt(1)
	} else if result == RESULT_DESTINATION_EXISTS {
		output.data = respInt(0)
	} else {
		output.data = respErrorString("ERR no such key")
	}
	return
}

func fnScan(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	cursor := args["cursor"].(int64)
	match, _ := args["pattern"].(string)
	count, countSpecified := args["count"].(int64)
	typeOption, _ := args["type"].(string)

	if countSpecified {
		if count < 1 {
			output.data = rstrSyntaxError
			return
		}
	} else {
		count = 10
	}

	requiredFlags := storeKeyTypeFlag(typeOption)
	if requiredFlags == 0 && typeOption != "" {
		requiredFlags = bitflags(0xFFFF) // forced no match
	}

	output = ctx.dsc.scan(uint32(cursor), match, int(count), requiredFlags)
	return
}

func fnTouch(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyNames := args["key"].([]any)

	count := 0
	for _, k := range keyNames {
		if ctx.dsc.touch(k.(string)) {
			count++
		}
	}

	output.data = respInt(count)
	return
}

func fnType(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	output.data = respSimpleString(ctx.dsc.getKeyType(keyName))
	return
}

func fnQuit(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output.data = rstrOK
	return
}

func fnCommandHelp(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	a := respArray{}

	lines := strings.Split(strings.ReplaceAll(cmdHelpText, "\r", ""), "\n")
	for _, line := range lines {
		rv := respValue{data: respSimpleString(line)}
		a = append(a, rv)
	}

	output.data = a
	return
}

func fnCommandCount(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output = ctx.cd.cmdCount()
	return
}

func fnCommandDocs(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	cmdNames := args["command-name"]

	var filter map[string]struct{}
	if cmdNames != nil {
		filter = map[string]struct{}{}
		a := cmdNames.([]any)
		for _, name := range a {
			filter[name.(string)] = struct{}{}
		}
	}

	output = ctx.cd.cmdDocs(filter)
	return
}

func fnCommandGetKeys(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output = ctx.cd.cmdGetKeys(ctx.rawArgs[2:], false)
	return
}

func fnCommandGetKeysAndFlags(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output = ctx.cd.cmdGetKeys(ctx.rawArgs[2:], true)
	return
}

func fnCommandInfo(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	cmdNames := args["command-name"]

	var filter map[string]struct{}
	if cmdNames != nil {
		filter = map[string]struct{}{}
		a := cmdNames.([]any)
		for _, name := range a {
			filter[name.(string)] = struct{}{}
		}
	}

	output = ctx.cd.cmdInfo(filter)
	return
}

func fnCommandList(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	module, _ := args["filterby.module-name"].(string)
	if module != "" {
		output.data = respArray{}
		return
	}

	var aclcat, pattern string
	aclcat, _ = args["filterby.category"].(string)
	pattern, _ = args["filterby.pattern"].(string)

	output = ctx.cd.cmdList(aclcat, pattern)
	return
}

func fnSort(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	sourceKeyName := args["key"].(string)
	byPattern, _ := args["by"].(string)
	offset_count, hasOffset := args["offset_count"].(map[string]any)
	getPatternsAny, _ := args["get"].([]any)
	_, isDesc := args["order.desc"]
	_, isAlpha := args["sorting"] // this name may be a redis bug
	destKeyName, _ := args["destination"].(string)

	start := -1
	count := -1
	if hasOffset {
		offset64, _ := offset_count["offset"].(int64)
		count64, _ := offset_count["count"].(int64)

		start = int(offset64)
		count = int(count64)
	}

	getPatterns := make([]string, 0, len(getPatternsAny))
	for _, getPattern := range getPatternsAny {
		str := getPattern.(string)
		getPatterns = append(getPatterns, str)
	}

	output = ctx.dsc.sort(sourceKeyName, byPattern, destKeyName, start, count, getPatterns, hasOffset, isDesc, isAlpha)
	return
}

func fnFlushAll(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	ctx.cs.dss.flushAll()
	ctx.cs.selectDb(ctx.cs.selectedDb, true)
	output.data = rstrOK
	return
}

func fnFlushDb(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	ctx.cs.dss.flushDb(ctx.cs.selectedDb)
	ctx.cs.selectDb(ctx.cs.selectedDb, true)
	output.data = rstrOK
	return
}
