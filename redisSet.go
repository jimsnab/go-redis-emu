package redisemu

func fnSGet(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	memberName := args["member"].(string)

	str, valid := ctx.dsc.getSetMember(keyName, memberName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_EXISTS {
		output.data = respBulkString(str)
	}
	return
}

func setAddCommon(ctx *cmdContext, args map[string]any, options bitflags) (output respValue, err error) {
	keyName := args["key"].(string)
	members := args["member"].([]any)

	memberNames := make([]string, 0, len(members))

	for _, m := range members {
		memberNames = append(memberNames, m.(string))
	}

	added, wrongType := ctx.dsc.setSetMembers(keyName, memberNames)
	if wrongType {
		output.data = wrongTypeError
	} else {
		output.data = respInt(added)
	}
	return
}

func fnSAdd(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return setAddCommon(ctx, args, 0)
}

func fnSCard(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	count, wrongType := ctx.dsc.getSetCount(keyName)
	if wrongType {
		output.data = wrongTypeError
	} else {
		output.data = respInt(count)
	}
	return
}

func fnSDel(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	members := args["member"].([]any)

	memberNames := make([]string, 0, len(members))
	for _, memberName := range members {
		memberNames = append(memberNames, memberName.(string))
	}

	deleted, wrongType := ctx.dsc.deleteSetMembers(keyName, memberNames)
	if wrongType {
		output.data = wrongTypeError
	} else {
		output.data = respInt(deleted)
	}
	return
}

func fnSExists(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	memberName := args["member"].(string)

	_, valid := ctx.dsc.getSetMember(keyName, memberName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_EXISTS {
		output.data = respInt(1)
	} else {
		output.data = respInt(0)
	}
	return
}

func fnSGetAll(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	val, valid := ctx.dsc.getHashTable(keyName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else {
		output = val
	}
	return
}

func fnSDiff(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyNames := args["key"].([]any)

	strs := make([]string, 0, len(keyNames)-1)
	for i := 1; i < len(keyNames); i++ {
		strs = append(strs, keyNames[i].(string))
	}
	output = ctx.dsc.diffSet(keyNames[0].(string), strs...)
	return
}

func fnSDiffStore(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	destination := args["destination"].(string)
	keyNames := args["key"].([]any)

	strs := make([]string, 0, len(keyNames)-1)
	for i := 1; i < len(keyNames); i++ {
		strs = append(strs, keyNames[i].(string))
	}
	output = ctx.dsc.diffSetStore(destination, keyNames[0].(string), strs...)
	return
}

func fnSInter(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyNames := args["key"].([]any)

	strs := make([]string, 0, len(keyNames)-1)
	for i := 1; i < len(keyNames); i++ {
		strs = append(strs, keyNames[i].(string))
	}
	output = ctx.dsc.intersectSet(keyNames[0].(string), strs...)
	return
}

func fnSInterCard(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyNames := args["key"].([]any)
	limit64, _ := args["limit"].(int64)
	numkeys64 := args["numkeys"].(int64)

	numkeys := int(numkeys64)
	if numkeys < len(keyNames) {
		output.data = rstrSyntaxError
		return
	}
	if numkeys > len(keyNames) {
		output.data = rstrNumKeysGreater
		return
	}

	strs := make([]string, 0, len(keyNames))
	for i := 0; i < len(keyNames); i++ {
		strs = append(strs, keyNames[i].(string))
	}
	output = ctx.dsc.intersectSetCount(int(limit64), strs...)
	return
}

func fnSInterStore(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	destination := args["destination"].(string)
	keyNames := args["key"].([]any)

	strs := make([]string, 0, len(keyNames)-1)
	for i := 1; i < len(keyNames); i++ {
		strs = append(strs, keyNames[i].(string))
	}
	output = ctx.dsc.intersectSetStore(destination, keyNames[0].(string), strs...)
	return
}

func fnSMembers(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	members, wrongType := ctx.dsc.getSetMembers(keyName)
	if wrongType {
		output.data = wrongTypeError
	} else {
		output = nativeValueToResp(members)
	}
	return
}

func fnSMIsMember(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	members, _ := args["member"].([]any)

	memberNames := make([]string, 0, len(members))
	for _, keyObj := range members {
		memberNames = append(memberNames, keyObj.(string))
	}

	output = ctx.dsc.setHasMembers(keyName, memberNames...)
	return
}

func fnSMSet(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output, err = hsetCommon(ctx, args, 0)
	if !output.isErrorType() {
		output.data = respSimpleString(rstrOK)
	}
	return
}

func fnSRandField(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	options, _ := args["options"].(map[string]any)
	var withValues bool
	var c *int
	var c32 int

	if options != nil {
		count, hasCount := options["count"].(int64)
		if hasCount {
			c32 = int(count)
			c = &c32
		}
		_, withValues = options["withvalues"]
	}
	output = ctx.dsc.getHashTableRandField(keyName, c, withValues)
	return
}

func fnSScan(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	cursor := args["cursor"].(int64)
	match, _ := args["pattern"].(string)
	count, countSpecified := args["count"].(int64)

	if countSpecified {
		if count < 1 {
			output.data = rstrSyntaxError
			return
		}
	} else {
		count = 10
	}

	output = ctx.dsc.setScan(keyName, uint32(cursor), match, int(count))
	return
}

func fnSSetNx(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output, err = hsetCommon(ctx, args, SET_NOT_EXIST)
	return
}

func fnSStrLen(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	memberName := args["member"].(string)

	str, valid := ctx.dsc.getSetMember(keyName, memberName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_EXISTS {
		output.data = respInt(len(str))
	} else {
		output.data = respInt(0)
	}
	return
}

func fnSVals(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	values, wrongType := ctx.dsc.getHashTableValues(keyName)
	if wrongType {
		output.data = wrongTypeError
	} else {
		output = nativeValueToResp(values)
	}
	return
}

func fnSUnion(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyNames := args["key"].([]any)

	strs := make([]string, 0, len(keyNames)-1)
	for i := 1; i < len(keyNames); i++ {
		strs = append(strs, keyNames[i].(string))
	}
	output = ctx.dsc.unionSet(keyNames[0].(string), strs...)
	return
}

func fnSUnionStore(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	destination := args["destination"].(string)
	keyNames := args["key"].([]any)

	strs := make([]string, 0, len(keyNames)-1)
	for i := 1; i < len(keyNames); i++ {
		strs = append(strs, keyNames[i].(string))
	}
	output = ctx.dsc.unionSetStore(destination, keyNames[0].(string), strs...)
	return
}

func fnSIsMember(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	member := args["member"].(string)

	output = ctx.dsc.setHasMember(keyName, member)
	return
}

func fnSMove(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	source := args["source"].(string)
	destination := args["destination"].(string)
	member := args["member"].(string)

	output = ctx.dsc.setMove(source, destination, member)
	return
}

func fnSRandMember(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	count64, countSpecified := args["count"].(int64)

	var countPtr *int
	count := int(count64)
	if countSpecified {
		countPtr = &count
	}

	output = ctx.dsc.getSetRandMember(keyName, countPtr)
	return
}

func fnSRem(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	membersArray := args["member"].([]any)

	members := make([]string, 0, len(membersArray))
	for _, member := range membersArray {
		members = append(members, member.(string))
	}
	output = ctx.dsc.setRemove(keyName, members)
	return
}
