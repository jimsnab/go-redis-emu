package redisemu

func fnHGet(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	fieldName := args["field"].(string)

	str, valid := ctx.dsc.getHashTableField(keyName, fieldName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_EXISTS {
		output.data = respBulkString(str)
	}
	return
}

func hsetCommon(ctx *cmdContext, args map[string]any, options bitflags) (output respValue, err error) {
	keyName := args["key"].(string)
	pairs, hasPairs := args["data"].([]any)

	var fieldNames, values []string
	if !hasPairs {
		fieldNames = []string{args["field"].(string)}
		values = []string{args["value"].(string)}
	} else {
		fieldNames = make([]string, 0, len(pairs))
		values = make([]string, 0, len(pairs))

		for _, pair := range pairs {
			m := pair.(map[string]any)
			fieldNames = append(fieldNames, m["field"].(string))
			values = append(values, m["value"].(string))
		}
	}

	added, wrongType := ctx.dsc.setHashTableFields(keyName, fieldNames, values)
	if wrongType {
		output.data = wrongTypeError
	} else {
		output.data = respInt(added)
	}
	return
}

func fnHSet(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return hsetCommon(ctx, args, 0)
}

func fnHDel(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	fields := args["field"].([]any)

	fieldNames := make([]string, 0, len(fields))
	for _, fieldName := range fields {
		fieldNames = append(fieldNames, fieldName.(string))
	}

	deleted, wrongType := ctx.dsc.deleteHashTableFields(keyName, fieldNames)
	if wrongType {
		output.data = wrongTypeError
	} else {
		output.data = respInt(deleted)
	}
	return
}

func fnHExists(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	fieldName := args["field"].(string)

	_, valid := ctx.dsc.getHashTableField(keyName, fieldName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_EXISTS {
		output.data = respInt(1)
	} else {
		output.data = respInt(0)
	}
	return
}

func fnHGetAll(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	val, valid := ctx.dsc.getHashTable(keyName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else {
		output = val
	}
	return
}

func fnHIncrBy(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	fieldName := args["field"].(string)
	delta := args["increment"].(int64)

	result, valid := ctx.dsc.fieldAddInt(keyName, fieldName, delta)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_WRONG_FORMAT || valid == VALUE_OVERFLOW {
		output.data = respErrorString("ERR value is not an integer or out of range")
	} else {
		output.data = respInt(result)
	}
	return
}

func fnHIncrByFloat(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	fieldName := args["field"].(string)
	delta := args["increment"].(float64)

	result, valid := ctx.dsc.fieldAddFloat(keyName, fieldName, delta)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_WRONG_FORMAT {
		output.data = respErrorString("ERR value is not an integer or out of range")
	} else if valid == VALUE_OVERFLOW {
		output.data = respErrorString("ERR increment would produce NaN or Infinity")
	} else {
		output.data = respDouble(result)
	}
	return
}

func fnHKeys(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	fields, wrongType := ctx.dsc.getHashTableFields(keyName)
	if wrongType {
		output.data = wrongTypeError
	} else {
		output = nativeValueToResp(fields)
	}
	return
}

func fnHLen(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	count, wrongType := ctx.dsc.getHashTableCount(keyName)
	if wrongType {
		output.data = wrongTypeError
	} else {
		output.data = respInt(count)
	}
	return
}

func fnHMGet(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	fields, _ := args["field"].([]any)

	fieldNames := make([]string, 0, len(fields))
	for _, keyObj := range fields {
		fieldNames = append(fieldNames, keyObj.(string))
	}

	vals, wrongType := ctx.dsc.getHashTableFieldValues(keyName, fieldNames...)
	if wrongType {
		output.data = wrongTypeError
		return
	}

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

func fnHMSet(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output, err = hsetCommon(ctx, args, 0)
	if !output.isErrorType() {
		output.data = respSimpleString(rstrOK)
	}
	return
}

func fnHRandField(ctx *cmdContext, args map[string]any) (output respValue, err error) {
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

func fnHScan(ctx *cmdContext, args map[string]any) (output respValue, err error) {
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

	output = ctx.dsc.hashTableScan(keyName, uint32(cursor), match, int(count))
	return
}

func fnHSetNx(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output, err = hsetCommon(ctx, args, SET_NOT_EXIST)
	return
}

func fnHStrLen(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	fieldName := args["field"].(string)

	str, valid := ctx.dsc.getHashTableField(keyName, fieldName)
	if valid == VALUE_WRONG_TYPE {
		output.data = wrongTypeError
	} else if valid == VALUE_EXISTS {
		output.data = respInt(len(str))
	} else {
		output.data = respInt(0)
	}
	return
}

func fnHVals(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)

	values, wrongType := ctx.dsc.getHashTableValues(keyName)
	if wrongType {
		output.data = wrongTypeError
	} else {
		output = nativeValueToResp(values)
	}
	return
}
