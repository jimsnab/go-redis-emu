package goredisemu

import (
	"fmt"
	"time"
)

func pushWorker(args map[string]any, fn func(keyName string, values [][]byte) (output respValue)) (output respValue, err error) {
	keyName := args["key"].(string)
	elements := args["element"].([]any)

	values := make([][]byte, 0, len(elements))
	for _, e := range elements {
		values = append(values, []byte(e.(string)))
	}

	output = fn(keyName, values)
	return
}

func fnLPush(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return pushWorker(args, ctx.dsc.lpush)
}

func popWorker(args map[string]any, fn func(keyName string, count int) (values [][]byte, err *respErrorString)) (output respValue, err error) {
	keyName := args["key"].(string)
	count, countSpecified := args["count"].(int64)

	if !countSpecified {
		count = 1
	} else if count < 0 {
		output.data = respErrorString("ERR value is out of range, must be positive")
		return
	}

	values, fnErr := fn(keyName, int(count))
	if fnErr != nil {
		output.data = *fnErr
		return
	}

	if !countSpecified {
		if len(values) != 1 {
			return
		}
		output.data = respBulkString(string(values[0]))
	} else {
		if len(values) == 0 && count > 0 {
			return
		}

		strList := make([]any, 0, len(values))
		for _, v := range values {
			strList = append(strList, string(v))
		}

		output = nativeValueToResp(strList)
	}
	return
}

func popMultiKeyWorker(ctx *cmdContext, args map[string]any, fn func(keyName string, count int) (values [][]byte, err *respErrorString)) (output respValue) {
	timeout := args["timeout"].(float64)
	keyNamesArg := args["key"].([]any)

	keyNames := make([]string, 0, len(keyNamesArg))
	for _, keyName := range keyNamesArg {
		keyNames = append(keyNames, keyName.(string))
	}

	timeoutNs := int64(timeout * float64(time.Second))
	output = blockOnListChangeMultiKey(
		ctx, keyNames, timeoutNs,
		func() (output respValue) {
			for _, keyName := range keyNames {
				values, fnErr := fn(keyName, 1)
				if fnErr != nil {
					output.data = *fnErr
					return
				} else if len(values) == 1 {
					strList := []string{keyName, string(values[0])}
					output = nativeValueToResp(strList)
					return
				}
			}
			return
		})
	return
}

func fnLPop(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return popWorker(args, ctx.dsc.lpop)
}

func fnRPush(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return pushWorker(args, ctx.dsc.rpush)
}

func fnRPop(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return popWorker(args, ctx.dsc.rpop)
}

func fnLIndex(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	index := args["index"].(int64)

	output = ctx.dsc.lindex(keyName, int(index))
	return
}

func fnLInsert(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	_, before := args["where.before"] // caller's arg parsing ensures either 'before' or 'after' is specified
	pivot := args["pivot"].(string)
	element := args["element"].(string)

	output = ctx.dsc.linsert(keyName, before, pivot, element)
	return
}

func fnLLen(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	output = ctx.dsc.llen(keyName)
	return
}

func fnLMove(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	srcKeyName := args["source"].(string)
	destKeyName := args["destination"].(string)

	// caller's arg parsing ensures either 'left' or 'right' is specified for each src & dest
	_, srcLeft := args["wherefrom.left"]
	_, destLeft := args["whereto.left"]

	output = ctx.dsc.lmove(srcKeyName, destKeyName, srcLeft, destLeft)
	return
}

func fnLRange(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	start := args["start"].(int64)
	stop := args["stop"].(int64)

	output = ctx.dsc.lrange(keyName, int(start), int(stop))
	return
}

func fnLMPop(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	numkeys := args["numkeys"].(int64)
	keyNames := args["key"].([]any)
	_, left := args["where.left"]
	count, hasCount := args["count"].(int64)

	if len(keyNames) != int(numkeys) {
		output.data = rstrSyntaxError
		return
	}

	strKeyNames := make([]string, 0, len(keyNames))
	for _, k := range keyNames {
		strKeyNames = append(strKeyNames, k.(string))
	}

	if !hasCount {
		count = 1
	} else if count < 1 {
		output.data = rstrSyntaxError
		return
	}

	output = ctx.dsc.lmpop(strKeyNames, left, int(count))
	return
}

func fnLPos(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	element := args["element"].(string)
	rank, hasRank := args["rank"].(int64)
	count, hasCount := args["num-matches"].(int64)
	maxLength, hasMaxLength := args["len"].(int64)

	forward := true
	if hasRank {
		if rank == 0 {
			output.data = respErrorString("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list")
			return
		}
		if rank < 0 {
			rank = -rank
			forward = false
		}
	}

	if hasCount {
		if count < 0 {
			output.data = respErrorString("ERR COUNT can't be negative")
			return
		}
	} else {
		count = 1
	}

	if hasMaxLength {
		if maxLength < 0 {
			output.data = respErrorString("ERR MAXLEN can't be negative")
			return
		}
	}

	matches, errText := ctx.dsc.lpos(keyName, element, forward, int(rank), int(count), int(maxLength))
	if errText != nil {
		output.data = *errText
		return
	}

	if hasCount {
		output = nativeValueToResp(matches)
	} else {
		if len(matches) > 0 {
			output.data = respInt(matches[0])
		}
	}
	return
}

func fnLPushX(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return pushWorker(args, ctx.dsc.lpushx)
}

func fnRPushX(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	return pushWorker(args, ctx.dsc.rpushx)
}

func fnLRem(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	count := args["count"].(int64)
	element := args["element"].(string)

	removed, errText := ctx.dsc.lremove(keyName, element, int(count))
	if errText != nil {
		output.data = *errText
		return
	}

	output.data = respInt(removed)
	return
}

func fnLSet(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	index := args["index"].(int64)
	element := args["element"].(string)

	output = ctx.dsc.lset(keyName, element, int(index))
	return
}

func fnLTrim(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	keyName := args["key"].(string)
	start := args["start"].(int64)
	stop := args["stop"].(int64)

	output = ctx.dsc.ltrim(keyName, int(start), int(stop))
	return
}

func fnRPopLPush(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	srcKeyName := args["source"].(string)
	destKeyName := args["destination"].(string)

	output = ctx.dsc.lmove(srcKeyName, destKeyName, false, true)
	return
}

func blockOnListChange(ctx *cmdContext, keyName string, timeoutNs int64, op func() (output respValue)) (output respValue) {
	return blockOnListChangeWorker(
		ctx,
		timeoutNs,
		op,
		func() string { return fmt.Sprintf("key '%s'", keyName) },
		func() *wakeSignal { return ctx.dsc.ds.enterListBlock(keyName) },
	)
}

func blockOnListChangeMultiKey(ctx *cmdContext, keyNames []string, timeoutNs int64, op func() (output respValue)) (output respValue) {
	return blockOnListChangeWorker(
		ctx,
		timeoutNs,
		op,
		func() string { return fmt.Sprintf("keys %s", keyNames) },
		func() *wakeSignal { return ctx.dsc.ds.enterListMultiBlock(keyNames) },
	)
}

func blockOnListChangeWorker(
	ctx *cmdContext,
	timeoutNs int64,
	op func() (output respValue),
	keyNameStr func() string,
	blockFn func() *wakeSignal,
) (output respValue) {

	// initial non blocking call
	output = op()

	if output.data != nil || ctx.multi {
		return
	}

	// wait for the list to change, unless the client timeout is reached,
	// or the client connection is closing
	var end time.Time
	if timeoutNs == 0 {
		end = maxTime
		ctx.l.Tracef("waiting for %s to get a list item", keyNameStr())
	} else {
		end = time.Now().Add(time.Duration(timeoutNs) * time.Nanosecond)
		ctx.l.Tracef("waiting for %s to get a list item until %s", keyNameStr(), end.Format(time.StampMilli))
	}

	ws := blockFn()
	defer ctx.dsc.ds.leaveListBlock(ws)

	// with notification registered, try operation again immediately
	output = op()
	if output.data != nil {
		ctx.l.Tracef("list at %s got an item before waiting on the channel", keyNameStr())
		return
	}

	// still need data, so loop until data comes or a cancel condition exists
	for {
		if func() bool {
			timeout := time.Until(end)
			waitTimer := time.NewTimer(timeout)
			defer waitTimer.Stop()

			unblockCh := ctx.cs.capture()
			defer ctx.cs.releaseCapture()

			select {
			case reason := <-unblockCh:
				// abort this command - connectivity lost, or explicitly unblocked via another client
				ctx.l.Tracef("client connectivity event aborts wait for list %s", keyNameStr())
				if reason.isError {
					output.data = respErrorString(reason.reason)
				}
				return true
			case <-waitTimer.C:
				// the block timed out, fail this command
				ctx.l.Tracef("wait timer for %s has expired", keyNameStr())
				return true
			case <-ws.ready:
				// acquire completed
				return false
			}
		}() {
			// select above returned true - the command is done
			return
		}

		// list element probably exists and the operation will succeed
		output = op()
		if output.data != nil {
			return
		}
		// a different client obtained the list element before this client could, so try again
	}
}

func fnBLMove(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	srcKeyName := args["source"].(string)
	timeout := args["timeout"].(float64)

	timeoutNs := int64(timeout * float64(time.Second))
	output = blockOnListChange(ctx, srcKeyName, timeoutNs, func() (output respValue) {
		output, _ = fnLMove(ctx, args)
		return
	})
	return
}

func fnBLMPop(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	timeout := args["timeout"].(float64)
	keyNamesArg := args["key"].([]any)

	keyNames := make([]string, 0, len(keyNamesArg))
	for _, keyName := range keyNamesArg {
		keyNames = append(keyNames, keyName.(string))
	}

	timeoutNs := int64(timeout * float64(time.Second))
	output = blockOnListChangeMultiKey(ctx, keyNames, timeoutNs, func() (output respValue) {
		output, _ = fnLMPop(ctx, args)
		return
	})
	return
}

func fnBLPop(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output = popMultiKeyWorker(ctx, args, ctx.dsc.lpop)
	return
}

func fnBRPop(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output = popMultiKeyWorker(ctx, args, ctx.dsc.rpop)
	return
}

func fnBRPopLPush(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	timeout := args["timeout"].(float64)
	srcKeyName := args["source"].(string)

	timeoutNs := int64(timeout * float64(time.Second))
	output = blockOnListChange(ctx, srcKeyName, timeoutNs, func() (output respValue) {
		output, _ = fnRPopLPush(ctx, args)
		return
	})
	return
}
