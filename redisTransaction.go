package redisemu

func fnWatch(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	if ctx.multi {
		output.data = respErrorString("ERR WATCH inside MULTI is not allowed")
		return
	}

	// capture the current IDs for each key being watched
	keys, _ := args["key"].([]any)
	keyStrs := make([]string, 0, len(keys))
	for _, key := range keys {
		keyStrs = append(keyStrs, key.(string))
	}

	ids := ctx.dsc.getIds(keyStrs...)
	for idx, id := range ids {
		ctx.cs.watches[watchKey{ds: ctx.dsc.ds, key: keyStrs[idx]}] = id
	}

	output.data = rstrOK
	return
}

func fnUnwatch(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	// clear out watch map
	ctx.cs.watches = map[watchKey]uint64{}
	output.data = rstrOK
	return
}

func fnDiscard(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	if ctx.cs.cmdQueue == nil {
		output.data = respErrorString("ERR DISCARD without MULTI")
		return
	}

	// clear out watch map and discard multi command queue
	ctx.cs.watches = map[watchKey]uint64{}
	ctx.cs.cmdQueue = nil
	output.data = rstrOK
	return
}

func isAbortedExecUnlocked(cs *clientState) bool {
	for watch, id := range cs.watches {
		// caller holds exclusive lock, so go directly to the data store for this check
		if watch.ds.hasChangedUnlocked(watch.key, id) {
			return true
		}
	}
	return false
}

func fnExec(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	if ctx.cs.cmdQueue == nil {
		output.data = respErrorString("ERR EXEC without MULTI")
		return
	}

	// take complete ownership of the data store
	ctx.dsc.acquireExclusive()
	defer ctx.dsc.releaseExclusive()

	// maintain in-progress flag
	ctx.cs.setMultiInProgress(true)
	defer ctx.cs.setMultiInProgress(false)

	// check the watches; if anything has changed, return null
	if isAbortedExecUnlocked(ctx.cs) {
		return
	}

	// process all of the queued commands, regardless if one errors
	results := make([]any, 0, len(*ctx.cs.cmdQueue))
	for _, cc := range *ctx.cs.cmdQueue {
		// use the multi command id instead of each queued command's id,
		// so that the commands won't try to acquire a lock that we already own
		cc.dsc.id = ctx.dsc.id
		results = append(results, ctx.cd.dispatchHandler(cc))
	}

	// reset multi state and return the results
	ctx.cs.watches = map[watchKey]uint64{}
	ctx.cs.cmdQueue = nil
	output.data = nativeArrayToResp(results)
	return
}

func fnMulti(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	if ctx.cs.cmdQueue != nil {
		// if the queue array is defined, then multi mode is already active
		output.data = respErrorString("ERR MULTI calls can not be nested")
	} else {
		ctx.cs.cmdQueue = &[]*cmdContext{}
		output.data = rstrOK
	}
	return
}
