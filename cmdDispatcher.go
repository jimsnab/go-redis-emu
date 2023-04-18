package redisemu

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/jimsnab/go-lane"
)

type (
	cmdContext struct {
		l        lane.Lane
		cmdName  string
		cmdToken string
		cmd      *redisCommand
		cs       *clientState
		dsc      *dataStoreCommand
		cd       *cmdDispatcher
		args     map[string]any
		rawArgs  respArray
		multi    bool
	}
	cmdHandler func(ctx *cmdContext, args map[string]any) (respValue, error)

	cmdDispatcher struct {
		cmds      redisCommands
		infoTable *redisInfoTable
		dss       *dataStoreSet
		active    map[string]*redisCommand
		handlers  map[string]cmdHandler
	}
)

const strQueued = "QUEUED"
const strOK = "OK"
const rstrQueued = respSimpleString(strQueued)
const rstrOK = respSimpleString(strOK)
const rstrSyntaxError = respErrorString("ERR Syntax error")
const rstrNumKeysGreater = respErrorString("ERR Number of keys can't be greater than number of args")

var errInvalidCmdInput = respErrorString("ERR Invalid command input")
var errMissingCmdName = respErrorString("ERR Missing command name")

var handlerTable = map[string]cmdHandler{
	"append":                  fnAppend,
	"bitcount":                fnBitCount,
	"bitfield":                fnBitfield,
	"bitfield_ro":             fnBitfield,
	"bitop":                   fnBitOp,
	"bitpos":                  fnBitPos,
	"blmove":                  fnBLMove,
	"blmpop":                  fnBLMPop,
	"blpop":                   fnBLPop,
	"brpop":                   fnBRPop,
	"brpoplpush":              fnBRPopLPush,
	"client|getname":          fnClientGetName,
	"client|id":               fnClientGetId,
	"client|info":             fnClientInfo,
	"client|list":             fnClientList,
	"client|kill":             fnClientKill,
	"client|no-evict":         fnClientNoEvict,
	"client|setname":          fnClientSetName,
	"client|unblock":          fnClientUnblock,
	"command|count":           fnCommandCount,
	"command|docs":            fnCommandDocs,
	"command|getkeys":         fnCommandGetKeys,
	"command|getkeysandflags": fnCommandGetKeysAndFlags,
	"command|help":            fnCommandHelp,
	"command|info":            fnCommandInfo,
	"command|list":            fnCommandList,
	"copy":                    fnCopy,
	"decr":                    fnDecr,
	"decrby":                  fnDecrBy,
	"del":                     fnDel,
	"discard":                 fnDiscard,
	"dump":                    fnDump,
	"echo":                    fnEcho,
	"exec":                    fnExec,
	"exists":                  fnExists,
	"expire":                  fnExpire,
	"expireat":                fnExpireAt,
	"expiretime":              fnExpireTime,
	"flushall":                fnFlushAll,
	"flushdb":                 fnFlushDb,
	"get":                     fnGet,
	"getbit":                  fnGetBit,
	"getdel":                  fnGetDel,
	"getex":                   fnGetEx,
	"getrange":                fnGetRange,
	"getset":                  fnGetSet,
	"incr":                    fnIncr,
	"incrby":                  fnIncrBy,
	"incrbyfloat":             fnIncrByFloat,
	"info":                    fnInfo,
	"hdel":                    fnHDel,
	"hexists":                 fnHExists,
	"hget":                    fnHGet,
	"hgetall":                 fnHGetAll,
	"hincrby":                 fnHIncrBy,
	"hincrbyfloat":            fnHIncrByFloat,
	"hkeys":                   fnHKeys,
	"hlen":                    fnHLen,
	"hmget":                   fnHMGet,
	"hmset":                   fnHMSet,
	"hrandfield":              fnHRandField,
	"hscan":                   fnHScan,
	"hset":                    fnHSet,
	"hsetnx":                  fnHSetNx,
	"hstrlen":                 fnHStrLen,
	"hvals":                   fnHVals,
	"lcs":                     fnLcs,
	"lindex":                  fnLIndex,
	"linsert":                 fnLInsert,
	"llen":                    fnLLen,
	"lmove":                   fnLMove,
	"lmpop":                   fnLMPop,
	"lpush":                   fnLPush,
	"lpushx":                  fnLPushX,
	"lpop":                    fnLPop,
	"lpos":                    fnLPos,
	"lrange":                  fnLRange,
	"lrem":                    fnLRem,
	"lset":                    fnLSet,
	"ltrim":                   fnLTrim,
	"mget":                    fnMget,
	"mset":                    fnMset,
	"msetnx":                  fnMset,
	"multi":                   fnMulti,
	"keys":                    fnKeys,
	"pexpire":                 fnPExpire,
	"pexpireat":               fnPExpireAt,
	"pexpiretime":             fnPExpireTime,
	"persist":                 fnPersist,
	"psetex":                  fnSet,
	"ping":                    fnPing,
	"pttl":                    fnPTtl,
	"quit":                    fnQuit,
	"randomkey":               fnRandomKey,
	"rename":                  fnRename,
	"renamenx":                fnRenameNx,
	"restore":                 fnRestore,
	"rpush":                   fnRPush,
	"rpushx":                  fnRPushX,
	"rpop":                    fnRPop,
	"rpoplpush":               fnRPopLPush,
	"sadd":                    fnSAdd,
	"scard":                   fnSCard,
	"scan":                    fnScan,
	"sdiff":                   fnSDiff,
	"sdiffstore":              fnSDiffStore,
	"select":                  fnSelect,
	"set":                     fnSet,
	"setbit":                  fnSetBit,
	"setex":                   fnSet,
	"setnx":                   fnSet,
	"setrange":                fnSetRange,
	"sinter":                  fnSInter,
	"sintercard":              fnSInterCard,
	"sinterstore":             fnSInterStore,
	"sismember":               fnSIsMember,
	"smembers":                fnSMembers,
	"smismember":              fnSMIsMember,
	"smove":                   fnSMove,
	"sort":                    fnSort,
	"srandmember":             fnSRandMember,
	"srem":                    fnSRem,
	"strlen":                  fnStrLen,
	"substr":                  fnGetRange,
	"sscan":                   fnSScan,
	"sunion":                  fnSUnion,
	"sunionstore":             fnSUnionStore,
	"touch":                   fnTouch,
	"ttl":                     fnTtl,
	"type":                    fnType,
	"unlink":                  fnUnlink,
	"unwatch":                 fnUnwatch,
	"watch":                   fnWatch,
}

var unqueuedCmdTable = map[string]bool{
	"multi":   true,
	"discard": true,
	"exec":    true,
	"watch":   true,
}

func (ctx *cmdContext) info(cs *clientState) string {
	// take complete ownership of the data store
	ctx.dsc.acquireExclusive()
	defer ctx.dsc.releaseExclusive()

	return ctx.infoUnlocked(cs)
}

func (ctx *cmdContext) infoUnlocked(cs *clientState) string {
	info := cs.client.ClientInfo()

	multi := -1
	if ctx.multi {
		multi = 1
	}

	var flags strings.Builder

	if cs.isBlocked() {
		flags.WriteRune('b')
	}
	if cs.client.IsCloseRequested() {
		flags.WriteRune('c')
	}
	if isAbortedExecUnlocked(cs) {
		flags.WriteRune('d')
	}
	if cs.isMultiInProgress() {
		flags.WriteRune('x')
	}

	if flags.Len() == 0 {
		flags.WriteString("N")
	}

	info = append(info,
		fmt.Sprintf("id=%d", cs.id),
		"name="+cs.name,
		fmt.Sprintf("db=%d", cs.selectedDb),
		fmt.Sprintf("multi=%d", multi),
		fmt.Sprintf("flags=%s", flags.String()),
		"cmd="+ctx.cmdToken,
		"user="+cs.user,
		fmt.Sprintf("resp=%d", cs.respVersion),
	)

	var sb strings.Builder
	for _, v := range info {
		if sb.Len() > 0 {
			sb.WriteRune(' ')
		}
		sb.WriteString(v)
	}
	sb.WriteRune('\n')

	return sb.String()
}

func newCmdDispatcher(cmds redisCommands, info *redisInfoTable, dss *dataStoreSet) *cmdDispatcher {
	cd := &cmdDispatcher{
		cmds:      cmds,
		infoTable: info,
		dss:       dss,
		active:    map[string]*redisCommand{},
		handlers:  map[string]cmdHandler{},
	}

	for name, fn := range handlerTable {
		cd.addHandler(name, fn)
	}

	return cd
}

func (cd *cmdDispatcher) addHandlerWorker(cmds redisCommands, cmdInfo *redisInfoTable, cmdToken string, handler cmdHandler) (subCmds redisCommands) {
	cutPoint := strings.LastIndex(cmdToken, "|")
	if cutPoint >= 0 {
		cmds = cd.addHandlerWorker(cmds, cmdInfo, cmdToken[0:cutPoint], nil)
	}
	cmd, exists := cmds[cmdToken]
	if !exists {
		panic(fmt.Sprintf("command %s is not defined; can't add handler", cmdToken))
	}

	_, exists = cmdInfo.table[cmdToken]
	if !exists {
		panic(fmt.Sprintf("command %s does not have info; can't add hanler", cmdToken))
	}

	if storedHandler, exists := cd.active[cmdToken]; exists && storedHandler != nil && handler != nil {
		panic(fmt.Sprintf("command %s already has a handler", cmdToken))
	}

	cd.active[cmdToken] = cmd
	cd.handlers[cmdToken] = handler
	return cmd.Subcommands
}

func (cd *cmdDispatcher) addHandler(cmdToken string, handler cmdHandler) {
	cd.addHandlerWorker(cd.cmds, cd.infoTable, cmdToken, handler)
}

func (cd *cmdDispatcher) prepare(cs *clientState, input respValue) (ctx *cmdContext, response any) {
	l := cs.l

	traceJson, _ := json.Marshal(input.toNative())
	l.Tracef("client %d dispatching %s", cs.id, string(traceJson))

	args, valid := input.data.(respArray)
	if !valid || len(args) == 0 {
		l.Info("command request was not an argument array")
		response = errInvalidCmdInput
		return
	}

	cmdNameArg, valid := args[0].toString()
	if !valid {
		l.Info("command name argument was an invalid data type")
		response = errMissingCmdName
		return
	}

	cmdNameLower := strings.ToLower(cmdNameArg)

	cmd, exists := cd.active[cmdNameLower]
	if !exists {
		l.Infof("unsupported command '%s'", cmdNameLower)

		joinedArgs := ""
		for i := 1; i < len(args); i++ {
			joinedArgs += "`"
			joinedArgs += args[i].String()
			joinedArgs += "`, "
		}

		text := respErrorString(fmt.Sprintf("ERR Unknown command `%s`, with args beginning with: %s", cmdNameArg, joinedArgs))
		response = text
		return
	}

	// parse the input using the command definition
	cmdArgs := args[1:]

	opts := bitflags(0)
	if cmdNameLower == "bitfield" || cmdNameLower == "bitfield_ro" {
		// this command breaks integer parsing rules, and loses command order
		opts |= PARSE_SAVE_INTEGERS_AS_STRINGS | PARSE_ADD_ARG_INDEX_TO_BLOCK
	} else if cmdNameLower == "command" {
		// special case for command getkeys and command getkeysandflags:
		// the command has to be parsed without its arguments, because
		// the redis command spec doesn't have a concept of "any"
		str, _ := cmdArgs[0].toString()
		subcmd := strings.ToLower(str)
		if subcmd == "getkeys" || subcmd == "getkeysandflags" {
			cmdArgs = args[1:2]
		}
	}
	argTable, keywords, cmdToken := parseCommand(cmdNameLower, cmd, opts, cmdArgs.toValues()...)

	if keywords <= 0 {
		t, _ := json.MarshalIndent(cmd, "", "  ")
		l.Info(string(t))

		l.Infof("can't parse arguments for command '%s'", cmdNameLower)
		var text respErrorString
		if keywords == 0 {
			text = respErrorString(fmt.Sprintf("ERR Incorrect or wrong number of arguments for '%s'. Try COMMAND HELP.", cmdNameArg))
		} else {
			text = respErrorString(fmt.Sprintf("ERR unknown subcommand '%s'. Try CLIENT HELP.", cmdToken))
		}
		response = text
		return
	}

	if keywords > 0 {
		var sb strings.Builder
		for i := 0; i < keywords; i++ {
			if sb.Len() > 0 {
				sb.WriteRune(' ')
			}
			text, _ := args[i].toString()
			sb.WriteString(text)
		}
		cmdNameArg = sb.String()
	}

	// prepare a context structure for the handler
	ctx = &cmdContext{
		l:        l,
		cs:       cs,
		dsc:      cs.ds.newDataStoreCommand(),
		cmdName:  cmdNameArg,
		cmdToken: cmdToken,
		cmd:      cmd,
		cd:       cd,
		args:     argTable,
		rawArgs:  args,
	}

	// if multi was specified, queue the command (unless it is a transaction command)
	if cs.cmdQueue != nil {
		ctx.multi = true
		_, multiControl := unqueuedCmdTable[cmdNameLower]
		if !multiControl {
			*cs.cmdQueue = append(*cs.cmdQueue, ctx)
			response = rstrQueued
			return
		}
	}

	return
}

func (cd *cmdDispatcher) dispatch(cs *clientState, input respValue) (output respValue) {
	ctx, response := cd.prepare(cs, input)
	if response != nil {
		output.data = response
		return
	}
	return cd.dispatchHandler(ctx)
}

func (cd *cmdDispatcher) dispatchHandler(ctx *cmdContext) (output respValue) {
	l := ctx.l
	cmdToken := ctx.cmdToken

	// invoke the handler
	handler := cd.handlers[cmdToken]
	if handler == nil {
		l.Tracef("unsupported command '%s' rejected", cmdToken)
		output.data = respErrorString(fmt.Sprintf("ERR Unsupported command '%s'", cmdToken))
		return
	}
	l.Tracef("calling handler for command '%s'", cmdToken)
	result, err := handler(ctx, ctx.args)
	if err != nil {
		l.Warnf("error processing command '%s': %s", cmdToken, err)
		output.data = respErrorString(fmt.Sprintf("ERR Unknown command or wrong number of arguments for '%s'. Try COMMAND HELP.", ctx.cmdName))
		return
	}

	output.data = result.data

	traceJson, _ := json.Marshal(output.toNative())
	l.Tracef("response: %s", string(traceJson))

	return
}

func (cd *cmdDispatcher) cmdDocs(filter map[string]struct{}) (output respValue) {
	a := respArray{}

	keys := []string{}
	for cmdName := range cd.active {
		keys = append(keys, cmdName)
	}
	sort.Strings(keys)

	for _, cmdName := range keys {
		cmd := cd.active[cmdName]
		if cmd == nil {
			panic(fmt.Sprintf("can't get cmd for %s", cmdName))
		}
		if filter != nil {
			if _, exists := filter[cmdName]; !exists {
				continue
			}
		}

		rss := respValue{data: respBulkString(cmdName)}
		a = append(a, rss)
		a = append(a, cmd.respSerialize())
	}

	output.data = a
	return
}

func (cd *cmdDispatcher) cmdCount() (output respValue) {
	output.data = respInt(len(cd.active))
	return
}

func (cd *cmdDispatcher) cmdGetKeys(argArray respArray, includeFlags bool) (output respValue) {
	if len(argArray) == 0 {
		output.data = respErrorString("ERR Invalid number of arguments specified for command")
		return
	}

	args := make([]string, 0, len(argArray))
	for _, arg := range argArray {
		str, ok := arg.toString()
		if !ok {
			panic("command line arg arraytype error")
		}
		args = append(args, str)
	}

	cmdName := args[0]
	cmdNameLower := strings.ToLower(cmdName)
	cmd, exists := cd.active[cmdNameLower]
	if !exists {
		output.data = respErrorString("ERR Invalid command specified")
		return
	}
	info, exists := cd.infoTable.table[cmdNameLower]
	if !exists {
		panic("info missing for command " + cmdNameLower)
	}

	cmdArgs := argArray[1:]
	parsed, keywords, _ := parseCommand(cmdNameLower, cmd, bitflags(PARSE_ADD_TOKEN_INDEX_TO_ARGS), cmdArgs.toValues()...)

	if keywords <= 0 {
		output.data = respErrorString("ERR Invalid number of arguments specified for command")
		return
	}

	keys := []string{}
	flags := []any{}

	for _, keySpec := range info.KeySpecs {
		firstIndex := -1

		switch keySpec.BeginSearch.Type {
		case "index":
			bsi := keySpec.BeginSearch.Spec.(*redisInfoBeginSearchIndex)
			firstIndex = bsi.Index

		case "keyword":
			bsk := keySpec.BeginSearch.Spec.(*redisInfoBeginSearchKeyword)
			indicies, exists := parsed[fmt.Sprintf("%s-index", bsk.Keyword)].([]int)
			if !exists {
				output.data = respErrorString("ERR Invalid number of arguments specified for command")
				return
			}

			// no command reuses a keyword
			if len(indicies) != 1 {
				output.data = respErrorString("ERR Unexpected repetition of token " + bsk.Keyword)
				return
			}
			firstIndex = indicies[0]

		case "unknown":
			output.data = respErrorString("ERR Key info for " + info.Name + "unsupported")
			return
		}

		step := 0
		end := 0

		switch keySpec.FindKeys.Type {
		case "range":
			fkr := keySpec.FindKeys.Spec.(*redisInfoFindKeysRange)
			step = fkr.KeyStep
			end = fkr.LastKey
			if end < 0 {
				if fkr.Limit < 2 {
					end = len(argArray)
				} else {
					width := len(argArray) - firstIndex
					end = (width / fkr.Limit) + 1
				}
			} else {
				end = firstIndex + end + 1
			}

		case "keynum":
			fkkn := keySpec.FindKeys.Spec.(*redisInfoFindKeysKeyNum)

			countIndex := firstIndex + fkkn.KeyNumIdx
			count, valid := argArray[countIndex].toInt()
			if !valid {
				output.data = respErrorString("ERR Invalid arguments specified for command")
				return
			}

			firstIndex += fkkn.FirstKey
			step = fkkn.KeyStep
			end = firstIndex + (int(count) * step)
		}

		if firstIndex < 0 || end < firstIndex || step < 1 {
			output.data = respErrorString("ERR Invalid arguments specified for command")
			return
		}

		for i := firstIndex; i < end; i += step {
			if i >= len(args) {
				panic("out of bounds walking args of parsed command")
			}
			keys = append(keys, args[i])
			flags = append(flags, keySpec.Flags)
		}
	}

	if !includeFlags {
		output = nativeValueToResp(keys)
	} else {
		a := make([]any, 0, len(keys))
		for idx, key := range keys {
			inner := make([]any, 0, 2)
			inner = append(inner, key)
			inner = append(inner, flags[idx])
			a = append(a, inner)
		}
		output = nativeValueToResp(a)
	}
	return
}

func (cd *cmdDispatcher) cmdInfo(filter map[string]struct{}) (output respValue) {
	a := respArray{}

	keys := []string{}
	for cmdName := range cd.active {
		keys = append(keys, cmdName)
	}
	sort.Strings(keys)

	for _, cmdName := range keys {
		cmd := cd.active[cmdName]
		if cmd == nil {
			panic(fmt.Sprintf("can't get cmd for %s", cmdName))
		}
		info := cd.infoTable.table[cmdName]
		if info == nil {
			panic(fmt.Sprintf("no info found for cmd %s", cmdName))
		}

		if filter != nil {
			if _, exists := filter[cmdName]; !exists {
				continue
			}
		}

		rss := respValue{data: respBulkString(cmdName)}
		a = append(a, rss)
		a = append(a, info.respSerialize())
	}

	output.data = a
	return
}

func (cd *cmdDispatcher) cmdList(aclcat, pattern string) (output respValue) {
	a := []any{}
	pat := []rune(pattern)

	for name := range cd.active {
		info := cd.infoTable.table[name]
		if aclcat != "" {
			found := false
			for _, ac := range info.AclCategories {
				if ac == aclcat {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		if pattern != "" {
			if !redisGlob(pat, []rune(name)) {
				continue
			}
		}

		a = append(a, name)
	}

	output = nativeValueToResp(a)
	return
}
