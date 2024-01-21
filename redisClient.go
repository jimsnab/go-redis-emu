package redisemu

import (
	"fmt"
	"strings"
	"time"
)

type (
	clientStateEvent struct {
		newState  cxnState
		eventData any
	}

	RedisClient interface {
		ClientInfo() []string
		MatchFilter(filter map[string]string) bool
		RequestClose()
		IsCloseRequested() bool
		ServerAddr() string
		ClientAddr() string
		ServerNow() time.Time
	}
)

func fnEcho(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	message := args["message"].(string)
	output.data = respBulkString(message)
	return
}

func fnPing(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	message, exists := args["message"].(string)
	if exists {
		output.data = respBulkString(message)
	} else {
		output.data = respSimpleString("PONG")
	}
	return
}

func fnClientUnblock(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	id := args["client-id"].(int64)
	_, isError := args["unblock-type.error"]

	clientsMu.Lock()
	defer clientsMu.Unlock()

	client, exists := clients[id]
	if exists {
		reason := ""
		if isError {
			reason = "UNBLOCKED client unblocked via CLIENT UNBLOCK"
		}
		client.unblock(reason, isError)
		output.data = respInt(1)
	} else {
		output.data = respInt(0)
	}

	return
}

func fnClientSetName(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	name := args["connection-name"].(string)
	for _, ch := range name {
		if ch < 33 {
			output.data = respErrorString("ERR Client names cannot contain spaces, newlines or special characters.")
			return
		}
	}

	ctx.cs.name = name
	output.data = rstrOK
	return
}

func fnClientGetName(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	if ctx.cs.name != "" {
		output.data = respBulkString(ctx.cs.name)
	}
	return
}

func fnClientGetId(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	output.data = respInt(ctx.cs.id)
	return
}

func fnClientInfo(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	if ctx.multi {
		output.data = respBulkString(ctx.infoUnlocked(ctx.cs))
	} else {
		output.data = respBulkString(ctx.info(ctx.cs))
	}
	return
}

func fnClientKill(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	closed := 0

	filter := map[string]string{}

	hasOldFormat := false
	includeMe := false

	for _, arg := range ctx.args.order {
		v := ctx.args.mustGet(arg)
		valArray, valid := v.([]any)
		if !valid {
			valArray = []any{v}
		}

		for _, val := range valArray {
			switch arg {
			case "filter.old-format":
				hasOldFormat = true
				filter["addr"] = val.(string)
				closed = -1
			case "filter.new-format.addr": // 'addr' is a workaround name in our modified command docs template, instead of official name 'ip:port'
				filter["addr"] = val.(string)
			case "filter.new-format.client-id":
				filter["id"] = fmt.Sprintf("%v", val)
			case "filter.new-format.client-type.normal":
				filter["type"] = "normal"
			case "filter.new-format.client-type.master":
				filter["type"] = "master"
			case "filter.new-format.client-type.slave":
				filter["type"] = "slave"
			case "filter.new-format.client-type.pubsub":
				filter["type"] = "pubsub"
			case "filter.new-format.client-type.replica":
				filter["type"] = "replica"
			case "filter.new-format.username":
				userName := val.(string)
				_, exists := ctx.cs.dss.getUser(userName)
				if !exists {
					output.data = respErrorString(fmt.Sprintf("ERR No such user '%s'", userName))
					return
				}
				filter["user"] = userName
			case "filter.new-format.laddr": // 'laddr' is a workaround name in our modified command docs template, instead of official name 'ip:port'
				filter["laddr"] = val.(string)
			case "filter.new-format.skipme.no":
				includeMe = true
				fmt.Println("includeMe no", includeMe)
			case "filter.new-format.skipme.yes":
				includeMe = false
				fmt.Println("includeMe yes", includeMe)

			default:
				panic("unexpected arg")
			}
		}
	}

	if len(filter) > 1 && hasOldFormat {
		output.data = rstrSyntaxError
		return
	}

	processAllClients(func(id int64, cs *clientState) {
		shouldClose := cs.client.MatchFilter(filter)

		if shouldClose {
			// skipMe available in new format only
			fmt.Println("should close? ", ctx.cs.id, cs.id, closed)
			if ctx.cs.id == cs.id && closed >= 0 {
				shouldClose = includeMe
				fmt.Println("should close! ", shouldClose)
			}

			if shouldClose {
				for k, v := range filter {
					switch k {
					case "id":
						shouldClose = (v == fmt.Sprintf("%d", cs.id))
					case "type":
						shouldClose = strings.EqualFold(v, "normal")
					case "user":
						shouldClose = (v == cs.user)
					}
					if !shouldClose {
						break
					}
				}

				if shouldClose {
					cs.l.Infof("client kill requests client %d to close", cs.id)
					cs.client.RequestClose()
					if closed >= 0 {
						closed++
					} else {
						closed--
					}
				}
			}
		}
	})

	if closed >= 0 {
		output.data = respInt(closed)
	} else if closed == -2 {
		output.data = rstrOK
	} else {
		output.data = respErrorString("ERR No such client")
	}
	return
}

func fnClientList(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	_, hasPubSub := args["client-type.pubsub"]
	_, hasReplica := args["client-type.replica"]
	_, hasMaster := args["client-type.master"]
	if hasPubSub || hasReplica || hasMaster {
		output.data = respBulkString("")
		return
	}

	ids := map[int64]struct{}{}
	clientIds, exists := args["client-id"].([]any)
	if exists {
		for _, clientId := range clientIds {
			ids[clientId.(int64)] = struct{}{}
		}
	}

	var list strings.Builder

	processAllClients(func(id int64, cs *clientState) {
		included := true
		if len(ids) > 0 {
			_, included = ids[cs.id]
		}
		if included {
			info := ctx.info(cs)
			list.WriteString(info)
		}
	})

	output.data = respVerbatimString{format: "txt", text: list.String()}
	return
}

func fnClientNoEvict(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	_, on := args["enabled.on"]
	ctx.cs.noEvict = on
	ctx.l.Infof("client %d no-evict is now %v", ctx.cs.id, on)
	output.data = rstrOK
	return
}

func fnSelect(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	index := args["index"].(int64)
	_, valid := ctx.cs.selectDb(int(index), true)
	if !valid {
		output.data = respErrorString("ERR DB index is out of range")
		return
	}
	output.data = rstrOK
	return
}

func fnClientSetInfo(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	libname, present := args["attr.libname"].(string)
	if present {
		ctx.cs.libName = libname
	}
	libver, present := args["attr.libver"].(string)
	if present {
		ctx.cs.libVer = libver
	}
	output.data = rstrOK
	return
}
