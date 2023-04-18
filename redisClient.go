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
	_, isError := args["timeout_error.error"]

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

	for arg, v := range args {
		valArray, valid := v.([]any)
		if !valid {
			valArray = []any{v}
		}

		for _, val := range valArray {
			switch arg {
			case "filter.ip:port":
				hasOldFormat = true
				filter["addr"] = val.(string)
				closed = -1
			case "filter.new-format.addr": // 'addr' is a workaround name in our modified command docs template, instead of official name 'ip:port'
				filter["addr"] = val.(string)
			case "filter.new-format.client-id":
				filter["id"] = fmt.Sprintf("%v", val)
			case "filter.new-format.normal_master_slave_pubsub.normal":
				filter["type"] = "normal"
			case "filter.new-format.normal_master_slave_pubsub.master":
				filter["type"] = "master"
			case "filter.new-format.normal_master_slave_pubsub.slave":
				filter["type"] = "slave"
			case "filter.new-format.normal_master_slave_pubsub.pubsub":
				filter["type"] = "pubsub"
			case "filter.new-format.normal_master_slave_pubsub.replica":
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
			case "filter.new-format.skipme": // 'skipme' is a workaround name in our modified command docs template, instead of official name 'yes/no'
				s := val.(string)
				if strings.EqualFold(s, "no") {
					includeMe = true
				} else if strings.EqualFold(s, "yes") {
					includeMe = false
				} else {
					output.data = rstrSyntaxError
					return
				}

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
			if ctx.cs.id == cs.id && closed >= 0 {
				shouldClose = includeMe
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
	_, hasPubSub := args["normal_master_replica_pubsub.pubsub"]
	_, hasReplica := args["normal_master_replica_pubsub.replica"]
	_, hasMaster := args["normal_master_replica_pubsub.master"]
	if hasPubSub || hasReplica || hasMaster {
		output.data = respBulkString("")
		return
	}

	ids := map[int64]struct{}{}
	argIds, exists := args["id"].(map[string]any)
	if exists {
		clientIds := argIds["client-id"].([]any)
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

	output.data = respBulkString(list.String())
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
