package goredisemu

import (
	"sort"

	"github.com/jimsnab/go-lane"
)

type (
	redisCommands map[string]*redisCommand
)

func (rcmds *redisCommands) respSerialize() respValue {
	cmdObjs := map[string]any{}

	for name, cmd := range *rcmds {
		cmdObjs[name] = cmd.respSerialize()
	}

	return nativeValueToResp(cmdObjs)
}

func (rcmds *redisCommands) respDeserialize(l lane.Lane, allCmds respValue) (valid bool) {
	m := map[string]*redisCommand{}

	table, valid := allCmds.toTable()
	if !valid {
		l.Error("resp command list does not have a valid key/value root array")
		return
	}

	keys := make([]string, 0, len(m))
	for k := range table {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, cmdName := range keys {
		cmdData := table[cmdName]
		cmd := &redisCommand{}
		if valid = cmd.respDeserialize(l, cmdData); !valid {
			return
		}

		m[cmdName] = cmd
	}

	*rcmds = m
	return
}
