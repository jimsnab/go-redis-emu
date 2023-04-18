package goredisemu

import "github.com/jimsnab/go-lane"

const (
	FLAG_SUMMARY = 1 << iota
	FLAG_SINCE
	FLAG_GROUP
	FLAG_COMPLEXITY
	FLAG_DEPRECATED_SINCE
	FLAG_REPLACED_BY
)

type (
	redisHistoryEntry struct {
		Version     string
		Description string
	}

	redisCommand struct {
		fields          bitflags
		Summary         string               `json:"summary,omitempty"`
		Since           string               `json:"since,omitempty"`
		Group           string               `json:"group,omitempty"`
		Complexity      string               `json:"complexity,omitempty"`
		Syscmd          bool                 `json:"syscmd,omitempty"`
		Deprecated      bool                 `json:"deprecated,omitempty"`
		DeprecatedSince string               `json:"deprecated_since,omitempty"`
		ReplacedBy      string               `json:"replaced_by,omitempty"`
		History         []*redisHistoryEntry `json:"history,omitempty"`
		Arguments       redisArgs            `json:"arguments,omitempty"`
		Subcommands     redisCommands        `json:"subcommands,omitempty"`
	}
)

func (rcmd *redisCommand) respSerialize() respValue {
	table := map[string]any{}
	if flagHasOne(rcmd.fields, FLAG_SUMMARY) {
		table["summary"] = rcmd.Summary
	}

	if flagHasOne(rcmd.fields, FLAG_SINCE) {
		table["since"] = rcmd.Since
	}

	if flagHasOne(rcmd.fields, FLAG_GROUP) {
		table["group"] = rcmd.Group
	}

	if flagHasOne(rcmd.fields, FLAG_COMPLEXITY) {
		table["complexity"] = rcmd.Complexity
	}

	if flagHasOne(rcmd.fields, FLAG_DEPRECATED_SINCE) {
		table["deprecated_since"] = rcmd.DeprecatedSince
	}

	if flagHasOne(rcmd.fields, FLAG_REPLACED_BY) {
		table["replaced_by"] = rcmd.ReplacedBy
	}

	if rcmd.Deprecated || rcmd.Syscmd {
		docFlags := []any{}
		if rcmd.Deprecated {
			docFlags = append(docFlags, respSimpleString("deprecated"))
		}
		if rcmd.Syscmd {
			docFlags = append(docFlags, respSimpleString("syscmd"))
		}
		table["doc_flags"] = docFlags
	}

	if len(rcmd.History) > 0 {
		history := []any{}
		for _, he := range rcmd.History {
			entry := map[string]any{}
			entry[he.Version] = he.Description
			history = append(history, entry)
		}
		table["history"] = history
	}

	if len(rcmd.Arguments) > 0 {
		argObjs := rcmd.Arguments.respSerialize()
		table["arguments"] = argObjs
	}

	if len(rcmd.Subcommands) > 0 {
		cmdObjs := rcmd.Subcommands.respSerialize()
		table["subcommands"] = cmdObjs
	}

	return nativeValueToResp(table)
}

func (rcmd *redisCommand) getTableString(table map[string]respValue, key string, flag bitflags) string {
	value, valid := getTableString(table, key)
	if valid {
		rcmd.fields = flagSet(rcmd.fields, flag)
	} else {
		rcmd.fields = flagClear(rcmd.fields, flag)
	}

	return value
}

func (rcmd *redisCommand) respDeserialize(l lane.Lane, cmdSpec respValue) (valid bool) {
	table, valid := cmdSpec.toTable()
	if !valid {
		l.Error("resp command data does not have a valid key/value root array")
		return
	}

	rcmd.Summary = rcmd.getTableString(table, "summary", FLAG_SUMMARY)
	rcmd.Since = rcmd.getTableString(table, "since", FLAG_SINCE)
	rcmd.Complexity = rcmd.getTableString(table, "complexity", FLAG_COMPLEXITY)
	rcmd.Group = rcmd.getTableString(table, "group", FLAG_GROUP)
	rcmd.DeprecatedSince = rcmd.getTableString(table, "deprecated_since", FLAG_DEPRECATED_SINCE)
	rcmd.ReplacedBy = rcmd.getTableString(table, "replaced_by", FLAG_REPLACED_BY)

	docFlags, exists := table["doc_flags"]
	if exists {
		var flags respArray
		if flags, valid = docFlags.toArray(); !valid {
			l.Error("resp doc_flags does not have a valid array")
			return
		}
		for _, flag := range flags {
			text, _ := flag.toString()
			if text == "syscmd" {
				rcmd.Syscmd = true
			} else if text == "deprecated" {
				rcmd.Deprecated = true
			} else {
				l.Errorf("unsupported doc flag: %s ", text)
				valid = false
				return
			}
		}
	}

	history, exists := table["history"]
	if exists {
		var entries respArray
		if entries, valid = history.toArray(); !valid {
			l.Error("resp history does not have a valid array")
			return
		}
		for _, he := range entries {
			var entry map[string]respValue
			if entry, valid = he.toTable(); !valid {
				l.Error("resp history does not have a valid array element")
				return
			}
			if len(entry) != 1 {
				l.Error("resp history does not have a valid array length")
				valid = false
				return
			}

			for k, v := range entry { // only one
				var desc string
				if desc, valid = v.toString(); !valid {
					l.Error("resp history does not have a valid history description")
					return
				}
				rhe := &redisHistoryEntry{
					Version:     k,
					Description: desc,
				}
				rcmd.History = append(rcmd.History, rhe)
				break
			}
		}
	}

	args, exists := table["arguments"]
	if exists {
		arguments := redisArgs{}
		if valid = arguments.respDeserialize(l, args); !valid {
			return
		}
		rcmd.Arguments = arguments
	}

	cmds, exists := table["subcommands"]
	if exists {
		subcommands := redisCommands{}
		if valid = subcommands.respDeserialize(l, cmds); !valid {
			return
		}
		rcmd.Subcommands = subcommands
	}

	return
}
