package redisemu

import "github.com/jimsnab/go-lane"

const (
	FLAG_ARG_NAME = 1 << iota
	FLAG_ARG_TYPE
	FLAG_ARG_TOKEN
	FLAG_ARG_SINCE
	FLAG_ARG_DEPRECATED_SINCE
	FLAG_ARG_REPLACED_BY
	FLAG_ARG_DISPLAY_TEXT
)

type (
	redisArg struct {
		fields          bitflags
		Name            string    `json:"name,omitempty"`
		TypeName        string    `json:"type,omitempty"`
		DisplayText     string    `json:"display_text,omitempty"`
		Token           string    `json:"token,omitempty"`
		KeySpecIndex    *int      `json:"key_spec_index,omitempty"`
		Since           string    `json:"since,omitempty"`
		Optional        bool      `json:"optional,omitempty"`
		Multiple        bool      `json:"multiple,omitempty"`
		MultipleToken   bool      `json:"multiple_token,omitempty"`
		Arguments       redisArgs `json:"arguments,omitempty"`
		DeprecatedSince string    `json:"deprecated_since,omitempty"`
		ReplacedBy      string    `json:"replaced_by,omitempty"`
	}
)

func (rarg *redisArg) respSerialize() respValue {
	argObj := map[string]any{}

	if flagHasOne(rarg.fields, FLAG_ARG_NAME) {
		argObj["name"] = rarg.Name
	}

	if flagHasOne(rarg.fields, FLAG_ARG_TYPE) {
		argObj["type"] = rarg.TypeName
	}

	if flagHasOne(rarg.fields, FLAG_ARG_DISPLAY_TEXT) {
		argObj["display_text"] = rarg.DisplayText
	}

	if rarg.KeySpecIndex != nil {
		argObj["key_spec_index"] = *rarg.KeySpecIndex
	}

	if flagHasOne(rarg.fields, FLAG_ARG_SINCE) {
		argObj["since"] = rarg.Since
	}

	if flagHasOne(rarg.fields, FLAG_ARG_DEPRECATED_SINCE) {
		argObj["deprecated_since"] = rarg.DeprecatedSince
	}

	if flagHasOne(rarg.fields, FLAG_ARG_REPLACED_BY) {
		argObj["replaced_by"] = rarg.ReplacedBy
	}

	if flagHasOne(rarg.fields, FLAG_ARG_TOKEN) {
		argObj["token"] = rarg.Token
	}

	if rarg.Optional || rarg.Multiple || rarg.MultipleToken {
		flags := []any{}
		if rarg.Optional {
			flags = append(flags, respSimpleString("optional"))
		}
		if rarg.Multiple {
			flags = append(flags, respSimpleString("multiple"))
		}
		if rarg.MultipleToken {
			flags = append(flags, respSimpleString("multiple_token"))
		}
		argObj["flags"] = flags
	}

	if rarg.Arguments != nil {
		argObj["arguments"] = rarg.Arguments.respSerialize()
	}

	return nativeValueToResp(argObj)
}

func (rarg *redisArg) getTableString(table map[string]respValue, key string, flag bitflags) string {
	value, valid := getTableString(table, key)
	if valid {
		rarg.fields = flagSet(rarg.fields, flag)
	} else {
		rarg.fields = flagClear(rarg.fields, flag)
	}

	return value
}

func (rarg *redisArg) respDeserialize(l lane.Lane, argSpec respValue) (valid bool) {
	table, valid := argSpec.toTable()
	if !valid {
		l.Error("resp arg data does not have a valid key/value root array")
		return
	}

	rarg.Name = rarg.getTableString(table, "name", FLAG_ARG_NAME)
	rarg.TypeName = rarg.getTableString(table, "type", FLAG_ARG_TYPE)
	rarg.Token = rarg.getTableString(table, "token", FLAG_ARG_TOKEN)
	rarg.Since = rarg.getTableString(table, "since", FLAG_ARG_SINCE)
	rarg.DeprecatedSince = rarg.getTableString(table, "deprecated_since", FLAG_ARG_DEPRECATED_SINCE)
	rarg.ReplacedBy = rarg.getTableString(table, "replaced_by", FLAG_ARG_REPLACED_BY)
	rarg.DisplayText = rarg.getTableString(table, "display_text", FLAG_ARG_DISPLAY_TEXT)

	index, exists := getTableInt(table, "key_spec_index")
	if exists {
		indexInt := int(index)
		rarg.KeySpecIndex = &indexInt
	}

	flags, exists := table["flags"]
	if exists {
		var a respArray
		if a, valid = flags.toArray(); !valid {
			l.Error("resp arg flags does not have a valid array")
			return
		}
		for _, flag := range a {
			text, _ := flag.toString()
			if text == "optional" {
				rarg.Optional = true
			} else if text == "multiple" {
				rarg.Multiple = true
			} else if text == "multiple_token" {
				rarg.MultipleToken = true
			} else {
				l.Errorf("unsupported arg flag: %s ", text)
				valid = false
				return
			}
		}
	}

	subargs, exists := table["arguments"]
	if exists {
		sa := redisArgs{}
		if valid = sa.respDeserialize(l, subargs); !valid {
			return
		}
		rarg.Arguments = sa
	}

	return
}

func (rarg *redisArg) isToken() bool {
	if rarg.Token != "" {
		return true
	}
	if rarg.TypeName == "oneof" {
		for _, subarg := range rarg.Arguments {
			if !subarg.isToken() {
				return false
			}
		}
		return true
	}

	return false
}
