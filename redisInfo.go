package goredisemu

import "github.com/jimsnab/go-lane"

type (
	redisInfoBeginSearch struct {
		Type string
		Spec any
	}

	redisInfoBeginSearchIndex struct {
		Index int
	}

	redisInfoBeginSearchKeyword struct {
		Keyword   string
		StartFrom int
	}

	redisInfoBeginSearchUnknown struct {
		// empty
	}

	redisInfoFindKeys struct {
		Type string
		Spec any
	}

	redisInfoFindKeysRange struct {
		LastKey int
		KeyStep int
		Limit   int
	}

	redisInfoFindKeysKeyNum struct {
		KeyNumIdx int
		FirstKey  int
		KeyStep   int
	}

	redisInfoFindKeysUnknown struct {
		// empty
	}

	redisInfoKeySpec struct {
		Flags       []string
		BeginSearch *redisInfoBeginSearch
		FindKeys    *redisInfoFindKeys
		Notes       string
	}

	redisInfo struct {
		Name          string
		IsSubcommand  bool
		Arity         int
		Flags         []string
		FirstKey      int
		LastKey       int
		Step          int
		AclCategories []string
		CommandTips   []string
		KeySpecs      []*redisInfoKeySpec
		Subcommands   []*redisInfo
	}
)

func simpleStringsFromArray(strs []string) (output respValue) {
	a := make(respArray, 0, len(strs))
	for _, str := range strs {
		ss := respValue{data: respSimpleString(str)}
		a = append(a, ss)
	}
	output.data = a
	return
}

func (rinfo *redisInfo) respSerialize() (output respValue) {
	a := make(respArray, 0, 10)

	name := respValue{data: respBulkString(rinfo.Name)}
	a = append(a, name)

	arity := respValue{data: respInt(rinfo.Arity)}
	a = append(a, arity)

	flags := simpleStringsFromArray(rinfo.Flags)
	a = append(a, flags)

	firstKey := respValue{data: respInt(rinfo.FirstKey)}
	a = append(a, firstKey)

	lastKey := respValue{data: respInt(rinfo.LastKey)}
	a = append(a, lastKey)

	step := respValue{data: respInt(rinfo.Step)}
	a = append(a, step)

	aclCategories := simpleStringsFromArray(rinfo.AclCategories)
	a = append(a, aclCategories)

	commandTips := nativeValueToResp(rinfo.CommandTips)
	a = append(a, commandTips)

	keySpecsArray := make(respArray, 0, len(rinfo.KeySpecs))
	for _, keySpec := range rinfo.KeySpecs {
		table := map[string]any{}
		table["flags"] = simpleStringsFromArray(keySpec.Flags)

		if keySpec.BeginSearch != nil {
			bsm := map[string]any{}

			bsm["type"] = keySpec.BeginSearch.Type

			sm := map[string]any{}
			switch keySpec.BeginSearch.Type {
			case "index":
				bsi := keySpec.BeginSearch.Spec.(*redisInfoBeginSearchIndex)
				sm["index"] = bsi.Index

			case "keyword":
				bsk := keySpec.BeginSearch.Spec.(*redisInfoBeginSearchKeyword)
				sm["keyword"] = bsk.Keyword
				sm["startfrom"] = bsk.StartFrom

			case "unknown":
				// no members
			}

			bsm["spec"] = sm

			table["begin_search"] = bsm
		}

		if keySpec.FindKeys != nil {
			fkm := map[string]any{}

			fkm["type"] = keySpec.FindKeys.Type

			sm := map[string]any{}
			switch keySpec.FindKeys.Type {
			case "range":
				fkr := keySpec.FindKeys.Spec.(*redisInfoFindKeysRange)
				sm["lastkey"] = fkr.LastKey
				sm["keystep"] = fkr.KeyStep
				sm["limit"] = fkr.Limit

			case "keynum":
				fkkn := keySpec.FindKeys.Spec.(*redisInfoFindKeysKeyNum)
				sm["keynumidx"] = fkkn.KeyNumIdx
				sm["firstkey"] = fkkn.FirstKey
				sm["keystep"] = fkkn.KeyStep

			case "unknown":
				// no members
			}

			fkm["spec"] = sm

			table["find_keys"] = fkm
		}

		if keySpec.Notes != "" {
			table["notes"] = keySpec.Notes
		}

		keySpecVal := nativeValueToResp(table)
		keySpecsArray = append(keySpecsArray, keySpecVal)
	}

	keySpecs := respValue{data: keySpecsArray}
	a = append(a, keySpecs)

	subcommandsArray := make([]any, 0, len(rinfo.Subcommands))
	for _, subcommand := range rinfo.Subcommands {
		subcommandsArray = append(subcommandsArray, subcommand.respSerialize())
	}
	subcommands := nativeValueToResp(subcommandsArray)
	a = append(a, subcommands)

	output.data = a
	return
}

func stringArrayFromAny(obj any) (strs []string, valid bool) {
	a, valid := obj.([]any)
	if !valid {
		return
	}

	strs = make([]string, 0, len(a))
	for _, item := range a {
		var str string
		str, valid = item.(string)
		if !valid {
			return
		}
		strs = append(strs, str)
	}

	return
}

func tableFromAny(obj any) (table map[string]any, valid bool) {
	a, valid := obj.([]any)
	if !valid {
		return
	}
	if len(a)%2 != 0 {
		valid = false
		return
	}

	table = make(map[string]any, len(a)/2)

	for i := 0; i < len(a); i += 2 {
		var key string
		key, valid = a[i].(string)
		if !valid {
			return
		}
		table[key] = a[i+1]
	}

	return
}

func (rinfo *redisInfo) respDeserialize(l lane.Lane, infoSpec respValue, isSubcommand bool) (valid bool) {
	a, valid := infoSpec.toNative().([]any)
	if !valid || len(a) != 10 {
		l.Error("resp info data does not have a valid 10-element array")
		return
	}

	name, valid := a[0].(string)
	if !valid {
		l.Error("resp info name is invalid")
		return
	}

	arity, valid := a[1].(int64)
	if !valid {
		l.Error("resp info %s arity is invalid", name)
		return
	}

	flags, valid := stringArrayFromAny(a[2])
	if !valid {
		l.Error("resp info %s flags are invalid", name)
		return
	}

	firstKey, valid := a[3].(int64)
	if !valid {
		l.Error("resp info %s firstKey is invalid", name)
		return
	}

	lastKey, valid := a[4].(int64)
	if !valid {
		l.Error("resp info %s lastKey is invalid", name)
		return
	}

	step, valid := a[5].(int64)
	if !valid {
		l.Error("resp info %s step is invalid", name)
		return
	}

	aclCategories, valid := stringArrayFromAny(a[6])
	if !valid {
		l.Error("resp info %s acl categories are invalid", name)
		return
	}

	cmdTips, valid := stringArrayFromAny(a[7])
	if !valid {
		l.Error("resp info %s command tips are invalid", name)
		return
	}

	keySpecs, valid := a[8].([]any)
	if !valid {
		l.Error("resp info %s key specs are invalid", name)
		return
	}

	ks := []*redisInfoKeySpec{}
	for _, keySpecAny := range keySpecs {
		var keySpec map[string]any
		if keySpec, valid = tableFromAny(keySpecAny); !valid {
			l.Error("resp info %s key spec item is not a resp map", name)
			return
		}

		var flags []string
		flagsAny, exists := keySpec["flags"]
		if exists {
			flags, valid = stringArrayFromAny(flagsAny)
			if !valid {
				l.Error("resp info %s key spec item flags is not a string array", name)
				return
			}
		}

		var beginSearch *redisInfoBeginSearch
		beginSearchAny, exists := keySpec["begin_search"]
		if exists {
			var table map[string]any
			table, valid = tableFromAny(beginSearchAny)
			if !valid {
				l.Error("resp info %s key spec item beginSearch is not a map", name)
				return
			}

			var typeName string
			typeName, valid = table["type"].(string)
			if !valid {
				l.Error("resp info %s key spec item beginSearch type is not valid", name)
				return
			}

			var specAny any
			specAny, valid = table["spec"]
			if !valid {
				l.Error("resp info %s key spec item beginSearch spec is not valid", name)
				return
			}

			beginSearch = &redisInfoBeginSearch{
				Type: typeName,
			}

			var specMap map[string]any
			specMap, valid = tableFromAny(specAny)
			if !valid {
				l.Error("resp info %s key spec item beginSearch spec is not a map", name)
				return
			}

			switch typeName {
			case "index":
				if len(specMap) != 1 {
					valid = false
					l.Error("resp info %s key spec item beginSearch spec index is not a single item map", name)
					return
				}
				var idx int64
				idx, valid = specMap["index"].(int64)
				if !valid {
					l.Error("resp info %s key spec item beginSearch spec index is not an int64", name)
					return
				}
				beginSearch.Spec = &redisInfoBeginSearchIndex{Index: int(idx)}

			case "keyword":
				if len(specMap) != 2 {
					valid = false
					l.Error("resp info %s key spec item beginSearch spec keyword is not a two item map", name)
					return
				}
				var keyword string
				keyword, valid = specMap["keyword"].(string)
				if !valid {
					l.Error("resp info %s key spec item beginSearch spec keyword token is not a string", name)
					return
				}
				var startFrom int64
				startFrom, valid = specMap["startfrom"].(int64)
				if !valid {
					l.Error("resp info %s key spec item beginSearch spec keyword startfrom is not an int64", name)
					return
				}
				beginSearch.Spec = &redisInfoBeginSearchKeyword{Keyword: keyword, StartFrom: int(startFrom)}

			case "unknown":
				if len(specMap) != 0 {
					valid = false
					l.Error("resp info %s key spec item beginSearch spec unknown is not a zero item map", name)
					return
				}
				beginSearch.Spec = &redisInfoBeginSearchUnknown{}

			default:
				valid = false
				l.Error("resp info %s key spec item beginSearch spec has unexpected type %s", name, typeName)
				return
			}
		}

		var findKeys *redisInfoFindKeys
		findKeysAny := keySpec["find_keys"]
		if exists {
			var table map[string]any
			table, valid = tableFromAny(findKeysAny)
			if !valid {
				l.Error("resp info %s key spec item beginSearch is not a map", name)
				return
			}

			var typeName string
			typeName, valid = table["type"].(string)
			if !valid {
				l.Error("resp info %s key spec item findKeys type is not valid", name)
				return
			}

			var specAny any
			specAny, valid = table["spec"]
			if !valid {
				l.Error("resp info %s key spec item findKeys spec is not valid", name)
				return
			}

			findKeys = &redisInfoFindKeys{
				Type: typeName,
			}

			var specMap map[string]any
			specMap, valid = tableFromAny(specAny)
			if !valid {
				l.Error("resp info %s key spec item findKeys spec is not a map", name)
				return
			}

			switch typeName {
			case "range":
				if len(specMap) != 3 {
					valid = false
					l.Error("resp info %s key spec item findKeys spec range is not a three item map", name)
					return
				}
				var lastKey int64
				lastKey, valid = specMap["lastkey"].(int64)
				if !valid {
					l.Error("resp info %s key spec item findKeys spec range lastkey is not an int64", name)
					return
				}
				var keyStep int64
				keyStep, valid = specMap["keystep"].(int64)
				if !valid {
					l.Error("resp info %s key spec item findKeys spec range keystep is not an int64", name)
					return
				}
				var limit int64
				limit, valid = specMap["limit"].(int64)
				if !valid {
					l.Error("resp info %s key spec item findKeys spec range limit is not an int64", name)
					return
				}
				findKeys.Spec = &redisInfoFindKeysRange{LastKey: int(lastKey), KeyStep: int(keyStep), Limit: int(limit)}

			case "keynum":
				if len(specMap) != 3 {
					valid = false
					l.Error("resp info %s key spec item findKeys spec keynum is not a two item map", name)
					return
				}
				var keynumidx int64
				keynumidx, valid = specMap["keynumidx"].(int64)
				if !valid {
					l.Error("resp info %s key spec item findKeys spec keynum keynumidx is not an int64", name)
					return
				}
				var firstKey int64
				firstKey, valid = specMap["firstkey"].(int64)
				if !valid {
					l.Error("resp info %s key spec item findKeys spec keynum firstkey is not an int64", name)
					return
				}
				var keyStep int64
				keyStep, valid = specMap["keystep"].(int64)
				if !valid {
					l.Error("resp info %s key spec item findKeys spec keynum keystep is not an int64", name)
					return
				}
				findKeys.Spec = &redisInfoFindKeysKeyNum{KeyNumIdx: int(keynumidx), FirstKey: int(firstKey), KeyStep: int(keyStep)}

			case "unknown":
				if len(specMap) != 0 {
					valid = false
					l.Error("resp info %s key spec item findKeys spec unknown is not a zero item map", name)
					return
				}
				findKeys.Spec = &redisInfoFindKeysUnknown{}

			default:
				valid = false
				l.Error("resp info %s key spec item findKeys spec has unexpected type %s", name, typeName)
				return
			}
		}

		var notes string
		notesAny, exists := keySpec["notes"]
		if exists {
			notes, valid = notesAny.(string)
			if !valid {
				l.Error("resp info %s key spec item notes is not a string", name)
				return
			}
		}

		item := &redisInfoKeySpec{
			Flags:       flags,
			BeginSearch: beginSearch,
			FindKeys:    findKeys,
			Notes:       notes,
		}

		ks = append(ks, item)
	}

	subcommandsAny, valid := a[9].([]any)
	if !valid {
		l.Error("resp info %s subcommands are invalid", name)
		return
	}

	var subcommands []*redisInfo
	for _, subcommandAny := range subcommandsAny {
		subSpec := nativeValueToResp(subcommandAny)
		subcommand := &redisInfo{}
		valid = subcommand.respDeserialize(l, subSpec, true)
		if !valid {
			return
		}

		subcommands = append(subcommands, subcommand)
	}

	rinfo.Name = name
	rinfo.IsSubcommand = isSubcommand
	rinfo.Arity = int(arity)
	rinfo.Flags = flags
	rinfo.FirstKey = int(firstKey)
	rinfo.LastKey = int(lastKey)
	rinfo.Step = int(step)
	rinfo.AclCategories = aclCategories
	rinfo.CommandTips = cmdTips
	rinfo.KeySpecs = ks
	rinfo.Subcommands = subcommands

	return
}

func (rinfo *redisInfo) hasFlag(flag string) bool {
	for _, f := range rinfo.Flags {
		if f == flag {
			return true
		}
	}
	return false
}
