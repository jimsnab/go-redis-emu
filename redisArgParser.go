package redisemu

import (
	"fmt"
	"strings"
)

const (
	PARSE_SAVE_INTEGERS_AS_STRINGS = 1 << iota
	PARSE_ADD_ARG_INDEX_TO_BLOCK   // when a block is parsed, add "arg-index": <index> as arg output for the block
	PARSE_ADD_TOKEN_INDEX_TO_ARGS  // when a token is found, an additional args entry is added ("<keyword>-index": []int(<indicies>)) as arg output
)

const (
	PARSE_SINGLE_VALUE parseMultiStatus = iota
	PARSE_ONE_OF_TOKEN
	PARSE_MULTI_VALUE
	PARSE_MULTI_ONE_OF_TOKEN
)

type (
	parseMultiStatus int

	commandArgParser struct {
		opts       bitflags
		tokenIndex map[string][]int
	}
)

func parseCommand(cmdToken string, cmd *redisCommand, opts bitflags, input ...respValue) (args *orderedMap, keywords int, parsedToken string) {
	cap := &commandArgParser{
		opts:       opts,
		tokenIndex: map[string][]int{},
	}

	return cap.parseInput(cmdToken, cmd, input...)
}

func (cap *commandArgParser) parseInput(cmdToken string, cmd *redisCommand, input ...respValue) (args *orderedMap, keywords int, parsedToken string) {
	var valid bool

	if len(cmd.Subcommands) == 0 {
		args, _, valid = cap.parseEachInput(cmd.Arguments, input...)
		if valid {
			keywords = 1
			parsedToken = cmdToken
		}
	} else if len(input) > 0 {
		keywords = -1
		subCmdName, ok := input[0].toString()
		if ok {
			parsedToken = strings.ToLower(subCmdName) // default for when parsing fails

			subCmdToken := cmdToken + "|" + parsedToken
			subCmd, exists := cmd.Subcommands[subCmdToken]
			if exists {
				subArgs, subKeywords, subParsedToken := cap.parseInput(subCmdToken, subCmd, input[1:]...)
				parsedToken = subParsedToken
				if subKeywords > 0 {
					keywords = subKeywords + 1
					args = subArgs
				} else {
					keywords = subKeywords - 1
				}
			}
		}
	}

	if flagHasOne(cap.opts, PARSE_ADD_TOKEN_INDEX_TO_ARGS) {
		for name, index := range cap.tokenIndex {
			args.set(fmt.Sprintf("%s-index", name), index)
		}
	}
	return
}

func (cap *commandArgParser) parseOneInput(arg *redisArg, argIndex int, started bool, input ...respValue) (argKey string, argVal any, inputsUsed int, pms parseMultiStatus) {
	if len(input) == 0 {
		return
	}

	ipos := 0
	ival := input[ipos]
	if arg.Multiple || arg.MultipleToken {
		pms = PARSE_MULTI_VALUE
	}

	if arg.Token != "" && (!started || !arg.Multiple) {
		keyword, valid := ival.toString()
		if !valid {
			return
		}

		if !strings.EqualFold(keyword, arg.Token) {
			return
		}

		if flagHasOne(cap.opts, PARSE_ADD_TOKEN_INDEX_TO_ARGS) {
			indicies, exists := cap.tokenIndex[arg.Token]
			if !exists {
				indicies = []int{}
			}
			cap.tokenIndex[arg.Token] = append(indicies, argIndex)
		}

		ipos++

		if arg.TypeName == "pure-token" {
			argKey = arg.Name
			inputsUsed = 1
			return
		}

		if ipos >= len(input) {
			return
		}
		ival = input[ipos]
	}

	switch arg.TypeName {
	case "key", "string", "pattern":
		key, valid := ival.toString()
		if !valid {
			return
		}
		argKey = arg.Name
		argVal = key

	case "integer", "unix-time":
		if flagHasOne(cap.opts, PARSE_SAVE_INTEGERS_AS_STRINGS) {
			var valid bool
			argVal, valid = ival.toString()
			if !valid {
				return
			}
		} else {
			n, valid := ival.toInt()
			if !valid {
				return
			}
			argVal = n
		}
		argKey = arg.Name

	case "double":
		n, valid := ival.toFloat()
		if !valid {
			return
		}
		argKey = arg.Name
		argVal = n

	case "oneof":
		subkey, subValue, subInputsUsed, subPms := cap.parseOneOf(arg.Arguments, argIndex, input[ipos:]...)
		if subInputsUsed > 0 {
			argKey = arg.Name + "." + subkey
			argVal = subValue
			inputsUsed = subInputsUsed + ipos

			if subPms == PARSE_ONE_OF_TOKEN && pms == PARSE_MULTI_VALUE {
				// the result is a single value (versus an array of multiple values)
				// but additional single values for this argument are possible
				pms = PARSE_MULTI_ONE_OF_TOKEN
			} else if subPms != PARSE_SINGLE_VALUE {
				// a sub-argument is a multiple value, or the leaf status needs
				// to be propogated to parent(s), in case a parent is a multiple value
				pms = subPms
			}
		}
		return

	case "block":
		var block *orderedMap
		block, inputsUsed = cap.parseInputBlock(arg.Arguments, argIndex, input[ipos:]...)
		if inputsUsed > 0 {
			if flagHasOne(cap.opts, PARSE_ADD_ARG_INDEX_TO_BLOCK) {
				block.set("arg-index", argIndex)
			}
			argKey = arg.Name
			argVal = block
			inputsUsed += ipos
		}
		return

	default:
		panic(fmt.Sprintf("arg type %s not supported", arg.TypeName))
	}

	inputsUsed = ipos + 1
	return
}

func (cap *commandArgParser) parseOneOf(args redisArgs, argIndex int, input ...respValue) (argKey string, argVal any, inputsUsed int, pms parseMultiStatus) {
	for _, arg := range args {
		subArgKey, subArgVal, subInputsUsed, subPms := cap.parseOneInput(arg, argIndex, false, input...)
		if subInputsUsed > inputsUsed {
			inputsUsed = subInputsUsed
			argKey = subArgKey
			argVal = subArgVal
			pms = subPms
			if subPms == PARSE_SINGLE_VALUE {
				// if this match is a pure token, pass this condition to the parent arg
				if arg.TypeName == "pure-token" {
					pms = PARSE_ONE_OF_TOKEN
				}
			}
		}
	}
	return
}

func (cap *commandArgParser) parseInputBlock(args redisArgs, argIndex int, input ...respValue) (block *orderedMap, inputsUsed int) {
	argPos := 0
	block = newOrderedMap()
	skippedOptionals := redisArgs{}

	testPos := 0
	started := -1
	foundMultiple := false
	for {
		if testPos >= len(args) {
			break
		}
		arg := args[testPos]

		subKey, subVal, subInputsUsed, subPms := cap.parseOneInput(arg, argIndex, testPos == started, input[argPos:]...)
		if subInputsUsed == 0 {
			if !arg.Optional {
				if !foundMultiple {
					inputsUsed = 0
				}
				return
			}

			if arg.isToken() {
				// optional value args that have tokens can be reordered
				skippedOptionals = append(skippedOptionals, arg)
			}
			testPos++
		} else {
			if arg.Optional && arg.isToken() {
				// optional value args that have tokens can be reordered
				args = append(args, skippedOptionals...)
				skippedOptionals = redisArgs{}
			}
			inputsUsed += subInputsUsed
			argPos += subInputsUsed
			if subPms == PARSE_MULTI_VALUE || subPms == PARSE_MULTI_ONE_OF_TOKEN {
				foundMultiple = true
			}

			if arg.Multiple || arg.MultipleToken {
				t, _ := block.get(subKey)
				a, exists := t.([]any)
				if !exists {
					a = []any{}
				}
				block.set(subKey, append(a, subVal))

				if arg.Multiple {
					started = testPos
				}
			} else {
				block.set(subKey, subVal)
				testPos++
			}
		}
	}
	return
}

func (cap *commandArgParser) parseEachInput(args redisArgs, input ...respValue) (values *orderedMap, inputsUsed int, valid bool) {
	values = newOrderedMap()
	apos := 0
	ipos := 0
	skippedOptionals := redisArgs{}

	foundMultiple := false
	started := -1

	for {
		if apos >= len(args) {
			valid = (ipos >= len(input))
			break
		}

		arg := args[apos]

		argKey, argValue, length, pms := cap.parseOneInput(arg, ipos, started == apos, input[ipos:]...)
		if length == 0 {
			if !foundMultiple && !arg.Optional {
				return
			}

			if !foundMultiple && arg.Optional && arg.isToken() {
				// optional value args that have tokens can be reordered
				skippedOptionals = append(skippedOptionals, arg)
			}
			foundMultiple = false
			apos++
		} else {
			if arg.Optional && arg.isToken() {
				// optional args that have tokens can be reordered
				args = append(args, skippedOptionals...)
				skippedOptionals = redisArgs{}
			}

			ipos += length

			switch pms {
			case PARSE_SINGLE_VALUE, PARSE_ONE_OF_TOKEN:
				values.set(argKey, argValue)
				apos++

			case PARSE_MULTI_ONE_OF_TOKEN:
				_, exists := values.get(argKey)
				if exists {
					return
				}
				values.set(argKey, argValue)

			case PARSE_MULTI_VALUE:
				t, _ := values.get(argKey)
				valueArray, exists := t.([]any)
				if !exists {
					valueArray = []any{}
				}
				values.set(argKey, append(valueArray, argValue))

				if arg.Multiple {
					started = apos
				}
			}

			if pms != PARSE_SINGLE_VALUE {
				foundMultiple = true

				// check recursively if multiple arguments stop here
				if apos+1 < len(args) {
					rightVals, testLength, subValid := cap.parseEachInput(args[apos+1:], input[ipos:]...)
					if subValid {
						ipos += testLength
						for _, k := range rightVals.order {
							_, exists := values.get(k)
							if exists {
								panic("reused argument conflict in arg definition")
							}
							values.set(k, rightVals.mustGet(k))
						}
						valid = true
						break
					}
				}
			}
		}
	}

	inputsUsed = ipos
	return
}
