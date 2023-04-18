package redisemu

import "github.com/jimsnab/go-lane"

type (
	redisArgs []*redisArg
)

func (rargs *redisArgs) respSerialize() respValue {
	argObjs := []any{}

	if len(*rargs) > 0 {
		for _, ra := range *rargs {
			argObjs = append(argObjs, ra.respSerialize())
		}
	}

	return nativeValueToResp(argObjs)
}

func (rargs *redisArgs) respDeserialize(l lane.Lane, allArgs respValue) (valid bool) {
	args := []*redisArg{}

	list, valid := allArgs.toArray()
	if !valid {
		l.Error("resp arg list does not have a valid root array")
		return
	}

	for _, argTable := range list {
		arg := &redisArg{}
		if valid = arg.respDeserialize(l, argTable); !valid {
			return
		}

		args = append(args, arg)
	}

	*rargs = args
	return
}
