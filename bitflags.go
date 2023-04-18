package goredisemu

type bitflags uint32

func flagSet(flags, flag bitflags) bitflags {
	return flags | flag
}

func flagClear(flags, flag bitflags) bitflags {
	return flags & (^flag)
}

func flagHasOne(flags, flag bitflags) bool {
	return flags&flag != 0
}

func flagHasAll(flags, flag bitflags) bool {
	return flags&flag == flag
}
