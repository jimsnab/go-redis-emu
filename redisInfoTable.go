package goredisemu

import "github.com/jimsnab/go-lane"

type (
	redisInfoTable struct {
		table map[string]*redisInfo
		order []string
	}
)

func newRedisInfoTable() *redisInfoTable {
	return &redisInfoTable{
		table: map[string]*redisInfo{},
		order: []string{},
	}
}

func (rinfoTable *redisInfoTable) respSerialize() respValue {
	infoList := make([]any, 0, len(rinfoTable.order))

	for _, name := range rinfoTable.order {
		rinfo := rinfoTable.table[name]
		if !rinfo.IsSubcommand {
			infoList = append(infoList, rinfo.respSerialize())
		}
	}

	return nativeValueToResp(infoList)
}

func addRedisInfoItem(m map[string]*redisInfo, order *[]string, rinfo *redisInfo) {
	m[rinfo.Name] = rinfo
	*order = append(*order, rinfo.Name)
	for _, subcommand := range rinfo.Subcommands {
		addRedisInfoItem(m, order, subcommand)
	}
}

func (rinfoTable *redisInfoTable) respDeserialize(l lane.Lane, allInfo respValue) (valid bool) {
	m := map[string]*redisInfo{}
	order := []string{}

	a, valid := allInfo.toArray()
	if !valid {
		l.Error("resp info list does not have a valid key/value root array")
		return
	}

	for _, record := range a {
		rinfo := &redisInfo{}
		if valid = rinfo.respDeserialize(l, record, false); !valid {
			return
		}

		addRedisInfoItem(m, &order, rinfo)
	}

	rinfoTable.table = m
	rinfoTable.order = order
	return
}
