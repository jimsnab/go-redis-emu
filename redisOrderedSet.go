package redisemu

type (
	zset struct {
		keyRoot *znode
	}

	znode struct {
		key    string
		score  float64
		left   *znode
		right  *znode
		parent *znode
	}

	zaddFlags int
)

const (
	ZADD_NX zaddFlags = 1 << iota
	ZADD_XX
	ZADD_GT
	ZADD_LT
	ZADD_CH
	ZADD_INCR
)

var _ = newZSet // not yet fully implemented
func newZSet() *zset {
	zs := &zset{}

	// temporary disable warnings about incomplete work
	_ = zs.add
	_ = zs.keyRoot
	_ = znode{"", 0, nil, nil, nil}
	return zs
}

func (z *zset) add(key string, score float64, flags zaddFlags) (out respValue) {
	return
}
