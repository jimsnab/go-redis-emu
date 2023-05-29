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

func newZSet() *zset {
	return &zset{}
}

func (z *zset) add(key string, score float64, flags zaddFlags) (out respValue) {
	return
}
