package goredisemu

import (
	"encoding/binary"
	"fmt"

	"github.com/jimsnab/go-lane"
)

var inversePower2 = map[uint32]int{
	1 << 0:  1,
	1 << 2:  2,
	1 << 3:  3,
	1 << 4:  4,
	1 << 5:  5,
	1 << 6:  6,
	1 << 7:  7,
	1 << 8:  8,
	1 << 9:  9,
	1 << 10: 10,
	1 << 11: 11,
	1 << 12: 12,
	1 << 13: 13,
	1 << 14: 14,
	1 << 15: 15,
	1 << 16: 16,
	1 << 17: 17,
	1 << 18: 18,
	1 << 19: 19,
	1 << 20: 20,
	1 << 21: 21,
	1 << 22: 22,
	1 << 23: 23,
	1 << 24: 24,
	1 << 25: 25,
	1 << 26: 26,
	1 << 27: 27,
	1 << 28: 28,
	1 << 29: 29,
	1 << 30: 30,
	1 << 31: 31,
}

func toBitmap(bytes []byte) string {
	text := ""
	for _, b := range bytes {
		if text != "" {
			text += " "
		}
		text += fmt.Sprintf("%08b", b)
	}
	return text
}

func toBitmapU64(val uint64) string {
	text := ""
	for i := 0; i < 8; i++ {
		if text != "" {
			text += " "
		}
		text += fmt.Sprintf("%08b", uint8(val>>56))
		val <<= 8
	}
	return text
}

func logBitmap(l lane.Lane, bytes []byte) {
	text := toBitmap(bytes)
	l.Tracef("%s", text)
}

func logBitmapVal(l lane.Lane, keyword string, val int64) {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(val))

	text := toBitmap(bytes)
	l.Tracef("%s: %s", keyword, text)
}

func logBitmapKey(l lane.Lane, dsc *dataStoreCommand, keyName string) {
	bytes, exists := dsc.getKeyBytes(keyName)

	switch exists {
	case VALUE_EXISTS:
		text := toBitmap(bytes)
		l.Tracef("%s: %s", keyName, text)
	case VALUE_DOESNT_EXIST:
		l.Tracef("%s: doesn't exist", keyName)
	default:
		l.Tracef("%s: is not a bitmap key", keyName)
	}
}

func isPowerOfTwo(n uint32) bool {
	return (n&(n-1) == 0) && (n > 0)
}

func bitPosition(n uint32) int {
	// less computation than bits.TrailingZeros
	p2, exists := inversePower2[n]
	if !exists {
		return -1
	}
	return p2
}
