package goredisemu

import "encoding/binary"

type (
	sipHash struct {
		data           []byte
		length         int
		remainder      int
		v0, v1, v2, v3 uint64
	}
)

func calcSipHash(s string) uint64 {
	sh := startSipHash([]byte(s))
	return sh.compute()
}

func startSipHash(data []byte) *sipHash {
	// adapted from https://github.com/veorq/SipHash/blob/master/siphash.c

	length := len(data)

	return &sipHash{
		data:      data,
		length:    length,
		remainder: length % 8,

		// using a zero key
		v0: uint64(0x736f6d6570736575),
		v1: uint64(0x646f72616e646f6d),
		v2: uint64(0x6c7967656e657261),
		v3: uint64(0x7465646279746573),
	}
}

func (sh *sipHash) round() {
	sh.v0 += sh.v1
	sh.v1 = sh.v1<<13 | sh.v1>>(64-13)
	sh.v1 ^= sh.v0
	sh.v0 = sh.v0<<32 | sh.v0>>(64-32)

	sh.v2 += sh.v3
	sh.v3 = sh.v3<<16 | sh.v3>>(64-16)
	sh.v3 ^= sh.v2

	sh.v0 += sh.v3
	sh.v3 = sh.v3<<21 | sh.v3>>(64-21)
	sh.v3 ^= sh.v0

	sh.v2 += sh.v1
	sh.v1 = sh.v1<<17 | sh.v1>>(64-17)
	sh.v1 ^= sh.v2
	sh.v2 = sh.v2<<32 | sh.v2>>(64-32)
}

func (sh *sipHash) compute() uint64 {
	length := sh.length
	b := uint64(length) << 56

	var index int
	end := ((sh.length - 1) / 8) * 8
	for index = 0; index < end; index += 8 {
		m := binary.LittleEndian.Uint64(sh.data[index:])

		// two compress rounds
		sh.v3 ^= m
		sh.round()
		sh.round()
		sh.v0 ^= m
	}

	n := uint64(0)
	for ; index < length; index++ {
		n <<= 8
		n |= uint64(sh.data[index])
	}
	b |= n

	// two compress rounds
	sh.v3 ^= b
	sh.round()
	sh.round()
	sh.v0 ^= b

	// finalizer
	sh.v2 ^= 0xff

	// decompress rounds
	sh.round()
	sh.round()
	sh.round()
	sh.round()

	return sh.v0 ^ sh.v1 ^ sh.v2 ^ sh.v3
}
