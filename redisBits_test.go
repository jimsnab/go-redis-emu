package redisemu

import (
	"fmt"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestBitCountMissing(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// missing keys tests
	output := ts.ProcessCommand("bitcount", "missing")
	if !output.isInt(0) {
		t.Fatal("bitcount missing key fail")
	}

	output = ts.ProcessCommand("bitcount", "missing", "1")
	if !output.isErrorType() {
		t.Log("bitcount missing end fail - ignoring (https://github.com/redis/redis/issues/11731)")
	}

	output = ts.ProcessCommand("bitcount", "missing", "1", "10")
	if !output.isInt(0) {
		t.Fatal("bitcount missing step 3 fail")
	}

	output = ts.ProcessCommand("bitcount", "missing", "1", "10", "bytE")
	if !output.isInt(0) {
		t.Fatal("bitcount missing step 4 fail")
	}

	output = ts.ProcessCommand("bitcount", "missing", "1", "10", "Bit")
	if !output.isInt(0) {
		t.Fatal("bitcount missing step 5 fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("bitcount can't make list")
	}

	output = ts.ProcessCommand("bitcount", "list", "1", "10")
	if !output.isErrorType() {
		t.Fatal("bitcount invalid op on list")
	}
}

func TestBitCountOneByteZeroBit(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// tests of various ranges for a zero byte
	output := ts.ProcessCommand("setbit", "mykey", "0", "0")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte0bit step 1 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte0bit step 2 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0")
	// redis is documented to require end, but its implementation supports an optional end;
	// we follow the documentation (and the definition in redis' commands descriptor)
	if !output.isErrorType() {
		t.Fatal("bitcount 1byte0bit step 3 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "0")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte0bit step 4 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "1")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte0bit step 5 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "-1", "0")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte0bit step 6 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "-1")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte0bit step 7 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "-1", "-1")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte0bit step 8 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "0", "bytE")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte0bit step 9 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "4", "Bit")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte0bit step 10 fail")
	}
}

func TestBitCountOneByteOneBit(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// tests of various ranges for a byte with one bit set
	output := ts.ProcessCommand("setbit", "mykey", "2", "1")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte1bit step 1 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey")
	if !output.isInt(1) {
		t.Fatal("bitcount 1byte1bit step 2 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0")
	// redis is documented to require end, but its implementation supports an optional end;
	// we follow the documentation (and the definition in redis' commands descriptor)
	if !output.isErrorType() {
		t.Fatal("bitcount 1byte1bit step 3 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "0")
	if !output.isInt(1) {
		t.Fatal("bitcount 1byte1bit step 4 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "1")
	if !output.isInt(1) {
		t.Fatal("bitcount 1byte1bit step 5 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "-1", "0")
	if !output.isInt(1) {
		t.Fatal("bitcount 1byte1bit step 6 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "-1")
	if !output.isInt(1) {
		t.Fatal("bitcount 1byte1bit step 7 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "-1", "-1")
	if !output.isInt(1) {
		t.Fatal("bitcount 1byte1bit step 8 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "0", "bytE")
	if !output.isInt(1) {
		t.Fatal("bitcount 1byte1bit step 9 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "4", "Bit")
	if !output.isInt(1) {
		t.Fatal("bitcount 1byte1bit step 10 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "3", "4", "Bit")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte1bit step 11 fail")
	}

	// range overflow tests
	output = ts.ProcessCommand("bitcount", "mykey", "-100", "1", "bit")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte1bit negative start fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "100", "4", "bit")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte1bit positive start overflow fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "400", "bit")
	if !output.isInt(1) {
		t.Fatal("bitcount 1byte1bit positive end overflow fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "-400", "bit")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte1bit negative end overflow fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "2", "1", "bit")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte1bit end less than start fail")
	}
}

func TestBitCountTwoBytesThreeBits(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// tests of various ranges across two bytes that have three bits set
	output := ts.ProcessCommand("setbit", "mykey", "2", "1")
	if !output.isInt(0) {
		t.Fatal("bitcount 2bytes3bits step 1 fail")
	}

	output = ts.ProcessCommand("setbit", "mykey", "7", "1")
	if !output.isInt(0) {
		t.Fatal("bitcount 2bytes3bits step 2 fail")
	}

	output = ts.ProcessCommand("setbit", "mykey", "8", "1")
	if !output.isInt(0) {
		t.Fatal("bitcount 2bytes3bits step 3 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey")
	if !output.isInt(3) {
		t.Fatal("bitcount 2bytes3bits step 4 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0")
	// redis is documented to require end, but its implementation supports an optional end;
	// we follow the documentation (and the definition in redis' commands descriptor)
	if !output.isErrorType() {
		t.Fatal("bitcount 2bytes3bits step 5 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "0")
	if !output.isInt(2) {
		t.Fatal("bitcount 2bytes3bits step 6 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "1")
	if !output.isInt(3) {
		t.Fatal("bitcount 2bytes3bits step 7 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "2")
	if !output.isInt(3) {
		t.Fatal("bitcount 2bytes3bits step 8 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "-1", "0")
	if !output.isInt(0) {
		t.Fatal("bitcount 2bytes3bits step 9 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "-1")
	if !output.isInt(3) {
		t.Fatal("bitcount 2bytes3bits step 10 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "-1", "-1")
	if !output.isInt(1) {
		t.Fatal("bitcount 2bytes3bits step 11 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "0", "bytE")
	if !output.isInt(2) {
		t.Fatal("bitcount 2bytes3bits step 12 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "0", "4", "Bit")
	if !output.isInt(1) {
		t.Fatal("bitcount 2bytes3bits step 13 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "5", "10", "Bit")
	if !output.isInt(2) {
		t.Fatal("bitcount 2bytes3bits step 14 fail")
	}

	output = ts.ProcessCommand("bitcount", "mykey", "1", "0")
	if !output.isInt(0) {
		t.Fatal("bitcount 1byte1bit step 6 fail")
	}
}

func TestBitfieldGet(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// invalid arg test
	output := ts.ProcessCommand("bitfield", "mykey")

	if !output.isArray() {
		t.Fatal("bitfield no operations fail")
	}

	// prepare for several tests
	output = ts.ProcessCommand("setbit", "mykey", "2", "1")
	if !output.isInt(0) {
		t.Fatal("bitfield get step 2 fail")
	}

	output = ts.ProcessCommand("setbit", "mykey", "7", "1")
	if !output.isInt(0) {
		t.Fatal("bitfield get step 3 fail")
	}

	output = ts.ProcessCommand("setbit", "mykey", "8", "1")
	if !output.isInt(0) {
		t.Fatal("bitfield get step 4 fail")
	}

	// invalid datatype test
	output = ts.ProcessCommand("bitfield", "mykey", "get", "x8", "0")
	if !output.isErrorType() {
		t.Fatal("bitfield invalid field data type x8 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "ix", "0")
	if !output.isErrorType() {
		t.Fatal("bitfield invalid field data type ix fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "I8", "0")
	if !output.isErrorType() {
		t.Fatal("bitfield invalid field data type I8 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "U8", "0")
	if !output.isErrorType() {
		t.Fatal("bitfield invalid field data type U8 fail")
	}

	// verify various field specs
	output = ts.ProcessCommand("bitfield", "mykey", "get", "i8", "0")
	if !output.isArray(33) {
		t.Fatal("bitfield get step 5 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i8", "2")
	if !output.isArray(-122) {
		t.Fatal("bitfield get step 6 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i16", "0")
	if !output.isArray(8576) {
		t.Fatal("bitfield get step 7 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i24", "0")
	if !output.isArray(2195456) {
		t.Fatal("bitfield get step 8 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i32", "0")
	if !output.isArray(562036736) {
		t.Fatal("bitfield get step 9 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i64", "0")
	if !output.isArray(2413929400270585856) {
		t.Fatal("bitfield get step 10 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i64", "2")
	if !output.isArray(-8791026472627208192) {
		t.Fatal("bitfield get step 11 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i1", "2")
	if !output.isArray(-1) {
		t.Fatal("bitfield get step 12 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i1", "3")
	if !output.isArray(0) {
		t.Fatal("bitfield get step 13 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "u1", "2")
	if !output.isArray(1) {
		t.Fatal("bitfield get step 14 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "u1", "3")
	if !output.isArray(0) {
		t.Fatal("bitfield get step 15 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "u10", "1")
	if !output.isArray(268) {
		t.Fatal("bitfield get step 16 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "u40", "1")
	if !output.isArray(287762808832) {
		t.Fatal("bitfield get step 17 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "u64", "1")
	if !output.isErrorType() {
		t.Fatal("bitfield get step 18 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "u0", "1")
	if !output.isErrorType() {
		t.Fatal("bitfield get step 19 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i0", "1")
	if !output.isErrorType() {
		t.Fatal("bitfield get step 20 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i65", "1")
	if !output.isErrorType() {
		t.Fatal("bitfield get step 21 fail")
	}

	// multiple requests tests
	output = ts.ProcessCommand("bitfield", "mykey", "get", "i3", "1", "get", "i5", "3")
	if !output.isArray(2, 1) {
		t.Fatal("bitfield get step 22 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i8", "0", "get", "u8", "0", "get", "u4", "2")
	if !output.isArray(33, 33, 8) {
		t.Fatal("bitfield get step 23 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i3", "#0", "get", "i3", "#1", "get", "i3", "#2", "get", "i3", "#200")
	if !output.isArray(1, 0, 3, 0) {
		t.Fatal("bitfield get step 24 fail")
	}

	// bad byte field tests
	output = ts.ProcessCommand("bitfield", "mykey", "get", "i3", "#z")
	if !output.isErrorType() {
		t.Fatal("bitfield bad byte index#z  fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i3", "z0")
	if !output.isErrorType() {
		t.Fatal("bitfield bad byte index z0 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "set", "i3", "0", "zz")
	if !output.isErrorType() {
		t.Fatal("bitfield bad set value zz fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "set", "i3", "#z", "20")
	if !output.isErrorType() {
		t.Fatal("bitfield bad set byte index #z fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "set", "i3", "z0")
	if !output.isErrorType() {
		t.Fatal("bitfield bad set byte index z0 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "incrby", "i3", "0", "n")
	if !output.isErrorType() {
		t.Fatal("bitfield bad incrby incr value n fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "incrby", "i3", "z0", "1")
	if !output.isErrorType() {
		t.Fatal("bitfield bad incrby offset z0 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "incrby", "u64", "0", "1")
	if !output.isErrorType() {
		t.Fatal("bitfield bad incrby type u64 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "incrby", "u6", "0")
	if !output.isErrorType() {
		t.Fatal("bitfield bad incrby missing arg fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "set", "u6", "0")
	if !output.isErrorType() {
		t.Fatal("bitfield bad set missing arg fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("bitfield can't make list")
	}

	output = ts.ProcessCommand("bitfield", "list", "set", "u8", "1", "10")
	if !output.isErrorType() {
		t.Fatal("bitfield invalid op on list")
	}

	output = ts.ProcessCommand("bitfield", "list", "get", "u8", "1", "10")
	if !output.isErrorType() {
		t.Fatal("bitfield invalid op on list")
	}
}

func TestBitfieldSetResp2(t *testing.T) {
	ts := NewRedisTestClientResp2(t)
	defer ts.Close()

	// set and return value verification tests
	output := ts.ProcessCommand("bitfield", "mykey", "set", "i4", "0", "13")
	if !output.isArray(0) {
		t.Fatal("bitfield set step 1 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "set", "i4", "0", "-1")
	if !output.isArray(-3) {
		t.Fatal("bitfield set step 2 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "set", "i4", "0", "1")
	if !output.isArray(-1) {
		t.Fatal("bitfield set step 3 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i4", "0")
	if !output.isArray(1) {
		t.Fatal("bitfield set step 4 fail")
	}

	// overflow option tests
	output = ts.ProcessCommand("bitfield", "mykey", "overflow", "fail", "set", "i4", "0", "13")
	if !output.isArray(nil) {
		t.Fatal("bitfield set step 5 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey2", "overflow", "sat", "set", "i4", "6", "13")
	if !output.isArray(0) {
		t.Fatal("bitfield set step 6 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey2", "get", "i4", "6")
	if !output.isArray(7) {
		t.Fatal("bitfield set step 7 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey3", "overflow", "sat", "set", "i4", "6", "-13")
	if !output.isArray(0) {
		t.Fatal("bitfield set step 8 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey3", "get", "i4", "6")
	if !output.isArray(-8) {
		t.Fatal("bitfield set step 9 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey4", "overflow", "wrap", "set", "i4", "6", "13")
	if !output.isArray(0) {
		t.Fatal("bitfield set step 10 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey4", "get", "i4", "6")
	if !output.isArray(-3) {
		t.Fatal("bitfield set step 11 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey5", "overflow", "wrap", "set", "i4", "6", "-13")
	if !output.isArray(0) {
		t.Fatal("bitfield set step 12 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey5", "get", "i4", "6")
	if !output.isArray(3) {
		t.Fatal("bitfield set step 13 fail")
	}

	// byte indexing test
	output = ts.ProcessCommand("bitfield", "test", "set", "i3", "#-3", "1")
	if !output.isErrorType() {
		t.Fatal("bitfield set step 14 fail")
	}
}

func TestBitfieldSetResp3(t *testing.T) {
	ts := NewRedisTestClientResp3(t)
	defer ts.Close()

	// set and return value verification tests
	output := ts.ProcessCommand("bitfield", "mykey", "set", "i4", "0", "13")
	if !output.isArray(0) {
		t.Fatal("bitfield resp3 set step 1 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "set", "i4", "0", "-1")
	if !output.isArray(-3) {
		t.Fatal("bitfield resp3 set step 2 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "set", "i4", "0", "1")
	if !output.isArray(-1) {
		t.Fatal("bitfield resp3 set step 3 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "get", "i4", "0")
	if !output.isArray(1) {
		t.Fatal("bitfield resp3 set step 4 fail")
	}

	// overflow option tests
	output = ts.ProcessCommand("bitfield", "mykey", "overflow", "fail", "set", "i4", "0", "13")
	if !output.isArray(nil) {
		t.Fatal("bitfield resp3 set step 5 fail")
	}
}

func testSetNeighbors(t *testing.T, bitwidth int) {
	ts := NewRedisTestClient(t)
	defer ts.Close()
	ts.Lane().Tracef("testing bitwidth %d", bitwidth)

	ts.Lane().SetLogLevel(lane.LogLevelInfo) // reduce logging; remove this to see all the data of the test

	idigits := 1 << (bitwidth - 1)
	udigits := 1 << bitwidth

	iEncoding := fmt.Sprintf("i%d", bitwidth)
	uEncoding := fmt.Sprintf("u%d", bitwidth)

	for i := -idigits; i < idigits; i++ {
		output := ts.ProcessCommand("bitfield", "test", "set", iEncoding, fmt.Sprintf("#%d", i+idigits), fmt.Sprintf("%d", i))
		if !output.isArray(0) {
			t.Fatal("bitfield setnghb step 1 fail")
		}

		ts.DumpBitmapKey("test")

		output = ts.ProcessCommand("bitfield", "test", "get", iEncoding, fmt.Sprintf("#%d", i+idigits))
		if !output.isArray(i) {
			t.Fatal("bitfield setnghb step 2 fail")
		}
	}

	for i := -idigits; i < idigits; i++ {
		output := ts.ProcessCommand("bitfield", "test", "get", iEncoding, fmt.Sprintf("#%d", i+idigits))
		if !output.isArray(i) {
			t.Fatal("bitfield setnghb step 3 fail")
		}
	}

	for i := 0; i < udigits; i++ {
		output := ts.ProcessCommand("bitfield", "test2", "set", uEncoding, fmt.Sprintf("#%d", i), fmt.Sprintf("%d", i))
		if !output.isArray(0) {
			t.Fatal("bitfield setnghb step 4 fail")
		}

		output = ts.ProcessCommand("bitfield", "test2", "get", uEncoding, fmt.Sprintf("#%d", i))
		if !output.isArray(i) {
			t.Fatal("bitfield setnghb step 5 fail")
		}
	}

	for i := 0; i < udigits; i++ {
		output := ts.ProcessCommand("bitfield", "test2", "get", uEncoding, fmt.Sprintf("#%d", i))
		if !output.isArray(i) {
			t.Fatal("bitfield setnghb step 6 fail")
		}
	}
}

func TestBitfieldSetNeighbors(t *testing.T) {
	// tests that stress the bit manipulation across boundaries
	for i := 1; i < 5; i++ {
		testSetNeighbors(t, i)
	}
}

func testIncrbyNeighbors(t *testing.T, bitwidth, incrby int) {
	ts := NewRedisTestClient(t)
	defer ts.Close()
	ts.Lane().SetLogLevel(lane.LogLevelInfo) // reduce logging; remove this to see all the data of the test
	ts.Lane().Tracef("testing bitwidth %d incrby %d", bitwidth, incrby)

	valDigits := 1 << (bitwidth - 1)
	valmask := valDigits | (valDigits - 1)
	udigits := 1 << bitwidth

	iEncoding := fmt.Sprintf("i%d", bitwidth)
	uEncoding := fmt.Sprintf("u%d", bitwidth)
	incrbyText := fmt.Sprintf("%d", incrby)

	for i := -valDigits; i < valDigits; i++ {
		output := ts.ProcessCommand("bitfield", "test", "set", iEncoding, fmt.Sprintf("#%d", i+valDigits), fmt.Sprintf("%d", i))
		if !output.isArray(0) {
			t.Fatal("bitfield incrnghb step 1 fail")
		}

		expected := int64(i + incrby)
		expected &= int64(valmask)
		expected = signExtend(expected, bitwidth)

		ts.DumpBitmapKey("test")

		output = ts.ProcessCommand("bitfield", "test", "incrby", iEncoding, fmt.Sprintf("#%d", i+valDigits), incrbyText)
		if !output.isArray(expected) {
			t.Fatal("bitfield incrnghb step 2 fail")
		}

		ts.DumpBitmapKey("test")

		output = ts.ProcessCommand("bitfield", "test", "get", iEncoding, fmt.Sprintf("#%d", i+valDigits))
		if !output.isArray(expected) {
			t.Fatal("bitfield incrnghb step 3 fail")
		}
	}

	for i := 1; i < udigits; i++ {
		output := ts.ProcessCommand("bitfield", "test2", "set", uEncoding, fmt.Sprintf("#%d", i), fmt.Sprintf("%d", i))
		if !output.isArray(0) {
			t.Fatal("bitfield incrnghb step 5 fail")
		}

		uExpected := uint64(i + incrby)
		uExpected &= uint64(valmask)
		expected := int64(uExpected)

		ts.DumpBitmapKey("test2")

		output = ts.ProcessCommand("bitfield", "test2", "incrby", uEncoding, fmt.Sprintf("#%d", i), incrbyText)
		if !output.isArray(expected) {
			t.Fatal("bitfield incrnghb step 6 fail")
		}

		ts.DumpBitmapKey("test2")

		output = ts.ProcessCommand("bitfield", "test2", "get", uEncoding, fmt.Sprintf("#%d", i))
		if !output.isArray(expected) {
			t.Fatal("bitfield incrnghb step 7 fail")
		}
	}
}

func TestBitfieldIncrbyNeighbors(t *testing.T) {
	// tests for incrementing across byte boundaries
	for i := 1; i < 5; i++ {
		if i == 1 {
			testIncrbyNeighbors(t, i, 0)
			testIncrbyNeighbors(t, i, 1)
		} else {
			for j := -2; j < 3; j++ {
				testIncrbyNeighbors(t, i, j)
			}
		}
	}
}

func TestBitfieldIncrby(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// basic bitfield incrby tests
	output := ts.ProcessCommand("bitfield", "mykey", "incrby", "i4", "0", "2")
	if !output.isArray(2) {
		t.Fatal("bitfield incrby step 1 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "incrby", "i4", "0", "-4")
	if !output.isArray(-2) {
		t.Fatal("bitfield incrby step 2 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey2", "incrby", "u5", "0", "2")
	if !output.isArray(2) {
		t.Fatal("bitfield incrby step 3 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey2", "incrby", "u5", "0", "-4")
	if !output.isArray(30) {
		t.Fatal("bitfield incrby step 4 fail")
	}

	// incrby overflow tests
	output = ts.ProcessCommand("bitfield", "mykey", "overflow", "fail", "incrby", "i4", "0", "-7")
	if !output.isArray(nil) {
		t.Fatal("bitfield incrby step 5 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "overflow", "fail", "incrby", "i4", "0", "12")
	if !output.isArray(nil) {
		t.Fatal("bitfield incrby step 6 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey3", "set", "i4", "0", "-4")
	if !output.isArray(0) {
		t.Fatal("bitfield incrby step 7 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey3", "overflow", "fail", "incrby", "i4", "0", "12")
	if !output.isArray(nil) {
		t.Fatal("bitfield incrby step 8 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey3", "overflow", "fail", "incrby", "i4", "0", "11")
	if !output.isArray(7) {
		t.Fatal("bitfield incrby step 9 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey4", "set", "i4", "0", "-4")
	if !output.isArray(0) {
		t.Fatal("bitfield incrby step 10 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey4", "overflow", "sat", "incrby", "i4", "0", "21")
	if !output.isArray(7) {
		t.Fatal("bitfield incrby step 11 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey5", "set", "u4", "0", "4")
	if !output.isArray(0) {
		t.Fatal("bitfield incrby step 12 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey5", "overflow", "sat", "incrby", "u4", "0", "21")
	if !output.isArray(15) {
		t.Fatal("bitfield incrby step 13 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey6", "set", "i4", "0", "-4")
	if !output.isArray(0) {
		t.Fatal("bitfield incrby step 14 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey6", "overflow", "sat", "incrby", "i4", "0", "-21")
	if !output.isArray(-8) {
		t.Fatal("bitfield incrby step 15 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey7", "set", "u4", "0", "4")
	if !output.isArray(0) {
		t.Fatal("bitfield incrby step 16 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey7", "overflow", "wrap", "incrby", "u4", "0", "21")
	if !output.isArray(9) {
		t.Fatal("bitfield incrby step 17 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey8", "set", "i4", "0", "-4")
	if !output.isArray(0) {
		t.Fatal("bitfield incrby step 18 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey8", "overflow", "wrap", "incrby", "i4", "0", "-21")
	if !output.isArray(7) {
		t.Fatal("bitfield incrby step 19 fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("bitfield incrby can't make list")
	}

	output = ts.ProcessCommand("bitfield", "list", "incrby", "u8", "1", "10")
	if !output.isErrorType() {
		t.Fatal("bitfield invalid op on list")
	}
}

func TestBitfieldRo(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// ensure writes aren't possible tests
	output := ts.ProcessCommand("bitfield_ro", "mykey", "incrby", "i4", "0", "2")
	if !output.isErrorType() {
		t.Fatal("bitfield_ro step 1 fail")
	}

	output = ts.ProcessCommand("bitfield_ro", "mykey", "set", "i4", "2", "5")
	if !output.isErrorType() {
		t.Fatal("bitfield_ro step 2 fail")
	}

	output = ts.ProcessCommand("bitfield", "mykey", "set", "i4", "2", "5")
	if !output.isArray(0) {
		t.Fatal("bitfield_ro step 3 fail")
	}

	output = ts.ProcessCommand("bitfield_ro", "mykey", "get", "i4", "2")
	if !output.isArray(5) {
		t.Fatal("bitfield_ro step 4 fail")
	}

	// 01100001 11100100 10000101 11001101 00100111 11011001 00010011 10101011
	testVal := int64(0x61E485CD27D913AB)
	logBitmapVal(ts.Lane(), "testVal", testVal)

	highBit := uint64(1) << 63
	for offset := 0; offset < 10; offset++ {
		n := uint64(testVal)
		// set the number into the bitmap one bit at a time, big endian order
		for i := 0; i < 64; i++ {
			bit := n & highBit
			if bit != 0 {
				bit = 1
			}
			n <<= 1

			output = ts.ProcessCommand("setbit", "k2", fmt.Sprintf("%d", offset+i), fmt.Sprintf("%d", bit))
			if !output.isInt(0) && !output.isInt(1) {
				t.Fatal("bitfield_ro step 5 fail")
			}
		}

		// verify the value is read properly
		output = ts.ProcessCommand("bitfield_ro", "k2", "get", "i64", fmt.Sprintf("%d", offset))
		ts.DumpBitmapKey("k2")
		a := output.toNative().([]any)
		v := a[0].(int64)
		logBitmapVal(ts.Lane(), "retrieved val", v)
		if !output.isArray(testVal) {
			t.Fatal("bitfield_ro step 6 fail")
		}
	}
}

func TestBitOp(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// bit inversion test
	output := ts.ProcessCommand("set", "mykey", "\x1A\x2B\x3C")
	if !output.isString("OK") {
		t.Fatal("bitop step 1 fail")
	}

	output = ts.ProcessCommand("bitop", "not", "mykey2", "mykey", "mykey3")
	if !output.isErrorType() {
		t.Fatal("bitop step 2 fail")
	}

	output = ts.ProcessCommand("bitop", "not", "mykey3", "missingkey")
	if !output.isInt(0) {
		t.Fatal("bitop step 3 fail")
	}

	output = ts.ProcessCommand("bitop", "not", "mykey2", "mykey")
	if !output.isInt(3) {
		t.Fatal("bitop step 4 fail")
	}

	output = ts.ProcessCommand("get", "mykey")
	if !output.isString("\x1A\x2B\x3C") {
		t.Fatal("bitop step 5 fail")
	}

	output = ts.ProcessCommand("get", "mykey2")
	if !output.isString("\xE5\xD4\xC3") {
		t.Fatal("bitop step 6 fail")
	}

	// and logic tests
	output = ts.ProcessCommand("bitop", "and", "and1", "missingkey")
	if !output.isInt(0) {
		t.Fatal("bitop step 7 fail")
	}

	output = ts.ProcessCommand("bitop", "and", "and2", "missingkey", "missingkey2")
	if !output.isInt(0) {
		t.Fatal("bitop step 8 fail")
	}

	output = ts.ProcessCommand("bitop", "and", "and3", "mykey", "missingkey")
	if !output.isInt(3) {
		t.Fatal("bitop step 9 fail")
	}

	output = ts.ProcessCommand("get", "and3")
	if !output.isString("\x00\x00\x00") {
		t.Fatal("bitop step 10 fail")
	}

	output = ts.ProcessCommand("set", "mykey2", "\xF0\xFF\x0F")
	if !output.isString("OK") {
		t.Fatal("bitop step 11 fail")
	}

	output = ts.ProcessCommand("bitop", "and", "and4", "mykey", "mykey2")
	if !output.isInt(3) {
		t.Fatal("bitop step 12 fail")
	}

	output = ts.ProcessCommand("get", "and4")
	if !output.isString("\x10\x2B\x0C") {
		t.Fatal("bitop step 13 fail")
	}

	output = ts.ProcessCommand("get", "mykey")
	if !output.isString("\x1A\x2B\x3C") {
		t.Fatal("bitop step 14 fail")
	}

	output = ts.ProcessCommand("get", "mykey2")
	if !output.isString("\xF0\xFF\x0F") {
		t.Fatal("bitop step 15 fail")
	}

	output = ts.ProcessCommand("set", "mykey3", "\xF0\x20\x0F\xFF")
	if !output.isString("OK") {
		t.Fatal("bitop step 16 fail")
	}

	output = ts.ProcessCommand("bitop", "and", "and5", "mykey", "mykey2", "mykey3")
	if !output.isInt(4) {
		t.Fatal("bitop step 17 fail")
	}

	output = ts.ProcessCommand("get", "and5")
	if !output.isString("\x10\x20\x0C\x00") {
		t.Fatal("bitop step 18 fail")
	}

	// or logic tests
	output = ts.ProcessCommand("bitop", "or", "or1", "mykey", "mykey2", "mykey3")
	if !output.isInt(4) {
		t.Fatal("bitop step 19 fail")
	}

	output = ts.ProcessCommand("get", "or1")
	if !output.isString("\xFA\xFF\x3F\xFF") {
		t.Fatal("bitop step 20 fail")
	}

	// xor logic tests
	output = ts.ProcessCommand("bitop", "xor", "xor1", "mykey", "mykey2", "missing")
	if !output.isInt(3) {
		t.Fatal("bitop step 21 fail")
	}

	output = ts.ProcessCommand("get", "xor1")
	if !output.isString("\xEA\xD4\x33") {
		t.Fatal("bitop step 22 fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("bitop can't make list")
	}

	output = ts.ProcessCommand("bitop", "not", "destkey", "list")
	if !output.isErrorType() {
		t.Fatal("bitop invalid not op on list")
	}

	output = ts.ProcessCommand("bitop", "and", "destkey", "list", "mykey")
	if !output.isErrorType() {
		t.Fatal("bitop invalid and op on list")
	}

	output = ts.ProcessCommand("bitop", "or", "destkey", "list", "mykey")
	if !output.isErrorType() {
		t.Fatal("bitop invalid or op on list")
	}

	output = ts.ProcessCommand("bitop", "xor", "destkey", "list", "mykey")
	if !output.isErrorType() {
		t.Fatal("bitop invalid xor op on list")
	}

	output = ts.ProcessCommand("bitop", "exp", "destkey", "mykey")
	if !output.isErrorType() {
		t.Fatal("bitop invalid exp op")
	}
}

func TestBitPos(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// no end arg tests
	output := ts.ProcessCommand("set", "k", "\x1A\x2B\x3C")
	if !output.isString("OK") {
		t.Fatal("bitpos step 1 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "1")
	if !output.isInt(3) {
		t.Fatal("bitpos step 2 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "0")
	if !output.isInt(0) {
		t.Fatal("bitpos step 3 fail")
	}

	// byte range tests
	output = ts.ProcessCommand("bitpos", "k", "1", "1")
	if !output.isInt(10) {
		t.Fatal("bitpos step 4 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "0", "1")
	if !output.isInt(8) {
		t.Fatal("bitpos step 5 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "4", "1")
	if !output.isErrorType() { // "ERR The bit argument must be 1 or 0."
		t.Fatal("bitpos step 6 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "-4", "1")
	if !output.isErrorType() { // "ERR The bit argument must be 1 or 0."
		t.Fatal("bitpos step 7 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "1", "4")
	if !output.isInt(-1) {
		t.Fatal("bitpos step 8 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "0", "4")
	if !output.isInt(-1) {
		t.Fatal("bitpos step 9 fail")
	}

	// bit range tests
	output = ts.ProcessCommand("bitpos", "k", "1", "1", "BIT")
	if !output.isErrorType() {
		t.Fatal("bitpos step 10 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "1", "1", "-1", "Bit")
	if !output.isInt(3) {
		t.Fatal("bitpos step 11 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "0", "1", "-1", "BiT")
	if !output.isInt(1) {
		t.Fatal("bitpos step 12 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "1", "4", "-1", "bit")
	if !output.isInt(4) {
		t.Fatal("bitpos step 13 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "0", "4", "-1", "bit")
	if !output.isInt(5) {
		t.Fatal("bitpos step 14 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "1", "-3", "-1", "bit")
	if !output.isInt(21) {
		t.Fatal("bitpos step 15 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "0", "-2", "-1", "bit")
	if !output.isInt(22) {
		t.Fatal("bitpos step 16 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "1", "-2", "-1", "byte")
	if !output.isInt(10) {
		t.Fatal("bitpos step 17 fail")
	}

	// byte token tests
	output = ts.ProcessCommand("bitpos", "k", "0", "-2", "-1", "byte")
	if !output.isInt(8) {
		t.Fatal("bitpos step 18 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "1", "-2", "-3", "byte")
	if !output.isInt(-1) {
		t.Fatal("bitpos step 19 fail")
	}

	output = ts.ProcessCommand("bitpos", "k", "0", "-2", "-3", "byte")
	if !output.isInt(-1) {
		t.Fatal("bitpos step 20 fail")
	}

	// missing key tests
	output = ts.ProcessCommand("bitpos", "missing", "0")
	if !output.isInt(0) {
		t.Fatal("bitpos search for 0 in missing key fail")
	}

	output = ts.ProcessCommand("bitpos", "missing", "1")
	if !output.isInt(-1) {
		t.Fatal("bitpos search for 1 in missing key fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("bitpos can't make list")
	}

	output = ts.ProcessCommand("bitpos", "list", "0")
	if !output.isErrorType() {
		t.Fatal("bitpos invalid op on list")
	}
}

func TestGetBit(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	// get bits of various positions test
	output := ts.ProcessCommand("set", "k", "\x1A\x2B\x3C")
	if !output.isString("OK") {
		t.Fatal("getbit step 1 fail")
	}

	output = ts.ProcessCommand("getbit", "k", "1")
	if !output.isInt(0) {
		t.Fatal("getbit step 2 fail")
	}

	output = ts.ProcessCommand("getbit", "k", "3")
	if !output.isInt(1) {
		t.Fatal("getbit step 3 fail")
	}

	output = ts.ProcessCommand("getbit", "k", "10")
	if !output.isInt(1) {
		t.Fatal("getbit step 4 fail")
	}

	output = ts.ProcessCommand("getbit", "k", "11")
	if !output.isInt(0) {
		t.Fatal("getbit step 5 fail")
	}

	output = ts.ProcessCommand("getbit", "k", "40")
	if !output.isInt(0) {
		t.Fatal("getbit step 6 fail")
	}

	// invalid offset test
	output = ts.ProcessCommand("getbit", "k", "-1")
	if !output.isErrorType() {
		t.Fatal("getbit invalid offset fail")
	}

	// missing key test
	output = ts.ProcessCommand("getbit", "missing", "0")
	if !output.isInt(0) {
		t.Fatal("getbit step 7 fail")
	}

	// wrong type test
	output = ts.ProcessCommand("rpush", "list", "x")
	if !output.isInt(1) {
		t.Fatal("getbit can't make list")
	}

	output = ts.ProcessCommand("getbit", "list", "1")
	if !output.isErrorType() {
		t.Fatal("getbit invalid op on list")
	}
}

func TestSetBit(t *testing.T) {
	ts := NewRedisTestClient(t)
	defer ts.Close()

	output := ts.ProcessCommand("setbit", "k", "0", "1")
	if !output.isInt(0) {
		t.Fatal("setbit step 1 fail")
	}

	output = ts.ProcessCommand("getbit", "k", "0")
	if !output.isInt(1) {
		t.Fatal("setbit step 2 fail")
	}

	output = ts.ProcessCommand("setbit", "k", "0", "1")
	if !output.isInt(1) {
		t.Fatal("setbit step 3 fail")
	}

	output = ts.ProcessCommand("setbit", "k", "0", "0")
	if !output.isInt(1) {
		t.Fatal("setbit step 4 fail")
	}

	output = ts.ProcessCommand("getbit", "k", "0")
	if !output.isInt(0) {
		t.Fatal("setbit step 5 fail")
	}

	output = ts.ProcessCommand("setbit", "k", "10", "1")
	if !output.isInt(0) {
		t.Fatal("setbit step 6 fail")
	}

	output = ts.ProcessCommand("getbit", "k", "10")
	if !output.isInt(1) {
		t.Fatal("setbit step 7 fail")
	}

	output = ts.ProcessCommand("getbit", "k", "100")
	if !output.isInt(0) {
		t.Fatal("setbit step 8 fail")
	}

	output = ts.ProcessCommand("setbit", "k", "100", "1")
	if !output.isInt(0) {
		t.Fatal("setbit step 9 fail")
	}

	output = ts.ProcessCommand("getbit", "k", "100")
	if !output.isInt(1) {
		t.Fatal("setbit step 10 fail")
	}

	// invalid offset test
	output = ts.ProcessCommand("setbit", "k", "-1", "0")
	if !output.isErrorType() {
		t.Fatal("setbit invalid offset fail")
	}

	output = ts.ProcessCommand("setbit", "k", "1", "10")
	if !output.isErrorType() {
		t.Fatal("setbit invalid bit value fail")
	}
}
