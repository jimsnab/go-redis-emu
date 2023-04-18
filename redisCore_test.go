package redisemu

import (
	"fmt"
	"testing"
	"time"
)

func TestRedisCopyStrings(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key tests
	output := ts.ProcessCommand("copy", "missing", "target")
	if !output.isInt(0) {
		t.Fatal("copy step 1 fail")
	}

	output = ts.ProcessCommand("copy", "missing", "target", "rePlace")
	if !output.isInt(0) {
		t.Fatal("copy step 2 fail")
	}

	// existing dest, missing source test
	output = ts.ProcessCommand("set", "source", "x")
	if !output.isString("OK") {
		t.Fatal("copy step 3 fail")
	}

	output = ts.ProcessCommand("set", "target", "y")
	if !output.isString("OK") {
		t.Fatal("copy step 4 fail")
	}

	output = ts.ProcessCommand("copy", "missing", "target")
	if !output.isInt(0) {
		t.Fatal("copy step 5 fail")
	}

	output = ts.ProcessCommand("get", "target")
	if !output.isString("y") {
		t.Fatal("copy step 6 fail")
	}

	// existing dest, missing source, with replace
	output = ts.ProcessCommand("copy", "missing", "target", "replace")
	if !output.isInt(0) {
		t.Fatal("copy step 7 fail")
	}

	output = ts.ProcessCommand("get", "target")
	if !output.isString("y") {
		t.Fatal("copy step 8 fail")
	}

	// basic copy test
	output = ts.ProcessCommand("del", "target")
	if !output.isInt(1) {
		t.Fatal("copy step 9 fail")
	}

	output = ts.ProcessCommand("copy", "source", "target")
	if !output.isInt(1) {
		t.Fatal("copy step 10 fail")
	}

	output = ts.ProcessCommand("get", "target")
	if !output.isString("x") {
		t.Fatal("copy step 11 fail")
	}

	// can't overwite test
	output = ts.ProcessCommand("copy", "source", "target")
	if !output.isInt(0) {
		t.Fatal("copy step 12 fail")
	}

	// can overwrite test
	output = ts.ProcessCommand("set", "target", "z")
	if !output.isString("OK") {
		t.Fatal("copy step 13 fail")
	}

	output = ts.ProcessCommand("copy", "source", "target", "replace")
	if !output.isInt(1) {
		t.Fatal("copy step 14 fail")
	}

	output = ts.ProcessCommand("get", "target")
	if !output.isString("x") {
		t.Fatal("copy step 15 fail")
	}

	// BUGBUG no support for cross database copy yet
	_, isTestClient := ts.(*testClient)
	if isTestClient {
		output = ts.ProcessCommand("copy", "source", "target", "db", "2", "replace")
		if !output.isErrorType() {
			t.Fatal("copy step 16 fail")
		}
	}
}

func TestRedisDel(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key test
	output := ts.ProcessCommand("del", "missing")
	if !output.isInt(0) {
		t.Fatal("del step 1 fail")
	}

	// single delete test
	output = ts.ProcessCommand("set", "k1", "cat")
	if !output.isString("OK") {
		t.Fatal("del step 2 fail")
	}

	output = ts.ProcessCommand("set", "k2", "dog")
	if !output.isString("OK") {
		t.Fatal("del step 3 fail")
	}

	output = ts.ProcessCommand("set", "k3", "fox")
	if !output.isString("OK") {
		t.Fatal("del step 4 fail")
	}

	output = ts.ProcessCommand("del", "k1")
	if !output.isInt(1) {
		t.Fatal("del step 5 fail")
	}

	// two key delete with return val checks
	output = ts.ProcessCommand("del", "k1", "k2")
	if !output.isInt(1) {
		t.Fatal("del step 6 fail")
	}

	output = ts.ProcessCommand("set", "k1", "cat")
	if !output.isString("OK") {
		t.Fatal("del step 7 fail")
	}

	output = ts.ProcessCommand("set", "k2", "dog")
	if !output.isString("OK") {
		t.Fatal("del step 8 fail")
	}

	output = ts.ProcessCommand("del", "k1", "k2")
	if !output.isInt(2) {
		t.Fatal("del step 9 fail")
	}

	// non existing key delete
	output = ts.ProcessCommand("set", "k1", "cat")
	if !output.isString("OK") {
		t.Fatal("del step 10 fail")
	}

	output = ts.ProcessCommand("set", "k2", "dog")
	if !output.isString("OK") {
		t.Fatal("del step 11 fail")
	}

	output = ts.ProcessCommand("del", "k1", "k2", "k3")
	if !output.isInt(3) {
		t.Fatal("del step 12 fail")
	}

	output = ts.ProcessCommand("del")
	if !output.isErrorType() {
		t.Fatal("del step 13 fail")
	}
}

func TestRedisUnlink(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// unlink a missing key test
	output := ts.ProcessCommand("unlink", "missing")
	if !output.isInt(0) {
		t.Fatal("unlink step 1 fail")
	}

	// unlink existing keys test
	output = ts.ProcessCommand("set", "k1", "cat")
	if !output.isString("OK") {
		t.Fatal("unlink step 2 fail")
	}

	output = ts.ProcessCommand("set", "k2", "dog")
	if !output.isString("OK") {
		t.Fatal("unlink step 3 fail")
	}

	output = ts.ProcessCommand("set", "k3", "fox")
	if !output.isString("OK") {
		t.Fatal("unlink step 4 fail")
	}

	output = ts.ProcessCommand("unlink", "k1")
	if !output.isInt(1) {
		t.Fatal("unlink step 5 fail")
	}

	output = ts.ProcessCommand("unlink", "k1", "k2")
	if !output.isInt(1) {
		t.Fatal("unlink step 6 fail")
	}

	// keys can be recreated and unlinked again
	output = ts.ProcessCommand("set", "k1", "cat")
	if !output.isString("OK") {
		t.Fatal("unlink step 7 fail")
	}

	output = ts.ProcessCommand("set", "k2", "dog")
	if !output.isString("OK") {
		t.Fatal("unlink step 8 fail")
	}

	output = ts.ProcessCommand("unlink", "k1", "k2")
	if !output.isInt(2) {
		t.Fatal("unlink step 9 fail")
	}

	output = ts.ProcessCommand("set", "k1", "cat")
	if !output.isString("OK") {
		t.Fatal("unlink step 10 fail")
	}

	output = ts.ProcessCommand("set", "k2", "dog")
	if !output.isString("OK") {
		t.Fatal("unlink step 11 fail")
	}

	output = ts.ProcessCommand("unlink", "k1", "k2", "k3")
	if !output.isInt(3) {
		t.Fatal("unlink step 12 fail")
	}

	// missing args test
	output = ts.ProcessCommand("unlink")
	if !output.isErrorType() {
		t.Fatal("unlink step 13 fail")
	}
}

func TestRedisExists(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing args test
	output := ts.ProcessCommand("exists")
	if !output.isErrorType() {
		t.Fatal("exists step 1 fail")
	}

	// missing key test
	output = ts.ProcessCommand("exists", "missing")
	if !output.isInt(0) {
		t.Fatal("exists step 2 fail")
	}

	// basic exists test
	output = ts.ProcessCommand("set", "k1", "cat")
	if !output.isString("OK") {
		t.Fatal("exists step 3 fail")
	}

	output = ts.ProcessCommand("exists", "k1")
	if !output.isInt(1) {
		t.Fatal("exists step 4 fail")
	}

	// mix of non existing and existing
	output = ts.ProcessCommand("exists", "missing", "k1")
	if !output.isInt(1) {
		t.Fatal("exists step 4 fail")
	}

	output = ts.ProcessCommand("exists", "k1", "missing")
	if !output.isInt(1) {
		t.Fatal("exists step 5 fail")
	}

	// duplicates ok
	output = ts.ProcessCommand("exists", "k1", "k1")
	if !output.isInt(2) {
		t.Fatal("exists step 6 fail")
	}

	// two key test
	output = ts.ProcessCommand("set", "k2", "dog")
	if !output.isString("OK") {
		t.Fatal("exists step 7 fail")
	}

	output = ts.ProcessCommand("exists", "k1", "k2")
	if !output.isInt(2) {
		t.Fatal("exists step 8 fail")
	}
}

func TestRedisDumpRestore(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// basic dump restore cycle
	output := ts.ProcessCommand("set", "test", "cat")
	if !output.isString("OK") {
		t.Fatal("dump step 1 fail")
	}

	output = ts.ProcessCommand("dump", "test")
	val, valid := output.toString()
	if !valid {
		t.Fatal("dump step 2 fail")
	}

	output = ts.ProcessCommand("restore", "test2", "0", val)
	if !output.isString("OK") {
		t.Fatal("dump step 3 fail")
	}

	output = ts.ProcessCommand("get", "test2")
	if !output.isString("cat") {
		t.Fatal("dump step 4 fail")
	}

	// restore with expiration
	output = ts.ProcessCommand("restore", "test3", "50", val)
	if !output.isString("OK") {
		t.Fatal("dump step 5 fail")
	}

	output = ts.ProcessCommand("get", "test3")
	if !output.isString("cat") {
		t.Fatal("dump step 6 fail")
	}

	time.Sleep(51 * time.Millisecond)
	output = ts.ProcessCommand("get", "test3")
	if !output.isNull() {
		t.Fatal("dump step 7 fail")
	}

	// restore with absolute time expiration
	end := ts.ServerNow().Add(time.Duration(50) * time.Millisecond).UnixMilli()

	output = ts.ProcessCommand("restore", "test4", fmt.Sprintf("%d", end), val, "absttl")
	if !output.isString("OK") {
		t.Fatal("dump step 8 fail")
	}

	output = ts.ProcessCommand("get", "test4")
	if !output.isString("cat") {
		t.Fatal("dump step 9 fail")
	}

	time.Sleep(51 * time.Millisecond)
	output = ts.ProcessCommand("get", "test4")
	if !output.isNull() {
		t.Fatal("dump step 10 fail")
	}

	// restore replace test
	output = ts.ProcessCommand("set", "test4", "x")
	if !output.isString("OK") {
		t.Fatal("dump step 11 fail")
	}

	output = ts.ProcessCommand("restore", "test4", val)
	if !output.isErrorType() {
		t.Fatal("dump step 12 fail")
	}

	output = ts.ProcessCommand("restore", "test4", "0", val, "replace")
	if !output.isString("OK") {
		t.Fatal("dump step 13 fail")
	}

	output = ts.ProcessCommand("get", "test4")
	if !output.isString("cat") {
		t.Fatal("dump step 14 fail")
	}

	// restore to an existing key
	output = ts.ProcessCommand("restore", "test4", "0", val)
	if !output.isErrorType() {
		t.Fatal("dump restore to existing fail")
	}

	// restore an invalid string test
	output = ts.ProcessCommand("restore", "key", "0", val[0:len(val)-1]+string(val[len(val)-1]<<1))
	if !output.isErrorType() {
		t.Fatal("dump restore invalid checksum fail")
	}

	output = ts.ProcessCommand("restore", "key", "0", "invalid")
	if !output.isErrorType() {
		t.Fatal("dump restore invalid data fail")
	}
}

func TestRedisExpire(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// key missing test
	output := ts.ProcessCommand("expire", "missing", "10")
	if !output.isInt(0) {
		t.Fatal("expire step 1 fail")
	}

	// 1 second expiration test
	output = ts.ProcessCommand("set", "k", "myval")
	if !output.isString("OK") {
		t.Fatal("expire step 2 fail")
	}

	output = ts.ProcessCommand("expire", "k", "1")
	if !output.isInt(1) {
		t.Fatal("expire step 3 fail")
	}

	output = ts.ProcessCommand("get", "k")
	if !output.isString("myval") {
		t.Fatal("expire step 4 fail")
	}

	time.Sleep(1001 * time.Millisecond)

	output = ts.ProcessCommand("get", "k")
	if !output.isNull() {
		t.Fatal("expire step 5 fail")
	}

	// not exist option test
	output = ts.ProcessCommand("set", "k", "myval")
	if !output.isString("OK") {
		t.Fatal("expire step 6 fail")
	}

	output = ts.ProcessCommand("expire", "k", "1", "nx")
	if !output.isInt(1) {
		t.Fatal("expire step 7 fail")
	}

	output = ts.ProcessCommand("expire", "k", "1", "nx")
	if !output.isInt(0) {
		t.Fatal("expire step 8 fail")
	}

	// exist option test
	output = ts.ProcessCommand("set", "k", "myval")
	if !output.isString("OK") {
		t.Fatal("expire step 9 fail")
	}

	output = ts.ProcessCommand("expire", "k", "1", "xx")
	if !output.isInt(0) {
		t.Fatal("expire step 10 fail")
	}

	output = ts.ProcessCommand("expire", "k", "1")
	if !output.isInt(1) {
		t.Fatal("expire step 11 fail")
	}

	output = ts.ProcessCommand("expire", "k", "1", "xx")
	if !output.isInt(1) {
		t.Fatal("expire step 12 fail")
	}

	// greather than option test
	output = ts.ProcessCommand("set", "k", "myval")
	if !output.isString("OK") {
		t.Fatal("expire step 13 fail")
	}

	output = ts.ProcessCommand("expire", "k", "2")
	if !output.isInt(1) {
		t.Fatal("expire step 14 fail")
	}

	output = ts.ProcessCommand("expire", "k", "1", "gt")
	if !output.isInt(0) {
		t.Fatal("expire step 15 fail")
	}

	output = ts.ProcessCommand("expire", "k", "3", "gt")
	if !output.isInt(1) {
		t.Fatal("expire step 16 fail")
	}

	// less than option test
	output = ts.ProcessCommand("set", "k", "myval")
	if !output.isString("OK") {
		t.Fatal("expire step 17 fail")
	}

	output = ts.ProcessCommand("expire", "k", "5")
	if !output.isInt(1) {
		t.Fatal("expire step 18 fail")
	}

	output = ts.ProcessCommand("expire", "k", "10", "lt")
	if !output.isInt(0) {
		t.Fatal("expire step 19 fail")
	}

	output = ts.ProcessCommand("expire", "k", "2", "lt")
	if !output.isInt(1) {
		t.Fatal("expire step 20 fail")
	}
}

func TestRedisExpireAt(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// expire at on missing key
	ttl := ts.ServerNow().Add(10 * time.Second)
	ttlText := fmt.Sprintf("%d", ttl.Unix())

	output := ts.ProcessCommand("expireat", "missing", ttlText)
	if !output.isInt(0) {
		t.Fatal("expireat step 1 fail")
	}

	// expire at on existing key
	output = ts.ProcessCommand("set", "k", "myval")
	if !output.isString("OK") {
		t.Fatal("expireat step 2 fail")
	}

	ttl = ts.ServerNow().Add(1 * time.Second)
	ttlText = fmt.Sprintf("%d", ttl.Unix())

	output = ts.ProcessCommand("expireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("expireat step 3 fail")
	}

	output = ts.ProcessCommand("get", "k")
	if !output.isString("myval") {
		t.Fatal("expireat step 4 fail")
	}

	time.Sleep(1001 * time.Millisecond)

	output = ts.ProcessCommand("get", "k")
	if !output.isNull() {
		t.Fatal("expireat step 5 fail")
	}

	// expire at nx option test
	ttl = ts.ServerNow().Add(1 * time.Second)
	ttlText = fmt.Sprintf("%d", ttl.Unix())

	output = ts.ProcessCommand("set", "k", ttlText)
	if !output.isString("OK") {
		t.Fatal("expireat step 6 fail")
	}

	output = ts.ProcessCommand("expireat", "k", ttlText, "nx")
	if !output.isInt(1) {
		t.Fatal("expireat step 7 fail")
	}

	output = ts.ProcessCommand("expireat", "k", ttlText, "nx")
	if !output.isInt(0) {
		t.Fatal("expireat step 8 fail")
	}

	// expire at xx option test
	output = ts.ProcessCommand("set", "k", ttlText)
	if !output.isString("OK") {
		t.Fatal("expireat step 9 fail")
	}

	output = ts.ProcessCommand("expireat", "k", ttlText, "xx")
	if !output.isInt(0) {
		t.Fatal("expireat step 10 fail")
	}

	output = ts.ProcessCommand("expireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("expireat step 11 fail")
	}

	output = ts.ProcessCommand("expireat", "k", ttlText, "xx")
	if !output.isInt(1) {
		t.Fatal("expireat step 12 fail")
	}

	// expire at gt option test
	output = ts.ProcessCommand("set", "k", ttlText)
	if !output.isString("OK") {
		t.Fatal("expireat step 13 fail")
	}

	output = ts.ProcessCommand("expireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("expireat step 14 fail")
	}

	output = ts.ProcessCommand("expireat", "k", ttlText, "gt")
	if !output.isInt(0) {
		t.Fatal("expireat step 15 fail")
	}

	ttl2 := ts.ServerNow().Add(3 * time.Second)
	ttlText2 := fmt.Sprintf("%d", ttl2.Unix())

	output = ts.ProcessCommand("expireat", "k", ttlText2, "gt")
	if !output.isInt(1) {
		t.Fatal("expireat step 16 fail")
	}

	// expire at lt option test
	output = ts.ProcessCommand("set", "k", ttlText)
	if !output.isString("OK") {
		t.Fatal("expireat step 17 fail")
	}

	ttl = ts.ServerNow().Add(5 * time.Second)
	ttlText = fmt.Sprintf("%d", ttl.Unix())

	output = ts.ProcessCommand("expireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("expireat step 18 fail")
	}

	ttl2 = ts.ServerNow().Add(10 * time.Second)
	ttlText2 = fmt.Sprintf("%d", ttl2.Unix())

	output = ts.ProcessCommand("expireat", "k", ttlText2, "lt")
	if !output.isInt(0) {
		t.Fatal("expireat step 19 fail")
	}

	ttl2 = ts.ServerNow().Add(2 * time.Second)
	ttlText2 = fmt.Sprintf("%d", ttl2.Unix())

	output = ts.ProcessCommand("expireat", "k", ttlText2, "lt")
	if !output.isInt(1) {
		t.Fatal("expireat step 20 fail")
	}
}

func TestRedisExpireTime(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// basic expire time test
	ttl := ts.ServerNow().Add(10 * time.Second)
	ttlText := fmt.Sprintf("%d", ttl.Unix())

	output := ts.ProcessCommand("set", "k", "1")
	if !output.isString("OK") {
		t.Fatal("expiretime step 1 fail")
	}

	output = ts.ProcessCommand("expireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("expiretime step 2 fail")
	}

	output = ts.ProcessCommand("expiretime", "k")
	if !output.isInt64(ttl.Unix()) {
		t.Fatal("expiretime step 3 fail")
	}

	// missing key test
	output = ts.ProcessCommand("expiretime", "k2")
	if !output.isInt(-2) {
		t.Fatal("expiretime step 4 fail")
	}

	// no expiration test
	output = ts.ProcessCommand("set", "k3", "1")
	if !output.isString("OK") {
		t.Fatal("expiretime step 5 fail")
	}

	output = ts.ProcessCommand("expiretime", "k3")
	if !output.isInt(-1) {
		t.Fatal("expiretime step 6 fail")
	}
}

func TestRedisPExpire(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key test
	output := ts.ProcessCommand("pexpire", "missing", "10")
	if !output.isInt(0) {
		t.Fatal("pexpire can't expire missing key fail")
	}

	// basic ms expiration test
	output = ts.ProcessCommand("set", "k", "myval")
	if !output.isString("OK") {
		t.Fatal("pexpire create test key fail")
	}

	output = ts.ProcessCommand("pexpire", "k", "60")
	if !output.isInt(1) {
		t.Fatal("pexpire set a short expiration fail")
	}

	output = ts.ProcessCommand("get", "k")
	if !output.isString("myval") {
		t.Fatal("pexpire get the key before expiration fail")
	}

	time.Sleep(61 * time.Millisecond)

	output = ts.ProcessCommand("get", "k")
	if !output.isNull() {
		t.Fatal("pexpire get the key after expiration fail")
	}

	// nx option test
	output = ts.ProcessCommand("set", "k", "myval")
	if !output.isString("OK") {
		t.Fatal("pexpire create nx key fail")
	}

	output = ts.ProcessCommand("pexpire", "k", "40", "nx")
	if !output.isInt(1) {
		t.Fatal("pexpire expire nx 40ms fail")
	}

	output = ts.ProcessCommand("pexpire", "k", "40", "nx")
	if !output.isInt(0) {
		t.Fatal("pexpire expire nx that already has expiration fail")
	}

	// xx option test
	output = ts.ProcessCommand("set", "k", "myval")
	if !output.isString("OK") {
		t.Fatal("pexpire create xx key fail")
	}

	output = ts.ProcessCommand("pexpire", "k", "40", "xx")
	if !output.isInt(0) {
		t.Fatal("pexpire expire xx that doesn't have expiration fail")
	}

	output = ts.ProcessCommand("pexpire", "k", "40")
	if !output.isInt(1) {
		t.Fatal("pexpire set expiration for xx test fail")
	}

	output = ts.ProcessCommand("pexpire", "k", "40", "xx")
	if !output.isInt(1) {
		t.Fatal("pexpire expire xx that has expiration fail")
	}

	// gt option test
	output = ts.ProcessCommand("set", "k", "myval")
	if !output.isString("OK") {
		t.Fatal("pexpire create gt key fail")
	}

	output = ts.ProcessCommand("pexpire", "k", "100")
	if !output.isInt(1) {
		t.Fatal("pexpire set gt initial expiration fail")
	}

	output = ts.ProcessCommand("pexpire", "k", "90", "gt")
	if !output.isInt(0) {
		t.Fatal("pexpire can't set gt that is less than fail")
	}

	output = ts.ProcessCommand("pexpire", "k", "110", "gt")
	if !output.isInt(1) {
		t.Fatal("pexpire can set gt that is greater than fail")
	}

	// lt option test
	output = ts.ProcessCommand("set", "k", "myval")
	if !output.isString("OK") {
		t.Fatal("pexpire create lt key fail")
	}

	output = ts.ProcessCommand("pexpire", "k", "50")
	if !output.isInt(1) {
		t.Fatal("pexpire set initial expiration fail")
	}

	output = ts.ProcessCommand("pexpire", "k", "100", "lt")
	if !output.isInt(0) {
		t.Fatal("pexpire can't set lt that is greater than fail")
	}

	output = ts.ProcessCommand("pexpire", "k", "20", "lt")
	if !output.isInt(1) {
		t.Fatal("pexpire can set lt that is less than fail")
	}
}

func TestRedisPExpireAt(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key test
	ttl := ts.ServerNow().Add(10 * time.Millisecond)
	ttlText := fmt.Sprintf("%d", ttl.UnixMilli())

	output := ts.ProcessCommand("pexpireat", "missing", ttlText)
	if !output.isInt(0) {
		t.Fatal("pexpireat missing key fail")
	}

	// expire at basic test
	output = ts.ProcessCommand("set", "k", "myval")
	if !output.isString("OK") {
		t.Fatal("pexpireat create basic test key fail")
	}

	ttl = ts.ServerNow().Add(40 * time.Millisecond)
	ttlText = fmt.Sprintf("%d", ttl.UnixMilli())

	output = ts.ProcessCommand("pexpireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("pexpireat expire at 40ms fail")
	}

	output = ts.ProcessCommand("get", "k")
	if !output.isString("myval") {
		t.Fatal("pexpireat get value before expiration fail")
	}

	time.Sleep(41 * time.Millisecond)

	output = ts.ProcessCommand("get", "k")
	if !output.isNull() {
		t.Fatal("pexpireat get value after expiration fail")
	}

	// nx option test
	ttl = ts.ServerNow().Add(40 * time.Millisecond)
	ttlText = fmt.Sprintf("%d", ttl.UnixMilli())

	output = ts.ProcessCommand("set", "k", ttlText)
	if !output.isString("OK") {
		t.Fatal("pexpireat create nx key fail")
	}

	output = ts.ProcessCommand("pexpireat", "k", ttlText, "nx")
	if !output.isInt(1) {
		t.Fatal("pexpireat nx expireat set fail")
	}

	output = ts.ProcessCommand("pexpireat", "k", ttlText, "nx")
	if !output.isInt(0) {
		t.Fatal("pexpireat nx expireat already has expiration fail")
	}

	// xx option test
	output = ts.ProcessCommand("set", "k", ttlText)
	if !output.isString("OK") {
		t.Fatal("pexpireat create xx key fail")
	}

	output = ts.ProcessCommand("pexpireat", "k", ttlText, "xx")
	if !output.isInt(0) {
		t.Fatal("pexpireat xx expireat has no expiration yet fail")
	}

	output = ts.ProcessCommand("pexpireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("pexpireat set expiration fail")
	}

	output = ts.ProcessCommand("pexpireat", "k", ttlText, "xx")
	if !output.isInt(1) {
		t.Fatal("pexpireat xx expireat of key with expiration fail")
	}

	// gt option test
	output = ts.ProcessCommand("set", "k", ttlText)
	if !output.isString("OK") {
		t.Fatal("pexpireat create gt key fail")
	}

	output = ts.ProcessCommand("pexpireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("pexpireat set expiration for gt test fail")
	}

	output = ts.ProcessCommand("pexpireat", "k", ttlText, "gt")
	if !output.isInt(0) {
		t.Fatal("pexpireat can't set gt expiration (this expiration is not greater than itself) fail")
	}

	ttl2 := ts.ServerNow().Add(60 * time.Millisecond)
	ttlText2 := fmt.Sprintf("%d", ttl2.UnixMilli())

	output = ts.ProcessCommand("pexpireat", "k", ttlText2, "gt")
	if !output.isInt(1) {
		t.Fatal("pexpireat can set gt expiration (now + 60ms > original expiration) fail")
	}

	// lt option test
	output = ts.ProcessCommand("set", "k", ttlText)
	if !output.isString("OK") {
		t.Fatal("pexpireat create lt key fail")
	}

	ttl = ts.ServerNow().Add(90 * time.Millisecond)
	ttlText = fmt.Sprintf("%d", ttl.UnixMilli())

	output = ts.ProcessCommand("pexpireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("pexpireat set expiration for lt test fail")
	}

	ttl2 = ts.ServerNow().Add(150 * time.Millisecond)
	ttlText2 = fmt.Sprintf("%d", ttl2.UnixMilli())

	output = ts.ProcessCommand("pexpireat", "k", ttlText2, "lt")
	if !output.isInt(0) {
		t.Fatal("pexpireat can't set lt expiration (150ms > ~90ms) fail")
	}

	ttl2 = ts.ServerNow().Add(60 * time.Millisecond)
	ttlText2 = fmt.Sprintf("%d", ttl2.UnixMilli())

	output = ts.ProcessCommand("pexpireat", "k", ttlText2, "lt")
	if !output.isInt(1) {
		t.Fatal("pexpireat can set lt expiration (60ms < ~90ms) fail")
	}
}

func TestRedisPExpireTime(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// basic expiretime test
	ttl := ts.ServerNow().Add(10 * time.Millisecond)
	ttlText := fmt.Sprintf("%d", ttl.UnixMilli())

	output := ts.ProcessCommand("set", "k", "1")
	if !output.isString("OK") {
		t.Fatal("pexpiretime step 1 fail")
	}

	output = ts.ProcessCommand("pexpireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("pexpiretime step 2 fail")
	}

	output = ts.ProcessCommand("pexpiretime", "k")
	if !output.isInt64(ttl.UnixMilli()) {
		t.Fatal("pexpiretime step 3 fail")
	}

	// missing key test
	output = ts.ProcessCommand("pexpiretime", "k2")
	if !output.isInt(-2) {
		t.Fatal("pexpiretime step 4 fail")
	}

	// no expiration test
	output = ts.ProcessCommand("set", "k3", "1")
	if !output.isString("OK") {
		t.Fatal("pexpiretime step 5 fail")
	}

	output = ts.ProcessCommand("pexpiretime", "k3")
	if !output.isInt(-1) {
		t.Fatal("pexpiretime step 6 fail")
	}
}

func TestRedisPersist(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// basic persist test
	ttl := ts.ServerNow().Add(40 * time.Millisecond)
	ttlText := fmt.Sprintf("%d", ttl.UnixMilli())

	output := ts.ProcessCommand("set", "k", "1")
	if !output.isString("OK") {
		t.Fatal("persist create persist key fail")
	}

	output = ts.ProcessCommand("pexpireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("persist set expiration for persist test fail")
	}

	output = ts.ProcessCommand("pexpiretime", "k")
	if !output.isInt64(ttl.UnixMilli()) {
		t.Fatal("persist get the expiration fail")
	}

	output = ts.ProcessCommand("persist", "k")
	if !output.isInt(1) {
		t.Fatal("persist persist to remove expiration fail")
	}

	output = ts.ProcessCommand("pexpiretime", "k")
	if !output.isInt(-1) {
		t.Fatal("persist confirm no expiration fail")
	}

	// persist on key without an expiration
	output = ts.ProcessCommand("persist", "k")
	if !output.isInt(0) {
		t.Fatal("persist persist without expiration fail")
	}
}

func TestRedisTtl(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// basic ttl test
	now := ts.ServerNow()
	msToNextSec := 1000 - (now.UnixMilli() % 1000)

	ttl := ts.ServerNow().Add(time.Duration(3100+msToNextSec) * time.Millisecond)
	ttlText := fmt.Sprintf("%d", ttl.UnixMilli())

	output := ts.ProcessCommand("set", "k", "1")
	if !output.isString("OK") {
		t.Fatal("ttl step 1 fail")
	}

	output = ts.ProcessCommand("pexpireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("ttl step 2 fail")
	}

	output = ts.ProcessCommand("pexpiretime", "k")
	if !output.isInt64(ttl.UnixMilli()) {
		t.Fatal("ttl step 3 fail")
	}

	output = ts.ProcessCommand("ttl", "k")
	if !output.isAtLeast(3) {
		t.Fatal("ttl expires in 3 or 4 seconds fail")
	}

	// ttl on key without expiration
	output = ts.ProcessCommand("set", "k2", "dog")
	if !output.isString("OK") {
		t.Fatal("ttl step 5 fail")
	}

	output = ts.ProcessCommand("ttl", "k2")
	if !output.isInt(-1) {
		t.Fatal("ttl step 6 fail")
	}

	// missing key test
	output = ts.ProcessCommand("ttl", "k3")
	if !output.isInt(-2) {
		t.Fatal("ttl step 7 fail")
	}
}

func TestRedisPTtl(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// basic ms ttl test
	ttl := ts.ServerNow().Add(4500 * time.Millisecond)
	ttlText := fmt.Sprintf("%d", ttl.UnixMilli())

	output := ts.ProcessCommand("set", "k", "1")
	if !output.isString("OK") {
		t.Fatal("pttl step 1 fail")
	}

	output = ts.ProcessCommand("pexpireat", "k", ttlText)
	if !output.isInt(1) {
		t.Fatal("pttl step 2 fail")
	}

	output = ts.ProcessCommand("pexpiretime", "k")
	if !output.isInt64(ttl.UnixMilli()) {
		t.Fatal("pttl step 3 fail")
	}

	output = ts.ProcessCommand("pttl", "k")
	n, valid := output.toInt()
	if !valid || n < 4400 {
		t.Fatal("pttl step 4 fail")
	}

	// ms ttl on key without an expiration
	output = ts.ProcessCommand("set", "k2", "dog")
	if !output.isString("OK") {
		t.Fatal("pttl step 5 fail")
	}

	output = ts.ProcessCommand("pttl", "k2")
	if !output.isInt(-1) {
		t.Fatal("pttl step 6 fail")
	}

	// missing key test
	output = ts.ProcessCommand("pttl", "k3")
	if !output.isInt(-2) {
		t.Fatal("pttl step 7 fail")
	}

	// bad arg
	output = ts.ProcessCommand("pexpireat", "k", "invalid")
	if !output.isErrorType() {
		t.Fatal("pttl invalid ms arg fail")
	}
}

func TestRedisRandomKey(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// empty db test
	output := ts.ProcessCommand("randomkey")
	if !output.isNull() {
		t.Fatal("randomkey step 1 fail")
	}

	// one key test
	output = ts.ProcessCommand("set", "sample", "foo")
	if !output.isString("OK") {
		t.Fatal("randomkey step 2 fail")
	}

	output = ts.ProcessCommand("randomkey")
	if !output.isString("sample") {
		t.Fatal("randomkey step 3 fail")
	}

	// two key test
	output = ts.ProcessCommand("set", "test", "bar")
	if !output.isString("OK") {
		t.Fatal("randomkey step 4 fail")
	}

	// loop to ensure random starting position hits all possible buckets
	for i := 0; i < 256; i++ {
		output = ts.ProcessCommand("randomkey")
		if !output.isString("sample") && !output.isString("test") {
			t.Fatal("randomkey pick one of two keys fail")
		}
	}
}

func TestRedisRename(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// basic rename test
	output := ts.ProcessCommand("set", "k1", "cat")
	if !output.isString("OK") {
		t.Fatal("rename can't make k1 fail")
	}

	output = ts.ProcessCommand("set", "k2", "dog")
	if !output.isString("OK") {
		t.Fatal("rename can't make k2 fail")
	}

	output = ts.ProcessCommand("rename", "k1", "k2")
	if !output.isString("OK") {
		t.Fatal("rename k1 to k2 fail")
	}

	output = ts.ProcessCommand("get", "k1")
	if !output.isNull() {
		t.Fatal("rename k1 should be gone fail")
	}

	output = ts.ProcessCommand("get", "k2")
	if !output.isString("cat") {
		t.Fatal("rename k2 should have the original k1 value fail")
	}

	// missing source key test
	output = ts.ProcessCommand("rename", "k1", "k2")
	if !output.isErrorType() {
		t.Fatal("rename missing k1 fail")
	}
}

func TestRedisRenameNx(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// basic rename no overwrite test
	output := ts.ProcessCommand("set", "k1", "cat")
	if !output.isString("OK") {
		t.Fatal("renamenx step 1 fail")
	}

	output = ts.ProcessCommand("renamenx", "k1", "k2")
	if !output.isInt(1) {
		t.Fatal("renamenx step 2 fail")
	}

	output = ts.ProcessCommand("get", "k1")
	if !output.isNull() {
		t.Fatal("renamenx step 3 fail")
	}

	output = ts.ProcessCommand("get", "k2")
	if !output.isString("cat") {
		t.Fatal("renamenx step 4 fail")
	}

	// dest exists test
	output = ts.ProcessCommand("rename", "k1", "k2")
	if !output.isErrorType() {
		t.Fatal("renamenx step 5 fail")
	}

	output = ts.ProcessCommand("set", "k1", "dog")
	if !output.isString("OK") {
		t.Fatal("renamenx step 6 fail")
	}

	output = ts.ProcessCommand("renamenx", "k1", "k2")
	if !output.isInt(0) {
		t.Fatal("renamenx step 7 fail")
	}

	// missing key
	output = ts.ProcessCommand("renamenx", "missing", "k2")
	if !output.isErrorType() {
		t.Fatal("renamenx missing key fail")
	}
}

func TestRedisScan(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	_, isTestClient := ts.(*testClient)
	if !isTestClient {
		// hashing is not predictable with real clients, so skip this test
		return
	}

	// scan empty db
	output := ts.ProcessCommand("scan", "0")
	if !output.isValue([]any{"0", []any{}}) {
		t.Fatal("scan step 1 fail")
	}

	// scan single key db
	output = ts.ProcessCommand("set", "k1", "cat")
	if !output.isString("OK") {
		t.Fatal("scan step 2 fail")
	}

	output = ts.ProcessCommand("scan", "0")
	if !output.isValue([]any{"0", []any{"k1"}}) {
		t.Fatal("scan step 3 fail")
	}

	// scan two key db
	output = ts.ProcessCommand("set", "k2", "dog")
	if !output.isString("OK") {
		t.Fatal("scan step 4 fail")
	}

	output = ts.ProcessCommand("scan", "0")
	if !output.isValue([]any{"0", []any{"k2", "k1"}}) {
		t.Fatal("scan step 5 fail")
	}

	// count option tests
	output = ts.ProcessCommand("scan", "0", "Count", "1")
	if !output.isValue([]any{"15", []any{"k2"}}) {
		t.Fatal("scan step 6 fail")
	}

	output = ts.ProcessCommand("scan", "15", "count", "1")
	if !output.isValue([]any{"0", []any{"k1"}}) {
		t.Fatal("scan step 7 fail")
	}

	// negative values tests
	output = ts.ProcessCommand("scan", "-1")
	if !output.isValue([]any{"0", []any{"k1"}}) {
		t.Fatal("scan step 8 fail")
	}

	output = ts.ProcessCommand("scan", "0", "count", "-1")
	if !output.isErrorType() {
		t.Fatal("scan step 9 fail")
	}

	// loop to ensure hash table size reduction is reached
	for j := 0; j < 256; j++ {
		for n := 0; n < 16; n++ {
			output = ts.ProcessCommand("set", fmt.Sprintf("test%d", n), fmt.Sprintf("%d", n))
			if !output.isString("OK") {
				t.Fatal("scan step 10 fail")
			}

			output = ts.ProcessCommand("scan", "15", "count", "1")
			if !output.isValue([]any{"0", []any{"k1"}}) {
				t.Fatal("scan step 11 fail")
			}
		}

		for n := 0; n < 16; n++ {
			output = ts.ProcessCommand("del", fmt.Sprintf("test%d", n))
			if !output.isInt(1) {
				t.Fatal("scan step 12 fail")
			}

			output = ts.ProcessCommand("scan", "15", "count", "1")
			if !output.isValue([]any{"0", []any{"k1"}}) {
				t.Fatal("scan step 13 fail")
			}
		}
	}

	// type tests
	output = ts.ProcessCommand("scan", "0", "count", "1", "type", "foo")
	if !output.isValue([]any{"0", []any{}}) {
		t.Fatal("scan step 14 fail")
	}

	output = ts.ProcessCommand("scan", "0", "type", "string", "count", "1")
	if !output.isValue([]any{"63", []any{"k2"}}) {
		t.Fatal("scan step 15 fail")
	}

	// match tests
	output = ts.ProcessCommand("scan", "0", "match", "k*", "count", "1")
	if !output.isValue([]any{"63", []any{"k2"}}) {
		t.Fatal("scan step 16 fail")
	}

	output = ts.ProcessCommand("scan", "0", "count", "1", "match", "z*")
	if !output.isValue([]any{"0", []any{}}) {
		t.Fatal("scan step 17 fail")
	}
}

func TestRedisTouch(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// touch missing key test
	output := ts.ProcessCommand("touch", "missing", "target")
	if !output.isInt(0) {
		t.Fatal("touch step 1 fail")
	}

	// touch existing key test
	output = ts.ProcessCommand("set", "cat", "meow")
	if !output.isString("OK") {
		t.Fatal("touch step 2 fail")
	}

	output = ts.ProcessCommand("touch", "missing", "cat")
	if !output.isInt(1) {
		t.Fatal("touch step 3 fail")
	}

	// duplicates ok test
	output = ts.ProcessCommand("touch", "cat", "cat")
	if !output.isInt(2) {
		t.Fatal("touch step 3 fail")
	}
}

func TestRedisType(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key test
	output := ts.ProcessCommand("type", "missing")
	if !output.isString("none") {
		t.Fatal("type can't get type of a missing key fail")
	}

	// string test
	output = ts.ProcessCommand("set", "cat", "meow")
	if !output.isString("OK") {
		t.Fatal("type can't create test string fail")
	}

	output = ts.ProcessCommand("type", "cat")
	if !output.isString("string") {
		t.Fatal("type can't get type of string fail")
	}

	// list test
	output = ts.ProcessCommand("rpush", "list", "dog")
	if !output.isInt(1) {
		t.Fatal("type can't make list fail")
	}

	output = ts.ProcessCommand("type", "list")
	if !output.isString("list") {
		t.Fatal("type can't get type of list fail")
	}
}

func TestRedisKeys(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// empty test
	output := ts.ProcessCommand("keys", "*")
	if !output.isValue([]any{}) {
		t.Fatal("keys empty test fail")
	}

	// one key test
	output = ts.ProcessCommand("set", "cat", "meow")
	if !output.isString("OK") {
		t.Fatal("keys can't set single key")
	}

	output = ts.ProcessCommand("keys", "*")
	if !output.isValue([]any{"cat"}) {
		t.Fatal("keys glob * fail")
	}

	// pattern test
	output = ts.ProcessCommand("keys", "c*")
	if !output.isValue([]any{"cat"}) {
		t.Fatal("keys glob c* fail")
	}

	output = ts.ProcessCommand("keys", "?a?")
	if !output.isValue([]any{"cat"}) {
		t.Fatal("keys glob ?a? fail")
	}

	output = ts.ProcessCommand("keys", "c*a*t")
	if !output.isValue([]any{"cat"}) {
		t.Fatal("keys glob c*a*t fail")
	}

	// two keys test
	output = ts.ProcessCommand("set", "dog", "woof")
	if !output.isString("OK") {
		t.Fatal("keys can't set second key")
	}

	output = ts.ProcessCommand("keys", "*")
	if !output.isArraySet("cat", "dog") {
		t.Fatal("keys two keys fail")
	}

	// two string keys and a list test
	output = ts.ProcessCommand("rpush", "list", "moo")
	if !output.isInt(1) {
		t.Fatal("keys can't set list")
	}

	output = ts.ProcessCommand("keys", "*")
	if !output.isArraySet("cat", "list", "dog") {
		t.Fatal("keys three keys fail")
	}

	// four keys test
	output = ts.ProcessCommand("set", "dogs", "6")
	if !output.isString("OK") {
		t.Fatal("keys can't set fourth key")
	}

	output = ts.ProcessCommand("keys", "?og*")
	if !output.isArraySet("dogs", "dog") {
		t.Fatal("keys glob ?og* fail")
	}

	output = ts.ProcessCommand("keys", "[cl]*t")
	if !output.isArraySet("cat", "list") {
		t.Fatal("keys glob [cl]*t fail")
	}

	// six keys test
	output = ts.ProcessCommand("set", "[brackets]", "test")
	if !output.isString("OK") {
		t.Fatal("keys can't set fifth key")
	}

	output = ts.ProcessCommand("set", "*wildcard", "escape")
	if !output.isString("OK") {
		t.Fatal("keys can't set sixth key")
	}

	output = ts.ProcessCommand("keys", `[\[c]*[t\]]`)
	if !output.isValue([]any{"cat", "[brackets]"}) && !output.isValue([]any{"[brackets]", "cat"}) {
		t.Fatalf("keys glob %s fail", `[\[c]*[t\]]`)
	}

	output = ts.ProcessCommand("keys", `\**`)
	if !output.isValue([]any{"*wildcard"}) {
		t.Fatalf("keys glob %s fail", `\**`)
	}

	output = ts.ProcessCommand("keys", `\**\*`)
	if !output.isValue([]any{}) {
		t.Fatalf("keys glob %s fail", `\**\*`)
	}

	// expired key test
	output = ts.ProcessCommand("pexpire", "*wildcard", "1")
	if !output.isInt(1) {
		t.Fatal("keys can't expire key")
	}

	time.Sleep(10 * time.Millisecond)

	output = ts.ProcessCommand("keys", "*")
	if !output.isArraySet("cat", "[brackets]", "list", "dogs", "dog") {
		t.Fatal("keys iterate with expired key fail")
	}

	// empty test
	output = ts.ProcessCommand("keys", "")
	if !output.isValue([]any{}) {
		t.Fatal("keys empty pattern fail")
	}
}

func TestRedisDiagnotics(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// empty test
	ts.DumpKey("missing")

	// string key
	ts.ProcessCommand("set", "cat", "meow")
	ts.DumpKey("cat")

	// list key
	ts.ProcessCommand("rpush", "list", "fox", "dog", "hen")
	ts.DumpKey("list")

	//t.Fatal("fail to see output")
}

func TestRedisFlushAll(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("set", "cat", "meow")
	if !output.isString("OK") {
		t.Fatal("make a value fail")
	}

	output = ts.ProcessCommand("select", "1")
	if !output.isString("OK") {
		t.Fatal("make select db 1 fail")
	}

	output = ts.ProcessCommand("set", "dog", "woof")
	if !output.isString("OK") {
		t.Fatal("make a value in db 1 fail")
	}

	output = ts.ProcessCommand("flushall")
	if !output.isString("OK") {
		t.Fatal("flushall fail")
	}

	output = ts.ProcessCommand("get", "dog")
	if !output.isNull() {
		t.Fatal("get db 1 val fail")
	}

	output = ts.ProcessCommand("select", "0")
	if !output.isString("OK") {
		t.Fatal("make select db 0 fail")
	}

	output = ts.ProcessCommand("get", "cat")
	if !output.isNull() {
		t.Fatal("get db 0 val fail")
	}
}

func TestRedisFlushDb(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	output := ts.ProcessCommand("set", "cat", "meow")
	if !output.isString("OK") {
		t.Fatal("make a value fail")
	}

	output = ts.ProcessCommand("select", "1")
	if !output.isString("OK") {
		t.Fatal("make select db 1 fail")
	}

	output = ts.ProcessCommand("set", "dog", "woof")
	if !output.isString("OK") {
		t.Fatal("make a value in db 1 fail")
	}

	output = ts.ProcessCommand("flushdb")
	if !output.isString("OK") {
		t.Fatal("flushall fail")
	}

	output = ts.ProcessCommand("get", "dog")
	if !output.isNull() {
		t.Fatal("get db 1 val fail")
	}

	output = ts.ProcessCommand("select", "0")
	if !output.isString("OK") {
		t.Fatal("make select db 0 fail")
	}

	output = ts.ProcessCommand("get", "cat")
	if !output.isString("meow") {
		t.Fatal("get db 0 val fail")
	}
}
