package redisemu

import "testing"

func TestSAdd(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// single member test
	output := ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("sadd single member add fail")
	}

	// single member already exists test
	output = ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(0) {
		t.Fatal("sadd single member already exists fail")
	}

	// second member
	output = ts.ProcessCommand("sadd", "k1", "dog")
	if !output.isInt(1) {
		t.Fatal("sadd second member fail")
	}

	// verify members
	output = ts.ProcessCommand("smembers", "k1")
	if !output.isArraySet("cat", "dog") {
		t.Fatal("sadd two member verify fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("sadd prepare list fail")
	}

	output = ts.ProcessCommand("sadd", "list", "fox")
	if !output.isErrorType() {
		t.Fatal("sadd add to a list fail")
	}
}

func TestSCard(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key test
	output := ts.ProcessCommand("scard", "missing")
	if !output.isInt(0) {
		t.Fatal("scard missing key fail")
	}

	// single member test
	output = ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("scard single member add fail")
	}

	output = ts.ProcessCommand("scard", "k1")
	if !output.isInt(1) {
		t.Fatal("scard single member count fail")
	}

	// multiple member test
	output = ts.ProcessCommand("sadd", "k1", "dog", "cow", "fox")
	if !output.isInt(3) {
		t.Fatal("scard multiple member add fail")
	}

	output = ts.ProcessCommand("scard", "k1")
	if !output.isInt(4) {
		t.Fatal("scard multiple member count fail")
	}

	// non-set key test
	output = ts.ProcessCommand("set", "k2", "value")
	if !output.isString("OK") {
		t.Fatal("scard prepare non-set key fail")
	}

	output = ts.ProcessCommand("scard", "k2")
	if !output.isErrorType() {
		t.Fatal("scard non-set key fail")
	}
}

func TestSDiff(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing keys
	output := ts.ProcessCommand("sdiff", "missing")
	if !output.isArraySet() {
		t.Fatal("sdiff single missing key fail")
	}

	output = ts.ProcessCommand("sdiff", "missing1", "missing2")
	if !output.isArraySet() {
		t.Fatal("sdiff two missing keys fail")
	}

	// missing key diff with existing key
	output = ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("sdiff make single member set fail")
	}

	output = ts.ProcessCommand("sdiff", "missing", "k1")
	if !output.isArraySet() {
		t.Fatal("sdiff missing diff with existing fail")
	}

	output = ts.ProcessCommand("sdiff", "missing", "k1")
	if !output.isArraySet() {
		t.Fatal("sdiff existing diff with missing fail")
	}

	// matching sets test
	output = ts.ProcessCommand("sadd", "k2", "cat")
	if !output.isInt(1) {
		t.Fatal("sdiff make second single member set fail")
	}

	output = ts.ProcessCommand("sdiff", "k1", "k2")
	if !output.isArraySet() {
		t.Fatal("sdiff diff of two identical single member sets fail")
	}

	output = ts.ProcessCommand("sdiff", "k2", "k1")
	if !output.isArraySet() {
		t.Fatal("sdiff diff of two identical single member sets reversed fail")
	}

	// set 1 with more members than set 2
	output = ts.ProcessCommand("sadd", "k3", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("sdiff make two member set fail")
	}

	output = ts.ProcessCommand("sdiff", "k1", "k3")
	if !output.isArraySet() {
		t.Fatal("sdiff diff of a set with more members fail")
	}

	output = ts.ProcessCommand("sdiff", "k3", "k1")
	if !output.isArraySet("dog") {
		t.Fatal("sdiff diff of a set with more members k3 first fail")
	}

	// three set test
	output = ts.ProcessCommand("sdiff", "k1", "k2", "k3")
	if !output.isArraySet() {
		t.Fatal("sdiff diff of three sets fail")
	}

	output = ts.ProcessCommand("sdiff", "k3", "k2", "k1")
	if !output.isArraySet("dog") {
		t.Fatal("sdiff diff of three sets k3 first fail")
	}

	// sets with nothing in common
	output = ts.ProcessCommand("sadd", "k4", "cow", "fox", "hen")
	if !output.isInt(3) {
		t.Fatal("sdiff make set k4 fail")
	}

	output = ts.ProcessCommand("sadd", "k5", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("sdiff make set k5 fail")
	}

	output = ts.ProcessCommand("sdiff", "k4", "k5")
	if !output.isArraySet("cow", "fox", "hen") {
		t.Fatal("sdiff diff of two identical single member sets fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("sdiff prepare list fail")
	}

	output = ts.ProcessCommand("sdiff", "k1", "list")
	if !output.isErrorType() {
		t.Fatal("sdiff diff with a list fail")
	}

	output = ts.ProcessCommand("sdiff", "list", "k1")
	if !output.isErrorType() {
		t.Fatal("sdiff diff from a list fail")
	}
}

func TestSDiffStore(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing keys
	output := ts.ProcessCommand("sdiffstore", "output", "missing")
	if !output.isInt(0) {
		t.Fatal("sdiffstore single missing key fail")
	}

	output = ts.ProcessCommand("sdiffstore", "output", "missing1", "missing2")
	if !output.isInt(0) {
		t.Fatal("sdiffstore two missing keys fail")
	}

	// missing key diff with existing key
	output = ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("sdiffstore make single member set fail")
	}

	output = ts.ProcessCommand("sdiffstore", "output", "missing", "k1")
	if !output.isInt(0) {
		t.Fatal("sdiffstore missing diff with existing fail")
	}

	output = ts.ProcessCommand("sdiffstore", "output", "k1", "missing")
	if !output.isInt(1) {
		t.Fatal("sdiffstore existing diff with missing fail")
	}
	output = ts.ProcessCommand("smembers", "output")
	if !output.isArraySet("cat") {
		t.Fatal("sdiffstore existing diff with missing members fail")
	}

	// matching sets test
	output = ts.ProcessCommand("sadd", "k2", "cat")
	if !output.isInt(1) {
		t.Fatal("sdiffstore make second single member set fail")
	}

	output = ts.ProcessCommand("sdiffstore", "output", "k1", "k2")
	if !output.isInt(0) {
		t.Fatal("sdiffstore diff of two identical single member sets fail")
	}

	output = ts.ProcessCommand("sdiffstore", "output", "k2", "k1")
	if !output.isInt(0) {
		t.Fatal("sdiffstore diff of two identical single member sets reversed fail")
	}

	// set 1 with more members than set 2
	output = ts.ProcessCommand("sadd", "k3", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("sdiffstore make two member set fail")
	}

	output = ts.ProcessCommand("sdiffstore", "output", "k1", "k3")
	if !output.isInt(0) {
		t.Fatal("sdiffstore diff of a set with more members fail")
	}

	output = ts.ProcessCommand("sdiffstore", "output", "k3", "k1")
	if !output.isInt(1) {
		t.Fatal("sdiffstore diff of a set with more members k3 first fail")
	}
	output = ts.ProcessCommand("smembers", "output")
	if !output.isArraySet("dog") {
		t.Fatal("sdiffstore diff of a set with more members k3 first fail")
	}

	// three set test
	output = ts.ProcessCommand("sdiffstore", "threesets", "k1", "k2", "k3")
	if !output.isInt(0) {
		t.Fatal("sdiffstore diff of three sets into threesets fail")
	}

	output = ts.ProcessCommand("sdiffstore", "threesets", "k3", "k1", "k2")
	if !output.isInt(1) {
		t.Fatal("sdiffstore diff of three sets into threesets k3 first fail")
	}
	output = ts.ProcessCommand("smembers", "threesets")
	if !output.isArraySet("dog") {
		t.Fatal("sdiffstore diff of a set with more members k3 first fail")
	}

	// sets with nothing in common
	output = ts.ProcessCommand("sadd", "k4", "cow", "fox", "hen")
	if !output.isInt(3) {
		t.Fatal("sdiff make set k4 fail")
	}

	output = ts.ProcessCommand("sadd", "k5", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("sdiff make set k5 fail")
	}

	output = ts.ProcessCommand("sdiffstore", "fulldiff", "k4", "k5")
	if !output.isInt(3) {
		t.Fatal("sdiffstore diff of two sets nothing in common fail")
	}
	output = ts.ProcessCommand("smembers", "fulldiff")
	if !output.isArraySet("cow", "fox", "hen") {
		t.Fatal("sdiffstore diff of two sets nothing in common fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("sdiffstore prepare list fail")
	}

	output = ts.ProcessCommand("sdiffstore", "k10", "k1", "list")
	if !output.isErrorType() {
		t.Fatal("sdiffstore diff with a list fail")
	}

	output = ts.ProcessCommand("sdiffstore", "k10", "list", "k1")
	if !output.isErrorType() {
		t.Fatal("sdiffstore diff from a list fail")
	}
}

func TestSInter(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing keys
	output := ts.ProcessCommand("sinter", "missing")
	if !output.isArraySet() {
		t.Fatal("sinter single missing key fail")
	}

	output = ts.ProcessCommand("sinter", "missing1", "missing2")
	if !output.isArraySet() {
		t.Fatal("sinter two missing keys fail")
	}

	// single member set test
	output = ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("sinter make single member set fail")
	}

	output = ts.ProcessCommand("sinter", "k1", "k1")
	if !output.isArraySet("cat") {
		t.Fatal("sinter single member set self-intersection fail")
	}

	// missing key intersection with existing key
	output = ts.ProcessCommand("sinter", "missing", "k1")
	if !output.isArraySet() {
		t.Fatal("sinter missing key intersection with existing fail")
	}

	output = ts.ProcessCommand("sinter", "k1", "missing")
	if !output.isArraySet() {
		t.Fatal("sinter existing key intersection with missing fail")
	}

	// matching sets test
	output = ts.ProcessCommand("sadd", "k2", "cat")
	if !output.isInt(1) {
		t.Fatal("sinter make second single member set fail")
	}

	output = ts.ProcessCommand("sinter", "k1", "k2")
	if !output.isArraySet("cat") {
		t.Fatal("sinter intersection of two identical single member sets fail")
	}

	output = ts.ProcessCommand("sinter", "k2", "k1")
	if !output.isArraySet("cat") {
		t.Fatal("sinter intersection of two identical single member sets reversed fail")
	}

	// set 1 with more members than set 2
	output = ts.ProcessCommand("sadd", "k3", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("sinter make two member set fail")
	}

	output = ts.ProcessCommand("sinter", "k1", "k3")
	if !output.isArraySet("cat") {
		t.Fatal("sinter intersection of a set with more members fail")
	}

	output = ts.ProcessCommand("sinter", "k3", "k1")
	if !output.isArraySet("cat") {
		t.Fatal("sinter intersection of a set with more members k3 first fail")
	}

	// three set test
	output = ts.ProcessCommand("sinter", "k1", "k2", "k3")
	if !output.isArraySet("cat") {
		t.Fatal("sinter intersection of a set with more members fail")
	}

	output = ts.ProcessCommand("sinter", "k2", "k1", "k3")
	if !output.isArraySet("cat") {
		t.Fatal("sinter intersection of a set with more members fail")
	}

	output = ts.ProcessCommand("sinter", "k3", "k2", "k1")
	if !output.isArraySet("cat") {
		t.Fatal("sinter intersection of a set with more members fail")
	}

	// two sets with one common member
	output = ts.ProcessCommand("sadd", "k4", "cat", "fox")
	if !output.isInt(2) {
		t.Fatal("sinter two keys with one common element setup fail")
	}

	output = ts.ProcessCommand("sinter", "k1", "k4")
	if !output.isArraySet("cat") {
		t.Fatal("sinter two keys with one common element fail")
	}

	output = ts.ProcessCommand("sinter", "k4", "k1")
	if !output.isArraySet("cat") {
		t.Fatal("sinter two keys with one common element reversed fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("sinter prepare list fail")
	}

	output = ts.ProcessCommand("sinter", "k1", "list")
	if !output.isErrorType() {
		t.Fatal("sinter intersect with a list fail")
	}

	output = ts.ProcessCommand("sinter", "list", "k1")
	if !output.isErrorType() {
		t.Fatal("sinter intersect from a list fail")
	}
}

func TestSInterStore(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing keys
	output := ts.ProcessCommand("sinterstore", "destination", "missing")
	if !output.isInt(0) {
		t.Fatal("sinterstore one missing key fail")
	}

	output = ts.ProcessCommand("sinterstore", "destination", "missing1", "missing2")
	if !output.isInt(0) {
		t.Fatal("sinterstore two missing keys fail")
	}

	// one missing key
	output = ts.ProcessCommand("sadd", "set1", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("sinterstore add members to set1 fail")
	}

	output = ts.ProcessCommand("sinterstore", "destination", "set1", "missing")
	if !output.isInt(0) {
		t.Fatal("sinterstore one missing key fail")
	}

	// empty intersection
	output = ts.ProcessCommand("sadd", "set2", "cow", "fox")
	if !output.isInt(2) {
		t.Fatal("sinterstore add members to set2 fail")
	}

	output = ts.ProcessCommand("sinterstore", "destination", "set1", "set2")
	if !output.isInt(0) {
		t.Fatal("sinterstore empty intersection fail")
	}

	// non-empty intersection
	output = ts.ProcessCommand("sadd", "set2", "dog")
	if !output.isInt(1) {
		t.Fatal("sinterstore add member to set2 fail")
	}

	output = ts.ProcessCommand("sinterstore", "destination", "set1", "set2")
	if !output.isInt(1) {
		t.Fatal("sinterstore non-empty intersection fail")
	}

	output = ts.ProcessCommand("smembers", "destination")
	if !output.isArraySet("dog") {
		t.Fatal("sinterstore verify non-empty intersection fail")
	}

	// three set test
	output = ts.ProcessCommand("sadd", "set3", "dog", "hen")
	if !output.isInt(2) {
		t.Fatal("sinterstore add members to set3 fail")
	}

	output = ts.ProcessCommand("sinterstore", "destination", "set1", "set2", "set3")
	if !output.isInt(1) {
		t.Fatal("sinterstore three set intersection fail")
	}

	output = ts.ProcessCommand("smembers", "destination")
	if !output.isArraySet("dog") {
		t.Fatal("sinterstore verify three set intersection fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("sinterstore prepare list fail")
	}

	output = ts.ProcessCommand("sinterstore", "destination", "set1", "list")
	if !output.isErrorType() {
		t.Fatal("sinterstore intersection with a list fail")
	}
}

func TestSInterCard(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing keys
	output := ts.ProcessCommand("sintercard", "1", "missing")
	if !output.isInt(0) {
		t.Fatal("sintercard single missing key fail")
	}

	output = ts.ProcessCommand("sintercard", "2", "missing1", "missing2")
	if !output.isInt(0) {
		t.Fatal("sintercard two missing keys fail")
	}

	// single member set
	output = ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("sintercard make single member set fail")
	}

	output = ts.ProcessCommand("sintercard", "2", "k1", "missing")
	if !output.isInt(0) {
		t.Fatal("sintercard single member set with missing fail")
	}

	output = ts.ProcessCommand("sintercard", "2", "missing", "k1")
	if !output.isInt(0) {
		t.Fatal("sintercard single member set with missing reversed fail")
	}

	// invalid count of keys
	output = ts.ProcessCommand("sadd", "k2", "cat")
	if !output.isInt(1) {
		t.Fatal("sintercard make second single member set fail")
	}

	output = ts.ProcessCommand("sintercard", "1", "k1", "k2")
	if !output.isErrorType() {
		t.Fatal("sintercard short length fail")
	}

	output = ts.ProcessCommand("sintercard", "3", "k1", "k2")
	if !output.isErrorString("ERR Number of keys can't be greater than number of args") {
		t.Fatal("sintercard long length fail")
	}

	// matching sets
	output = ts.ProcessCommand("sintercard", "2", "k1", "k2")
	if !output.isInt(1) {
		t.Fatal("sintercard identical single member sets fail")
	}

	// different sets
	output = ts.ProcessCommand("sadd", "k3", "dog")
	if !output.isInt(1) {
		t.Fatal("sintercard make second single member set fail")
	}

	output = ts.ProcessCommand("sintercard", "2", "k1", "k3")
	if !output.isInt(0) {
		t.Fatal("sintercard different single member sets fail")
	}

	// sets with some common members
	output = ts.ProcessCommand("sadd", "k4", "cat", "dog", "fox")
	if !output.isInt(3) {
		t.Fatal("sintercard make set with common members fail")
	}

	output = ts.ProcessCommand("sadd", "k5", "cat", "hen", "cow")
	if !output.isInt(3) {
		t.Fatal("sintercard make second set with common members fail")
	}

	output = ts.ProcessCommand("sintercard", "2", "k4", "k5")
	if !output.isInt(1) {
		t.Fatal("sintercard sets with some common members fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("sintercard prepare list fail")
	}

	output = ts.ProcessCommand("sintercard", "list", "set1")
	if !output.isErrorType() {
		t.Fatal("sintercard intersection with a list fail")
	}

	output = ts.ProcessCommand("sintercard", "set1", "list")
	if !output.isErrorType() {
		t.Fatal("sintercard intersection with a list fail")
	}
}

func TestSInterCardWithLimit(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// create a set with multiple common members
	output := ts.ProcessCommand("sadd", "set1", "cat", "dog", "fox", "hen", "cow")
	if !output.isInt(5) {
		t.Fatal("sintercard make set1 fail")
	}

	output = ts.ProcessCommand("sadd", "set2", "cat", "dog", "fox", "sheep", "goat")
	if !output.isInt(5) {
		t.Fatal("sintercard make set2 fail")
	}

	// unlimited
	output = ts.ProcessCommand("sintercard", "2", "set1", "set2", "LIMIT", "0")
	if !output.isInt(3) {
		t.Fatal("sintercard with limit of 0 fail")
	}

	// limit of 1
	output = ts.ProcessCommand("sintercard", "2", "set1", "set2", "LIMIT", "1")
	if !output.isInt(1) {
		t.Fatal("sintercard with limit of 1 fail")
	}

	// limit of 2
	output = ts.ProcessCommand("sintercard", "2", "set1", "set2", "LIMIT", "2")
	if !output.isInt(2) {
		t.Fatal("sintercard with limit of 2 fail")
	}

	// limit of 4
	output = ts.ProcessCommand("sintercard", "2", "set1", "set2", "LIMIT", "4")
	if !output.isInt(3) {
		t.Fatal("sintercard with limit of 4 fail")
	}
}

func TestSIsMember(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// add a single member to a set
	output := ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("sadd single member add fail")
	}

	// check if a member exists in the set
	output = ts.ProcessCommand("sismember", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("sismember check for existing member fail")
	}

	// check if a non-existent member exists in the set
	output = ts.ProcessCommand("sismember", "k1", "dog")
	if !output.isInt(0) {
		t.Fatal("sismember check for non-existent member fail")
	}

	// check the result of a missing key
	output = ts.ProcessCommand("sismember", "missing", "dog")
	if !output.isInt(0) {
		t.Fatal("sismember check for non-existent key fail")
	}

	// check for a key of the wrong type
	output = ts.ProcessCommand("set", "string_key", "some_value")
	if !output.isString("OK") {
		t.Fatal("sismember prepare string key fail")
	}
	output = ts.ProcessCommand("sismember", "string_key", "some_value")
	if !output.isErrorType() {
		t.Fatal("sismember check on a string key fail")
	}
}

func TestSMembers(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// empty set
	output := ts.ProcessCommand("smembers", "empty")
	if !output.isArraySet() {
		t.Fatal("smembers empty set fail")
	}

	// single member set
	output = ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("smembers prepare single member set fail")
	}
	output = ts.ProcessCommand("smembers", "k1")
	if !output.isArraySet("cat") {
		t.Fatal("smembers single member set fail")
	}

	// multiple member set
	output = ts.ProcessCommand("sadd", "k2", "cat", "dog", "fox")
	if !output.isInt(3) {
		t.Fatal("smembers prepare multiple member set fail")
	}
	output = ts.ProcessCommand("smembers", "k2")
	if !output.isArraySet("cat", "dog", "fox") {
		t.Fatal("smembers multiple member set fail")
	}

	// wrong type
	output = ts.ProcessCommand("set", "str", "hello")
	if !output.isString(strOK) {
		t.Fatal("smembers prepare string fail")
	}
	output = ts.ProcessCommand("smembers", "str")
	if !output.isErrorType() {
		t.Fatal("smembers string as key fail")
	}
}

func TestSMIsMember(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing keys
	output := ts.ProcessCommand("smismember", "missing", "member1")
	if !output.isArray(0) {
		t.Fatal("smismember single missing key fail")
	}

	output = ts.ProcessCommand("smismember", "missing1", "member1", "member2")
	if !output.isArray(0, 0) {
		t.Fatal("smismember two missing keys fail")
	}

	// single member set test
	output = ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("smismember make single member set fail")
	}

	output = ts.ProcessCommand("smismember", "k1", "cat")
	if !output.isArray(1) {
		t.Fatal("smismember single member set self-mismember fail")
	}

	output = ts.ProcessCommand("smismember", "k1", "dog")
	if !output.isArray(0) {
		t.Fatal("smismember single member set mismember fail")
	}

	// two member set tests
	output = ts.ProcessCommand("sadd", "k2", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("smismember make two member set fail")
	}

	output = ts.ProcessCommand("smismember", "k2", "cat", "dog")
	if !output.isArray(1, 1) {
		t.Fatal("smismember test two member set fail")
	}

	output = ts.ProcessCommand("smismember", "k2", "cat", "cat")
	if !output.isArray(1, 1) {
		t.Fatal("smismember test two member set with repeated member fail")
	}

	output = ts.ProcessCommand("smismember", "k2", "fox", "cat")
	if !output.isArray(0, 1) {
		t.Fatal("smismember one of two members exist fail")
	}

	// wrong type
	output = ts.ProcessCommand("set", "string", "data")
	if !output.isString("OK") {
		t.Fatal("smismember prepare string fail")
	}

	output = ts.ProcessCommand("smismember", "string", "data")
	if !output.isErrorType() {
		t.Fatal("smismember check for member in a string fail")
	}
}

func TestSMove(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing keys
	output := ts.ProcessCommand("smove", "missing", "destination", "member")
	if !output.isInt(0) {
		t.Fatal("smove missing source key fail")
	}

	output = ts.ProcessCommand("smove", "source", "missing", "member")
	if !output.isInt(0) {
		t.Fatal("smove missing destination key fail")
	}

	// single member set test
	output = ts.ProcessCommand("sadd", "source", "cat")
	if !output.isInt(1) {
		t.Fatal("smove make single member set fail")
	}

	output = ts.ProcessCommand("smove", "source", "destination", "cat")
	if !output.isInt(1) {
		t.Fatal("smove move member to destination fail")
	}

	output = ts.ProcessCommand("smove", "source", "destination", "dog")
	if !output.isInt(0) {
		t.Fatal("smove move non-existing member fail")
	}

	// multi member set test
	output = ts.ProcessCommand("sadd", "source", "cat", "dog", "bird")
	if !output.isInt(3) {
		t.Fatal("smove make multi member set fail")
	}

	output = ts.ProcessCommand("smove", "source", "destination", "dog")
	if !output.isInt(1) {
		t.Fatal("smove move member to destination fail")
	}

	output = ts.ProcessCommand("smove", "source", "destination", "fox")
	if !output.isInt(0) {
		t.Fatal("smove move non-existing member fail")
	}

	// wrong type
	output = ts.ProcessCommand("set", "string", "data")
	if !output.isString("OK") {
		t.Fatal("smove prepare string key fail")
	}

	output = ts.ProcessCommand("smove", "string", "destination", "data")
	if !output.isErrorType() {
		t.Fatal("smove move member from a string key fail")
	}
}

func TestSRandMember(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("srandmember", "missing")
	if !output.isNull() {
		t.Fatal("srandmember missing key fail")
	}

	// single member set test
	output = ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("srandmember make single member set fail")
	}

	output = ts.ProcessCommand("srandmember", "k1")
	if !output.isString("cat") {
		t.Fatal("srandmember single member set fail")
	}

	// two member set test
	output = ts.ProcessCommand("sadd", "k2", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("srandmember make two member set fail")
	}

	output = ts.ProcessCommand("srandmember", "k2")
	if !output.isStringInSet("cat", "dog") {
		t.Fatal("srandmember two member set fail")
	}

	// count test
	output = ts.ProcessCommand("sadd", "k3", "cat", "dog", "fox")
	if !output.isInt(3) {
		t.Fatal("srandmember make three member set fail")
	}

	output = ts.ProcessCommand("srandmember", "k3", "2")
	if !output.isArrayInSet(2, "cat", "dog", "fox") {
		t.Fatal("srandmember two member set count fail")
	}
	if !output.isArrayASet() {
		t.Fatal("srandmember return array duplicate check fail")
	}

	// negative count test
	output = ts.ProcessCommand("srandmember", "k3", "-2")
	if !output.isArrayInSet(2, "cat", "dog", "fox") {
		t.Fatal("srandmember two member set negative count fail")
	}

	// wrong type
	output = ts.ProcessCommand("set", "string", "data")
	if !output.isString("OK") {
		t.Fatal("srandmember prepare string fail")
	}

	output = ts.ProcessCommand("srandmember", "string")
	if !output.isErrorType() {
		t.Fatal("srandmember check for member in a string fail")
	}
}

func TestSRem(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("srem", "missing", "member")
	if !output.isInt(0) {
		t.Fatal("srem missing key fail")
	}

	// single member set test
	output = ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("srem make single member set fail")
	}

	output = ts.ProcessCommand("srem", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("srem single member set fail")
	}

	output = ts.ProcessCommand("srem", "k1", "cat")
	if !output.isInt(0) {
		t.Fatal("srem single member set again fail")
	}

	// multi member set test
	output = ts.ProcessCommand("sadd", "k1", "cat", "dog", "bird")
	if !output.isInt(3) {
		t.Fatal("srem make multi member set fail")
	}

	output = ts.ProcessCommand("srem", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("srem multi member set fail")
	}

	output = ts.ProcessCommand("srem", "k1", "cat")
	if !output.isInt(0) {
		t.Fatal("srem multi member set again fail")
	}

	output = ts.ProcessCommand("smembers", "k1")
	if !output.isArraySet("dog", "bird") {
		t.Fatal("srem multi member set result fail")
	}

	// non-existing member test
	output = ts.ProcessCommand("srem", "k1", "mouse")
	if !output.isInt(0) {
		t.Fatal("srem non-existing member fail")
	}

	output = ts.ProcessCommand("smembers", "k1")
	if !output.isArraySet("dog", "bird") {
		t.Fatal("srem non-existing member result fail")
	}

	// wrong type
	output = ts.ProcessCommand("set", "string", "data")
	if !output.isString("OK") {
		t.Fatal("srem prepare string fail")
	}

	output = ts.ProcessCommand("srem", "string")
	if !output.isErrorType() {
		t.Fatal("srem check for member in a string fail")
	}
}

func TestSScan(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing key
	output := ts.ProcessCommand("sscan", "missing", "0")
	if !output.isArray("0", []any{}) {
		t.Fatal("sscan missing key fail")
	}

	// single key
	ts.ProcessCommand("sadd", "single", "member1")
	output = ts.ProcessCommand("sscan", "single", "0")
	if !output.isArray("0", []any{"member1"}) {
		t.Fatal("sscan single key fail")
	}

	// multiple keys
	ts.ProcessCommand("sadd", "multiple", "member1")
	ts.ProcessCommand("sadd", "multiple", "member2")
	ts.ProcessCommand("sadd", "multiple", "member3")
	output = ts.ProcessCommand("sscan", "multiple", "0")
	a, ok := output.toArray()
	if !ok || len(a) != 2 {
		t.Fatal("sscan multiple keys return array fail")
	}
	str, ok := a[0].toString()
	if !ok || str != "0" {
		t.Fatal("sscan multiple keys cursor fail")
	}
	if !a[1].isArrayInSet(3, "member1", "member2", "member3") {
		t.Fatal("sscan multiple key fail")
	}

	// wrong type
	output = ts.ProcessCommand("set", "string", "data")
	if !output.isString(strOK) {
		t.Fatal("sscan prepare string fail")
	}

	output = ts.ProcessCommand("sscan", "string", "0")
	if !output.isErrorType() {
		t.Fatal("sscan scan a string fail")
	}
}

func TestSScanMatchCount(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// prepare set with multiple members
	allMembers := []any{"member1", "member2", "member3", "member4", "member5", "member10", "member11"}
	args := append([]any{"theset"}, allMembers...)
	ts.ProcessCommand("sadd", args...)

	// test SSCAN with match and count options
	output := ts.ProcessCommand("sscan", "theset", "0", "match", "member1*", "count", "2")
	a, ok := output.toArray()
	if !ok || len(a) != 2 {
		t.Fatal("sscan match and count return array fail")
	}
	str, ok := a[0].toString()
	if !ok || str == "0" {
		t.Fatal("sscan match and count cursor fail")
	}
	if !a[1].isArrayInSet(1, allMembers...) && !a[1].isArrayInSet(2, allMembers...) {
		t.Fatal("sscan match and count fail")
	}

	// test SSCAN with match option and no count
	output = ts.ProcessCommand("sscan", "theset", "0", "match", "member*")
	a, ok = output.toArray()
	if !ok || len(a) != 2 {
		t.Fatal("sscan match return array fail")
	}
	str, ok = a[0].toString()
	if !ok || str != "0" {
		t.Fatal("sscan match cursor fail")
	}
	if !a[1].isArrayInSet(7, allMembers...) {
		t.Fatal("sscan match fail")
	}

	// test SSCAN with count option and no match
	output = ts.ProcessCommand("sscan", "theset", "0", "count", "2")
	a, ok = output.toArray()
	if !ok || len(a) != 2 {
		t.Fatal("sscan count return array fail")
	}
	str, ok = a[0].toString()
	if !ok || str == "0" {
		t.Fatal("sscan count cursor fail")
	}
	if !a[1].isArrayInSet(1, allMembers...) && !a[1].isArrayInSet(2, allMembers...) {
		t.Fatal("sscan count fail")
	}
}

func TestSUnion(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing keys
	output := ts.ProcessCommand("sunion", "missing")
	if !output.isArraySet() {
		t.Fatal("sunion single missing key fail")
	}

	output = ts.ProcessCommand("sunion", "missing1", "missing2")
	if !output.isArraySet() {
		t.Fatal("sunion two missing keys fail")
	}

	// single member set test
	output = ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("sunion make single member set fail")
	}

	output = ts.ProcessCommand("sunion", "k1", "k1")
	if !output.isArraySet("cat") {
		t.Fatal("sunion single member set self-union fail")
	}

	// missing key union with existing key
	output = ts.ProcessCommand("sunion", "missing", "k1")
	if !output.isArraySet("cat") {
		t.Fatal("sunion missing key union with existing fail")
	}

	output = ts.ProcessCommand("sunion", "k1", "missing")
	if !output.isArraySet("cat") {
		t.Fatal("sunion existing key union with missing fail")
	}

	// matching sets test
	output = ts.ProcessCommand("sadd", "k2", "cat")
	if !output.isInt(1) {
		t.Fatal("sunion make second single member set fail")
	}

	output = ts.ProcessCommand("sunion", "k1", "k2")
	if !output.isArraySet("cat") {
		t.Fatal("sunion union of two identical single member sets fail")
	}

	output = ts.ProcessCommand("sunion", "k2", "k1")
	if !output.isArraySet("cat") {
		t.Fatal("sunion union of two identical single member sets reversed fail")
	}

	// set 1 with more members than set 2
	output = ts.ProcessCommand("sadd", "k3", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("sunion make two member set fail")
	}

	output = ts.ProcessCommand("sunion", "k1", "k3")
	if !output.isArraySet("cat", "dog") {
		t.Fatal("sunion union of a set with more members fail")
	}

	output = ts.ProcessCommand("sunion", "k3", "k1")
	if !output.isArraySet("cat", "dog") {
		t.Fatal("sunion union of a set with more members k3 first fail")
	}

	// three set test
	output = ts.ProcessCommand("sadd", "k1", "bird")
	if !output.isInt(1) {
		t.Fatal("sunion add bird to k1 fail")
	}

	output = ts.ProcessCommand("sunion", "k1", "k2", "k3")
	if !output.isArraySet("cat", "dog", "bird") {
		t.Fatal("sunion union of three sets fail")
	}

	output = ts.ProcessCommand("sunion", "k2", "k1", "k3")
	if !output.isArraySet("cat", "dog", "bird") {
		t.Fatal("sunion union of three sets fail")
	}

	output = ts.ProcessCommand("sunion", "k3", "k2", "k1")
	if !output.isArraySet("cat", "dog", "bird") {
		t.Fatal("sunion union of three sets fail")
	}

	// two sets with one common member
	output = ts.ProcessCommand("sadd", "k4", "cat", "fox")
	if !output.isInt(2) {
		t.Fatal("sunion two keys with one common element setup fail")
	}

	output = ts.ProcessCommand("sunion", "k1", "k4")
	if !output.isArraySet("cat", "bird", "fox") {
		t.Fatal("sunion two keys with one common element fail")
	}

	output = ts.ProcessCommand("sunion", "k4", "k1")
	if !output.isArraySet("cat", "bird", "fox") {
		t.Fatal("sunion two keys with one common element reversed fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("sunion prepare list fail")
	}

	output = ts.ProcessCommand("sunion", "k1", "list")
	if !output.isErrorType() {
		t.Fatal("sunion union with a list fail")
	}

	output = ts.ProcessCommand("sunion", "list", "k1")
	if !output.isErrorType() {
		t.Fatal("sunion union from a list fail")
	}
}

func TestSUnionStore(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// missing keys
	output := ts.ProcessCommand("sunionstore", "destination", "missing")
	if !output.isInt(0) {
		t.Fatal("sunionstore single missing key fail")
	}

	output = ts.ProcessCommand("sunionstore", "destination", "missing1", "missing2")
	if !output.isInt(0) {
		t.Fatal("sunionstore two missing keys fail")
	}

	// single member set test
	output = ts.ProcessCommand("sadd", "k1", "cat")
	if !output.isInt(1) {
		t.Fatal("sunionstore make single member set fail")
	}

	output = ts.ProcessCommand("sunionstore", "destination", "k1", "k1")
	if !output.isInt(1) {
		t.Fatal("sunionstore single member set self-union fail")
	}

	output = ts.ProcessCommand("smembers", "destination")
	if !output.isArraySet("cat") {
		t.Fatal("sunionstore single member set self-union failed to store")
	}

	// missing key union with existing key
	output = ts.ProcessCommand("sunionstore", "destination", "missing", "k1")
	if !output.isInt(1) {
		t.Fatal("sunionstore missing key union with existing fail")
	}

	output = ts.ProcessCommand("smembers", "destination")
	if !output.isArraySet("cat") {
		t.Fatal("sunionstore missing key union with existing failed to store")
	}

	output = ts.ProcessCommand("sunionstore", "destination", "k1", "missing")
	if !output.isInt(1) {
		t.Fatal("sunionstore existing key union with missing fail")
	}

	output = ts.ProcessCommand("smembers", "destination")
	if !output.isArraySet("cat") {
		t.Fatal("sunionstore existing key union with missing failed to store")
	}

	// matching sets test
	output = ts.ProcessCommand("sadd", "k2", "cat")
	if !output.isInt(1) {
		t.Fatal("sunionstore make second single member set fail")
	}

	output = ts.ProcessCommand("sunionstore", "destination", "k1", "k2")
	if !output.isInt(1) {
		t.Fatal("sunionstore union of two identical single member sets fail")
	}

	output = ts.ProcessCommand("smembers", "destination")
	if !output.isArraySet("cat") {
		t.Fatal("sunionstore union of two identical single member sets failed to store")
	}

	// set 1 with more members than set 2
	output = ts.ProcessCommand("sadd", "k1", "cat", "dog", "bird")
	if !output.isInt(2) {
		t.Fatal("sunionstore make set 1 with three members fail")
	}

	output = ts.ProcessCommand("sunionstore", "k3", "k1", "k2")
	if !output.isInt(3) {
		t.Fatal("sunionstore set 1 with more members than set 2 fail")
	}

	output = ts.ProcessCommand("smembers", "k3")
	if !output.isArraySet("cat", "dog", "bird") {
		t.Fatal("sunionstore set 1 with more members than set 2 validation fail")
	}

	// sets with nothing in common
	output = ts.ProcessCommand("sadd", "k4", "cow", "fox", "hen")
	if !output.isInt(3) {
		t.Fatal("sunionstore make set k4 fail")
	}

	output = ts.ProcessCommand("sadd", "k5", "cat", "dog")
	if !output.isInt(2) {
		t.Fatal("sunionstore make set k5 fail")
	}

	output = ts.ProcessCommand("sunionstore", "k4+5", "k4", "k5")
	if !output.isInt(5) {
		t.Fatal("sunionstore sets with nothing in common fail")
	}

	output = ts.ProcessCommand("smembers", "k4+5")
	if !output.isArraySet("cow", "fox", "hen", "cat", "dog") {
		t.Fatal("sunionstore diff of two identical single member sets fail")
	}

	// wrong type
	output = ts.ProcessCommand("rpush", "list", "data")
	if !output.isInt(1) {
		t.Fatal("sunionstore prepare list fail")
	}

	output = ts.ProcessCommand("sunionstore", "k1", "list")
	if !output.isErrorType() {
		t.Fatal("sunionstore diff with a list fail")
	}
}

func TestSUnionStoreThreeSets(t *testing.T) {
	ts := NewRedisTestClient()
	defer ts.Close()

	// create 3 sets with unique members
	output := ts.ProcessCommand("sadd", "set1", "cat", "dog", "bird")
	if !output.isInt(3) {
		t.Fatal("sunionstore test setup failed to create set1")
	}

	output = ts.ProcessCommand("sadd", "set2", "fish", "lizard", "snake")
	if !output.isInt(3) {
		t.Fatal("sunionstore test setup failed to create set2")
	}

	output = ts.ProcessCommand("sadd", "set3", "mouse", "hamster", "guinea pig")
	if !output.isInt(3) {
		t.Fatal("sunionstore test setup failed to create set3")
	}

	// perform union and store to a new set
	output = ts.ProcessCommand("sunionstore", "result", "set1", "set2", "set3")
	if !output.isInt(9) {
		t.Fatal("sunionstore test failed to store union of three sets")
	}

	// check that the new set contains the correct members
	output = ts.ProcessCommand("smembers", "result")
	if !output.isArraySet("cat", "dog", "bird", "fish", "lizard", "snake", "mouse", "hamster", "guinea pig") {
		t.Fatal("sunionstore test failed to store correct members in new set")
	}
}
