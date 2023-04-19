package redisemu

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jimsnab/go-lane"
)

type (
	realRedisClient struct {
		l           lane.Lane
		mu          sync.Mutex
		started     time.Time
		cxn         net.Conn
		laddr       string
		addr        string
		id          int64
		noEvict     bool
		processing  int32
		terminated  bool
		respBuffer  []byte
		clockBiasUs int64
	}
)

var realClientsActive int32 = 0
var testRealRedis = false

func newRealRedisClient() RedisTestClient {
	l := lane.NewLogLane(context.Background())

	rrc := &realRedisClient{
		l:          l,
		started:    time.Now(),
		respBuffer: []byte{},
	}

	rrc.connect()
	return rrc
}

func (rrc *realRedisClient) connect() {
	cxn, err := net.Dial("tcp", "redis.local:6379")
	if err != nil {
		panic(err)
	}
	defer func() {
		if rrc.cxn == nil {
			cxn.Close()
		}
	}()

	output := rrc.issueCommand(cxn, "client", "info")
	str, ok := output.toString()
	if !ok {
		panic(fmt.Sprintf("failed to get redis client info: %v", output.data))
	}

	id := int64(0)
	addr := ""
	laddr := ""
	tokens := strings.Split(str, " ")
	for _, token := range tokens {
		pairs := strings.SplitN(token, "=", 2)
		if len(pairs) == 2 {
			switch pairs[0] {
			case "id":
				var err error
				id, err = strconv.ParseInt(pairs[1], 10, 64)
				if err != nil {
					panic(fmt.Sprintf("id is not int64: %v", pairs[1]))
				}
			case "addr":
				addr = pairs[1]
			case "laddr":
				laddr = pairs[1]
			}
		}
	}
	if id <= 0 || addr == "" || laddr == "" {
		panic(fmt.Sprintf("failed to get required fields from client info: %v", str))
	}

	now := time.Now()
	output = rrc.issueCommand(cxn, "time")
	vals, ok := output.toArray()
	if !ok || len(vals) != 2 {
		panic(fmt.Sprintf("failed to get redis client time: %v", output.data))
	}
	serverSeconds, ok := vals[0].toInt()
	if !ok {
		panic(fmt.Sprintf("failed to get redis client time: %v", output.data))
	}

	serverMilliseconds, ok := vals[1].toInt()
	if !ok {
		panic(fmt.Sprintf("failed to get redis client time: %v", output.data))
	}

	deltaSec := serverSeconds - now.Unix()
	deltaUs := serverMilliseconds - now.UnixMicro()%1000000

	rrc.clockBiasUs = deltaUs + (deltaSec * 1000000)

	if atomic.AddInt32(&realClientsActive, 1) == 1 {
		// erase all databases
		output = rrc.issueCommand(cxn, "flushall")
		if !output.isString("OK") {
			panic(fmt.Sprintf("failed to flush redis: %v", output.data))
		}
	}

	rrc.id = id
	rrc.laddr = laddr
	rrc.addr = addr
	rrc.cxn = cxn
}

func (rrc *realRedisClient) Lane() lane.Lane {
	return rrc.l
}

func (rrc *realRedisClient) AdditionalClient() RedisTestClient {
	client := &realRedisClient{
		started: time.Now(),
		l:       rrc.l.Derive(),
	}
	client.connect()
	return client
}

func (rrc *realRedisClient) ProcessCommand(cmdName string, args ...any) (output respValue) {
	if !atomic.CompareAndSwapInt32(&rrc.processing, 0, 1) {
		panic("another command is pending")
	}
	defer func() { atomic.StoreInt32(&rrc.processing, 0) }()

	return rrc.issueCommand(rrc.cxn, cmdName, args...)
}

func (rrc *realRedisClient) issueCommand(cxn net.Conn, cmdName string, args ...any) (output respValue) {
	a := make([]any, 0, len(args)+1)
	a = append(a, cmdName)
	a = append(a, args...)

	input := nativeValueToResp(a)

	_, err := cxn.Write(input.serialize())
	if err != nil {
		if err != io.EOF {
			output.data = respErrorString(fmt.Sprintf("ERR %v", err))
		} else {
			output.data = respErrorString("Error: Server closed the connection")
		}
		return
	}

	packet := make([]byte, 4096)
	for {
		des := newRespDeserializer(rrc.l, rrc.respBuffer)
		value, length, valid := des.deserializeNext()
		if valid {
			rrc.respBuffer = rrc.respBuffer[length:]
			output = value

			// capture no-evict change, because there is no way to get its status from redis
			if strings.EqualFold(cmdName, "client") &&
				len(args) == 2 &&
				strings.EqualFold(args[0].(string), "no-evict") {
				rrc.noEvict = strings.EqualFold(args[1].(string), "on")
			}

			return
		}

		n, err := cxn.Read(packet)
		if err != nil {
			output.data = respErrorString(fmt.Sprintf("ERR %v", err))
			return
		}

		rrc.respBuffer = append(rrc.respBuffer, packet[0:n]...)
	}
}

func (rrc *realRedisClient) DumpKey(keyName string) {
	output := rrc.issueCommand(rrc.cxn, "get", keyName)
	str, valid := output.toString()
	if !valid {
		rrc.l.Tracef("no string found at key '%s'", keyName)
	} else {
		rrc.l.Tracef("key '%s' string '%s'", keyName, str)
	}
}

func (rrc *realRedisClient) DumpBitmapKey(keyName string) {
	output := rrc.issueCommand(rrc.cxn, "get", keyName)
	str, valid := output.toString()
	if !valid {
		rrc.l.Tracef("no bitmap found at key '%s'", keyName)
	} else {
		text := toBitmap([]byte(str))
		rrc.l.Tracef("%s: %s", keyName, text)
	}
}

func (rrc *realRedisClient) Terminate() {
	rrc.mu.Lock()
	defer rrc.mu.Unlock()

	if !rrc.terminated {
		rrc.terminated = true
		rrc.cxn.Close()
		atomic.AddInt32(&realClientsActive, -1)
	}
}

func (rrc *realRedisClient) Close() {
	rrc.Terminate()
}

func (rrc *realRedisClient) ClientInfo() []string {
	panic("unreachable")
}

func (rrc *realRedisClient) MatchFilter(filter map[string]string) bool {
	panic("unreachable")
}

func (rrc *realRedisClient) RequestClose() {
	rrc.Terminate()
}

func (rrc *realRedisClient) IsCloseRequested() bool {
	rrc.mu.Lock()
	defer rrc.mu.Unlock()

	if rrc.terminated {
		return true
	}

	output := rrc.issueCommand(rrc.cxn, "client", "info")
	return output.isErrorType()
}

func (rrc *realRedisClient) ClientID() int64 {
	return rrc.id
}

func (rrc *realRedisClient) IsNoEvict() bool {
	return rrc.noEvict
}

func (rrc *realRedisClient) ServerAddr() string {
	return rrc.laddr
}

func (rrc *realRedisClient) ClientAddr() string {
	return rrc.addr
}

func (rrc *realRedisClient) ServerNow() time.Time {
	return time.Now().Add(time.Microsecond * time.Duration(rrc.clockBiasUs))
}
