package redisemu

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jimsnab/go-lane"
	"github.com/redis/go-redis/v9"
)

const kRedisTestPort = 7379

func TestRedisClientConnect(t *testing.T) {
	l := lane.NewTestingLane(context.Background())

	emu, err := NewEmulator(
		l,
		kRedisTestPort, // test port
		"",             // default interface
		"",             // no persistence
		nil,            // no keypress termination
	)
	if err != nil {
		t.Fatal("Error creating redis emulator: ", err)
	}

	emu.Start()
	defer func() {
		emu.RequestTermination()
		emu.WaitForTermination()
	}()

	redisTestUrl := fmt.Sprintf("redis://localhost:%d", kRedisTestPort)
	opt, err := redis.ParseURL(redisTestUrl + "/1")
	if err != nil {
		t.Fatal("Error parsing redis emulator url: ", err)
	}

	testClient := redis.NewClient(opt)
	defer testClient.Close()

	_, err = testClient.Keys(l, "*").Result()
	if err != nil {
		t.Fatal("keys error: ", err)
	}

	_, err = testClient.DBSize(l).Result()
	if err != nil {
		t.Fatal("Error getting dbsize: ", err)
	}

	_, err = testClient.FlushDB(l).Result()
	if err != nil {
		t.Fatal("Flush error: ", err)
	}

	redisUrl := fmt.Sprintf("redis://localhost:%d/0", kRedisTestPort)
	opt, err = redis.ParseURL(redisUrl)
	if err != nil {
		t.Fatal("error parsing redis emulator url: ", err)
		return
	}

	redisSingleton := redis.NewClient(opt)
	_, err = redisSingleton.DBSize(l).Result()
	if err != nil {
		t.Fatal("Error getting dbsize: ", err)
	}

	_, err = redisSingleton.FlushDB(l).Result()
	if err != nil {
		t.Fatal("Flush error: ", err)
	}
}

func TestRedisClientConnectNoSetInfo(t *testing.T) {
	l := lane.NewTestingLane(context.Background())

	emu, err := NewEmulator(
		l,
		kRedisTestPort, // test port
		"",             // default interface
		"",             // no persistence
		nil,            // no keypress termination
	)
	if err != nil {
		t.Fatal("Error creating redis emulator: ", err)
	}
	emu.DisableClientSetInfo()

	emu.Start()
	defer func() {
		emu.RequestTermination()
		emu.WaitForTermination()
	}()

	redisTestUrl := fmt.Sprintf("redis://localhost:%d", kRedisTestPort)
	opt, err := redis.ParseURL(redisTestUrl + "/1")
	if err != nil {
		t.Fatal("Error parsing redis emulator url: ", err)
	}

	opt.DisableIndentity = true

	testClient := redis.NewClient(opt)
	defer testClient.Close()

	_, err = testClient.Keys(l, "*").Result()
	if err != nil {
		t.Fatal("keys error: ", err)
	}

	_, err = testClient.DBSize(l).Result()
	if err != nil {
		t.Fatal("Error getting dbsize: ", err)
	}

	_, err = testClient.FlushDB(l).Result()
	if err != nil {
		t.Fatal("Flush error: ", err)
	}

	redisUrl := fmt.Sprintf("redis://localhost:%d/0", kRedisTestPort)
	opt, err = redis.ParseURL(redisUrl)
	if err != nil {
		t.Fatal("error parsing redis emulator url: ", err)
		return
	}
	opt.DisableIndentity = true

	redisSingleton := redis.NewClient(opt)
	_, err = redisSingleton.DBSize(l).Result()
	if err != nil {
		t.Fatal("Error getting dbsize: ", err)
	}

	_, err = redisSingleton.FlushDB(l).Result()
	if err != nil {
		t.Fatal("Flush error: ", err)
	}
}

func TestRedisClientConnectHook(t *testing.T) {
	l := lane.NewTestingLane(context.Background())

	emu, err := NewEmulator(
		l,
		kRedisTestPort, // test port
		"",             // default interface
		"",             // no persistence
		nil,            // no keypress termination
	)
	if err != nil {
		t.Fatal("Error creating redis emulator: ", err)
	}

	emu.SetHook(func(cmd string, args map[string]any) (hooked bool, result any, err error) {
		if cmd == "dbsize" {
			err = errors.New("not right now")
		} else if cmd == "incr" {
			result = 22
			hooked = true
		}

		return
	})

	emu.Start()
	defer func() {
		emu.RequestTermination()
		emu.WaitForTermination()
	}()

	redisTestUrl := fmt.Sprintf("redis://localhost:%d", kRedisTestPort)
	opt, err := redis.ParseURL(redisTestUrl + "/1")
	if err != nil {
		t.Fatal("Error parsing redis emulator url: ", err)
	}

	testClient := redis.NewClient(opt)
	defer testClient.Close()
	_, err = testClient.DBSize(l).Result()
	if err.Error() != "ERR not right now" {
		t.Error("didn't get hook error")
	}

	_, err = testClient.FlushDB(l).Result()
	if err != nil {
		t.Fatal("Flush error: ", err)
	}

	n, err := testClient.Incr(l, "test").Result()
	if err != nil {
		t.Fatal("Incr error: ", err)
	}
	if n != 22 {
		t.Error("didn't get hook response value")
	}
}
