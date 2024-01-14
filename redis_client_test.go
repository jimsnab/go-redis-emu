package redisemu

import (
	"context"
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
		false,          // no keypress termination
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
	_, err = testClient.DBSize(l).Result()
	if err != nil {
		t.Fatal("Error getting dbsize: ", err)
	}
}
