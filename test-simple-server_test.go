package redisemu

import (
	"context"
	"testing"

	"github.com/jimsnab/go-lane"
	"github.com/redis/go-redis/v9"
)

func TestSimpleServer1(t *testing.T) {
	server := NewServer(nil, 7777)
	defer server.Close()

	rdcli := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:7777"},
	})
	_, err := rdcli.Set(context.Background(), "test", "sample", 0).Result()
	if err != nil {
		t.Fatal(err)
	}
	sample, err := rdcli.Get(context.Background(), "test").Result()
	if err != nil {
		t.Fatal(err)
	}
	if sample != "sample" {
		t.Error("set-get failed")
	}
}

func TestSimpleServer2(t *testing.T) {
	l := lane.NewLogLane(nil)
	server := NewServer(l, 7777)
	defer server.Close()

	rdcli := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"localhost:7777"},
	})
	_, err := rdcli.Set(context.Background(), "test", "sample", 0).Result()
	if err != nil {
		t.Fatal(err)
	}
	sample, err := rdcli.Get(context.Background(), "test").Result()
	if err != nil {
		t.Fatal(err)
	}
	if sample != "sample" {
		t.Error("set-get failed")
	}
}
