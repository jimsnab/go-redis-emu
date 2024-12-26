package redisemu

import (
	"time"

	"github.com/jimsnab/go-lane"
)

func NewServer(l lane.Lane, port int) (server *RedisEmu) {
	if l == nil {
		l = lane.NewNullLane(nil)
	}

	// spin a few times in case there is latency on port closure from a prior emulator
	var err error
	for range 10 {
		server, err = NewEmulator(
			l,
			port,
			"",  // use default interface
			"",  // don't persist to disk
			nil, // optional chan struct{} to signal termination (such as termination via keypress)
		)

		if err == nil {
			break
		}

		time.Sleep(time.Millisecond * 100)
	}

	if err != nil {
		l.Fatalf("unable to create redis emulator on port %d: %v", err)
	}

	server.Start()
	return
}

func Close() (server *RedisEmu) {
	server.RequestTermination()
	server.WaitForTermination()
	return
}
