package redisemu

import (
	"context"
	"testing"

	"github.com/jimsnab/go-lane"
)

// this test requires manual keypress to terminate the test server; disabled by default
const testServerTestEnabled = false

func TestServer(t *testing.T) {
	if testServerTestEnabled {
		l := lane.NewTestingLane(context.Background())
		eng, err := NewEmulator(l, 7500, "", "", make(chan struct{}))
		if err != nil {
			t.Fatal(err)
		}
		eng.Start()
		//time.Sleep(30 * time.Second)
		eng.RequestTermination()
		eng.WaitForTermination()
	}
}
