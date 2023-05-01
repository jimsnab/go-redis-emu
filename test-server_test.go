package redisemu

import (
	"context"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestServer(t *testing.T) {
	l := lane.NewTestingLane(context.Background())
	eng, err := NewEmulator(l, 7500, "", "", true)
	if err != nil {
		t.Fatal(err)
	}
	eng.Start()
	//time.Sleep(30 * time.Second)
	eng.RequestTermination()
	eng.WaitForTermination()
}