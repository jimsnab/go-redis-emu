package redisemu

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

type (
	testConnection struct {
		l       lane.Lane
		conn    net.Conn
		inbound []byte
	}
)

func newTestConnection(t *testing.T, l lane.Lane) *testConnection {
	conn, err := net.Dial("tcp", "localhost:7679")
	if err != nil {
		t.Fatal(err)
	}

	return &testConnection{
		l:       l,
		conn:    conn,
		inbound: []byte{},
	}
}

func (tc *testConnection) readMessage(t *testing.T) (value respValue, length int) {
	for {
		rd := newRespDeserializer(tc.l, tc.inbound)

		if len(tc.inbound) > 0 {
			var valid bool
			value, length, valid = rd.deserializeNext()
			if valid {
				return
			}
		}

		packet := [8192]byte{}
		n, err := tc.conn.Read(packet[:])
		if err != nil {
			t.Fatal(err)
		}

		tc.inbound = append(tc.inbound, packet[:n]...)
		time.Sleep(time.Millisecond * 100)
	}
}

func TestBundledCommands(t *testing.T) {
	tl := lane.NewTestingLane(context.Background())
	tl.AddTee(lane.NewLogLane(tl))

	emu, err := NewEmulator(tl, 7679, "localhost", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer emu.Close()

	emu.Start()

	// for this test send two commands together directly via a socker connection
	tc := newTestConnection(t, tl)
	defer tc.conn.Close()

	msg := "*2\r\n$5\r\nhello\r\n$1\r\n3\r\n"
	_, err = tc.conn.Write([]byte(msg))
	if err != nil {
		t.Fatal(err)
	}

	_, length := tc.readMessage(t)
	tc.inbound = tc.inbound[length:]

	msg = "*2\r\n$4\r\nincr\r\n$4\r\ntest\r\n*3\r\n$7\r\npexpire\r\n$4\r\ntest\r\n$2\r\n50\r\n"
	_, err = tc.conn.Write([]byte(msg))
	if err != nil {
		t.Fatal(err)
	}

	// incr response
	_, length = tc.readMessage(t)
	if string(tc.inbound[:length]) != ":1\r\n" {
		t.Error("unexpected incr response")
	}
	tc.inbound = tc.inbound[length:]

	// pexpire response
	_, length = tc.readMessage(t)
	if string(tc.inbound[:length]) != ":1\r\n" {
		t.Error("unexpected pexpire response")
	}
	tc.inbound = tc.inbound[length:]
}
