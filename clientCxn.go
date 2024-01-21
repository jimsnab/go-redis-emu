package redisemu

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jimsnab/go-lane"
)

// The following client state machine progresses through the lifecycle
// of a client connection. A client processes only one command at a
// time.
const (
	csNone            cxnState = iota
	csInitialize               // can progress to csWaitForCommand or csTerminate
	csWaitForCommand           // can progress to csDispatchCommand or csTerminate
	csDispatchCommand          // can progress to csTerminate on an interruption, or csWaitForCommand after command processing is complete
	csTerminate                // closes the client
)

type (
	cxnState int

	// clientCxn holds state about the socket connection. It links
	// 1-to-1 to a clientState instance that is common to any type
	// of client connection.
	clientCxn struct {
		cs          *clientState
		started     time.Time
		mu          sync.Mutex // synchronizes access to waiting, closing flags
		cxn         net.Conn
		socketState cxnState
		csceCh      chan *clientStateEvent
		waiting     bool
		closing     bool
		inbound     []byte
		respVersion int
	}
)

func newClientCxn(l lane.Lane, cxn net.Conn, dispatcher *cmdDispatcher) *clientCxn {
	cc := &clientCxn{
		cxn:         cxn,
		started:     time.Now(),
		socketState: csNone,
		csceCh:      make(chan *clientStateEvent, 3),
	}

	cc.cs = newClientState(l, cc, dispatcher)

	cc.queueStateChange(csInitialize, nil)

	go cc.run()

	return cc
}

func (cc *clientCxn) ClientInfo() []string {
	since := time.Since(cc.started)
	return []string{
		"addr=" + cc.cxn.RemoteAddr().String(),
		"laddr=" + cc.cxn.LocalAddr().String(),
		"age=" + fmt.Sprintf("%d", int64(since.Seconds())),
	}
}

func (cc *clientCxn) MatchFilter(filter map[string]string) bool {
	for k, v := range filter {
		switch k {
		case "addr":
			str := cc.cxn.RemoteAddr().String()
			if v != str {
				return false
			}

		case "laddr":
			str := cc.cxn.LocalAddr().String()
			if v != str {
				return false
			}
		}
	}
	return true
}

func (cc *clientCxn) queueStateChange(newState cxnState, eventData any) {
	cc.csceCh <- &clientStateEvent{
		newState:  newState,
		eventData: eventData,
	}
}

// request connection close
func (cc *clientCxn) RequestClose() {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if !cc.closing {
		cc.closing = true
		if cc.waiting {
			// in a blocking read, close the socket
			cc.cxn.Close()
		}
		cc.queueStateChange(csTerminate, nil)
	}
}

func (cc *clientCxn) IsCloseRequested() bool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.closing
}

func requestAllCxnClose() {
	processAllClients(func(id int64, cs *clientState) {
		cc, ok := cs.client.(*clientCxn)
		if ok {
			cc.RequestClose()
		}
	})
}

func waitForAllCxnClose() {
	for {
		if !isClientActive() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (cc *clientCxn) run() {
	for {
		event := <-cc.csceCh

		cc.socketState = event.newState
		switch cc.socketState {
		case csInitialize:
			cc.onInitialize()
		case csTerminate:
			cc.onTerminate()
			cc.cs.l.Tracef("client %d at %s terminated", cc.cs.id, cc.cxn.RemoteAddr().String())
			return
		case csWaitForCommand:
			if cc.closing {
				cc.queueStateChange(csTerminate, nil)
			} else {
				cc.onWaitForCommand()
			}
		case csDispatchCommand:
			cc.onDispatchCommand(event.eventData.(respValue))
		}
	}
}

func (cc *clientCxn) onTerminate() {
	cc.cxn.Close()
	cc.cs.unregister()
}

func (cc *clientCxn) onInitialize() {
	cc.queueStateChange(csWaitForCommand, nil)
}

func (cc *clientCxn) onWaitForCommand() {
	buffer := make([]byte, 1024*8)

	cmd, length := cc.parseCommand()
	if length == 0 {
		cc.mu.Lock()
		cc.waiting = true
		cc.mu.Unlock()

		n, err := cc.cxn.Read(buffer)

		cc.mu.Lock()
		cc.waiting = false
		cc.mu.Unlock()

		if err != nil {
			if !errors.Is(err, io.EOF) {
				cc.cs.l.Debugf("read error from %s: %s", cc.cxn.RemoteAddr().String(), err)
			}
			cc.queueStateChange(csTerminate, nil)
			return
		}

		if cc.inbound == nil {
			cc.inbound = buffer[0:n]
		} else {
			cc.inbound = append(cc.inbound, buffer[0:n]...)
		}

		cc.cs.l.Tracef("received command data from client")
		cmd, length = cc.parseCommand()
	}

	if length == 0 {
		cc.queueStateChange(csWaitForCommand, nil)
	} else {
		infoMu.Lock()
		info.total_net_input_bytes += int64(length)
		info.total_reads_processed++
		infoMu.Unlock()
		cc.inbound = cc.inbound[length:]
		cc.queueStateChange(csDispatchCommand, cmd)
	}
}

func (cc *clientCxn) parseCommand() (cmd respValue, length int) {
	l := lane.NewNullLane(context.Background())

	rd := newRespDeserializer(l, cc.inbound)
	value, length, valid := rd.deserializeNext()
	if !valid {
		return
	}
	cmd = value
	return
}

func (cc *clientCxn) onDispatchCommand(cmd respValue) {
	go func() {
		returnVal := cc.cs.dispatch(cmd)
		sendData := returnVal.serialize()
		n, err := cc.cxn.Write(sendData)
		if err != nil {
			cc.cs.l.Debugf("write error: %s", err)
			cc.cxn.Close()
		} else {
			cc.cs.l.Tracef("wrote %d bytes", n)
			infoMu.Lock()
			info.total_net_output_bytes += int64(n)
			info.total_writes_processed++
			info.total_commands_processed++
			infoMu.Unlock()
			cc.queueStateChange(csWaitForCommand, nil)
		}
	}()
}

func (cc *clientCxn) ServerAddr() string {
	return cc.cxn.LocalAddr().String()
}

func (cc *clientCxn) ClientAddr() string {
	return cc.cxn.RemoteAddr().String()
}

func (cc *clientCxn) ServerNow() time.Time {
	return time.Now()
}
