package redisemu

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/jimsnab/go-lane"
	"golang.org/x/term"
)

type (
	RedisEmu struct {
		mu       sync.Mutex
		l        lane.Lane
		dss      *dataStoreSet
		server   net.Listener
		cancelFn context.CancelFunc
		wg       sync.WaitGroup

		port            int
		iface           string
		persistBasePath string
		quitOnKeypress  bool
	}
)

func NewEmulator(l lane.Lane, port int, iface string, persistBasePath string, quitOnKeypress bool) (eng *RedisEmu, err error) {
	l2, cancelFn := l.DeriveWithCancel()

	eng = &RedisEmu{
		l:               l2,
		cancelFn:        cancelFn,
		port:            port,
		iface:           iface,
		persistBasePath: persistBasePath,
		quitOnKeypress:  quitOnKeypress,
	}

	return
}

func (eng *RedisEmu) Port() int {
	return eng.port
}

func (eng *RedisEmu) NetInterface() string {
	return eng.iface
}

func (eng *RedisEmu) Start() {
	if eng.quitOnKeypress {
		fmt.Printf("\n\nREDIS Emulator is now running\n\nPress any key to quit\n\n")
	}

	eng.dss = newDataStoreSet(eng.l, eng.persistBasePath)

	// launch termination monitiors
	eng.killSignalMonitor()

	if eng.quitOnKeypress {
		eng.exitKeyMonitor()
	}

	// launch periodic save goroutine
	eng.periodicSave()

	// start accepting connections and processing them
	eng.startServer()
}

func (eng *RedisEmu) RequestTermination() {
	eng.mu.Lock()
	defer eng.mu.Unlock()

	if eng.server != nil {
		// the only way to stop the blocking listen is to close its connection
		eng.server.Close()
		eng.server = nil
	}

	if eng.cancelFn != nil {
		eng.cancelFn()
		eng.cancelFn = nil
	}
}

func (eng *RedisEmu) killSignalMonitor() {
	// register a graceful termination handler
	sigs := make(chan os.Signal, 10)
	signal.Notify(sigs, os.Interrupt)

	eng.wg.Add(1)
	go func() {
		defer eng.wg.Done()

		select {
		case sig := <-sigs:
			eng.l.Info("kill signal received: %s", sig.String())
			eng.RequestTermination()
			return

		case <-eng.l.Done():
			eng.l.Info("kill monitor canceled")
			return
		}
	}()
}

func (eng *RedisEmu) exitKeyMonitor() {
	// Start a go routine to detect a keypress. Upon termination
	// triggered another way, this goroutine will leak. Go does
	// not give a reasonable way to cancel a blocking I/O call.
	eng.wg.Add(1)
	go func() {
		defer eng.wg.Done()

		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Println(err)
			return
		}
		defer term.Restore(int(os.Stdin.Fd()), oldState)

		b := make([]byte, 1)
		_, err = os.Stdin.Read(b)
		if err == nil {
			eng.l.Info("exit key pressed")
			eng.RequestTermination()
		} else {
			eng.l.Info("exit key monitor canceled")
		}
	}()
}

func (eng *RedisEmu) periodicSave() {
	// make a periodic save that will also ensure save upon termination
	if eng.dss.basePath != "" {
		eng.wg.Add(1)
		go func() {
			defer eng.wg.Done()

			timer := time.NewTicker(time.Second)
			for {
				select {
				case <-eng.l.Done():
					eng.l.Trace("saver loop is exiting")
					timer.Stop()
					eng.dss.save(eng.l)
					return
				case <-timer.C:
					eng.dss.save(eng.l)
				}
			}
		}()
	}
}

func (eng *RedisEmu) startServer() {
	// establish socket service
	var err error

	if eng.iface == "" {
		eng.iface = fmt.Sprintf(":%d", eng.port)
	} else {
		eng.iface = fmt.Sprintf("%s:%d", eng.iface, eng.port)
	}
	server, err := net.Listen("tcp", eng.iface)
	if err != nil {
		fmt.Println("Error listening: ", err.Error())
		os.Exit(1)
	}

	eng.server = server
	eng.l.Infof("listening on %s", server.Addr().String())

	// make a command dispatcher
	rd := newRespDeserializerFromResource(eng.l, cmdSpec)
	value, _, valid := rd.deserializeNext()
	if !valid {
		eng.l.Fatal("invalid command definition content")
	}

	cmds := redisCommands{}
	if valid = cmds.respDeserialize(eng.l, value); !valid {
		eng.l.Fatal("failed to deserialize command definitions")
	}

	ri := newRespDeserializerFromResource(eng.l, cmdInfoSpec)
	value, _, valid = ri.deserializeNext()
	if !valid {
		eng.l.Warnf("command info definition error at pos %d line %d", ri.pos, ri.lineNumber)
		eng.l.Fatal("invalid command definition content")
	}

	info := newRedisInfoTable()
	if valid = info.respDeserialize(eng.l, value); !valid {
		eng.l.Fatal("failed to deserialize command info definitions")
	}

	dispatcher := newCmdDispatcher(eng.port, eng.iface, cmds, info, eng.dss)

	eng.wg.Add(1)
	go func() {
		defer eng.wg.Done()

		// accept connections and process commands
		for {
			connection, err := server.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					eng.l.Errorf("accept error: %s", err)
				}
				break
			}
			eng.l.Infof("client connected: %s", connection.RemoteAddr().String())
			newClientCxn(eng.l, connection, dispatcher)
		}
	}()
}

func (eng *RedisEmu) WaitForTermination() {
	// wait for server to quiesque
	eng.wg.Wait()
	eng.l.Info("finished serving requests")
}
