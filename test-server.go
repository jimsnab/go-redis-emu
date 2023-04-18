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
		mu              sync.Mutex
		l               lane.Lane
		dss             *dataStoreSet
		server          net.Listener
		exitSaver       chan struct{}
		saverTerminated chan struct{}
		canExit         chan struct{}
		terminating     bool

		enableTrace     bool
		port            int
		iface           string
		persistBasePath string
		quitOnKeypress  bool
	}
)

var netInterface string

func NewEmulator(l lane.Lane, enableTrace bool, port int, iface string, persistBasePath string, quitOnKeypress bool) (eng *RedisEmu, err error) {
	eng = &RedisEmu{
		l:               l,
		enableTrace:     enableTrace,
		port:            port,
		iface:           iface,
		persistBasePath: persistBasePath,
		quitOnKeypress:  quitOnKeypress,
	}

	return
}

func (eng *RedisEmu) Start() {
	eng.l = lane.NewLogLane(context.Background())

	fmt.Printf("\n\nREDIS Emulator is now running\n\nPress any key to quit\n\n")

	if !eng.enableTrace {
		eng.l.SetLogLevel(lane.LogLevelInfo)
	}

	if eng.port != 0 {
		ServerPort = eng.port
	}

	if eng.iface != "" {
		netInterface = eng.iface
	}

	eng.dss = newDataStoreSet(eng.l, eng.persistBasePath)

	// launch termination monitiors
	eng.canExit = make(chan struct{})
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
	// ensure only one termination
	eng.mu.Lock()
	isTerminating := eng.terminating
	eng.terminating = true
	eng.mu.Unlock()

	if isTerminating {
		return
	}

	go func() { eng.onTerminate() }()
}

func (eng *RedisEmu) onTerminate() {
	if eng.server != nil {
		// close the server and wait for all active connections to finish
		eng.l.Tracef("closing server")
		eng.server.Close()

		eng.l.Infof("waiting for any open request connections to complete")
		requestAllCxnClose()
		waitForAllCxnClose()
		eng.l.Infof("termination of %s completed", eng.server.Addr().String())
	}

	// stop the periodic saver (if running)
	if eng.exitSaver != nil {
		eng.l.Tracef("closing database saver")
		eng.exitSaver <- struct{}{}
		<-eng.saverTerminated
		eng.l.Tracef("database saver closed")
	}

	eng.canExit <- struct{}{}
}

func (eng *RedisEmu) killSignalMonitor() {
	// register a graceful termination handler
	sigs := make(chan os.Signal, 10)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		sig := <-sigs
		eng.l.Infof("termination %s signaled for %s", sig, eng.server.Addr().String())
		eng.RequestTermination()
	}()
}

func (eng *RedisEmu) exitKeyMonitor() {
	// Start a go routine to detect a keypress. Upon termination
	// triggered another way, this goroutine will leak. Go does
	// not give a reasonable way to cancel a blocking I/O call.
	go func() {
		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Println(err)
			return
		}
		defer term.Restore(int(os.Stdin.Fd()), oldState)

		b := make([]byte, 1)
		_, err = os.Stdin.Read(b)
		if err == nil {
			eng.RequestTermination()
		}
	}()
}

func (eng *RedisEmu) periodicSave() {
	// make a periodic save that will also ensure save upon termination
	if eng.dss.basePath != "" {
		eng.exitSaver = make(chan struct{})
		eng.saverTerminated = make(chan struct{})
		go func() {
			timer := time.NewTicker(time.Second)
			for {
				select {
				case <-eng.exitSaver:
					eng.l.Trace("saver loop is exiting")
					timer.Stop()
					eng.dss.save(eng.l)
					eng.saverTerminated <- struct{}{}
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

	if netInterface == "" {
		netInterface = fmt.Sprintf(":%d", ServerPort)
	} else {
		netInterface = fmt.Sprintf("%s:%d", netInterface, ServerPort)
	}
	eng.server, err = net.Listen("tcp", netInterface)
	if err != nil {
		fmt.Println("Error listening: ", err.Error())
		os.Exit(1)
	}
	eng.l.Infof("listening on %s", eng.server.Addr().String())

	// make a command dispatcher
	rd := newRespDeserializer(eng.l, cmdSpec)
	value, _, valid := rd.deserializeNext()
	if !valid {
		eng.l.Fatal("invalid command definition content")
	}

	cmds := redisCommands{}
	if valid = cmds.respDeserialize(eng.l, value); !valid {
		eng.l.Fatal("failed to deserialize command definitions")
	}

	ri := newRespDeserializer(eng.l, cmdInfoSpec)
	value, _, valid = ri.deserializeNext()
	if !valid {
		eng.l.Fatal("invalid command definition content")
	}

	info := newRedisInfoTable()
	if valid = info.respDeserialize(eng.l, value); !valid {
		eng.l.Fatal("failed to deserialize command info definitions")
	}

	dispatcher := newCmdDispatcher(cmds, info, eng.dss)

	go func() {
		// accept connections and process commands
		for {
			connection, err := eng.server.Accept()
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
	<-eng.canExit
	eng.l.Info("finished serving requests")
}
