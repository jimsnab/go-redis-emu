# Before Using This

Check out https://github.com/alicebob/miniredis

It may be better suited for your needs. However, it has been found to have
incorrect emulation - such as expiration not honoring the time correctly.

# Redis Emulator
This go library emulates Redis for the purposes of unit tests that do not have
a convenient Redis server to use (for example, when using a free hosting
environment that lacks capacity to run the real Redis).

It's not fully featured, and it's not intended for anything but testing.
However what's implemented should be trustworthy, as the unit tests of this
emulator have good coverage and pass whether running against the emulator
or real redis.

# Simple Usage

Create an instance of the emulator as shown in the following fragment:

```go
	// The emulator has a lot of logging which can be useful,
	// and can be annoyingly noisy. Use a separate lane if you
	// wish to isolate the logging and potentially filter or
	// disable it.
	serverLane := lane.NewLogLane(context.Background())

	// The server initializes, including loading from disk if
	// a persist path is specified, but does not start listening
	// until Start() is called.
	redisServer, err := redisemu.NewEmulator(
		serverLane,     
		kRedisTestPort, // such as 7379
		"",
		"",
		false,
	)
	if err != nil {
		panic(err)
	}
	redisServer.Start()
```

Terminate the emulator with:

```go
	redisServer.RequestTermination()    // immediately reject new requests
	redisServer.WaitForTermination()    // wait for all in-flight requests to complete
```

The two step termination allows you to request termination, then do other
work such as termination of other parts of your service, and then block
until redisServer completes its termination.

It is sometimes required to request termination in one task and wait for
termination in another. For example, a signal handler task might request
termination, while the main task waits for termination.

The emulator includes a "press a key to terminate" capability, for which
a keypress will invoke `RequestTermination()`. This is useful in a stand-alone
test server.

# Testing

See [go-redisemu-server](https://github.com/jimsnab/go-redisemu-server) for
a console executable that implements a stand-alone emulator server.

See `test-main.go.example` for sample code that stands up a redis server emulator
as part of `TestMain()`, and maintains a redis client singleton that can
be shared between production and test code, ensuring the production code is
tested without modification, and, test steps such as flushing the database
do not execute against a production redis server -- as long as the production
code only uses that redis client singleton to interact with redis, and
the test only flushes the database via `testInitializeDb()`.


