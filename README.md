# Redis Emulator
This go library emulates Redis for the purposes of unit tests that do not have
a convenient Redis server to use (for example, when using a free hosting
environment that lacks capacity to run the real Redis).

It's not fully featured, and it's not intended for anything but testing.

# Testing

See `main.go.example` for the method to launch the emulated server. That code
implements a stand-alone redis server executable.

See `test-main.go.example` for sample code that stands up a redis server emulator
as part of `TestMain()`, and maintains a redis client singleton that can
be shared between production and test code, ensuring the production code is
tested without modification, and, test steps such as flushing the database
do not execute against a production redis server -- as long as the production
code only uses that redis client singleton to interact with redis, and
the test only flushes the database via `testInitializeDb()`.


