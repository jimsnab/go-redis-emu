# Redis Emulator
This go library emulates Redis for the purposes of unit tests that do not have
a convenient Redis server to use (for example, when using a free hosting
environment that lacks capacity to run the real Redis).

It's not fully featured, and it's not intended for anything but testing.

See `main.go.example` for the method to launch the emulated server.