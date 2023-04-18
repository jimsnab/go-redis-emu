package goredisemu

import (
	"bufio"
	"os"
)

func readKey(reader *bufio.Reader, input chan rune) {
	char, _, err := reader.ReadRune()
	if err == nil {
		input <- char
	}
}

func waitForKey(abort chan struct{}) chan struct{} {
	done := make(chan struct{})

	go func() {
		pressed := make(chan struct{})

		go func() {
			reader := bufio.NewReader(os.Stdin)
			reader.ReadRune()
			pressed <- struct{}{}
		}()

		select {
		case <-abort:
			break // leaks the go routine above, but there is no easy way to avoid that

		case <-pressed:
			break
		}

		done <- struct{}{}
	}()

	return done
}
