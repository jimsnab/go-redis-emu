package redisemu

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestResp(t *testing.T) {
	l := lane.NewTestingLane(context.Background())

	content, err := os.ReadFile("redis7-fixed.txt")
	if err != nil {
		t.Fatal(err)
	}

	// git will mess with the line endings - so normalize them to redis standard
	fixedContent := strings.ReplaceAll(string(content), "\r\n", "\n")
	content = []byte(strings.ReplaceAll(fixedContent, "\n", "\r\n"))

	rd := newRespDeserializerFromResource(l, content)
	value, length, valid := rd.deserializeNext()
	if !valid {
		t.Fatal("invalid test input content")
	}

	fmt.Printf("parsed %d bytes\n", length)

	out := value.serialize()

	if len(content) != len(out) {
		t.Fatal("serialization length does not match input")
	}

	err = os.WriteFile("hello-test.txt", out, 0644)
	if err != nil {
		t.Fatal("failed to write hello-test.txt")
	}

	rd2 := newRespDeserializer(l, out)
	if !valid {
		t.Fatal("invalid content made by serialization")
	}
	value2, length2, valid := rd2.deserializeNext()
	if !valid {
		t.Fatal("invalid content made by serialization")
	}

	if length != length2 {
		t.Fatal("lengths aren't equal")
	}

	if !reflect.DeepEqual(value, value2) {
		t.Fatal("values aren't equal")
	}
}
