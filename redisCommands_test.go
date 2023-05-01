package redisemu

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestRedisCommands(t *testing.T) {
	l := lane.NewTestingLane(context.Background())

	content, err := os.ReadFile("official-commands.txt")
	if err != nil {
		t.Fatal(err)
	}

	rd := newRespDeserializerFromResource(l, content)
	value, length, valid := rd.deserializeNext()
	if !valid {
		t.Fatal("invalid test input content")
	}

	fmt.Printf("parsed %d bytes\n", length)

	cmds := &redisCommands{}
	if valid = cmds.respDeserialize(l, value); !valid {
		t.Fatal("failed to deserialize commands")
	}

	value3 := cmds.respSerialize()
	value2 := resp3To2(value3)

	out := value2.serialize()

	err = os.WriteFile("test-commands.txt", out, 0644)
	if err != nil {
		t.Fatal("failed to write test-commands.txt")
	}

	if length != len(out) {
		t.Fatal("serialization length does not match input")
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

	cmds2 := &redisCommands{}
	if valid = cmds2.respDeserialize(l, value); !valid {
		t.Fatal("failed to deserialize serialized commands")
	}

	if !reflect.DeepEqual(cmds, cmds2) {

		json1, _ := json.MarshalIndent(value.toNative(), "", "  ")
		json2, _ := json.MarshalIndent(value2.toNative(), "", "  ")
		os.WriteFile("redis-commands-test-1.json", json1, 0644)
		os.WriteFile("redis-commands-test-2.json", json2, 0644)

		t.Fatal("values aren't equal")
	}
}

func TestRedisCommandsFixed(t *testing.T) {
	l := lane.NewTestingLane(context.Background())

	content, err := os.ReadFile("redis7-fixed.txt")
	if err != nil {
		t.Fatal(err)
	}

	rd := newRespDeserializerFromResource(l, content)
	value, length, valid := rd.deserializeNext()
	if !valid {
		t.Fatal("invalid test input content")
	}

	fmt.Printf("parsed %d bytes\n", length)

	cmds := &redisCommands{}
	if valid = cmds.respDeserialize(l, value); !valid {
		t.Fatal("failed to deserialize commands")
	}

	value3 := cmds.respSerialize()
	value2 := resp3To2(value3)

	out := value2.serialize()

	err = os.WriteFile("test-commands-fixed.txt", out, 0644)
	if err != nil {
		t.Fatal("failed to write test-commands-fixed.txt")
	}

	if length != len(out) {
		t.Fatal("serialization length does not match input")
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

	cmds2 := &redisCommands{}
	if valid = cmds2.respDeserialize(l, value); !valid {
		t.Fatal("failed to deserialize serialized commands")
	}

	if !reflect.DeepEqual(cmds, cmds2) {

		json1, _ := json.MarshalIndent(value.toNative(), "", "  ")
		json2, _ := json.MarshalIndent(value2.toNative(), "", "  ")
		os.WriteFile("redis-commands-test-1.json", json1, 0644)
		os.WriteFile("redis-commands-test-2.json", json2, 0644)

		t.Fatal("values aren't equal")
	}
}
