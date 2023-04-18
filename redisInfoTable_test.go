package goredisemu

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/jimsnab/go-lane"
)

func TestRedisInfoTable(t *testing.T) {
	l := lane.NewTestingLane(context.Background())

	content, err := os.ReadFile("redis7-info.txt")
	if err != nil {
		t.Fatal(err)
	}

	rd := newRespDeserializer(l, content)
	value, length, valid := rd.deserializeNext()
	if !valid {
		t.Fatal("invalid test input content")
	}

	fmt.Printf("parsed %d bytes\n", length)

	infoTable := &redisInfoTable{}
	if valid = infoTable.respDeserialize(l, value); !valid {
		t.Fatal("failed to deserialize info table")
	}

	value2 := infoTable.respSerialize()

	out := value2.serialize()

	err = os.WriteFile("test-info.txt", out, 0644)
	if err != nil {
		t.Fatal("failed to write test-info.txt")
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

	infoTable2 := &redisInfoTable{}
	if valid = infoTable2.respDeserialize(l, value); !valid {
		t.Fatal("failed to deserialize serialized info table")
	}

	if !reflect.DeepEqual(infoTable, infoTable2) {

		json1, _ := json.MarshalIndent(value.toNative(), "", "  ")
		json2, _ := json.MarshalIndent(value2.toNative(), "", "  ")
		os.WriteFile("redis-commands-test-1.json", json1, 0644)
		os.WriteFile("redis-commands-test-2.json", json2, 0644)

		t.Fatal("values aren't equal")
	}
}

func TestRedisInfoMovableKeys(t *testing.T) {
	l := lane.NewTestingLane(context.Background())

	content, err := os.ReadFile("redis7-info.txt")
	if err != nil {
		t.Fatal(err)
	}

	rd := newRespDeserializer(l, content)
	value, length, valid := rd.deserializeNext()
	if !valid {
		t.Fatal("invalid test input content")
	}

	fmt.Printf("parsed %d bytes\n", length)

	infoTable := &redisInfoTable{}
	if valid = infoTable.respDeserialize(l, value); !valid {
		t.Fatal("failed to deserialize info table")
	}

	for _, rinfo := range infoTable.table {
		isMovable := rinfo.hasFlag("movablekeys")
		if isMovable {
			fmt.Println(rinfo.Name)
		}
	}
}
