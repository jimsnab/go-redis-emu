package goredisemu

import "fmt"

func dump(obj any) {
	// should just use json.MarshalIndented but there was originally a non-json format here
	dumpWorker("", "", obj, false)
}

func dumpWorker(indent string, key string, obj any, comma bool) {
	ending := ",\n"
	if !comma {
		ending = "\n"
	}

	switch o := obj.(type) {
	case int:
		if key == "" {
			fmt.Printf("%s%d%s", indent, o, ending)
		} else {
			fmt.Printf("%s\"%s\": %d%s", indent, key, o, ending)
		}
	case string:
		if key == "" {
			fmt.Printf("%s\"%s\"%s", indent, o, ending)
		} else {
			fmt.Printf("%s\"%s\": \"%s\"%s", indent, key, o, ending)
		}
	case []any:
		if key == "" {
			fmt.Printf("%s[\n", indent)
		} else {
			fmt.Printf("%s\"%s\": [\n", indent, key)
		}
		nextIndent := indent + " "
		for index, item := range o {
			dumpWorker(nextIndent, "", item, index < len(o)-1)
		}
		fmt.Printf("%s]%s", indent, ending)
	case []string:
		if key == "" {
			fmt.Printf("%s[\n", indent)
		} else {
			fmt.Printf("%s\"%s\": [\n", indent, key)
		}
		nextIndent := indent + " "
		for index, item := range o {
			dumpWorker(nextIndent, "", item, index < len(o)-1)
		}
		fmt.Printf("%s]%s", indent, ending)
	case map[string]any:
		if key == "" {
			fmt.Printf("%s{\n", indent)
		} else {
			fmt.Printf("%s\"%s\": {\n", indent, key)
		}
		nextIndent := indent + " "
		count := 0
		for k, v := range o {
			dumpWorker(nextIndent, k, v, count < len(o)-1)
			count++
		}
		fmt.Printf("%s}%s", indent, ending)
	default:
		panic(fmt.Sprintf("don't know how to dump type %T", obj))
	}
}
