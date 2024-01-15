package redisemu

import (
	"bufio"
	"bytes"
	"fmt"
)

func dump(obj any) string {
	// should just use json.MarshalIndented but there was originally a non-json format here
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	dumpWorker(w, "", "", obj, false)
	w.Flush()

	return string(buf.Bytes())
}

func dumpWorker(w *bufio.Writer, indent string, key string, obj any, comma bool) {
	ending := ",\n"
	if !comma {
		ending = "\n"
	}

	switch o := obj.(type) {
	case int:
		if key == "" {
			w.WriteString(fmt.Sprintf("%s%d%s", indent, o, ending))
		} else {
			w.WriteString(fmt.Sprintf("%s\"%s\": %d%s", indent, key, o, ending))
		}
	case string:
		if key == "" {
			w.WriteString(fmt.Sprintf("%s\"%s\"%s", indent, o, ending))
		} else {
			w.WriteString(fmt.Sprintf("%s\"%s\": \"%s\"%s", indent, key, o, ending))
		}
	case []any:
		if key == "" {
			w.WriteString(fmt.Sprintf("%s[\n", indent))
		} else {
			w.WriteString(fmt.Sprintf("%s\"%s\": [\n", indent, key))
		}
		nextIndent := indent + " "
		for index, item := range o {
			dumpWorker(w, nextIndent, "", item, index < len(o)-1)
		}
		w.WriteString(fmt.Sprintf("%s]%s", indent, ending))
	case []string:
		if key == "" {
			w.WriteString(fmt.Sprintf("%s[\n", indent))
		} else {
			w.WriteString(fmt.Sprintf("%s\"%s\": [\n", indent, key))
		}
		nextIndent := indent + " "
		for index, item := range o {
			dumpWorker(w, nextIndent, "", item, index < len(o)-1)
		}
		w.WriteString(fmt.Sprintf("%s]%s", indent, ending))
	case map[string]any:
		if key == "" {
			w.WriteString(fmt.Sprintf("%s{\n", indent))
		} else {
			w.WriteString(fmt.Sprintf("%s\"%s\": {\n", indent, key))
		}
		nextIndent := indent + " "
		count := 0
		for k, v := range o {
			dumpWorker(w, nextIndent, k, v, count < len(o)-1)
			count++
		}
		w.WriteString(fmt.Sprintf("%s}%s", indent, ending))
	default:
		panic(fmt.Sprintf("don't know how to dump type %T", obj))
	}
}
