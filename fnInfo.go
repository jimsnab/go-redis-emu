package redisemu

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type redisStats struct {
	run_id                     string
	update_in_seconds          int64
	update_in_days             int64
	connected_clients          int64
	used_memory                int64
	used_memory_rss            int64
	used_memory_peak           int64
	used_memory_peak_perc      int64
	used_memory_overhead       int64
	used_memory_startup        int64
	used_memory_dataset        int64
	used_memory_dataset_perc   int64
	allocator_allocated        int64
	allocator_active           int64
	allocator_resident         int64
	total_system_memory        int64
	rdb_last_save_time         int64
	rdb_saves                  int64
	total_connections_received int64
	total_commands_processed   int64
	total_net_input_bytes      int64
	total_net_output_bytes     int64
	total_error_replies        int64
	total_reads_processed      int64
	total_writes_processed     int64
	keys                       int64
}

var info redisStats = redisStats{
	run_id: strings.ReplaceAll(uuid.NewString(), "-", ""),
}
var infoMu sync.Mutex
var started time.Time = time.Now()

func fnInfo(ctx *cmdContext, args map[string]any) (output respValue, err error) {
	templates := map[string]string{}
	order := []string{}
	var metrics strings.Builder
	section := ""
	lines := strings.Split(string(infoTemplate), "\n")
	lines = append(lines, "# ") // make sure last section gets processed
	for _, line := range lines {
		if strings.HasPrefix(line, "# ") {
			if section != "" {
				order = append(order, section)
				templates[section] = metrics.String()
				metrics.Reset()
			}
			section = strings.TrimSpace(line[2:])
		} else {
			text := strings.TrimSpace(line)
			if text != "" {
				metrics.WriteString(text)
				metrics.WriteString("\r\n")
			}
		}
	}

	uptime := time.Since(started)

	data := map[string]any{}
	data["run_id"] = info.run_id
	data["tcp_port"] = ctx.cd.port
	data["server_time_usec"] = uptime.Microseconds()
	data["update_in_seconds"] = info.update_in_seconds
	data["update_in_days"] = info.update_in_days
	data["connected_clients"] = info.connected_clients
	data["used_memory"] = info.used_memory
	data["used_memory_rss"] = info.used_memory_rss
	data["used_memory_peak"] = info.used_memory_peak
	data["used_memory_peak_perc"] = info.used_memory_peak_perc
	data["used_memory_overhead"] = info.used_memory_overhead
	data["used_memory_startup"] = info.used_memory_startup
	data["used_memory_dataset"] = info.used_memory_dataset
	data["used_memory_dataset_perc"] = info.used_memory_dataset_perc
	data["allocator_allocated"] = info.allocator_allocated
	data["allocator_active"] = info.allocator_active
	data["allocator_resident"] = info.allocator_resident
	data["total_system_memory"] = info.total_system_memory
	data["rdb_last_save_time"] = info.rdb_last_save_time
	data["rdb_saves"] = info.rdb_saves
	data["total_connections_received"] = info.total_connections_received
	data["total_commands_processed"] = info.total_commands_processed
	data["total_net_input_bytes"] = info.total_net_input_bytes
	data["total_net_output_bytes"] = info.total_net_output_bytes
	data["total_error_replies"] = info.total_error_replies
	data["total_reads_processed"] = info.total_reads_processed
	data["total_writes_processed"] = info.total_writes_processed
	data["keys"] = info.keys

	data["used_memory_human"] = info.humanValue(info.used_memory)
	data["used_memory_rss_human"] = info.humanValue(info.used_memory_rss)
	data["used_memory_peak_human"] = info.humanValue(info.used_memory_peak)
	data["total_system_memory_human"] = info.humanValue(info.total_system_memory)

	// construct output for the requested sections
	var sb strings.Builder
	filters, _ := args["section"].([]any)

	for _, section := range order {
		if filters != nil {
			found := false
			for _, filter := range filters {
				if strings.EqualFold(section, filter.(string)) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		if sb.Len() > 0 {
			sb.WriteString("\r\n")
		}
		sb.WriteString("# ")
		sb.WriteString(section)
		sb.WriteString("\r\n")
		sb.WriteString(templates[section])
	}

	// expand the symbols in the output
	t := []rune(sb.String())
	sb.Reset()

	pos := 0
	for pos < len(t) {
		ch := t[pos]
		if pos+2 < len(t) && ch == '$' && t[pos+1] == '{' {
			pos += 2
			keyword := ""
			for end := pos; end < len(t); end++ {
				if t[end] == '}' {
					keyword = string(t[pos:end])
					pos = end + 1
					break
				}
			}

			sb.WriteString(fmt.Sprintf("%v", data[keyword]))
		} else if ch == '\r' {
			pos++
		} else if ch == '\n' {
			sb.WriteString("\r\n")
			pos++
		} else {
			sb.WriteRune(ch)
			pos++
		}
	}

	output.data = respVerbatimString{format: "txt", text: sb.String()}
	return
}

func (ri *redisStats) humanValue(value int64) string {
	v := float64(value)
	if value < 1024 {
		return fmt.Sprintf("%.2fB", v)
	}

	if value < 1024*1024 {
		return fmt.Sprintf("%.2fK", v/1024.0)
	}

	if value < 1024*1024*1024 {
		return fmt.Sprintf("%.2fM", v/(1024.0*1024.0))
	}

	return fmt.Sprintf("%.2fG", v/(1024.0*1024.0*1024.0))
}
