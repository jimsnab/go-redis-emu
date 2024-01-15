package redisemu

import _ "embed"

// cmdSpec is the official command definition, captured with wireshark by getting the response of
// the COMMAND DOCS command that is set upon a client's initial connection. Use the "follow"
// option in wireshark to get the whole text. Discard the request; take only the response.
// Sometimes a blank line appears in the middle and has to be manually removed.
// Run TestRedisCommandsFixed() to debug the defintion descriptor.

//go:embed redis7-fixed.txt
var cmdSpec []byte

// cmdInfoSpec is the official command definition, captured with wireshark by getting the response of
// the COMMAND INFO command that must be manually requested with the redis cli.

//go:embed redis7-info.txt
var cmdInfoSpec []byte

//go:embed info-template.txt
var infoTemplate []byte

//go:embed command-help.txt
var cmdHelpText string
