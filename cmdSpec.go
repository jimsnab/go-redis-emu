package redisemu

import _ "embed"

//go:embed redis7-fixed.txt
var cmdSpec []byte

//go:embed redis7-info.txt
var cmdInfoSpec []byte

//go:embed info-template.txt
var infoTemplate []byte

//go:embed command-help.txt
var cmdHelpText string
