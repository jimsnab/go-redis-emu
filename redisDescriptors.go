package redisemu

import _ "embed"

//go:embed redis7-fixed.txt
var redis7FixedTxt []byte

//go:embed redis7-info.txt
var redis7InfoTxt []byte
