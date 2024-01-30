package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
// 
//  install neccessary package
//   pip install typer rich
//
//  Use the following to generate better logs, provided by TA
//   go test -run 2A | python3 parselog.py -c 3 -i Test,Test2
const Debug = true

var (
	debugStart time.Time
)

func init() {
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

type ptopic string

const (
	INIT ptopic = "INIT"
)

func DPrintf(topic ptopic, me int ,format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(fmt.Sprintf("%06d %v S%d ",time.Since(debugStart).Microseconds(),topic,me)+format, a...)
	}
	return
}
