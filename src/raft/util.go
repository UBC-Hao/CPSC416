package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
//
//	install neccessary package
//	 pip install typer rich
//
//	Use the following to generate better logs, provided by TA
//	 go test -run 2A | python3 parselog.py -c 3 -i Test,Test2
const Debug = false

var (
	debugStart time.Time
)

func init() {
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

type ptopic string

const (
	PANIC ptopic = "panic"
	INIT  ptopic = "INIT"
	REQV  ptopic = "REQV"
	LOG1  ptopic = "LOG1"
	LOG2  ptopic = "LOG2"
	LOG3  ptopic = "LOG3"
	LOG4  ptopic = "LOG4"
	LOG5  ptopic = "LOG5"
	APPE  ptopic = "APPE"
)

func DPrintf(topic ptopic, me int, format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(fmt.Sprintf("%06d %v S%d ", time.Since(debugStart).Microseconds(), topic, me)+format, a...)
	}
	return
}

func DPrintfline() (n int, err error) {
	if Debug {
		log.Printf("xxx")
	}
	return
}
