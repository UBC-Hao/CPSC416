package kvraft

import (
	"fmt"
	"log"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	CommitFailed   = "CommitFailed"
)

const (
	PutAction = "Put"
	AppendAction = "Append"
	GetAction = "Get"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	UID int64 // uid for the client
	RpcNum int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}


type StateReply struct{
	Term int 
	IsLeader bool
}


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
	FATAL ptopic = "FATAL"
	SUPE ptopic = "LOG4" // ignore debug
)

func DPrintf(topic ptopic, me int, format string, a ...interface{}) (n int, err error) {
	if topic == FATAL {
		log.Fatal(fmt.Sprintf("%06d %v S%d ", time.Since(debugStart).Microseconds(), topic, me)+format)
	}
	if Debug {
		log.Printf(fmt.Sprintf("%06d %v S%d [%v]", time.Since(debugStart).Microseconds(), topic, me,time.Since(debugStart).Seconds())+format, a...)
	}
	return
}