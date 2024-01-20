package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	RequestWork int = iota
	FinishedWork
	Failed
	ShutDown
	ReduceWork
	MapWork
	GetNumReduce
	GetNumMap
)

type Work struct {
	Type     int
	ID       int
	filename string
}

type Packet struct {
	Type int    // packet type
	Msg0 int    // msg0 in this packet
	Msg1 string // msg1 in this packet
	Msg2 string // msg2 in this packet
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
