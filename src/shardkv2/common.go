package shardkv

import (
	"fmt"
	"net/http"
	_ "net/http"
	_ "net/http/pprof"
	"runtime"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

func init() {
	runtime.SetMutexProfileFraction(1)
	go http.ListenAndServe("localhost:6060", nil)
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UID    int64 // uid for the client
	RpcNum int
	CfgNum int 
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UID    int64 // uid for the client
	RpcNum int
	CfgNum int 
}

type GetReply struct {
	Err   Err
	Value string
}

type RequestShardsArgs struct {
	ShardsOldNum []int
	Shards  []int
	Me int 
	CfgNum int 
	//MigrateNumOnly bool
}

type RequestShardsReply struct {
	//MigrateNum int
	IsOk    bool
	OutDate bool
	Applied map[int64]int
	Data    map[int]map[string]string
}

type ACKShardsArgs struct{
	Shard int
	Num int
}

type ACKShardsReply struct{
	OK bool
}

const (
	PutAction    = "Put"
	AppendAction = "Append"
	GetAction    = "Get"
)
const Debug = false
const Debug2 = true
const Verbose = 10

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}


func DPrintf2(format string, a ...interface{}) (n int, err error) {
	if Debug2 {
		fmt.Printf(format, a...)
	}
	return
}
