package shardkv

import "fmt"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

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
	UID    int64  // uid for the client
	RpcNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UID    int64 // uid for the client
	RpcNum int
}

type GetReply struct {
	Err   Err
	Value string
}


const (
	PutAction    = "Put"
	AppendAction = "Append"
	GetAction    = "Get"
)
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug{
		fmt.Printf(format, a...)
	}
	return
}
