package shardkv

import (
	"cpsc416/shardctrler"
	"fmt"
)

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
	ErrDup         = "Dup"
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
	Shard  int
}

type PutAppendReply struct {
	Err   Err
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UID    int64 // uid for the client
	RpcNum int
	Shard  int
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	CfgNum int
	Shard  int
}


type DelShardArgs struct {
	CfgNum int
	Shard  int
}

type DelShardReply struct {
	OK       bool
	IsLeader bool
}


type GetShardReply struct {
	OK       bool
	IsLeader bool
	Shard
}

const (
	PutAction    = "Put"
	AppendAction = "Append"
	GetAction    = "Get"
)
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format+"\n", a...)
	}
	return
}

const NShards = shardctrler.NShards

type Status int

const (
	EMPTY    Status = 0
	SERVING  Status = 1
	UPDATING Status = 2
	HANDING  Status = 3
)

func (state Status) String() string {
	switch state {
	case EMPTY:
		return "E"
	case SERVING:
		return "S"
	case UPDATING:
		return "U"
	case HANDING:
		return "H"
	}
	return "UNDEFINED"
}

type Shard struct {
	ShardID int
	Data    map[string]string
	Applied map[int64]int
	State   Status
	Cfg     int
}

type ShardsData [NShards]*Shard

func (s ShardsData) String() string {
	ret := "("
	for i := 0; i < NShards; i++ {
		ret += fmt.Sprintf("%v_%v, ", s[i].State, s[i].Cfg)
	}
	ret += ")"
	return ret
}

func (shard *Shard) String() string {
	return fmt.Sprintf("(%v, ID: %v ,Cfg: %v)", shard.State, shard.ShardID, shard.Cfg)
}

func (shard *Shard) copy() Shard {
	ret := Shard{
		ShardID: shard.ShardID,
		State:   shard.State,
		Cfg:     shard.Cfg,
	}
	ret.Data = make(map[string]string)
	ret.Applied = make(map[int64]int)
	for k, v := range shard.Data {
		ret.Data[k] = v
	}
	for k, v := range shard.Applied {
		ret.Applied[k] = v
	}
	return ret
}

// server ops
type NewConfig struct {
	Config shardctrler.Config
}

// used to refresh the logs when first becomes a leader
type DummyLog struct {
}

type ShardStateChange struct{
	ShardId int
	FromState Status
	FromNum int 
	NextState Status
	NextNum int
	Data Shard // for updating shard
}

func assert(a bool, b ...string) {
	if !a {
		if len(b)!=0{
			panic(b[0])
		}else{
			panic("Impossible!")
		}
	}
}


type DaemonType int
const(
	CHECKSHARDS DaemonType = 0
	CHECKCONFIG DaemonType = 1
)