package kvraft2

import (
	"cpsc416/labrpc"
	"crypto/rand"
	"math/big"
	"sync"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	mu sync.Mutex

	LeaderId int // LeaderId could be a stale leader.
	Term int 
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := GetArgs{
		Key: key,
	}
	for {
		reply := GetReply{}
		ok := ck.servers[ck.LeaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err!=OK {
			ck.GetNewLeader()
		}else{
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	// You will have to modify this function.
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		SerialNum: nrand(), // TODO: Make sure this is actually unique
	}

	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.LeaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err!=OK {
			ck.GetNewLeader()
		}else{
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// returns true if a new leader is found
// This is called when the current Leader failed to execute the command, we try to find a new Leader, this new leader might be the same one
func (ck *Clerk) GetNewLeader(){
	numServers := len(ck.servers)
	for i:=0;i<numServers;i++{
		j := (i + ck.LeaderId + 1) % numServers // we don't want to use ck.LeaderId again.
		reply := StateReply{}
		dumArgs := new(struct{})
		ok := ck.servers[j].Call("KVServer.GetState", dumArgs, &reply)
		if ok && reply.IsLeader{
			ck.LeaderId = j
			return 
		}
	}
}