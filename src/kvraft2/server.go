package kvraft2

import (
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"sync"
	"sync/atomic"
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SerialNum int64
	Action   string
	Key string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	replyChans map[int]chan *Op
	applied map[int64]bool

	data map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	command := Op{
		Action:  GetAction,
		Key: args.Key,
	}
	kv.mu.Lock()
	index , _ , isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	retChan := make(chan *Op, 1)
	kv.replyChans[index] = retChan
	kv.mu.Unlock()

	command2 := <- retChan
	if command2.Action == GetAction && command2.Key == command.Key {
		reply.Err = OK
		reply.Value = command2.Value
	}else{
		reply.Err = CommitFailed
	}

	kv.mu.Lock()
	close(kv.replyChans[index])
	delete(kv.replyChans, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if _,ok:=kv.applied[args.SerialNum]; ok{
		kv.mu.Unlock()
		// already applied 
		reply.Err = OK
		return
	}

	command := Op{
		SerialNum: args.SerialNum,
		Action: args.Op,
		Key: args.Key,
		Value: args.Value,
	}
	kv.mu.Lock()
	index , _ , isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	retChan := make(chan *Op, 1)
	kv.replyChans[index] = retChan
	kv.mu.Unlock()

	command2 := <- retChan
	if *command2 == command {
		reply.Err = OK
	}else{
		reply.Err = CommitFailed
	}

	kv.mu.Lock()
	close(kv.replyChans[index])
	delete(kv.replyChans, index)
	kv.mu.Unlock()
}

func (kv *KVServer) GetState(args struct{}, reply *StateReply) {
	term,isLeaeder := kv.rf.GetState()
	reply.Term = term
	reply.IsLeader = isLeaeder
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) handleApplyMsg() {
	for applyMsg := range(kv.applyCh) {

		if applyMsg.SnapshotValid{
			continue
		}

		cmd, valid := applyMsg.Command.(Op)
		if !valid { cmd = Op{}}


		//process, don't run the same request twice.
		if cmd.Action==PutAction || cmd.Action == AppendAction {
		kv.mu.Lock()
		if _,ok := kv.applied[cmd.SerialNum]; ok{ 
			DPrintf(LOG2, kv.me, "Double Put or Append in ApplyChan")
			// tell the client this is OK
			opChan, ok:= kv.replyChans[applyMsg.CommandIndex]
			if ok { opChan <- &cmd }
			//already applied
			kv.mu.Unlock()
			continue
		}
		}

		val, ok2 := kv.data[cmd.Key]
		if !ok2 { val = "" }
		if cmd.Action == PutAction{
			kv.data[cmd.Key] = cmd.Value
		}else if (ok2 && cmd.Action == AppendAction){
			//AppendAction
			kv.data[cmd.Key] = val + cmd.Value
		}else if (cmd.Action == GetAction){
			cmd.Value = val
		}else{
			DPrintf(LOG3, kv.me,"Unkown cmd action")
		}
		
		kv.applied[cmd.SerialNum] = true

		// tell the client this is OK
		opChan, ok:= kv.replyChans[applyMsg.CommandIndex]
		if ok { opChan <- &cmd }
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.handleApplyMsg()
	return kv
}
