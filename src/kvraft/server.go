package kvraft

import (
	"bytes"
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UID int64
	RpcNum int
	Action    string
	Key       string
	Value     string
}

func (op Op) String() string {
	return fmt.Sprintf("(cmd: Ac: %v, Key: %v)", op.Action, op.Key)
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
	applied    map[int64]int

	data map[string]string
	die  chan struct{}
	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf(LOG2, kv.me, "Got Get Request")
	command := Op{
		Action: GetAction,
		Key:    args.Key,
	}
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	retChan := make(chan *Op, 1)
	kv.replyChans[index] = retChan
	kv.mu.Unlock()
	select {
	case command2 := <-retChan:
		if command2.Action == GetAction && command2.Key == command.Key {
			reply.Err = OK
			reply.Value = command2.Value
			DPrintf(LOG2, kv.me, "Committed")
		} else {
			reply.Err = CommitFailed
			kv.flushChans(index)
			DPrintf(LOG2, kv.me, "fail to reach agreement")
		}

		kv.mu.Lock()
		delete(kv.replyChans, index)
		close(retChan)
		kv.mu.Unlock()
	case <-kv.die:
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf(LOG2, kv.me, "Got PutAppend Request")
	kv.mu.Lock()
	if  ok := kv.checkApplied(args.UID, args.RpcNum); ok {
		DPrintf(LOG2, kv.me, "Stale PutAppend Request")
		kv.mu.Unlock()
		// already applied
		reply.Err = OK
		return
	}

	command := Op{
		UID: args.UID,
		RpcNum: args.RpcNum,
		Action:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	retChan := make(chan *Op, 1)
	kv.replyChans[index] = retChan
	kv.mu.Unlock()
	DPrintf(LOG2, kv.me, "Waiting for commitment")

	select {
	case command2 := <-retChan:
		if *command2 == command {
			reply.Err = OK
			DPrintf(LOG2, kv.me, "Committed")
		} else {
			reply.Err = CommitFailed
			kv.flushChans(index)
			DPrintf(LOG2, kv.me, "fail to reach agreement")
		}

		kv.mu.Lock()
		delete(kv.replyChans, index)
		close(retChan)
		kv.mu.Unlock()
	case <-kv.die:
	}

}

func (kv *KVServer) GetState(args *struct{}, reply *StateReply) {
	term, isLeaeder := kv.rf.GetState()
	reply.Term = term
	reply.IsLeader = isLeaeder
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.die)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) handleApplyMsg() {
	for applyMsg := range kv.applyCh {

		if applyMsg.SnapshotValid {
			//This is a snapshot
			kv.readFromSnapshot(applyMsg.Snapshot)
			continue
		}

		cmd, valid := applyMsg.Command.(Op)
		if !valid {
			cmd = Op{}
		}

		kv.mu.Lock()
		//process, don't run the same request twice.
		if cmd.Action == PutAction || cmd.Action == AppendAction {
			if ok :=kv.checkApplied(cmd.UID, cmd.RpcNum); ok {
				DPrintf(LOG2, kv.me, "Double Put or Append in ApplyChan")
				// tell the client the result, may not be ok
				opChan, ok := kv.replyChans[applyMsg.CommandIndex] //rare
				if ok {opChan <- &cmd}
				//already applied
				kv.mu.Unlock()
				continue
			}
		}

		val, ok2 := kv.data[cmd.Key]
		if !ok2 {
			val = ""
		}
		if cmd.Action == PutAction {
			kv.data[cmd.Key] = cmd.Value
		} else if cmd.Action == AppendAction {
			//AppendAction
			kv.data[cmd.Key] = val + cmd.Value
		} else if cmd.Action == GetAction {
			cmd.Value = val
		}

		kv.setApplied(cmd.UID, cmd.RpcNum)
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate * 9 / 10{
			kv.dumpCurrentState(applyMsg.CommandIndex)
		}

		// tell the client this is OK
		opChan, ok := kv.replyChans[applyMsg.CommandIndex]
		if ok {
			opChan <- &cmd
		}
		kv.mu.Unlock()
	}
}

// called out side of  mutex, used when first step down as a follower
func (kv *KVServer) flushChans(except int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for k, v := range kv.replyChans {
		if k == except {
			continue
		}
		v <- &Op{}
		delete(kv.replyChans, k)
	}
}

func (kv *KVServer) leaderShipCheck() {
	for {
		time.Sleep(30 * time.Millisecond)
		kv.mu.Lock()
		isok := len(kv.replyChans) != 0 
		kv.mu.Unlock()
		if isok {
			_, IsLeader := kv.rf.GetState()
			if !IsLeader {
				kv.flushChans(-1)
			}
		}
	}
}

//called within mutex
func (kv *KVServer) checkApplied(UID int64, rpcnum int) bool{
	val,ok := kv.applied[UID]
	if !ok {val = 0}
	if val < rpcnum {
		return false
	}else{
		return true
	}
}

//called within mutex
func (kv *KVServer) setApplied(UID int64, rpcnum int) {
	val,ok := kv.applied[UID]
	if !ok {val = 0}
	if val < rpcnum { 
		kv.applied[UID] = rpcnum
	}
}

//called within mutex
func (kv *KVServer) dumpCurrentState(index int){
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.applied)
	e.Encode(kv.data)
	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}


//called outside of a mutex
func (kv *KVServer) readFromSnapshot(data []byte){
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var applied map[int64]int
	var data_kv map[string]string
	if d.Decode(&applied) != nil ||
		d.Decode(&data_kv) != nil{
		//log.Print("FAIL")
	} else {
		kv.applied = applied
		kv.data = data_kv
	}
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.replyChans = make(map[int]chan *Op, 1000)
	kv.applied = make(map[int64]int, 1000)
	kv.data = make(map[string]string, 1000)
	kv.die = make(chan struct{})
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.handleApplyMsg()
	go kv.leaderShipCheck()
	return kv
}
