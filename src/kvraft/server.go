package kvraft

import (
	"bytes"
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
)

const Debug = false

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UID    int64
	RpcNum int
	Args   interface{}
}

type retOp struct {
	RetOP    Op
	CallBack interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	replyChans map[int]chan *retOp
	applied    map[int64]int

	data      map[string]string
	die       chan struct{}
	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	ok, callback := kv.PutCommand(args.UID, args.RpcNum, *args)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
		return
	}

	if callback != nil {
		retstr, ok := callback.(string)
		if !ok {
			//fmt.Printf("Something's wrong 1")
		}
		reply.Value = retstr
	}else{
		reply.Err = DUPGET
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	ok, _ := kv.PutCommand(args.UID, args.RpcNum, *args)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sc *KVServer) PutCommand(UID int64, rpcnum int, args interface{}) (bool, interface{}) {
	sc.mu.Lock()
	if ok := sc.checkApplied(UID, rpcnum); ok {
		sc.mu.Unlock()
		// already applied
		return true, nil
	}

	command := Op{
		UID:    UID,
		RpcNum: rpcnum,
		Args:   args,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		sc.mu.Unlock()
		return false, nil
	}
	retChan := make(chan *retOp, 1)
	sc.replyChans[index] = retChan
	sc.mu.Unlock()

	command2 := <-retChan
	replyOK := true
	if command2.RetOP.RpcNum == command.RpcNum && command2.RetOP.UID == command.UID {
		replyOK = true
	} else {
		replyOK = false
		sc.flushChans(index)
	}

	sc.mu.Lock()
	delete(sc.replyChans, index)
	close(retChan)
	sc.mu.Unlock()
	return replyOK, command2.CallBack

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

func (kv *KVServer) putAppend(key string, val_append string, action string) {
	val, ok2 := kv.data[key]
	if !ok2 {
		val = ""
	}
	if action == PutAction {
		kv.data[key] = val_append
	} else if action == AppendAction {
		kv.data[key] = val + val_append
	}
}

func (kv *KVServer) get(key string) string{
	val, ok2 := kv.data[key]
	if !ok2 {
		val = ""
	}
	return val
}

func (sc *KVServer) handleApplyMsg() {
	for applyMsg := range sc.applyCh {

		if applyMsg.SnapshotValid {
			//This is a snapshot
			sc.readFromSnapshot(applyMsg.Snapshot)
			continue
		}

		cmd, valid := applyMsg.Command.(Op)
		if !valid {
			cmd = Op{}
		}
		retCmd := retOp{
			RetOP:    cmd,
			CallBack: nil,
		}

		sc.mu.Lock()
		//process, don't run the same request twice.

		if ok := sc.checkApplied(cmd.UID, cmd.RpcNum); ok {
			opChan, ok := sc.replyChans[applyMsg.CommandIndex] //rare
			if query, isquery := cmd.Args.(GetArgs); isquery {
				val := sc.get(query.Key)
				retCmd.CallBack = val
			}
			if ok {
				opChan <- &retCmd
			}
			sc.mu.Unlock()
			continue
		}

		//take action
		if putAppend, ok := cmd.Args.(PutAppendArgs); ok {
			sc.putAppend(putAppend.Key, putAppend.Value, putAppend.Op)
		}

		if query, ok := cmd.Args.(GetArgs); ok {
			retCmd.CallBack = sc.get(query.Key)
			if retCmd.CallBack == nil{
				fmt.Println("Impossible?!")
			}
		}

		sc.setApplied(cmd.UID, cmd.RpcNum)
		if sc.maxraftstate != -1 && sc.persister.RaftStateSize() >= sc.maxraftstate * 9 / 10{
			sc.dumpCurrentState(applyMsg.CommandIndex)
		}


		// tell the client this is OK
		opChan, ok := sc.replyChans[applyMsg.CommandIndex]
		if ok {
			opChan <- &retCmd
		}
		sc.mu.Unlock()
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
		v <- &retOp{}
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

// called within mutex
func (kv *KVServer) checkApplied(UID int64, rpcnum int) bool {
	val, ok := kv.applied[UID]
	if !ok {
		val = 0
	}
	if val < rpcnum {
		return false
	} else {
		return true
	}
}

// called within mutex
func (kv *KVServer) setApplied(UID int64, rpcnum int) {
	val, ok := kv.applied[UID]
	if !ok {
		val = 0
	}
	if val < rpcnum {
		kv.applied[UID] = rpcnum
	}
}

// called within mutex
func (kv *KVServer) dumpCurrentState(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.applied)
	e.Encode(kv.data)
	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}

// called outside of a mutex
func (kv *KVServer) readFromSnapshot(data []byte) {
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
		d.Decode(&data_kv) != nil {
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
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.replyChans = make(map[int]chan *retOp, 1000)
	kv.applied = make(map[int64]int, 1000)
	kv.data = make(map[string]string, 1000)
	kv.die = make(chan struct{})
	kv.persister = persister
	kv.readFromSnapshot(persister.ReadSnapshot())

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.handleApplyMsg()
	go kv.leaderShipCheck()
	return kv
}
