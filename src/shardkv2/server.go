package shardkv

import (
	"bytes"
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"cpsc416/shardctrler2"
	//"fmt"
	"sync"
	"time"
)

type Op struct {
	UID    int64
	RpcNum int
	Args   interface{}
}

type retOp struct {
	RetOP    Op
	rightShard bool
	CallBack interface{}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister 	*raft.Persister

	// Your definitions here.
	mck *shardctrler2.Clerk // used to communicate with the clerk
	currCfg *shardctrler2.Config

	replyChans map[int]chan *retOp
	applied    map[int64]int

	data      [shardctrler2.NShards]map[string]string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	ok, callback, rightShard := kv.PutCommand(args.UID, args.RpcNum, *args)

	if !rightShard{
		reply.Err = ErrWrongGroup
		return
	}
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
		return
	}

	if callback != nil {
		retstr, _ := callback.(string)
		reply.Value = retstr
	}
}


func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	ok, _, rightShard := kv.PutCommand(args.UID, args.RpcNum, *args)
	if !rightShard{
		reply.Err = ErrWrongGroup
		return
	}
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}


func (sc *ShardKV) PutCommand(UID int64, rpcnum int, args interface{}) (bool, interface{}, bool) {
	sc.mu.Lock()
	if ok := sc.checkApplied(UID, rpcnum); ok {
		var callback interface{}
		rightShard := true // default is true
		if query, isquery := args.(GetArgs); isquery {
			var val string
			val,rightShard = sc.get(query.Key)
			callback = val
		}
		sc.mu.Unlock()
		// already applied
		return true, callback, rightShard
	}

	command := Op{
		UID:    UID,
		RpcNum: rpcnum,
		Args:   args,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		sc.mu.Unlock()
		return false, nil, true
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
	return replyOK, command2.CallBack, command2.rightShard

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//should only be called within mutex
func (kv *ShardKV) containShards(shard int) bool{
	return kv.currCfg.Shards[shard] == kv.gid
}


func (kv *ShardKV) putAppend(key string, val_append string, action string) bool {
	//fmt.Printf("(%v, %v) \n", key, val_append )
	if !kv.containShards(key2shard(key)){
		return false
	}
	val, ok2 := kv.data[key2shard(key)][key]
	if !ok2 {
		val = ""
	}
	if action == PutAction {
		kv.data[key2shard(key)][key] = val_append
	} else if action == AppendAction {
		kv.data[key2shard(key)][key] = val + val_append
	}
	return true
}

func (kv *ShardKV) get(key string) (string, bool){
	val, ok2 := kv.data[key2shard(key)][key]
	if !ok2 {
		val = ""
	}
	//fmt.Printf("Get (%v, %v) \n", key,val )
	return val, kv.containShards(key2shard(key))
}


// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.replyChans = make(map[int]chan *retOp, 1000)
	kv.applied = make(map[int64]int, 1000)
	for i:=0 ;i < shardctrler2.NShards; i++{
		kv.data[i] = make(map[string]string)
	}
	kv.persister = persister

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler2.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.handleApplyMsg()
	go kv.leaderShipCheck()
	go kv.watchConfig()

	return kv
}


func (sc *ShardKV) watchConfig(){
	for {
		time.Sleep(100 * time.Millisecond)
		_,isLeader :=  sc.rf.GetState()
		if !isLeader{
			continue
		}

		config_new := sc.mck.Query(-1)
		sc.mu.Lock()
		if config_new.Num > sc.currCfg.Num { 
			// updated, we prepare for update
		}
		sc.mu.Unlock()
	}
}



func (sc *ShardKV) handleApplyMsg() {
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
			rightShard := true
			if query, isquery := cmd.Args.(GetArgs); isquery {
				var val string 
				val,rightShard = sc.get(query.Key)
				retCmd.CallBack = val
			}
			retCmd.rightShard = rightShard // for put ops, this has to be true, as UID is related to Shards ID. 
			if ok {
				opChan <- &retCmd
			}
			sc.mu.Unlock()
			continue
		}

		//take action
		if putAppend, ok := cmd.Args.(PutAppendArgs); ok {
			retCmd.rightShard = sc.putAppend(putAppend.Key, putAppend.Value, putAppend.Op)
		}

		if query, ok := cmd.Args.(GetArgs); ok {
			retCmd.CallBack, retCmd.rightShard = sc.get(query.Key)
			if retCmd.CallBack == nil{
			//	fmt.Println("Impossible?!")
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


func (kv *ShardKV) leaderShipCheck() {
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
func (kv *ShardKV) checkApplied(UID int64, rpcnum int) bool {
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
func (kv *ShardKV) setApplied(UID int64, rpcnum int) {
	val, ok := kv.applied[UID]
	if !ok {
		val = 0
	}
	if val < rpcnum {
		kv.applied[UID] = rpcnum
	}
}


// called within mutex
func (kv *ShardKV) dumpCurrentState(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.applied)
	e.Encode(kv.data)
	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}


// called outside of a mutex
func (kv *ShardKV) readFromSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var applied map[int64]int
	var data_kv [shardctrler2.NShards]map[string]string
	if d.Decode(&applied) != nil ||
		d.Decode(&data_kv) != nil {
		//log.Print("FAIL")
	} else {
		kv.applied = applied
		kv.data = data_kv
	}
}



// called out side of  mutex, used when first step down as a follower
func (kv *ShardKV) flushChans(except int) {
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