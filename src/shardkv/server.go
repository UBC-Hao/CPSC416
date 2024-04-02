package shardkv

import (
	"bytes"
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"cpsc416/shardctrler"
	"fmt"
	"sync"
	"time"
)

type Op struct {
	ClerkOp bool
	ShardID int
	UID     int64
	RpcNum  int
	Args    interface{}
}

type retOp struct {
	RetOP    Op
	CallBack CallBackMsg
}

type CallBackMsg struct {
	Value interface{}
	Error Err
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

	// Your definitions here.
	replyChans map[int]chan *retOp
	mck        *shardctrler.Clerk
	isLeader   bool
	persister *raft.Persister

	// state machine, we can only change the following in handleApplyMsg
	Shards ShardsData
	Config shardctrler.Config // Current Config
}



func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	ok, callback := kv.PutCommand(true, args.Shard, args.UID, args.RpcNum, *args)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
		return
	}

	if callback.Error == OK {
		retstr, _ := callback.Value.(string)
		reply.Value = retstr
	} else {
		reply.Err = callback.Error
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("GOT PUTAPPEND REQUEST")
	ok, callback := kv.PutCommand(true, args.Shard, args.UID, args.RpcNum, *args)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
		return
	}

	// OK to have duplicate request
	if callback.Error != ErrDup {
		reply.Err = callback.Error
	}
}

func (sc *ShardKV) PutCommand(ClerkOp bool, ShardID int, UID int64, rpcnum int, args interface{}) (bool, *CallBackMsg) {
	sc.mu.Lock()
	// we only check dup for Clerk Operations
	if ClerkOp {
		if ok := sc.checkApplied(ShardID, UID, rpcnum); ok {
			sc.mu.Unlock()
			// already applied
			return true, &CallBackMsg{Error: ErrDup}
		}
	}

	command := Op{
		ClerkOp: ClerkOp,
		ShardID: ShardID,
		UID:     UID,
		RpcNum:  rpcnum,
		Args:    args,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		sc.mu.Unlock()
		return false, &CallBackMsg{Error: ErrWrongLeader}
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
	return replyOK, &command2.CallBack
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	//for debug purpose
	kv.isLeader = false
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
	labgob.Register(DummyLog{})
	labgob.Register(NewConfig{})
	labgob.Register(Shard{})
	labgob.Register(ShardStateChange{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister


	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.replyChans = make(map[int]chan *retOp, 1000)
	kv.applyCh = make(chan raft.ApplyMsg)

	for i := 0; i < NShards; i++ {
		kv.Shards[i] = &Shard{}
		kv.Shards[i].ShardID = i
		kv.Shards[i].State = EMPTY
		kv.Shards[i].Applied = make(map[int64]int)
		kv.Shards[i].Data = make(map[string]string)
	}
	//read from persister here
	kv.readFromSnapshot(persister.ReadSnapshot())
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.leaderShipCheck()
	go kv.handleApplyMsg()
	go kv.leaderDaemon(CHECKSHARDS)
	go kv.leaderDaemon(CHECKCONFIG)
	return kv
}

func (sc *ShardKV) handleApplyMsg() {
	for applyMsg := range sc.applyCh {

		if applyMsg.SnapshotValid {
			// This is a snapshot
			sc.readFromSnapshot(applyMsg.Snapshot)
			continue
		}

		cmd, valid := applyMsg.Command.(Op)
		if !valid {
			cmd = Op{}
		}
		retCmd := retOp{
			RetOP:    cmd,
			CallBack: CallBackMsg{Error: OK},
		}

		if cmd.ClerkOp {
			sc.handleClerkOp(&cmd, &applyMsg, &retCmd)
		} else {
			sc.handleServerOp(&cmd, &applyMsg, &retCmd)
		}

		sc.mu.Lock()
		// tell the client this is OK
		opChan, ok := sc.replyChans[applyMsg.CommandIndex]
		if ok {
			opChan <- &retCmd
		}

		if sc.maxraftstate != -1 && sc.persister.RaftStateSize() >= sc.maxraftstate * 9 / 10{
			sc.dumpCurrentState(applyMsg.CommandIndex)
		}
		sc.mu.Unlock()
	}
}

func (kv *ShardKV) handleClerkOp(cmd *Op, applyMsg *raft.ApplyMsg, retCmd *retOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//don't run the same request twice.
	if ok := kv.checkApplied(cmd.ShardID, cmd.UID, cmd.RpcNum); ok {
		retCmd.CallBack.Error = ErrDup
		return
	}
	if get, ok := cmd.Args.(GetArgs); ok {
		if rightGroup, str := kv.get(get.Shard, get.Key); rightGroup {
			retCmd.CallBack.Value = str
		} else {
			retCmd.CallBack.Error = ErrWrongGroup
			return // do not call setApplied, we do not modify the shard if we do not serve the shard
		}
	}
	if put, ok := cmd.Args.(PutAppendArgs); ok {
		if rightGroup := kv.putAppend(put.Shard, put.Key, put.Value, put.Op); !rightGroup {
			retCmd.CallBack.Error = ErrWrongGroup
			return // do not call setApplied
		}
	}
	kv.setApplied(cmd.ShardID, cmd.UID, cmd.RpcNum)
}

func (kv *ShardKV) handleServerOp(cmd *Op, applyMsg *raft.ApplyMsg, retCmd *retOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if newcfgEvent, ok := cmd.Args.(NewConfig); ok {
		kv.applyNewCfg(newcfgEvent.Config)
	}

	if shard, ok := cmd.Args.(ShardStateChange); ok{
		kv.applyShardStateChange(shard)
	}
}

func (kv *ShardKV) checkApplied(ShardID int, UID int64, rpcnum int) bool {
	val, ok := kv.Shards[ShardID].Applied[UID]
	if !ok {
		val = 0
	}
	if val < rpcnum {
		return false
	} else {
		return true
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

func (kv *ShardKV) setApplied(ShardID int, UID int64, rpcnum int) {
	assert(kv.Shards[ShardID].State == SERVING)
	val, ok := kv.Shards[ShardID].Applied[UID]
	if !ok {
		val = 0
	}
	if val < rpcnum {
		kv.Shards[ShardID].Applied[UID] = rpcnum
	}
}

// called within mutex
func (kv *ShardKV) dumpCurrentState(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Config)
	for _,shard := range(kv.Shards){
		e.Encode(*shard)
	}
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
	var cfg shardctrler.Config
	d.Decode(&cfg)
	var shards [NShards]*Shard
	for i := 0; i< NShards; i++{
		var shard Shard
		d.Decode(&shard)
		shards[i] = &shard
		//DPrintf("EXAMPLE output %v", shard.Data)
	}
	kv.Shards = shards
	kv.Config = cfg 
}

func (kv *ShardKV) leaderDaemon(daemontype DaemonType) {
	for {
		time.Sleep(30 * time.Millisecond)
		_, IsLeader := kv.rf.GetState()
		kv.mu.Lock()
		kvisLeader := kv.isLeader
		kv.mu.Unlock()
		if !kvisLeader && IsLeader {
			// first becomes a Leader, sends an empty log
			//intf("Put Empty Log")DPr
			kv.PutCommand(false, -1, -1, -1, DummyLog{})
		}
		kv.mu.Lock()
		kv.isLeader = IsLeader
		kv.mu.Unlock()
		if !IsLeader {
			continue
		}
		// Leader: responsible for checking udpates
		switch daemontype{
		case CHECKSHARDS:
			kv.checkShards()
		case CHECKCONFIG:
			kv.checkConfig()
		default:
			assert(false)
		}
	}
}

func (kv *ShardKV) checkShardDeleting(shardid int, currNum int, nextCfg shardctrler.Config){
	// we check if shardid in the nextCfg already got the shards ?
	//kv.print_once(4,"Try deleting Shard %v", shardid)
	delargs := DelShardArgs{
		CfgNum: currNum,
		Shard: shardid,
	}
	delCheckFrom := nextCfg.Groups[nextCfg.Shards[shardid]]
	for _,str := range delCheckFrom{
		srv := kv.make_end(str)
		var replyargs DelShardReply
		kv.mu.Unlock()
		ok := srv.Call("ShardKV.DelShard", &delargs, &replyargs)
		kv.mu.Lock()
		if ok {
			if !replyargs.IsLeader{
				kv.mu.Unlock()
				time.Sleep(30 * time.Millisecond)
				kv.mu.Lock()
				continue
			}
			if replyargs.OK{
				// pack the shard info and send it to the state machine 
				//blocking call
				kv.mu.Unlock()
				kv.PutCommand(false, -1, -1, -1, ShardStateChange{
					ShardId: shardid,
					FromState: HANDING,
					FromNum: currNum,
					NextState: EMPTY,
					NextNum: currNum + 1, 
				})
				kv.mu.Lock()
				break
			}else{
				return // we deal this later.
			}
		}
	}
}


func (kv *ShardKV) checkShardUpdating(shardid int ,currCfg shardctrler.Config){
	// [ 1 1 1 1 ] -> [ 2 2 2 2],  2 asks for data from 1
	//kv.print_once(4,"Try updating Shard %v", shardid)
	getargs := GetShardArgs{
				CfgNum: currCfg.Num,
				Shard: shardid,
				} 
				
	getFrom := currCfg.Groups[currCfg.Shards[shardid]]
	for _,str := range getFrom{
		srv := kv.make_end(str)
		var replyargs GetShardReply
		kv.mu.Unlock()
		ok := srv.Call("ShardKV.GetShard", &getargs, &replyargs)
		kv.mu.Lock()
		if ok {
			if !replyargs.IsLeader{
				kv.mu.Unlock()
				time.Sleep(30 * time.Millisecond)
				kv.mu.Lock()
				continue
			}
			if replyargs.OK{
				// pack the shard info and send it to the state machine 
				//blocking call
				kv.mu.Unlock()
				kv.PutCommand(false, -1, -1, -1, ShardStateChange{
					ShardId: shardid,
					FromState: UPDATING,
					FromNum: currCfg.Num,
					NextState: SERVING,
					NextNum: currCfg.Num + 1, 
					Data: replyargs.Shard,
				})
				kv.mu.Lock()
				break
			}else{
				return // we deal this later.
			}
		}
	}
}

func (kv *ShardKV) checkShards() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	clusterCfgNum := kv.Config.Num

	//For debug only
	for _, shard := range kv.Shards {
		if shard.Cfg >= clusterCfgNum{
			continue // nothing to update
		}
		kv.print_once(1, "shards: %v", kv.Shards)
		defer kv.print_once(1, "shards: %v", kv.Shards)
		break
	}

	for shardid, shard := range kv.Shards {

		if shard.Cfg >= clusterCfgNum{
			continue // nothing to update
		}

		kv.mu.Unlock()
		currCfg := kv.query(shard.Cfg)
		nextCfg := kv.query(shard.Cfg + 1)
		assert(currCfg.Num == shard.Cfg)
		assert(nextCfg.Num == shard.Cfg + 1)
		kv.mu.Lock()

		switch shard.State{
		case UPDATING:
			// pull data from other servers, call GetShards
			// u i -> s i+1
			// checkShardUpdating does not gurantee updating successfully
			kv.checkShardUpdating(shardid, currCfg)
		case HANDING:
			// ask others, can I delete this now?
			// h i -> e i+1
			kv.checkShardDeleting(shardid, currCfg.Num, nextCfg)
		case EMPTY:
			// e i -> e i+1 if not serving in the next round
			// e i -> u i if needs to serve
			if nextCfg.Shards[shardid] == kv.gid{
				kv.mu.Unlock()
				kv.PutCommand(false, -1, -1, -1, ShardStateChange{
					ShardId: shardid,
					FromState: EMPTY,
					FromNum: currCfg.Num,
					NextState: UPDATING,
					NextNum: currCfg.Num, 
				})
				kv.mu.Lock()
			}else{
				kv.mu.Unlock()
				kv.PutCommand(false, -1, -1, -1, ShardStateChange{
					ShardId: shardid,
					FromState: EMPTY,
					FromNum: currCfg.Num,
					NextState: EMPTY,
					NextNum: currCfg.Num + 1, 
				})
				kv.mu.Lock()
			}
		case SERVING:
			// s i -> s i+1 continue serving if possible
			// s i -> h i   if not in the new cfg
			if nextCfg.Shards[shardid] == kv.gid {
				kv.mu.Unlock()
				kv.PutCommand(false, -1, -1, -1, ShardStateChange{
					ShardId: shardid,
					FromState: SERVING,
					FromNum: currCfg.Num,
					NextState: SERVING,
					NextNum: currCfg.Num + 1, 
				})
				kv.mu.Lock()
			}else{
				kv.mu.Unlock()
				kv.PutCommand(false, -1, -1, -1, ShardStateChange{
					ShardId: shardid,
					FromState: SERVING,
					FromNum: currCfg.Num,
					NextState: HANDING,
					NextNum: currCfg.Num, 
				})
				kv.mu.Lock()
			}
		}
	}
}


func (kv *ShardKV) applyShardStateChange(event ShardStateChange){

	old := kv.Shards[event.ShardId]
	if old.Cfg != event.FromNum || old.State != event.FromState{
		kv.print_once(3,"Shard Update Abort: Stale 2, %v, %v", old, event.FromNum)
		return 
	}
	//kv.print_once(1, "shards: %v", kv.Shards)
	assert(old.Cfg == event.FromNum)

	if event.Data.Cfg != 0{
		event.Data.State = old.State
		event.Data.Cfg = old.Cfg
		kv.Shards[event.ShardId] = &event.Data
		old = kv.Shards[event.ShardId]
	}
	
	old.Cfg = event.NextNum
	old.State = event.NextState
	if event.NextState == EMPTY{
		old.Data = nil  
		old.Applied = nil 
	}
	//kv.print_once(1, "shards: %v", kv.Shards)
}

func (kv *ShardKV) checkConfig() {
	kv.mu.Lock()
	currNum := kv.Config.Num
	kv.mu.Unlock()

	newcfg := kv.query(currNum + 1)
	if newcfg.Num <= currNum {
		return
	}
	kv.mu.Lock()
	DPrintf("%v -> %v", kv.Config.Shards,newcfg.Shards)
	kv.mu.Unlock()
	kv.PutCommand(false, -1, -1, -1, NewConfig{Config: newcfg})
}

func (kv *ShardKV) applyNewCfg(newcfg shardctrler.Config) {
	if newcfg.Num <= kv.Config.Num {
		return
	}
	kv.print_once(0, "shards: %v", kv.Shards)
	defer kv.print_once(0, "shards: %v", kv.Shards)

	assert(newcfg.Num == kv.Config.Num+1)
	for idx, shard := range kv.Shards {
		if newcfg.Shards[idx] == kv.gid {
			// I need to serve this shard
			if kv.Config.Shards[idx] == 0 {
				// e_0  -> s_1
				// nobody served before
				shard.State = SERVING
				shard.Cfg = newcfg.Num
			} else if shard.Cfg == newcfg.Num -1 {
				switch(shard.State){
				case SERVING:
					// s i  -> s i+1
					assert(kv.gid == kv.Config.Shards[idx] && shard.Cfg == newcfg.Num - 1)
					//continue serving
					shard.Cfg = newcfg.Num
				case EMPTY:
					// e_i  -> u_i
					//cfg stays the same
					shard.State = UPDATING
				}
			} 
		} else if shard.Cfg == newcfg.Num - 1{
			if shard.State == SERVING {
				// s_i -> h_i
				// I need to give this shard to others
				shard.State = HANDING
				assert(shard.Cfg == kv.Config.Num)
			}else if shard.State == EMPTY{ 
				// e_i  -> e_i+1
				shard.Cfg = newcfg.Num
			}
		}
	}
	
	kv.Config = newcfg
}

func (kv *ShardKV) isServing(shardId int) bool {
	return kv.Shards[shardId].State == SERVING && kv.Shards[shardId].Cfg == kv.Config.Num
}

func (kv *ShardKV) get(shardId int, key string) (bool, string) {
	if kv.isServing(shardId) {
		val, ok := kv.Shards[shardId].Data[key]
		if !ok {
			val = ""
		}
		return true, val
	}
	return false, ""
}

func (kv *ShardKV) putAppend(shardId int, key string, val_append string, action string) bool {
	if kv.isServing(shardId) {
		//DPrintf("PUT %v, %v", key, val_append )
		val, ok2 := kv.Shards[shardId].Data[key]
		if !ok2 {
			val = ""
		}
		if action == PutAction {
			kv.Shards[shardId].Data[key] = val_append
		} else if action == AppendAction {
			kv.Shards[shardId].Data[key] = val + val_append
		}

		return true
	}else{
		DPrintf("I DO NOT SERVE THIS SHARD")
	}
	return false
}

//func (kv *ShardKV) CleanShard(args *){}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _,isLeader := kv.rf.GetState(); !isLeader{
		return 
	}

	reply.IsLeader = true
	shard := kv.Shards[args.Shard]
	if shard.State == HANDING && shard.Cfg == args.CfgNum {
		reply.Shard = shard.copy()
		reply.OK = true
		DPrintf("Shard granted: %v", shard )
	} else {
		DPrintf("Shard not granted: Asks for %v, while I have %v", args.CfgNum ,shard )
		reply.OK = false
	}
}

// client -> server asking if I can delete my shard now?
func (kv *ShardKV) DelShard(args *DelShardArgs, reply *DelShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _,isLeader := kv.rf.GetState(); !isLeader{
		return 
	}

	reply.IsLeader = true
	shard := kv.Shards[args.Shard]
	if shard.Cfg > args.CfgNum {
		reply.OK = true
		DPrintf("Shard deletion granted: %v", shard.ShardID )
	} else {
		DPrintf("Shard deletion not granted: Asks for %v, while I have %v", args.CfgNum ,shard )
		reply.OK = false
	}
}

func (kv *ShardKV) query(num int) shardctrler.Config{
	// TODO: Add simple cache
	return kv.mck.Query(num)
}

func (kv *ShardKV) print_once(verbose int, format string, a ...interface{}){
	if !kv.isLeader{ return }
	s:=fmt.Sprintf("[%v] ", kv.gid)
	s+=format
	DPrintf(s, a)
}