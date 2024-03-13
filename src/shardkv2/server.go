package shardkv

import (
	"bytes"
	"cpsc416/labgob"
	"cpsc416/labrpc"
	"cpsc416/raft"
	"cpsc416/shardctrler2"
	"fmt"
	"sync/atomic"

	//"fmt"
	"sync"
	"time"
)

const NShards = shardctrler2.NShards

func (kv *ShardKV) String() string {
	return fmt.Sprintf("(%v, %v, %v)", kv.currCfg.Num, kv.migrateCfg.Num, kv.gid)
}

type ShardStatusOP struct {
	Num       int
	Shard     int
	NewStatus int
}

type MigrateOp struct {
	CfgNew shardctrler2.Config
}

type ShardsInfo struct {
	Shard int
	Data  map[string]string
}

type ShardsTransferOp struct {
	NextCfg shardctrler2.Config // next configuration num
	Applied map[int64]int
	Shards  []ShardsInfo
}

type Op struct {
	UID    int64
	RpcNum int
	Args   interface{}
}

type retOp struct {
	RetOP      Op
	rightShard bool
	CallBack   interface{}
}

const (
	Cleaned = 0
	Fixed   = 2 // shards that are fixed, disallow modification on this shard
	Own     = 1 // shards belong to me
)

type ShardStatus struct {
	Num    int // the configuration num that this shard serves
	Status int //
}

func (state ShardStatus) String() string {
	return fmt.Sprintf("[Num=%v, state=%v]", state.Num, state.Status)
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
	persister    *raft.Persister

	// Your definitions here.
	mck        *shardctrler2.Clerk // used to communicate with the clerk
	currCfg    shardctrler2.Config
	migrateCfg shardctrler2.Config

	replyChans      map[int]chan *retOp
	shardReplyChans map[int]chan ShardStatusOP
	applied         map[int64]int

	data       [shardctrler2.NShards]map[string]string
	dead       int32
	ShardStats [shardctrler2.NShards]ShardStatus
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if !kv.containShards(key2shard(args.Key)) || args.CfgNum > kv.migrateCfg.Num {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	ok, callback, rightShard := kv.PutCommand(args.UID, args.RpcNum, *args)

	if !rightShard {
		DPrintf2("Wrong Shards\n")
		reply.Err = ErrWrongGroup
		return
	} else {
		DPrintf2("Right Shards\n")
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
	kv.mu.Lock()

	//NOTE: This Line should come before contain Shards,
	// because this node may no longer be responsible for that task
	if ok := kv.checkApplied(args.UID, args.RpcNum); ok {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}

	if !kv.containShards(key2shard(args.Key)) || args.CfgNum > kv.migrateCfg.Num {
		contains := [10]bool{}
		for i := 0; i < 10; i++ {
			contains[i] = kv.containShards(i)
		}
		kv.mu.Unlock()
		DPrintf2("Not allowed: %v, %v", key2shard(args.Key), contains)
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	ok, _, rightShard := kv.PutCommand(args.UID, args.RpcNum, *args)
	if !rightShard {
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
			val, rightShard = sc.get(query.Key)
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
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) isDead() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// should only be called within mutex
func (kv *ShardKV) containShards(shard int) bool {
	return kv.ShardStats[shard].Num == kv.currCfg.Num && kv.ShardStats[shard].Status == Own
}

func (kv *ShardKV) putAppend(key string, val_append string, action string) bool {
	DPrintf("(%v, %v) \n", key, val_append)
	if !kv.containShards(key2shard(key)) {
		DPrintf("Not Allowed, wrong shards %v!\n", key)
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
	DPrintf("Put Append Res (%v, %v) (Shard: %v, GID: %v--%v,%v).\n", key, kv.data[key2shard(key)][key], key2shard(key), kv.gid, kv.currCfg.Num, kv.migrateCfg.Num)
	return true
}

func (kv *ShardKV) get(key string) (string, bool) {
	val, ok2 := kv.data[key2shard(key)][key]
	if !ok2 {
		if Debug {
			val = "NO SUCH KEY BUG!!"
		} else {
			val = ""
		}
	}

	DPrintf("Get (%v, %v) on (Gid: %v, Shard: %v, Num1: %v,Num2: %v)\n", key, val, kv.gid, key2shard(key), kv.currCfg.Num, kv.migrateCfg.Num)
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
	labgob.Register(shardctrler2.Config{})
	labgob.Register(MigrateOp{})
	labgob.Register(ShardsInfo{})
	labgob.Register(ShardsTransferOp{})
	labgob.Register(ShardStatusOP{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.replyChans = make(map[int]chan *retOp, 1000)
	kv.applied = make(map[int64]int, 1000)
	kv.shardReplyChans = make(map[int]chan ShardStatusOP, 1000)
	for i := 0; i < shardctrler2.NShards; i++ {
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
	go kv.watchMigration()

	return kv
}

func (kv *ShardKV) checkNoneFixed(){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i,shard:=range kv.ShardStats{
		if shard.Status == Fixed {
			
		}
	}
}

func (kv *ShardKV) watchFixed(){
	for {
		time.Sleep(30 * time.Millisecond)
		
	}
}

func (kv *ShardKV) readyToMigrate(nextCfg shardctrler2.Config) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if nextCfg.Num == kv.currCfg.Num{
		return true
	}

	for _,i := range(nextCfg.Shards){
		if i!=kv.gid && kv.currCfg.Shards[i]!=0{
			if kv.ShardStats[i].Status != Cleaned{
				DPrintf2("%v Not cleaned yet for %v \n", kv.gid, i)
				return false
			}
		}
	}
	return true
}

func (kv *ShardKV) watchMigration() {
	for {
		time.Sleep(30 * time.Millisecond)
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}
		kv.mu.Lock()
		isMigrating := kv.isMigrating()
		newCfg := kv.migrateCfg
		if isMigrating {
			//send rpc to ask for new shards
			// it could happend that the new shards do not know yet, so we need to try multiple times
			shardsNeeded, shardsNumOld := kv.shardsNeeded(newCfg)
			DPrintf("%v needShards: %v\n", kv.gid, shardsNeeded)
			oldcfg := kv.currCfg
			make_end := kv.make_end
			gid_self := kv.gid
			kv.mu.Unlock()

			shardsData := make(map[int]map[string]string)
			finalApplied := make(map[int64]int)
			for gid, shards := range shardsNeeded {
				if gid == 0 {
					continue
				}
				kv.mu.Lock()
				DPrintf2("%v waits for %v log from %v\n", kv.gid, newCfg.Num, gid)
				kv.mu.Unlock()
				_, data2, applied := acquireShards(kv, gid, shards, shardsNumOld[gid], oldcfg, make_end)
				DPrintf2("%v->%v: %v... %v\n", gid, gid_self, data2, shards)
				for k, v := range data2 {
					shardsData[k] = cloneMap(v)
				}
				for k, v := range applied {
					finalApplied[k] = v
				}
			}

			infos := make([]ShardsInfo, 0, 10)
			for k, v := range shardsData {
				info := ShardsInfo{
					Shard: k,
					Data:  v,
				}
				infos = append(infos, info)
			}
			//merge data
			//we must still be in migrating status

			//wait for my shards to be cleaned to continue
			op := ShardsTransferOp{
				Shards:  infos,
				NextCfg: newCfg,
				Applied: finalApplied,
			}
			kv.rf.Start(op) 
		}else{
			kv.mu.Unlock()
		}
	}
}

func (sc *ShardKV) watchConfig() {
	for {
		time.Sleep(100 * time.Millisecond)
		_, isLeader := sc.rf.GetState()
		if !isLeader {
			continue
		}
		mig := false
		curNum := -1
		sc.mu.Lock()
		mig = sc.isMigrating()
		curNum = sc.currCfg.Num
		sc.mu.Unlock()
		if mig {
			continue
		}

		config_new := sc.mck.Query(curNum + 1)
		sc.mu.Lock()
		cfg_cur := sc.currCfg
		currNum := sc.currCfg.Num
		nextNum := sc.migrateCfg.Num
		sc.mu.Unlock()

		if config_new.Num > currNum && config_new.Num > nextNum && sc.readyToMigrate(config_new){
			// updated, we prepare for update
			DPrintf2("%v note Config difference! \n", sc.gid)
			DPrintf2("%v %v -> %v %v \n", cfg_cur.Num, cfg_cur.Shards, config_new.Num, config_new.Shards)
			sc.rf.Start(MigrateOp{
				CfgNew: config_new,
			})
		}
	}
}

func (sc *ShardKV) handleNewCfg(newCfg shardctrler2.Config) {

	//DPrintf("Config Migrate operation\n")

	if sc.currCfg.Num >= newCfg.Num || sc.migrateCfg.Num >= newCfg.Num {
		DPrintf("Stale CFG update\n")
		//stale , do nothing
		return
	} else {
		// migrate start
		sc.updateShardStatus(sc.currCfg, newCfg)
		DPrintf("Migrate Log Detected %v\n", sc)
	}
}

func ackShards(sc *ShardKV, oldCfg shardctrler2.Config, shards []int) {
	// tell thoes servers I have got the shards I need, ok to modify shard status
	for _, shard := range shards {
		gid_send := oldCfg.Shards[shard]
	end1:
		for gid_send != sc.gid {
			DPrintf2("Trying to send ACK!..\n")
			servers, ok := oldCfg.Groups[gid_send]
			if ok {
				for si := 0; si < len(servers); si++ {
					//do I still need to send?
					/*sc.mu.Lock()
					if sc.ShardStats[shard].Num == oldCfg.Num{}
					sc.mu.Unlock()*/

					srv := sc.make_end(servers[si])
					args := ACKShardsArgs{
						Shard: shard,
						Num:   oldCfg.Num,
					}
					var reply ACKShardsReply
					ok := srv.Call("ShardKV.AckShards", &args, &reply)
					if ok && reply.OK {
						DPrintf2("%v ACK SEND Success for %v\n", sc.gid, shard)
						break end1
					}
					time.Sleep(20 * time.Millisecond)
				}
			} else {
				DPrintf2("BUG!!! Something's not right\n")
			}
		}
	}
}

func (sc *ShardKV) AckShards(args *ACKShardsArgs, reply *ACKShardsReply) {
	sc.mu.Lock()
	if sc.ShardStats[args.Shard].Num > args.Num || (sc.ShardStats[args.Shard].Num == args.Num && sc.ShardStats[args.Shard].Status == Cleaned) {
		// This is a stale request
		reply.OK = true
		sc.mu.Unlock()
		return
	}

	reply.OK = false
	if /*sc.ShardStats[args.Shard].Status == Fixed &&*/ sc.ShardStats[args.Shard].Num <= args.Num {
		// we must notify all nodes in the cluster to update this shardStatus
		reply.OK = true
		retChan := make(chan ShardStatusOP)
		sendOP := ShardStatusOP{
			Num:       args.Num,
			Shard:     args.Shard,
			NewStatus: Cleaned,
		}
		index, _, isLeader := sc.rf.Start(sendOP)
		sc.shardReplyChans[index] = retChan
		sc.mu.Unlock()

		if !isLeader {
			reply.OK = false
		} else {
			op2 := <-retChan
			if op2 == sendOP {
				reply.OK = true
			} else {
				reply.OK = false
			}
		}
		return
	}
	sc.mu.Unlock()
}

func acquireShards(sc *ShardKV, remote_gid int, shards []int, shardsNum []int, oldCfg shardctrler2.Config, make_end func(string) *labrpc.ClientEnd) (bool, map[int]map[string]string, map[int64]int) {
	for {
		servers, ok := oldCfg.Groups[remote_gid]
		sc.mu.Lock()
		if sc.currCfg.Num > oldCfg.Num {
			sc.mu.Unlock()
			return true, nil, nil
		}
		sc.mu.Unlock()

		DPrintf("ACQ Trying.....%v\n", remote_gid)
		if ok {
			for si := 0; si < len(servers); si++ {
				srv := make_end(servers[si])
				args := RequestShardsArgs{
					Shards:       shards,
					ShardsOldNum: shardsNum, // the migrated num
					Me:           sc.gid,
				}
				var reply RequestShardsReply
				ok := srv.Call("ShardKV.GetShards", &args, &reply)
				if ok && reply.IsOk {
					return false, reply.Data, reply.Applied
				}
				time.Sleep(20 * time.Millisecond)
			}
			DPrintf2("Acquire Shards Fail. Retry....\n")
		} else {
			DPrintf("BUG!!! Something's not right")
		}
	}
}

func (sc *ShardKV) isMigrating() bool {
	return sc.currCfg.Num < sc.migrateCfg.Num
}

func (sc *ShardKV) GetShards(args *RequestShardsArgs, reply *RequestShardsReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// we can only return if one of the following holds:
	//       (1) This state has curr state num bigger than the request
	//       (2) This state is the same as the requested one, and this node is in migrate state
	reply.IsOk = false
	if sc.isDead() {
		return
	}

	for i, shard := range args.Shards {
		if sc.ShardStats[shard].Num >= args.ShardsOldNum[i] && sc.ShardStats[shard].Status == Fixed {
			DPrintf2("on %v, Shard %v OK to send out\n", sc.gid, shard)
		} else {
			reply.IsOk = false
			DPrintf2("%v Declined %v: Shard %v,  %v not ready for %v\n", sc.gid, args.Me, shard, sc.ShardStats[shard], args.ShardsOldNum[i])
			return
		}
	}

	reply.IsOk = true
	reply.Data = make(map[int]map[string]string)
	for _, shard := range args.Shards {
		reply.Data[shard] = cloneMap(sc.data[shard])
	}
	reply.Applied = make(map[int64]int)
	for k, v := range sc.applied {
		for _, shard := range args.Shards {
			if k%10 == int64(shard) {
				reply.Applied[k] = v
			}
		}
	}

	DPrintf2("%v Give out shards: %v, %v, %v\n", sc.gid, sc.currCfg.Num, sc.isMigrating(), reply.Data)
}

// return a map,  shards[]
func (sc *ShardKV) shardsNeeded(migrateCfg shardctrler2.Config) (map[int][]int, map[int][]int) {
	needMap := make(map[int][]int) // gid to list of int( shards)
	shardsNumMap := make(map[int][]int)
	for i := 0; i < shardctrler2.NShards; i++ {
		if migrateCfg.Shards[i] == sc.gid && sc.gid != sc.currCfg.Shards[i] && sc.currCfg.Num < migrateCfg.Num {
			// need this new shard from gid
			array, ok := needMap[sc.currCfg.Shards[i]]
			if !ok {
				array = make([]int, 0)
			}
			array = append(array, i)
			needMap[sc.currCfg.Shards[i]] = array

			array2, ok2 := shardsNumMap[sc.currCfg.Shards[i]]
			if !ok2 {
				array2 = make([]int, 0)
			}
			array2 = append(array2, sc.ShardStats[i].Num)
			shardsNumMap[sc.currCfg.Shards[i]] = array2
		}
	}
	return needMap, shardsNumMap
}

func (sc *ShardKV) handleStatusChange(index int, op ShardStatusOP) {
	if sc.ShardStats[op.Shard].Num == op.Num {
		sc.ShardStats[op.Shard].Status = op.NewStatus
	}
	if shard_chan, ok := sc.shardReplyChans[index]; ok {
		DPrintf2("Trying to use channel\n")
		shard_chan <- op
		DPrintf2("End using to use channel\n")
		close(shard_chan)

		delete(sc.shardReplyChans, index)
	}
}

func (sc *ShardKV) handleTransfer(nextCfg shardctrler2.Config, infos []ShardsInfo, applied map[int64]int) {

	//make sure it's not stale
	if sc.currCfg.Num >= nextCfg.Num {
		return
	}
	shards := make([]int, 0, 10)
	for _, info := range infos {
		sc.data[info.Shard] = cloneMap(info.Data)
		sc.ShardStats[info.Shard].Num = nextCfg.Num
		sc.ShardStats[info.Shard].Status = Own
		shards = append(shards, info.Shard)
	}

	for k, v := range applied {
		cmp, ok := sc.applied[k]
		if !ok {
			cmp = 0
		}
		if cmp < v {
			cmp = v
		}
		sc.applied[k] = v
	}
	//oldCfg := sc.currCfg
	//update the current config to the new one
	sc.currCfg = nextCfg
	DPrintf2("Transfer success for %v to %v\n", sc.gid, nextCfg.Num)
	/*go func() {
		ackShards(sc, oldCfg, shards)
	}()*/
}

func (sc *ShardKV) handleApplyMsg() {
	for applyMsg := range sc.applyCh {

		if applyMsg.SnapshotValid {
			//This is a snapshot
			sc.readFromSnapshot(applyMsg.Snapshot)
			continue
		}

		//DPrintf("Handle Apply MSG.\n")

		cmd, valid := applyMsg.Command.(Op)
		if !valid {
			sc.mu.Lock()
			if newCfg, isNewCfg := applyMsg.Command.(MigrateOp); isNewCfg {
				sc.handleNewCfg(newCfg.CfgNew)
			}

			if transferOp, isTransfer := applyMsg.Command.(ShardsTransferOp); isTransfer {
				sc.handleTransfer(transferOp.NextCfg, transferOp.Shards, transferOp.Applied)
			}

			if statusOp, isStatus := applyMsg.Command.(ShardStatusOP); isStatus {
				sc.handleStatusChange(applyMsg.CommandIndex, statusOp)
			}
			//snapshot to save space
			if sc.maxraftstate != -1 && sc.persister.RaftStateSize() >= sc.maxraftstate*9/10 {
				sc.dumpCurrentState(applyMsg.CommandIndex)
			}
			sc.mu.Unlock()
			continue
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
				val, rightShard = sc.get(query.Key)
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
			if retCmd.CallBack == nil {
				//	fmt.Println("Impossible?!")
			}
		}
		if retCmd.rightShard {
			sc.setApplied(cmd.UID, cmd.RpcNum)
		}

		if sc.maxraftstate != -1 && sc.persister.RaftStateSize() >= sc.maxraftstate*9/10 {
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
	e.Encode(kv.currCfg)
	e.Encode(kv.migrateCfg)
	e.Encode(kv.ShardStats)
	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
	DPrintf2("%v take snapshot %v, %v\n", kv.gid, kv.currCfg.Num, kv.migrateCfg.Num)
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
	var cfg shardctrler2.Config
	var cfg2 shardctrler2.Config
	var ShardStats [NShards]ShardStatus
	if d.Decode(&applied) != nil ||
		d.Decode(&data_kv) != nil ||
		d.Decode(&cfg) != nil ||
		d.Decode(&cfg2) != nil ||
		d.Decode(&ShardStats) != nil {
		//log.Print("FAIL")
	} else {
		kv.applied = applied
		kv.data = data_kv
		kv.currCfg = cfg
		kv.migrateCfg = cfg2
		kv.ShardStats = ShardStats
		DPrintf2("%v read from snap shot %v, %v\n", kv.gid, kv.currCfg.Num, kv.migrateCfg.Num)
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

// helper function
func cloneMap(from map[string]string) map[string]string {
	retMap := make(map[string]string)
	for k, v := range from {
		retMap[k] = v
	}
	return retMap
}

func (kv *ShardKV) updateShardStatus(from shardctrler2.Config, to shardctrler2.Config) {
	if kv.currCfg.Num != from.Num {
		DPrintf2("Stale Update Status\n")
		return
	}
	kv.migrateCfg = to
	gid := kv.gid
	for i := 0; i < NShards; i++ {
		if (from.Shards[i] == gid || from.Shards[i] == 0) && to.Shards[i] == gid {
			kv.ShardStats[i].Num = to.Num
			kv.ShardStats[i].Status = Own
		} else if from.Shards[i] == gid && to.Shards[i] != gid {
			kv.ShardStats[i].Status = Fixed
		}
	}
}
