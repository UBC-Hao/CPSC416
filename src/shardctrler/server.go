package shardctrler


import "cpsc416/raft"
import "cpsc416/labrpc"
import (
	"fmt"
	"sort"
	"sync"
	"time"
)
import "cpsc416/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs    []Config // indexed by config num
	replyChans map[int]chan *retOp
	applied    map[int64]int
}

type Op struct {
	UID    int64
	RpcNum int
	Args   interface{}
}

type retOp struct {
	RetOP    Op
	CallBack interface{}
}

// return isok, and isleader
func (sc *ShardCtrler) PutCommand(UID int64, rpcnum int, args interface{}) (bool, interface{}) {
	sc.mu.Lock()
	if ok := sc.checkApplied(UID, rpcnum); ok {
		var callback interface{}
		if query, isquery := args.(QueryArgs); isquery {
			val := sc.query(query.Num)
			callback = val
		}
		sc.mu.Unlock()
		// already applied
		return true, callback
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

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	ok, _ := sc.PutCommand(args.UID, args.RpcNum, *args)
	reply.WrongLeader = !ok
	reply.Err = OK
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	ok, _ := sc.PutCommand(args.UID, args.RpcNum, *args)
	reply.WrongLeader = !ok
	reply.Err = OK
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	ok, _ := sc.PutCommand(args.UID, args.RpcNum, *args)
	reply.WrongLeader = !ok
	reply.Err = OK
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	ok, callback := sc.PutCommand(args.UID, args.RpcNum, *args)
	reply.WrongLeader = !ok
	reply.Err = OK
	if callback != nil {
		cfg, _ := callback.(Config)
		reply.Config = cfg
	}
}

func (sc *ShardCtrler) config() *Config {
	if len(sc.configs) == 0 {
		// no config
		return nil // should not happen, intialize
	}
	return &sc.configs[len(sc.configs)-1]
}

// clone and increment the latest log
func (oldcfg *Config) cloneConfig() *Config {
	shardsNew := [NShards]int{}
	copy(shardsNew[:], oldcfg.Shards[:])
	groupsNew := make(map[int][]string, 200)
	for k, v := range oldcfg.Groups {
		groupsNew[k] = make([]string, len(v))
		copy(groupsNew[k], v)
	}
	cfg := &Config{
		Num:    oldcfg.Num + 1,
		Shards: shardsNew,
		Groups: groupsNew,
	}
	return cfg
}

func (cfg *Config) String() string {
	return fmt.Sprintf("config: %v", cfg.Shards)
}

// balance config, should only be called within mutex
func (sc *ShardCtrler) balance(cfg *Config) {
	//fmt.Printf("Before: %v \n", cfg)
	if NShards < len(cfg.Groups) {
		//something's wrong
		//fmt.Println("Nshards < num of groups")
	}
	//shard_per_group := NShards / len(cfg.Groups)

	for {
		//counts[i] = number of shards that this group take care of
		counts := make(map[int]int, 15)
		for k := range cfg.Groups {
			counts[k] = 0
		}

		emptySlots := 0
		empty_idx := 0
		//there may be gid not in cfg.Shards, default_gid will be used
		max_gid, max_count := -1, -1
		min_gid, min_count := -1, 10000
		// we find the maximum and the minimum
		for i := 0; i < NShards; i++ {
			gid := cfg.Shards[i]
			if gid == 0 {
				emptySlots += 1
				empty_idx = i
				continue
			}
			counts[gid] += 1
		}

		sorted_gids := make([]int, 0, len(cfg.Groups))
		for k := range cfg.Groups {
			sorted_gids = append(sorted_gids, k)
		}
		sort.Ints(sorted_gids)

		for i := 0; i < len(sorted_gids); i++ {
			gid := sorted_gids[i]
			if counts[gid] < min_count {
				min_count, min_gid = counts[gid], gid
			}
			if counts[gid] > max_count {
				max_count, max_gid = counts[gid], gid
			}
		}

		if min_gid == -1 {
			//no groups
			return
		}

		if emptySlots == 0 {
			if max_count <= min_count+1 {
				//balanced already
				//fmt.Printf("After: %v \n", cfg)
				return
			}
		}

		if emptySlots > 0 {
			// we let the min take one of the empty slot
			cfg.Shards[empty_idx] = min_gid
			continue
		}

		// now we have emptySlots == 0, but max_count >= min_count + 2
		// move one from max to min
		max_idx := -1
		for i := 0; i < NShards; i++ {
			if cfg.Shards[i] == max_gid {
				max_idx = i
				break
			}
		}
		cfg.Shards[max_idx] = min_gid
	}
}

// should only be called within mutex
func (sc *ShardCtrler) join(Servers map[int][]string) {
	newCfg := sc.config().cloneConfig()
	mergeMap(newCfg.Groups, Servers)
	sc.balance(newCfg)
	sc.configs = append(sc.configs, *newCfg)
}

func (sc *ShardCtrler) leave(GIDs []int) {
	newCfg := sc.config().cloneConfig()
	for _, gid := range GIDs {
		delete(newCfg.Groups, gid)
		for i := 0; i < NShards; i++ {
			if newCfg.Shards[i] == gid {
				newCfg.Shards[i] = 0
			}
		}
	}
	sc.balance(newCfg)
	sc.configs = append(sc.configs, *newCfg)
}

func (sc *ShardCtrler) move(Shard int, GID int) {
	newCfg := sc.config().cloneConfig()
	newCfg.Shards[Shard] = GID
	sc.configs = append(sc.configs, *newCfg)
}

func (sc *ShardCtrler) query(num int) Config {
	if num == -1 || num >= sc.config().Num {
		return *sc.config()
	} else {
		return sc.configs[num]
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) handleApplyMsg() {
	for applyMsg := range sc.applyCh {

		if applyMsg.SnapshotValid {
			//This is a snapshot
			//Ignore
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
			//DPrintf(LOG2, kv.me, "Double Put or Append in ApplyChan")
			// tell the client the result, may not be ok
			opChan, ok := sc.replyChans[applyMsg.CommandIndex] //rare
			if query, isquery := cmd.Args.(QueryArgs); isquery {
				retCmd.CallBack = sc.query(query.Num)
			}
			if ok {
				opChan <- &retCmd
			}
			//already applied
			sc.mu.Unlock()
			continue
		}

		//take action
		if join, ok := cmd.Args.(JoinArgs); ok {
			sc.join(join.Servers)
		}

		if leave, ok := cmd.Args.(LeaveArgs); ok {
			sc.leave(leave.GIDs)
		}

		if move, ok := cmd.Args.(MoveArgs); ok {
			sc.move(move.Shard, move.GID)
		}

		if query, ok := cmd.Args.(QueryArgs); ok {
			retCmd.CallBack = sc.query(query.Num)
		}

		sc.setApplied(cmd.UID, cmd.RpcNum)

		// tell the client this is OK
		opChan, ok := sc.replyChans[applyMsg.CommandIndex]
		if ok {
			opChan <- &retCmd
		}
		sc.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(retOp{})
	labgob.Register(QueryArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(MoveArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.replyChans = make(map[int]chan *retOp, 1000)
	sc.applied = make(map[int64]int, 1000)
	// Your code here.
	go sc.leaderShipCheck()
	go sc.handleApplyMsg()
	return sc
}

// helper functions
func mergeMap(dst map[int][]string, from map[int][]string) {
	for k, v := range from {
		dst[k] = v
	}
}

func (kv *ShardCtrler) checkApplied(UID int64, rpcnum int) bool {
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

// called out side of  mutex, used when first step down as a follower
func (kv *ShardCtrler) flushChans(except int) {
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

func (kv *ShardCtrler) leaderShipCheck() {
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

func (kv *ShardCtrler) setApplied(UID int64, rpcnum int) {
	val, ok := kv.applied[UID]
	if !ok {
		val = 0
	}
	if val < rpcnum {
		kv.applied[UID] = rpcnum
	}
}

