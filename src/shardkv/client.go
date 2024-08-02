package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"cpsc416/labrpc"
	"cpsc416/shardctrler"
	"crypto/rand"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"
)
var (
	once sync.Once 
)


func (ck *Clerk) CheckPeriodically() {
	go func() {
		for {
			next:
			time.Sleep( 30 * time.Second)
			cfg := ck.sm.Query(-1)
			cfgStrs := make(map[int]string)
			// for loop in ck.config.Groups
			maxdata := 0
			maxgid := 0
			maxshardinmaxgid := 0
			mingid := 0
			//minshardinmingid := 0
			mindata := int(1e9)
			for gid := range cfg.Groups {
				cfgStrs[gid] = ck.GetForce(gid)
				str := cfgStrs[gid]
				if str == "unavailable" {
					// still transfering data, do this later
					goto next
				}
				if len(str) == 0 {
					// no data, do this later
					goto next
				}
				// get the length of the data this shard responsible for
				// [1:2:3],[4:5:6]
				arr := strings.Split(str, ",")
				sums := 0
				maxshard := 0
				maxval := 0
				//minshard := 0
				minval := int(1e9)
				for _, v := range arr {
					arr2 := strings.Split(v, ":")
					if val,_ :=strconv.Atoi(arr2[1]); val != cfg.Num {
						goto next // not the latest configuration
					}
					val2,_ := strconv.Atoi(arr2[2])
					sums += val2
					if val2 > maxval {
						maxval = val2
						maxshard,_ = strconv.Atoi(arr2[0])
					}
					if val2 < minval {
						minval = val2
					//	minshard,_ = strconv.Atoi(arr2[0])
					}
				}
				if sums > maxdata {
					maxdata = sums
					maxgid = gid
					maxshardinmaxgid = maxshard
				}
				if sums < mindata {
					mindata = sums
					mingid = gid
					//minshardinmingid = minshard
				}
			}
			// now let's balance the data
			if maxgid != mingid {
				if maxdata > 2 * mindata {
					ck.sm.Move(maxshardinmaxgid, mingid)
				}
			}
		}
	}()
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mu     sync.Mutex
	UID    int64
	RpcNum int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.UID = nrand()
	ck.RpcNum = 1
	// You'll have to add code here.
	once.Do(func() {
		ck.CheckPeriodically()
	})
	return ck
}


// used to fetch information of the clusers gid.
func (ck *Clerk) GetForce(gid int) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{}
	key := "status"
	args.Key = key
	args.Shard = key2shard(key)
	
	for {
		args.UID = ck.UID
		args.RpcNum = ck.RpcNum
		ck.RpcNum += 1

		//shard := key2shard(key)
		//gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
			retry:
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && reply.Err == ErrDup {
					//duplicate request, send the get request again.
					args.RpcNum = ck.RpcNum
					ck.RpcNum += 1
					goto retry
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					//DPrintf("Get %v, %v", key,reply.Value)
					return reply.Value
				}
				// not a leader, continue searching
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}


// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{}
	args.Key = key
	args.Shard = key2shard(key)
	
	for {
		args.UID = ck.UID
		args.RpcNum = ck.RpcNum
		ck.RpcNum += 1

		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
			retry:
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && reply.Err == ErrDup {
					//duplicate request, send the get request again.
					args.RpcNum = ck.RpcNum
					ck.RpcNum += 1
					goto retry
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					//DPrintf("Get %v, %v", key,reply.Value)
					return reply.Value
				}
				// not a leader, continue searching
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	//DPrintf("Send Put %v , %v", key, value )

	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.RpcNum = ck.RpcNum
	args.Shard = key2shard(key)
	args.UID = ck.UID
	ck.RpcNum += 1

	// c -> s1 on cluster 1 : s1 updated succesfully but failed to reply, crashed, all server in the cluster 1 down.
	// configuration changed
	// c -> s2 on cluster 2: should reply OK due to duplicate request

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrDup) {
					//DPrintf("PUT Success %v, %v", key, value )
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
