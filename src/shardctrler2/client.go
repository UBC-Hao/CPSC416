package shardctrler2

//
// Shardctrler clerk.
//

import "cpsc416/labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	UID int64
	RpcNum int 
	mu sync.Mutex
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
	// Your code here.
	ck.UID = nrand()
	ck.RpcNum = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &QueryArgs{
		Num: num,
		RpcNum: ck.RpcNum,
		UID: ck.UID,
	}
	ck.RpcNum += 1
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			retry:
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
			if ok && reply.Err == DUPLICATE{
				args.RpcNum = ck.RpcNum
				ck.RpcNum += 1
				goto retry
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := &JoinArgs{
		RpcNum: ck.RpcNum,
		UID: ck.UID,
	}
	ck.RpcNum += 1
	// Your code here.
	args.Servers = servers
	

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := &LeaveArgs{
		RpcNum: ck.RpcNum,
		UID: ck.UID,
	}
	ck.RpcNum += 1
	args.GIDs = gids
	

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := &MoveArgs{
		RpcNum: ck.RpcNum,
		UID: ck.UID,
	}
	ck.RpcNum += 1
	args.Shard = shard
	args.GID = gid
	

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
