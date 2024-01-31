package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "cpsc416/labgob"
	"cpsc416/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	state       role
	lastHB      time.Time
	//log[]

	F int // total number of peers is 2*F+ 1
	// voteChan is used to collect results
	voteChan   chan *RequestVoteReply
	appendChan chan *AppendEntriesReply
}

// for debug
func (rf *Raft) String() string {
	return fmt.Sprintf("(me %v, term: %v,votedFor: %v, state: %v)", rf.me, rf.currentTerm, rf.votedFor, rf.state)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// Invoked by leader to replicate log or send heartbeats
type AppendEntriesArgs struct {
	Term     int // leader's term
	Leaderid int // so follower can redirect clients
	//PrevLogIndex
	//PrevLogTerm
	Entries []byte
	//LeaderCommit
}

type AppendEntriesReply struct {
	Term    int  //current term for leader to update itself
	Success bool //
}

// executed by the nodes reachable
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf(APPE, rf.me, "<- AppendEntries(%v, %v)   stateBefore = %v", args.Term, args.Leaderid, rf)
	//
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		//stale
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.ConvertTo(FOLLOWER)
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		//also change last recv
		rf.lastHB = time.Now()
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) bool {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.appendChan <- reply
	}
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateId int // the candidate
	//LastLogIndex int
	//lastLogTerm
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int // current term for candidate to update
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf(REQV, rf.me, "<- RequestVote(%v,%v)  stateBefore = %v", args.Term, args.CandidateId, rf)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		//stale
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.ConvertTo(FOLLOWER)
		}

		reply.Term = rf.currentTerm
		if rf.votedFor != args.CandidateId && rf.votedFor != -1 {
			reply.VoteGranted = false
		} else {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) bool {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.voteChan <- reply
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// nonblocking, ConvertTo should only be called within mutex
func (rf *Raft) ConvertTo(newstate role) {
	DPrintf(LOG1, rf.me," converted from %v to %v ",rf.state ,newstate)
	switch newstate {
	case CANDIDATE:
		rf.state = CANDIDATE
		//start election
		rf.currentTerm += 1
		rf.votedFor = rf.me
	case FOLLOWER:
		rf.votedFor = -1
		rf.state = FOLLOWER
	case LEADER:
		rf.votedFor = -1
		rf.state = LEADER
		rf.SendHeartBeats()
	}
}

// this is non-blocking, should only be called within mutex
func (rf *Raft) SendAllVoteReq() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			send := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			go rf.sendRequestVote(i, send)
		}
	}
}

// this is non-blocking, should only be called within mutex
func (rf *Raft) SendHeartBeats() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			send := &AppendEntriesArgs{
				Term:  rf.currentTerm,
				Leaderid: rf.me,
			}
			go rf.sendAppendEntries(i, send)
		}
	}
}

func voteTimeout() time.Duration {
	ms := 325 + (rand.Int63() % 200) //325 - 525
	return time.Duration(ms) * time.Millisecond
}

// blocking function,
func (rf *Raft) gatherVote() {
	num := 1 // including my self
	timeout := time.After(voteTimeout())
	term_max := 0
	for {
		select {
		case reply := <-rf.voteChan:
			if reply.Term < rf.currentTerm {
				//stale, do nothing
				continue
			}
			if reply.Term > term_max {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.ConvertTo(FOLLOWER)
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				num++
				if num >= rf.F{
					//becomes the leader
					rf.mu.Lock()
					rf.ConvertTo(LEADER)
					rf.mu.Unlock()
				}
			}
		case <-timeout:
			return
		}
	}
}

// owned by leader, blocking, can also be called by follower or candidates, but nothing happens if so
func (rf *Raft) handleAppendReply() {
	for reply := range rf.appendChan {
		rf.mu.Lock()
		if rf.state == LEADER {
			/*if reply.Term < rf.currentTerm{
				//stale , discard

			}*/
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.ConvertTo(FOLLOWER)
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		// Leader, send heartbeat
		if rf.state == LEADER {
			rf.mu.Unlock()
			rf.SendHeartBeats()
			time.Sleep(HB_INTERVAL)
			continue
		}
		//follower, check if timedout on HB
		if rf.state == FOLLOWER {
			time.Sleep(voteTimeout())
			rndTime := voteTimeout()
			now := time.Now()
			if rf.lastHB.Add(rndTime).Before(now) && rf.votedFor != -1{
				// timeout on HB, leader might crashed, follower -> Candidate
				DPrintf(LOG1, rf.me, "TIMEOUT ON HB, F -> C.")
				rf.ConvertTo(CANDIDATE)
				rf.mu.Unlock()
			} else {
				// did not timeout on receiving HB, sleep and check again
				rf.mu.Unlock()
				time.Sleep(rf.lastHB.Add(rndTime).Sub(now))
				continue
			}
		}
		// candidate
		rf.mu.Lock()
		rf.SendAllVoteReq()
		rf.mu.Unlock()
		// Candidate, gather votes, will block for random time
		rf.gatherVote()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	DPrintf(INIT, me, "Init")
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.voteChan = make(chan *RequestVoteReply)
	rf.appendChan = make(chan *AppendEntriesReply)
	rf.F = len(peers) / 2

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.handleAppendReply()

	return rf
}

// ----- definition for some custom variable
type role int

const (
	FOLLOWER    role = 0
	CANDIDATE   role = 1
	LEADER      role = 2
	HB_INTERVAL      = 160 * time.Millisecond
	//HB_TIMEOUT       = 2 * HB_INTERVAL
)

func (r* role) String() string{
	switch(*r){
	case FOLLOWER:
		return "FOLLOWER"
	case CANDIDATE:
		return "CANDIDATE"
	case LEADER:
		return "LEADER"
	}
	return "?"
}
