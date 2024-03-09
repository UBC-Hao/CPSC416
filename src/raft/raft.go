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

type RaftState int

const (
	Follower = iota
	Candidate
	Leader
)

const (
	APPEND_ENTRIES_RPC = "Raft.AppendEntries"
	REQUEST_VOTE_RPC   = "Raft.RequestVote"
)

// LogEntry represents each log entry in the rf.logs array
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu   sync.Mutex // Lock to protect shared access to this peer's state
	lock sync.Mutex // Lock to protect the shared accessed during log replication
	cond *sync.Cond // Cond variable to synchronized the log replication go routines

	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	logger *Logger

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
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
	DPrintf(LOG3, rf.me, "Persist %v logs, %v Snapshot", len(rf.log), len(rf.snapshot))
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.SetSnapshot(index, snapshot)
	rf.persist()
}

// can only be called within mutex
func (rf *Raft) SetSnapshot(index int, snapshot []byte) bool {
	if rf.log[0].Index > index {
		DPrintf(LOG3, rf.me, "STALE SNAPSHOT %v APPEND! DISCARD ", index)
		return false
	}
	DPrintf(LOG3, rf.me, "SNAPSHOT %v, slen = %v , log len before = %v", index, len(rf.snapshot), len(rf.log))
	rf.snapshot = snapshot
	if len(rf.log)-1 >= index-rf.X {
		rf.log = rf.log[index-rf.X:]
		rf.X = index
		//rf.log[0].Command = nil
	} else {
		rf.log = []Log{{}}
		rf.X = index
	}
	DPrintf(LOG3, rf.me, "SNAPSHOT %v , slen=%v, log len after = %v", index, len(snapshot), len(rf.log))
	return true
}

// Invoked by leader to replicate log or send heartbeats
type AppendEntriesArgs struct {
	Term         int // leader's term
	Leaderid     int // so follower can redirect clients
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int // leader's commit index
}

type AppendEntriesReply struct {
	Term    int  //current term for leader to update itself
	Success bool //

	XTerm  int //:  term in the conflicting entry (if any)
	XIndex int //: index of first entry with that term (if any)
	XLen   int //log length
}

type AppendEntriesReplyHelper struct {
	reply *AppendEntriesReply
	// not transferred using RPC
	server          int
	prevLogIndex    int
	entriesMaxIndex int // maximum index in Entries used for updating nextIndex
}

// executed by the nodes reachable
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf(LOG3, rf.me, "<- AppendEntries(%v, %v)", args.Term, args.Leaderid)
	DPrintf(APPE, rf.me, "PlogIdx: %v, Entries(%v)", args.PrevLogIndex, args.Entries)
	//
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//if args.Term == rf.currentTerm && rf.state == LEADER {
	//	DPrintf(FATAL, rf.me, "FAIL: DUPLICATE LEADER")
	//	}

	if args.Term < rf.currentTerm {
		DPrintf(LOG2, rf.me, "STALE APPEND")
		//stale
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		if args.Term > rf.currentTerm {
			rf.resetTimer()
			rf.currentTerm = args.Term
			rf.ConvertTo(FOLLOWER)
			rf.votedFor = -1
		} else if args.Term == rf.currentTerm {
			// == case:  It might be a current term Candidate,  now a leader is born, stop asking for votes
			rf.resetTimer()
			rf.ConvertTo(FOLLOWER)
		}
		reply.Term = rf.currentTerm

		// reply false if log doesn't contain an entry at
		//    prevLogIndex whose term matches prevLogTerm
		if (rf.lastLogIndex() < args.PrevLogIndex) || (rf.getLog(args.PrevLogIndex) != nil && rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm) {
			reply.Success = false
			if rf.lastLogIndex() < args.PrevLogIndex {
				reply.XLen = len(rf.log) + rf.X
				reply.XTerm = -1
			} else {
				reply.XTerm = rf.getLog(args.PrevLogIndex).Term
				xindex := rf.getLog(args.PrevLogIndex).Index
				for rf.getLog(xindex) != nil && rf.getLog(xindex).Term == reply.XTerm {
					xindex -= 1
				}
				reply.XIndex = xindex + 1
			}
			if rf.lastLogIndex() < args.PrevLogIndex {
				DPrintf(LOG3, rf.me, "<- Append Fail, R1(%v,%v) ", rf.lastLogIndex(), args.PrevLogIndex)
			} else {
				DPrintf(LOG3, rf.me, "STAT: %v", rf)
				DPrintf(LOG3, rf.me, "<- Append Fail, R2(%v,%v,%v,%v,%v) ", args.PrevLogIndex, rf.getLog(args.PrevLogIndex).Term, args.PrevLogTerm, reply.XTerm, reply.XIndex)
			}

		} else {
			DPrintf(LOG3, rf.me, "<- Append Success")
			if len(args.Entries) != 0 {
				rf.extend(args.Entries) //this will check for conflicting!! if not , will not update!!
			}
			rf.checkValid()
			if args.LeaderCommit > rf.commitIndex {
				newCommitIdx := args.LeaderCommit
				lastIndex := rf.lastLogIndex()
				if lastIndex < rf.commitIndex {
					newCommitIdx = lastIndex
				}

				rf.commitIndex = newCommitIdx
				rf.cond.Signal()
			}
			reply.Success = true
		}

		//reply.Success = true
		//also change last recv
		rf.resetTimer()
		DPrintf(LOG2, rf.me, "Beats sent from %v", args.Leaderid)
		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) bool {
	reply := &AppendEntriesReply{}
	ok := rf.Call(server, "Raft.AppendEntries", args, reply)
	replyHelper := &AppendEntriesReplyHelper{
		reply: reply,
	}
	replyHelper.server = server
	replyHelper.prevLogIndex = args.PrevLogIndex
	replyHelper.entriesMaxIndex = 0
	if len(args.Entries) != 0 {
		replyHelper.entriesMaxIndex = args.Entries[len(args.Entries)-1].Index
	}
	if ok && !rf.killed() {
		rf.appendChan <- replyHelper
	}
	return ok
}

type InstallSnapshotArgs struct {
	Term              int // leader's term
	LeaderId          int
	LastIncludedIndex int
	LastIncludeTerm   int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if rf.SetSnapshot(args.LastIncludedIndex, args.Data) &&
		(!(rf.log[0].Term == args.LastIncludeTerm && rf.log[0].Index == args.LastIncludedIndex)) {
		rf.log = []Log{{Term: args.LastIncludeTerm, Index: args.LastIncludedIndex}}
		rf.persist()
	} // else do nothing, already truncated in rf.SetSnapshot
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) bool {
	reply := &InstallSnapshotReply{}
	ok := rf.Call(server, "Raft.InstallSnapshot", args, reply)
	if ok && !rf.killed() {
		// we simply handle the reply here
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			// leader update itself
			rf.currentTerm = reply.Term
			rf.ConvertTo(FOLLOWER)
			rf.votedFor = -1
			rf.persist()
			return ok
		}

		// installSnapshot successful, now we update nextIndex and matchIndex
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	}
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
}

// RequestVote called by Candidates to ask for the peer's vote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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
	ok := rf.Call(server, "Raft.RequestVote", args, reply)
	if ok && !rf.killed() {
		rf.voteChan <- &RequestVoteReplyHelper{
			reply:  *reply,
			server: server,
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.logs)              // the command will be appeared at this index on the leader's log
	term := int(rf.currTerm)           // the leader current term for this command
	isLeader := rf.raftState == Leader // if the server believes it's a leader

	// Your code here (2B).

	return index, term, isLeader
}

type AppendEntriesArg struct {
	Term         int        // leader's term
	LeaderId     int        // leader's id so that followers can redirect them
	PrevLogIndex int        // index of previous log entry preceeding the new one
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for HB, more than one for logs)
	LeaderCommit int        // leader's commitIndex
}
type AppendEntriesReply struct {
	XIsShort bool // if the follower's log is shorter
	XLen     int  // the length of the follower's log
	XTerm    int  // the conflicting term in the follower's log
	XIndex   int  // the index of the first entry of XTerm
	Term     int  // currTerm, for leader to update itself
	Success  bool // true if the follower contined the prevLogIndex and preLogTerm
}

// fillReplyX finds the term and its first index in the log for the follower!
// Optimizations required for Lab2C
// Follower:
//
//	XTerm:  term in the conflicting entry (if any)
//	XIndex: index of first entry with that term (if any)
//	XLen:   log length
func (rf *Raft) fillReplyX(reply *AppendEntriesReply, mismatchIdx int, isShorter bool) {
	reply.XIsShort = isShorter
	reply.XLen = len(rf.logs)

	reply.XIndex = mismatchIdx
	reply.XTerm = rf.logs[reply.XIndex].Term

	// find the index where we don't have XTerm
	for reply.XIndex >= 0 && rf.logs[reply.XIndex].Term == reply.XTerm {
		reply.XIndex -= 1
	}
	reply.XIndex += 1
}

// AppendEntries called by the leader either to send a HB or a log entry
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the leader's term lower than mine, I reject the the HB
	if args.Term < rf.currTerm {
		rf.logger.Log(LogTopicAppendEntryRpc, fmt.Sprintf("APE REJECTED: S%d claims to be the leader but has a lower term! (args.Term=%d rf.currTerm=%d)", args.LeaderId, args.Term, rf.currTerm))
		reply.Term = rf.currTerm
		reply.Success = false
		return
	}

	// the leader has a higher term, I update my term and turn into a follower
	if args.Term > rf.currTerm {
		rf.logger.Log(LogTopicAppendEntryRpc, fmt.Sprintf("S%d is the leader with higher term! Turning to a follower. (args.Term=%d rf.currTerm=%d) (leader=S%d)", args.LeaderId, args.Term, rf.currTerm, args.LeaderId))
		rf.raftState = Follower
		rf.currTerm = args.Term
		rf.persist()
	}

	// reset my HB variable and set the leader id for this term
	rf.heartbeat = true
	rf.leaderId = args.LeaderId

	// I reject the call either:
	//   1- my log doesn't have the prevLogIndex or
	//   2- if it does have the prevLogIndex, the term in my log is different
	if !(args.PrevLogIndex < len(rf.logs)) {
		reply.Term = rf.currTerm
		reply.Success = false

		reply.XIsShort = true
		reply.XLen = len(rf.logs)

		rf.logger.Log(LogTopicRejectAppendEntry, fmt.Sprintf("APE REJECTED: my log is shorter than S%d's log! (currTerm=%d, entries=%v) (args.PrevLogIndex=%d, args.PrevLogTerm=%d) (reply.XLen=%d, reply.XIndex=%d, reply.XTerm=%d) (lastLogIndex=%d)\n\tlog=%v", args.LeaderId, rf.currTerm, args.Entries, args.PrevLogIndex, args.PrevLogTerm, reply.XLen, reply.XIndex, reply.Term, len(rf.logs)-1, rf.logs))
		return
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currTerm
		reply.Success = false

		rf.fillReplyX(reply, args.PrevLogIndex, false)

		rf.logger.Log(LogTopicRejectAppendEntry, fmt.Sprintf("APE REJECTED: my prev entry has a mismatched term than S%d! (currTerm=%d, entries=%v) (args.PrevLogIndex=%d, args.PrevLogTerm=%d)  (lastLogIndex=%d) (reply=%+v)\n\tlog=%v", args.LeaderId, rf.currTerm, args.Entries, args.PrevLogIndex, args.PrevLogTerm, len(rf.logs)-1, reply, rf.logs))
		return
	}

	rf.logger.Log(LogTopicMatchPrevApe, fmt.Sprintf("MATCHED Prev Log Entry from S%d; args.PrevLogIndex=%d, args.PrevLogTerm=%d,\n\targs.Entries=%v\n\tlogs=%v", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, rf.logs))

	// finding the conflicting index
	// if it is a HB; conflictIdx is -1
	conflictIdx := 0
	for conflictIdx < len(args.Entries) {
		nextIdx := args.PrevLogIndex + conflictIdx + 1 // next entry in my log
		if nextIdx >= len(rf.logs) {
			// my log is ended
			rf.logger.Log(LogTopicTruncateLogApe, fmt.Sprintf("(LOG Conflict, Leader S%d) There is no entry in my log with index=%d! setting conflictIdx=%d\n\tlog=%v", args.LeaderId, nextIdx, conflictIdx, rf.logs))
			break
		} else if rf.logs[nextIdx].Term != args.Entries[conflictIdx].Term {
			// truncate the log
			rf.logger.Log(LogTopicTruncateLogApe, fmt.Sprintf("(LOG Conflict, Leader S%d) Index %d has a different term than args.Entries[%d]!\n\tShared up to index %d with args.Entries Truncating my log ... (rf.logs[%d].Term=%d, args.Entries[%d].Term=%d)\n\tnew_log=%v\n\tlog=%v\n\targ.Entries=%v", args.LeaderId, nextIdx, conflictIdx, nextIdx, nextIdx, rf.logs[nextIdx].Term, conflictIdx, args.Entries[conflictIdx].Term, rf.logs[:nextIdx], rf.logs, args.Entries))

			rf.logs = rf.logs[:nextIdx]
			rf.persist()
			break
		}
		conflictIdx += 1
	}

	// find which part of the entries have to be added
	rf.logger.Log(LogTopicAppendingEntryApe, fmt.Sprintf("Adding the following portion from args.Entries!\n\tnew_portion=%v\n\targs.Entries=%v", args.Entries[conflictIdx:], args.Entries))
	args.Entries = args.Entries[conflictIdx:] // the portion of the entries we should add to the log; might be nothing

	if len(args.Entries) > 0 {
		// append the new entries
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()

		rf.logger.Log(LogTopicAppendingEntryApe, fmt.Sprintf("Appended the entries to my log! (currTerm=%d)\n\targs.Entries=%v\n\tnew_log=%v", rf.currTerm, args.Entries, rf.logs))
	}

	// if the leader is ahead of me; commit all the entries we haven't commited yet
	if args.LeaderCommit > rf.commitIndex {
		rf.logger.Log(LogTopicUpdateCommitIdxApe, fmt.Sprintf("The leaderCommit(%d) is greater than rf.commitIndex(%d)! updating mine to the minimum(leaderCommit, my_last_log_idx=%d), term=%d", args.LeaderCommit, rf.commitIndex, len(rf.logs)-1, rf.currTerm))

		rf.commitIndex = Min(args.LeaderCommit, len(rf.logs)-1)
	}

	reply.Term = rf.currTerm
	reply.Success = true

	if rf.lastApplied+1 <= rf.commitIndex {
		rf.logger.Log(LogTopicCommittingEntriesApe, fmt.Sprintf("Uncommited Entries from Leader=S%d -> applying indexes [from=%d to=%d] to the SM\n\tlog=%v", rf.leaderId, rf.lastApplied+1, rf.commitIndex, rf.logs))

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			}
		}
		rf.lastApplied = rf.commitIndex
	} else {
		rf.logger.Log(LogTopicLogUpdateApe, fmt.Sprintf("Nothing to commit! leaderCommit=%d, commitIndex=%d, lastApplied=%d\n\tlog=%v", args.LeaderCommit, rf.commitIndex, rf.lastApplied, rf.logs))
	}

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
	rf.cond.Broadcast()

	rf.logger.Log(LogTopicElection, fmt.Sprintln("I got killed :("))
	time.Sleep(10 * time.Millisecond) // this is ugly; fix it for 2D
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// nonblocking, ConvertTo should only be called within mutex
func (rf *Raft) ConvertTo(newstate role) {
	DPrintf(SUPE, rf.me, " converted from %v to %v ", rf.state, newstate)
	switch newstate {
	case CANDIDATE:
		rf.resetTimer() // in case this node becomes a FOLLOWER immediately
		rf.voteRecv = make([]bool, rf.F*2+1)
		rf.state = CANDIDATE
		//start election
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.persist()
	case FOLLOWER:
		//rf.resetTimer()
		//rf.votedFor = -1
		rf.state = FOLLOWER
		// perisist of converting to follower is handled outside for efficiency
	case LEADER:
		rf.votedFor = rf.me
		rf.state = LEADER
		rf.nextIndex = make([]int, 2*rf.F+1)
		//initialize nextIndex to the last log index + 1 ( = len(rf.log))
		for i := 0; i < 2*rf.F+1; i++ {
			rf.nextIndex[i] = len(rf.log) + rf.X
		}
		rf.matchIndex = make([]int, 2*rf.F+1)
		rf.persist()
		//rf.SendHeartBeats() (not neccessary, ticker will be executed after this)
	}
}

// this is non-blocking, should only be called within mutex
func (rf *Raft) SendAllVoteReq() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && !rf.voteRecv[i] {
			send := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			go rf.sendRequestVote(i, send)
		}
	}
}

func (rf *Raft) buildSendAppendEntries(i int) {
	rf.lastSend[i] = time.Now()

	if rf.nextIndex[i] <= rf.X {
		// Leader does not have the Log because of snapshot
		send := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.log[0].Index,
			LastIncludeTerm:   rf.log[0].Term,
			Data:              rf.snapshot,
		}
		go rf.sendInstallSnapshot(i, send)
		return
	}

	entries := rf.getTail(rf.nextIndex[i] - 1)
	send := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		Leaderid:     rf.me,
		Entries:      entries,
		PrevLogIndex: rf.getLog(rf.nextIndex[i] - 1).Index, //rf.log[rf.nextIndex[i]-1].Index,
		PrevLogTerm:  rf.getLog(rf.nextIndex[i] - 1).Term,
		LeaderCommit: rf.commitIndex,
	}
	// This packet might be lost, we should not update nextIndex or matchIndex here
	go rf.sendAppendEntries(i, send)
}

// this is non-blocking, should only be called within mutex
func (rf *Raft) BroadcastAppendEntries(force bool) {
	deadline := time.Now().Add(-HB_INTERVAL)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && (rf.lastSend[i].Before(deadline) || force) {
			rf.buildSendAppendEntries(i)
		}
	}
}

func voteTimeout() time.Duration {
	ms := 100 + HB_INTERVAL_RAW + (rand.Int63() % (2 * HB_INTERVAL_RAW)) //(2-4) * HB_INTERVAL_RAW
	return time.Duration(ms) * time.Millisecond
}

// blocking function,
func (rf *Raft) gatherVote(term int) {
	num := 1 // including my self
	rndTime := voteTimeout()
	timeout := time.After(rndTime)
	//timeout2 := time.After(rndTime / 2) // to send votes
	for {
		select {
		case replyHelper := <-rf.voteChan:
			reply := replyHelper.reply
			rf.mu.Lock()

			if !(rf.state == CANDIDATE && rf.currentTerm == term) {
				// rf is no longer a candidate
				rf.mu.Unlock()
				return
			}

			if reply.Term < rf.currentTerm {
				//stale, do nothing
				rf.mu.Unlock()
				continue
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.ConvertTo(FOLLOWER)
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				return
			}

			if rf.voteRecv[replyHelper.server] {
				rf.mu.Unlock()
				continue
			}

			if reply.VoteGranted { // in case of dup reply
				num++
				if num >= rf.F+1 {
					//becomes the leader
					rf.ConvertTo(LEADER)
					rf.mu.Unlock()
					return
				}
			}
			rf.voteRecv[replyHelper.server] = true
			rf.mu.Unlock()
		/*case <-timeout2: //we have a second timeout in case of lost packets.
		rf.mu.Lock()
		//rf may not be a candidate any more, we need to check first
		if rf.state == CANDIDATE && rf.currentTerm == term {
			rf.SendAllVoteReq()
		}
		rf.mu.Unlock()*/
		case <-timeout:
			DPrintf(LOG3, rf.me, "Timeout on ASKING for votes")
			return
		}
	}
}

func (rf *Raft) tryLeaderCommit(n int) {
	before := rf.commitIndex
	for i := n; i > rf.commitIndex; i-- {
		if rf.getLog(i).Term != rf.currentTerm {
			break
		}
		if rf.tryCommitIDX(i) {
			break
		}
	}
	if rf.commitIndex != before {
		rf.cond.Signal()
		rf.BroadcastAppendEntries(true) // tell every node to commit
	}
}

// blocking function to apply logs after committing
func (rf *Raft) applyLogs() {
	for {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.cond.Wait()
		}
		DPrintf(LOG1, rf.me, "TRY TO APPLY LOGS (%v,%v,%v)", rf.commitIndex, rf.lastApplied, rf.log)
		maxApply := rf.commitIndex
		toCommit := []*Log{}

		if rf.lastApplied < rf.X {
			// This means we need to first apply snapshot
			msg := &ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.log[0].Term,
				SnapshotIndex: rf.log[0].Index,
			}
			rf.applySnapshot(msg)
			rf.lastApplied = rf.log[0].Index
		}
		for i := rf.lastApplied + 1; i <= maxApply; i++ {
			toCommit = append(toCommit, rf.getLog(i))
		}
		for _, v := range toCommit {
			rf.apply(*v)
		}
		rf.lastApplied = maxApply
		rf.mu.Unlock()
	}
}

// should only be called within mutex
func (rf *Raft) tryCommitIDX(n int) bool {
	nMatched := 0
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.matchIndex[i] >= n {
			nMatched += 1
		}
	}
	if nMatched >= rf.F {
		//already copied on majority, ok to commit
		//for i := rf.commitIndex + 1; i <= n; i++ {
		//	rf.commit(rf.log[i])
		//}
		rf.commitIndex = n
		return true
	} else {
		return false
	}
}

// owned by leader, blocking, can also be called by follower or candidates, but nothing happens if so
func (rf *Raft) handleAppendReply() {
	for replyHelper := range rf.appendChan {
		if rf.killed() {
			return
		}
		reply := replyHelper.reply
		rf.mu.Lock()
		if rf.state == LEADER {
			if reply.Term < rf.currentTerm { // stale reply
				rf.mu.Unlock()
				continue
			}
			server := replyHelper.server
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.resetTimer()
				rf.ConvertTo(FOLLOWER)
				rf.votedFor = -1
				rf.persist()
			} else { // reply.Term == rf.currentTerm
				success := reply.Success
				if !success {
					// match index is not found, we need to quickly resend and check again
					// rf.nextIndex[server] = replyHelper.prevLogIndex
					// replies might not be in the seq time order
					if reply.XTerm != -1 {
						ok, idx := hasTerm(rf.log, reply.XTerm)
						if !ok {
							rf.nextIndex[server] = reply.XIndex
						} else {
							// it might be possible like   [ 2 3 3 3 3 ]
							// while leader is             [ 3 3 3 3 3 ]
							DPrintf(LOG4, rf.me, "%v %v", idx, rf.log)
							rf.nextIndex[server] = idx
						}
					} else {
						//follower's log is too short
						rf.nextIndex[server] = reply.XLen
					}
					rf.buildSendAppendEntries(server) // quickly resend
				} else {
					// we do not want stale requests to move the matchIndex backward.
					if replyHelper.prevLogIndex > rf.matchIndex[server] {
						rf.matchIndex[server] = replyHelper.prevLogIndex
					}
					if replyHelper.entriesMaxIndex != 0 {
						// this means some entries are sent to the server and are successfully updated
						rf.nextIndex[server] = replyHelper.entriesMaxIndex + 1
						if replyHelper.entriesMaxIndex > rf.matchIndex[server] {
							rf.matchIndex[server] = replyHelper.entriesMaxIndex
						}
					}
					rf.tryLeaderCommit(rf.matchIndex[server])
				}
			}
		}
		rf.mu.Unlock()
	}
}

// can only be called within mutex
func (rf *Raft) resetTimer() {
	rf.lastHB = time.Now()
	rf.rndTime = voteTimeout()
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	logger, err := NewLogger(me)
	if err != nil {
		fmt.Println("Couldn't open the log file", err)

	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.handleAppendReply()
	go rf.applyLogs()

	return rf
}

// ----- definition for some custom variable
type role int

const (
	FOLLOWER        role = 0
	CANDIDATE       role = 1
	LEADER          role = 2
	HB_INTERVAL_RAW      = 150
	HB_INTERVAL          = HB_INTERVAL_RAW * time.Millisecond
	//HB_TIMEOUT       = 2 * HB_INTERVAL
)

func (r role) String() string {
	switch r {
	case FOLLOWER:
		return "FOLLOWER"
	case CANDIDATE:
		return "CANDIDATE"
	case LEADER:
		return "LEADER"
	}
	return "?"
}

// returns the last index in the log, can only be called within the mutex
func (rf *Raft) lastLogIndex() int {
	sz := len(rf.log)
	sz -= 1
	return rf.log[sz].Index
}

type Log struct {
	Command interface{}
	Term    int
	Index   int
}

func (log Log) String() string {
	return fmt.Sprintf("(%v,%v)", log.Index, log.Term)
}

// given a Log array of raft server rf, extend using B
func (rf *Raft) extend(B []Log) {
	if len(B) == 0 {
		return
	}
	//check for conflicting
	lastLogInB := B[len(B)-1]
	if rf.lastLogIndex() >= lastLogInB.Index {
		if rf.getLog(lastLogInB.Index) == nil { // This means our snapshot already has this log
			return
		}
		if rf.getLog(lastLogInB.Index).Term == lastLogInB.Term {
			return
		}
	}
	start := B[0].Index
	if start <= rf.X {
		if len(B) > 1 {
			rf.extend(B[1:]) //TODO: Can be imprvoed, actually very rarely fells into this
		}
		return
	}

	rf.log = rf.log[:start-rf.X]
	rf.log = append(rf.log, B...)
}

// given a copied Log array of raft rf, returns rf.log[index + 1:] as if log is complete
func (rf *Raft) getTail(index int) []Log {
	if (index + 1) > rf.lastLogIndex() {
		return []Log{}
	} else {
		//ret := make([]Log, len(A[index+1:]))
		//copy(ret, A[index+1:])
		return cpLogs(rf.log[index+1-rf.X:])
	}
}

func hasTerm(A []Log, term int) (bool, int) {
	has := false
	for i := len(A) - 1; i >= 0; i-- {
		if A[i].Term == term {
			has = true
		} else if A[i].Term < term {
			if has {
				return true, i + 1
			} else {
				return false, -1
			}
		}
	}
	return false, -1
}

// debug function for checking if the log is valid
func (rf *Raft) checkValid() {
	if !Debug {
		return
	}
	log := rf.log
	for i := 1; i < len(log); i++ {
		assert(log[i].Index == i+rf.X)
	}
}

func assert(a bool) {
	if !a {
		DPrintf(FATAL, 0, "ASSERTION BUG")
	}
}

func (rf *Raft) applySnapshot(msg *ApplyMsg) {

	DPrintf(LOG3, rf.me, "APPLY SNAPSHOT %v", msg.SnapshotIndex)
	rf.mu.Unlock()
	defer rf.mu.Lock()
	// we don't need to lock for transmit for channel here
	rf.applyMsgChan <- *msg
}

func (rf *Raft) apply(log Log) {
	msg := ApplyMsg{
		CommandValid: true,
		Command:      log.Command,
		CommandIndex: log.Index,
	}
	DPrintf(LOG3, rf.me, "APPLY LOG %v...", log)
	rf.mu.Unlock()
	defer rf.mu.Lock()
	// we don't need to lock for transmit for channel here
	rf.applyMsgChan <- msg
}

func (rf *Raft) getLog(index int) *Log {
	if index < rf.X {
		return nil
	}
	return &rf.log[index-rf.X]
}

func cpLogs(input []Log) []Log {
	ret := make([]Log, len(input))
	copy(ret, input)
	return ret
}

func (rf *Raft) Call(server int, svcMeth string, args interface{}, reply interface{}) bool {
	// Because RPC packets might be lost or timeout
	//, we try to resend the request multiple times, until HB interval
	start := time.Now()
	for time.Since(start) < HB_INTERVAL {
		ok := rf.peers[server].Call(svcMeth, args, reply)
		if ok {
			return ok
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}
