package raft

import "fmt"

// syncLogEntries is run by the leader and sends AppendEntries to all followers
func (rf *Raft) syncLogEntries(pId int, term int) {
	// skip myself
	if pId == rf.me {
		return
	}
	rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("Start sending entries to S%d for term=%d ...", pId, term))

	// while I'm the leader, ask this peer to append the entry
	//  1- if pId is down; try again
	//  2- if pId's log is not up-to-date; try another index
	//  3- if pId's log is up-to-date; commit the log entry for current term if we got the majority of followers replicated the log entry
	for !rf.killed() {
		rf.mu.Lock()
		if rf.raftState != Leader || rf.currTerm > term {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("STOP sending entries; not a leader or obselete term, state=%v, termSent=%d, currTerm=%d", rf.raftState, term, rf.currTerm))
			rf.mu.Unlock()
			return
		}
		logLen := len(rf.logs)
		rf.mu.Unlock()

		rf.cond.L.Lock()
		if rf.nextIndex[pId] >= logLen {
			// there is no new logs to be added; just wait for someone to wake me up
			rf.cond.Wait()
			// someone asked to check if we need to sync entries
		}
		rf.cond.L.Unlock()

		rf.mu.Lock()
		if rf.killed() || rf.raftState != Leader || rf.currTerm != term {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("Woke up to when: 1-killed 2- I am not a leader or 3- the term is passed! term=%d, rf.currTerm=%d, rf.raftState=%d\n\tlen(logs)=%d\n\tnextIndex=%v\n\tmatchIndex=%v\n\tlogs=%v", term, rf.currTerm, rf.raftState, len(rf.logs), rf.nextIndex, rf.matchIndex, rf.logs))
			rf.mu.Unlock()
			return
		}

		nextIdx := rf.nextIndex[pId]
		prevLogIndex := nextIdx - 1               // index of the prev log entry on my log
		prevLogTerm := rf.logs[prevLogIndex].Term // the term of the prev log entry
		leaderCommit := rf.commitIndex            // the index of latest commited log entry
		leaderId := rf.me

		entries := append([]LogEntry{}, rf.logs[nextIdx:]...)

		rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("Sending the entry to S%d; leaderCommit=%d ( prevLogIndex=%d, prevLogTerm=%d )\n\tentries=%v", pId, leaderCommit, prevLogIndex, prevLogTerm, entries))
		rf.mu.Unlock()

		args := AppendEntriesArg{
			Term:         term,
			LeaderId:     leaderId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}
		reply := AppendEntriesReply{}

		ok := rf.peers[pId].Call(APPEND_ENTRIES_RPC, &args, &reply)
		if !ok {
			rf.mu.Lock()
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("S%d didn't respond to my call, (termSent=%d, rf.currTerm=%d) state=%v; try again ...", pId, term, rf.currTerm, rf.raftState))
			rf.mu.Unlock()
			continue
		}

		rf.mu.Lock()
		if rf.killed() || rf.raftState != Leader {
			rf.mu.Unlock()
			return
		}

		// 1- if the call was made during the previous terms; ignore the response and return
		if rf.currTerm > term {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(RETURN) Dropping the reply from S%d becuase it is old! (sentTerm=%d, rf.currTerm=%d)", pId, term, rf.currTerm))
			rf.mu.Unlock()
			return
		}

		// 2- if my term is behind the reply; turn to a follower and update my term
		if reply.Term > rf.currTerm {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(RETURN) Chaning to a Follower! S%d has a higher term than mine. (reply.Term=%d, rf.currTerm=%d)", pId, reply.Term, rf.currTerm))

			rf.raftState = Follower
			rf.persist()

			rf.currTerm = reply.Term
			rf.persist()

			rf.mu.Unlock()
			return
		}

		if args.Term != rf.currTerm {
			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("(OLD REPLY) args.Term=%d != rf.currTerm=%d! Comming from S%d ignoring the reply", args.Term, rf.currTerm, pId))
			rf.mu.Unlock()
			continue
		}

		if reply.Success {
			// the follower appended the entries for nextIndex
			rf.matchIndex[pId] = args.PrevLogIndex + len(entries)
			rf.nextIndex[pId] = rf.matchIndex[pId] + 1

			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("S%d appended the log entry; (sentTerm=%d, rf.currTerm=%d, rf.raftState=%d) (nextIndex[S%d]=%d, matchIndex[S%d]=%d) myLastLogIndex=%d\n\tlog=%v", pId, term, rf.currTerm, rf.raftState, pId, rf.nextIndex[pId], pId, rf.matchIndex[pId], len(rf.logs)-1, rf.logs))
		} else {
			// the follower rejected the entry!
			// Optimization for 2C by the leader
			// 	Case 1: leader doesn't have XTerm:
			// 		nextIndex = XIndex
			//  Case 2: leader has XTerm:
			// 		nextIndex = leader's last entry for XTerm
			//  Case 3: follower's log is too short:
			// 		nextIndex = XLen

			if reply.XIsShort {
				// case 3; we might get rejected again which we will ended up in case 1 or 2
				rf.nextIndex[pId] = reply.XLen
				rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("Follower (S%d) has a shorter log! nextIndex[S%d]=%d\n\tlen(logs)=%d\n\treply=%+v", pId, pId, reply.XLen, len(rf.logs), reply))
			} else {
				// check for case 1 and 2
				// check if the leader has the Xterm
				nextIdx = -1
				for idx := len(rf.logs) - 1; idx > 0; idx-- {
					logEntry := rf.logs[idx]
					if logEntry.Term == reply.XTerm {
						// case 2: leader has the term; idx is the index of the last entry
						nextIdx = idx
						break
					}
				}

				if nextIdx != -1 {
					// case 2: the leader has the term
					rf.nextIndex[pId] = nextIdx
					rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("Follower (S%d) has a mismatch Term! I found it in my log! nextIndex[S%d]=%d\n\tlen(logs)=%d\n\treply=%+v", pId, pId, nextIdx, len(rf.logs), reply))
				} else {
					// case 1: the leader doesn't have the term
					rf.nextIndex[pId] = reply.XIndex
					rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("Follower (S%d) has a mismatch Term! Couldn't find it my log! nextIndex[S%d]=%d\n\tlen(logs)=%d,\n\treply=%+v", pId, pId, reply.XIndex, len(rf.logs), reply))
				}

			}

			rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("S%d rejected the log entry; (nextIndex[S%d]=%d, matchIndex[S%d]=%d) myLastLogIndex=%d\n\t", pId, pId, rf.nextIndex[pId], pId, rf.matchIndex[pId], len(rf.logs)-1))
		}
		rf.mu.Unlock()

		if reply.Success {
			rf.commitEntries(term)
		}
	}
}

// commitEntries commits all uncommited entry in the leader's log
func (rf *Raft) commitEntries(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || term != rf.currTerm || rf.raftState != Leader {
		rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("SKIP Counting because I'm not the leader or this goroutine is obsolete, term=%d, rf.currTerm=%d, state=%d", term, rf.currTerm, rf.raftState))
		return
	}

	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	maxMatchIdx := -1
	majorityCount := make(map[int]int, 0)
	for _, mIdx := range rf.matchIndex {
		if mIdx == 0 || mIdx >= len(rf.logs) || rf.logs[mIdx].Term != rf.currTerm {
			continue
		}
		majorityCount[mIdx] += 1
		maxMatchIdx = Max(maxMatchIdx, mIdx)
	}

	majorityThrsh := len(rf.peers) / 2

	hIdxCommitted := -1
	for N := maxMatchIdx; N >= 0; N -= 1 {
		if cnt, ok := majorityCount[N]; ok && cnt >= majorityThrsh {
			hIdxCommitted = N
			break
		}
	}

	rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("Check if we should update rf.commitIndex:%d HIdxCommitted=%d\n\tcurrTerm=%d\n\trf.matchIndex=%v\n\tmajorityCount=%v", rf.commitIndex, hIdxCommitted, rf.currTerm, rf.matchIndex, majorityCount))

	if hIdxCommitted != -1 {
		rf.commitIndex = hIdxCommitted
		rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("Updated rf.commitIndex=%d\n\trf.matchIndex=%v\n\tmajorityCount=%d", rf.commitIndex, rf.matchIndex, majorityCount))
	}

	if term == rf.currTerm && rf.lastApplied < rf.commitIndex && rf.commitIndex < len(rf.logs) {
		rf.logger.Log(LogTopicCommittingEntriesApe, fmt.Sprintf("Leader is applying logs [from=%d, to=%d] ..., leaderId=%d, rf.commitIndex=%d\n\tlen(logs)=%d\n\tlogs=%v", rf.lastApplied+1, rf.commitIndex, rf.leaderId, rf.commitIndex, len(rf.logs), rf.logs))

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       rf.logs[i].Command,
				CommandIndex:  i,
				SnapshotValid: false,
				Snapshot:      []byte{},
				SnapshotTerm:  term,
				SnapshotIndex: 0,
			}
			rf.lastApplied += 1
		}

		rf.logger.Log(LogTopicCommittingEntriesApe, fmt.Sprintf("Leader is done applying log entries! APPLIED all indices up to=%d; (lastApplied=%d, commitIndex=%d)", rf.commitIndex, rf.lastApplied, rf.commitIndex))
	}
}
