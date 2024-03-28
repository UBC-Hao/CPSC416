package raft

import "fmt"

// commitEntries commits all uncommited entry in the leader's log
func (rf *Raft) commitEntries(term int) {
	if rf.killed() || term != rf.currTerm || rf.raftState != Leader {
		rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("SKIP Counting because I'm not the leader or this goroutine is obsolete, term=%d, rf.currTerm=%d, state=%d", term, rf.currTerm, rf.raftState))
		return
	}

	maxMatchIdx := -1
	majorityCount := make(map[int]int, 0)
	for _, mIdx := range rf.matchIndex {
		if adjMIdx := mIdx - rf.XIndex - 1; adjMIdx < 0 || rf.logs[adjMIdx].Term != rf.currTerm {
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
		rf.logger.Log(LogTopicSyncEntries, fmt.Sprintf("UPDATED rf.commitIndex=%d\n\trf.matchIndex=%v\n\tmajorityCount=%d", rf.commitIndex, rf.matchIndex, majorityCount))
	}
}
