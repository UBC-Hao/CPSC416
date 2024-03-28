package raft

import "fmt"

// sendInstallSnapshot sends the leader's snapshot to a follower; should be protected by the raft's lock
func (rf *Raft) sendInstallSnapshot(pId int) {
	args := &InstallSnapshotArgs{
		Term:              rf.currTerm,
		LeaderId:          rf.leaderId,
		LastIncludedIndex: rf.XIndex,
		LastIncludedTerm:  rf.XTerm,
		Offset:            0,
		Data:              rf.snapshot,
		Done:              true,
	}

	go func(pId int) {

		rf.logger.Log(LogTopicSendInstallSnapshot, fmt.Sprintf("(sendInstallSnapshot) ASKING FOLLOWER S%d TO INSTALL A SNAPSHOT; args=(Term=%d, LeaderId=%d, LastIncludedIndex=%d, LastIncludedTerm=%d, len(data)=%d)", pId, args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data)))

		reply := &InstallSnapshotReply{}

		resCh := make(chan bool)

		ok := false
		go func() {
			resCh <- rf.peers[pId].Call(INSTALL_SNAPSHOT_RPC, args, reply)
		}()

		select {
		case <-rf.quit:
			rf.logger.Log(-1, "CHANL CLOSED (SEND_INSTALL_SNAPSHOT GR)")
			return
		case ok = <-resCh:
		}

		if !rf.killed() && ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currTerm {
				rf.logger.Log(LogTopicSendInstallSnapshot, fmt.Sprintf("(sendInstallSnapshot) TURN TO A FOLLOWER; GOT A HIGHER TERM REPLY; Leader = S%d reply.Term=%d, rf.currTerm=%d", pId, reply.Term, rf.currTerm))
				rf.currTerm = reply.Term
				rf.raftState = Follower
				rf.votedFor = -1

				rf.persist()
				return
			} else if reply.Term != rf.currTerm {
				rf.logger.Log(LogTopicSendInstallSnapshot, fmt.Sprintf("(sendInstallSnapshot) STALE REPLY TERM RETURNING ... ; Leader = S%d reply.Term=%d, rf.currTerm=%d", rf.leaderId, reply.Term, rf.currTerm))
				return
			}

			if reply.Success {
				// the follower installed the snapshot
				if reply.XIndex >= rf.XIndex {
					// rf.cond.L.Lock()
					// rf.matchIndex[pId] = Max(rf.matchIndex[pId], args.LastIncludedIndex)
					rf.matchIndex[pId] = Max(rf.matchIndex[pId], args.LastIncludedIndex)
					rf.nextIndex[pId] = rf.matchIndex[pId] + 1
					// rf.cond.L.Unlock()
				}

				rf.logger.Log(LogTopicSendInstallSnapshot, fmt.Sprintf("(sendInstallSnapshot) INSTALLED S%d installed the snapshot successfully; rf.matchIndex[S%d]=%d, rf.nextIndex[S%d]=%d\n\tsent_args=(Term=%d, LeaderId=%d, LastIncludedIndex=%d, LastIncludedTerm=%d, Offset=%d, len(Data)=%d) \n\treply=%+v", pId, pId, rf.matchIndex[pId], pId, rf.nextIndex[pId], args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, args.Offset, len(args.Data), reply))
			} else {
				rf.logger.Log(LogTopicSendInstallSnapshot, fmt.Sprintf("FAILED_INSTALLED S%d didn't install the snapshot; rf.matchIndex[S%d]=%d, rf.nextIndex[S%d]=%d\n\tsent_args=(Term=%d, LeaderId=%d, LastIncludedIndex=%d, LastIncludedTerm=%d, Offset=%d, len(Data)=%d) \n\treply=%+v", pId, pId, rf.matchIndex[pId], pId, rf.nextIndex[pId], args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, args.Offset, len(args.Data), reply))
			}
		}
	}(pId)
}
