package raft

import (
	"fmt"
)

func (rf *Raft) applyEntries() {
	var lastApplied, commitIndex, XIndex, leaderId int
	var start, end int
	var logCpy []LogEntry

	for !rf.killed() {
		rf.logger.Log(LogTopicCommittingEntriesApe, "(applyEntries) WAITING FOR NEW ENTRIES TO APPLY...")

		select {
		case <-rf.quit:
			return
		case <-rf.appEntCh:
		}

		rf.mu.Lock()
		if rf.killed() {
			rf.logger.Log(LogTopicCommittingEntriesApe, "(applyEntries) ... KILLED")
			rf.mu.Unlock()
			return
		}

		if len(rf.snapshot) > 0 && rf.lastApplied <= rf.XIndex {
			rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("(applyEntries) SENDING THE SNAPSHOT TO SM ... ; rf.commitIndex=%d, xIndex=%d, rf.lastApplied=%d, rf.XIndex = %d, rf.XTerm=%d", rf.commitIndex, rf.XIndex, rf.lastApplied, rf.XIndex, rf.XTerm))

			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.XTerm,
				SnapshotIndex: rf.XIndex,
			}

			rf.logger.Log(LogTopicSnapshots, fmt.Sprintf("SENT SNAPSHOT TO SM ... rf.commitIndex after snapshot ...; rf.commitIndex=%d, xIndex=%d, rf.lastApplied=%d, rf.XIndex = %d, rf.XTerm=%d", rf.commitIndex, rf.XIndex, rf.lastApplied, rf.XIndex, rf.XTerm))
		}

		logCpy = logCpy[:0] // empty the log

		leaderId = rf.leaderId

		lastApplied, commitIndex, XIndex = rf.lastApplied, rf.commitIndex, rf.XIndex

		if lastApplied < XIndex {
			rf.logger.Log(LogTopicCommittingEntriesApe, fmt.Sprintf("(applyEntries) LASTAPPLIED < XINDEX? SET IT TO MAX(LASTAPPLIED=%d, XINDEX=%d)", lastApplied, XIndex))
			lastApplied = Max(lastApplied, XIndex)
		}
		if commitIndex < XIndex {
			rf.logger.Log(LogTopicCommittingEntriesApe, fmt.Sprintf("(applyEntries) COMMIT_INDEX < XINDEX? SET IT TO MAX(COMMIT_INDEX)=%d, XINDEX=%d)", commitIndex, XIndex))
			commitIndex = Max(commitIndex, XIndex)
		}
		start = lastApplied - XIndex
		end = commitIndex - XIndex - 1

		rf.logger.Log(LogTopicCommittingEntriesApe, fmt.Sprintf("(applyEntries) SOMEONE ASKED TO APPLY ENTRIES [start=%d end=%d]; lastApplied=%d, commitIndex=%d, XIndex=%d, logs=%+v", start, end, lastApplied, commitIndex, XIndex, rf.logs))

		if start <= end {
			logCpy = make([]LogEntry, end-start+1)
			copy(logCpy, rf.logs[start:end+1])
		}
		rf.mu.Unlock()

		if len(logCpy) > 0 {
			rf.logger.Log(LogTopicCommittingEntriesApe, fmt.Sprintf("(applyEntries) START APPLYING ENTRIES (leader = S%d, XIndex=%d lastApplied=%d commitIndex=%d) [FROM=%d TO=%d] applying_log=%v", leaderId, XIndex, lastApplied, commitIndex, start, end, logCpy))

			for idx, entry := range logCpy {
				rf.logger.Log(LogTopicCommittingEntriesApe, fmt.Sprintf("(applyEntries) SENDING ENTRY TO applyCh ... (%d/%d), Entry=%+v", idx+1, len(logCpy), entry))
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				rf.logger.Log(LogTopicCommittingEntriesApe, fmt.Sprintf("(applyEntries) SENT ENTRY TO applyCh ... (%d/%d) Entry=%+v", idx+1, len(logCpy), entry))
			}

			rf.logger.Log(LogTopicCommittingEntriesApe, fmt.Sprintf("(applyEntries) ALL APPLIED! to=%d; (lastApplied=%d, commitIndex=%d) applied_log=%v", commitIndex, lastApplied, commitIndex, logCpy))

			// check if we should update rf.appliedIndex
			rf.mu.Lock()
			rf.logger.Log(LogTopicCommittingEntriesApe, fmt.Sprintf("(appplyEntries) UPDATE rf.lastApplied? (rf.lastApplied=%d ? commitIndex=%d)", rf.lastApplied, commitIndex))
			rf.lastApplied = Max(rf.lastApplied, commitIndex)
			rf.mu.Unlock()
		}

	}
}
