package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) followerState() {
	rf.mu.Lock()
	fmt.Println("term", rf.currenTerm, rf.me, "become follower")
	rf.state = follower
	rf.mu.Unlock()
	go rf.ticker()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.heartbeatExist = true
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.heartbeatExist == false {
			go rf.candidateState()
			rf.mu.Unlock()
			break
		}
		rf.heartbeatExist = false
		rf.mu.Unlock()
		time.Sleep(rf.getElectionTimeout())
	}
}
