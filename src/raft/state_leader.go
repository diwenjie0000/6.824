package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) leaderState() {
	//fmt.Println("term ", rf.currenTerm, " ", rf.me, " become leader")
	rf.mu.Lock()
	rf.state = leader
	rf.agreeSet = make(map[int]int)
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	idx := len(rf.log)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, idx)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.mu.Unlock()
	go rf.sendHeartbeat()
}

func (rf *Raft) startAgreement() {
	rf.mu.Lock()
	index := len(rf.log) - 1
	rf.mu.Unlock()
	agree := make(chan bool, len(rf.peers))
	for key := range rf.peers {
		if key != rf.me {
			go rf.tryAppendEntries(key, agree)
		}
	}
	// if agree >= half, commit it

	for cnt := 0; cnt <= len(rf.peers)/2-1; cnt++ {
		<-agree
	}
	rf.mu.Lock()
	fmt.Println("term", rf.currenTerm, index, ":", rf.log[index], "reach agreement")
	rf.mu.Unlock()
	go rf.achieveAgreement(index)
}

func (rf *Raft) tryAppendEntries(key int, agree chan<- bool) {
	ok := false
	success := false
	for rf.killed() == false && ok == false {
		fmt.Println("term", rf.currenTerm, "leader", rf.me, rf.log, "try to append idx", rf.nextIndex[key], "on follower", key)
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		ok, success = rf.appendEntries(key, rf.nextIndex[key])
	}
	if success {
		//fmt.Println("leader ", rf.me, " receive a agree", rf.log[len(rf.log)-1])
		fmt.Println("term", rf.currenTerm, "leader", rf.me, "success to append idx", rf.nextIndex[key], rf.log[rf.nextIndex[key]], "on follower", key)
		agree <- true
		rf.mu.Lock()
		rf.nextIndex[key]++
		if rf.nextIndex[key] >= len(rf.log) {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.tryAppendEntries(key, agree)

	} else {
		fmt.Println("term", rf.currenTerm, "leader", rf.me, "failed to append idx", rf.nextIndex[key], "on follower", key)
		rf.mu.Lock()
		if rf.nextIndex[key] <= 0 {
			rf.mu.Unlock()
			return
		}
		rf.nextIndex[key]--
		rf.mu.Unlock()
		rf.tryAppendEntries(key, agree)
	}

}

// each time achieve a agreement, find the new commitIndex
func (rf *Raft) achieveAgreement(idx int) {
	rf.mu.Lock()
	fmt.Println("leader ", rf.me, " log: ", rf.log)
	defer rf.mu.Unlock()
	rf.agreeSet[idx] = 1
	lastCommit := rf.commitIndex
	for i := lastCommit + 1; ; i++ {
		if _, ok := rf.agreeSet[i]; !ok {
			rf.commitIndex = i - 1
			break
		}
	}
	for i := lastCommit + 1; i <= rf.commitIndex; i++ {
		fmt.Println("leader ", rf.me, " commit a log ", i, rf.log[i])
		rf.applyCh <- ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[i].LogCmd,
			CommandIndex:  i + 1,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
	}

}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	rf.becomeFollower = make(chan bool, 5)
	rf.mu.Unlock()
Loop:
	for rf.killed() == false {
		select {
		case <-rf.becomeFollower:
			go rf.followerState()
			break Loop
		default:
			for key := range rf.peers {
				if key != rf.me {
					go rf.appendEntries(key, -1)
				}
			}
			time.Sleep(heartbeatTimeout)
		}
		//send heartbeat to all peers
	}
}

func (rf *Raft) appendEntries(key, idx int) (bool, bool) {
	rf.mu.RLock()
	args := AppendEntriesArgs{
		Term:         rf.currenTerm,
		LeaderId:     rf.me,
		PrevLogIndex: idx - 1,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	// idx = 1 when it is a heartbeat89
	if idx != -1 {
		args.Entries = rf.log[idx : idx+1]
	}
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}
	reply := AppendEntriesReply{
		Term:    0,
		Success: false,
	}
	rf.mu.RUnlock()
	ok := rf.sendAppendEntries(key, &args, &reply)
	rf.mu.Lock()
	if reply.Term > rf.currenTerm {
		rf.currenTerm = reply.Term
		rf.state = follower
		rf.becomeFollower <- true
	}
	rf.mu.Unlock()
	return ok, reply.Success
}
