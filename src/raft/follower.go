package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) followerState() {
	rf.mu.Lock()
	//fmt.Println(rf.me, " become follower")
	rf.state = follower
	rf.mu.Unlock()
	go rf.ticker()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	//fmt.Println(rf.me, " start ticker")
	rf.heartbeatExist = true
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.heartbeatExist == false {
			go rf.candidateState()
			rf.mu.Unlock()
			break
		}
		//fmt.Println(rf.me, " get heartBeat")
		rf.heartbeatExist = false
		rf.mu.Unlock()
		rand.Seed(int64(rf.me) * time.Now().Unix())
		num := rand.Intn(150) // 1~2 times basic timeout
		time.Sleep(time.Duration(num)*time.Millisecond + electionTimeout)
	}
}
