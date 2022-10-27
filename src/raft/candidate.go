package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) candidateState() {
	rf.mu.Lock()
	rf.state = candidate
	rf.currenTerm = rf.currenTerm + 1
	rf.votedFor = rf.me
	rf.becomeFollower = make(chan bool)
	rf.becomeLeader = make(chan bool)
	rf.mu.Unlock()
	vote := make(chan bool, len(rf.peers))
	for key := range rf.peers {
		if key != rf.me {
			go rf.requestVote(key, vote)
		}
	}

	// control the state transition
	go rf.candidateTransition()

	//count the result of voting, and the chan guarantee the sync
	go rf.voteCount(vote)
}

func (rf *Raft) requestVote(key int, vote chan<- bool) {
	rf.mu.RLock()
	args := RequestVoteArgs{
		Term:         rf.currenTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  0,
	}
	if len(rf.log) > 0 {
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	reply := RequestVoteReply{
		Term:        -1,
		VoteGranted: false,
		Me:          -1,
	}
	rf.mu.RUnlock()
	ok := rf.sendRequestVote(key, &args, &reply)
	rf.mu.RLock()
	if ok {
		if reply.VoteGranted {
			vote <- true
		} else {
			vote <- false
		}
	}
	rf.mu.RUnlock()
}

func (rf *Raft) candidateTransition() {
	rand.Seed(int64(rf.me) * time.Now().Unix())
	num := rand.Intn(150) // 1~2 times basic timeout
	select {
	case <-rf.becomeFollower: //receive stop candidate
		go rf.followerState()
	case <-rf.becomeLeader:
		go rf.leaderState()
	case <-time.After(time.Duration(num)*time.Millisecond + electionTimeout): // set election timeout to close vote chan
		go rf.candidateState()
	}
}

func (rf *Raft) voteCount(vote <-chan bool) {
	voteGrantedCnt := 1 // vote for itself
	needVote := len(rf.peers) / 2
	voteCnt := 1
	for result := range vote {
		if result == true {
			voteGrantedCnt++
		}
		if voteGrantedCnt > needVote {
			rf.becomeLeader <- true
			return
		}
		voteCnt++
		if voteCnt == len(rf.peers) {
			return
		}
	}
}
