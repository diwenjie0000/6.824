package raft

import "time"

func (rf *Raft) leaderState() {
	//fmt.Println("term ", rf.currenTerm, " ", rf.me, " become leader")
	rf.mu.Lock()
	rf.state = leader
	rf.mu.Unlock()
	go rf.sendHeartbeat()
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
					go rf.appendEntries(key)
				}
			}
			time.Sleep(heartbeatTimeout)
		}
		//send heartbeat to all peers
	}
}

func (rf *Raft) appendEntries(key int) {
	rf.mu.RLock()
	args := AppendEntriesArgs{
		Term:         rf.currenTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log),
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}
	if len(rf.log) > 0 {
		args.PrevLogTerm = rf.log[len(rf.log)-1].Term
	}
	reply := AppendEntriesReply{
		Term:    0,
		Success: false,
	}
	rf.mu.RUnlock()
	rf.sendAppendEntries(key, &args, &reply)
	rf.mu.Lock()
	if reply.Term > rf.currenTerm {
		rf.becomeFollower <- true
	}
	rf.mu.Unlock()
}
