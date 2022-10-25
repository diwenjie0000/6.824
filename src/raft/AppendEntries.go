package raft

import "fmt"

type AppendEntriesArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store
	LeaderCommit int        //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.heartbeatExist = true
	if args.Term >= rf.currenTerm {
		rf.currenTerm = args.Term
		if rf.state == candidate {
			rf.becomeFollower <- true
			rf.state = follower
		}
		if args.Term > rf.currenTerm && rf.state == leader {
			rf.becomeFollower <- true
			fmt.Println(rf.me, " need to convert from leader to follower")
			rf.state = follower
		}
		reply.Term = rf.currenTerm
		reply.Success = true
	} else {
		reply.Term = rf.currenTerm
		reply.Success = false
	}

	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
