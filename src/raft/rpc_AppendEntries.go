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
	defer rf.mu.Unlock()
	rf.heartbeatExist = true
	//Reply false if term < currentTerm
	if args.Term < rf.currenTerm {
		reply.Term = rf.currenTerm
		reply.Success = false
		return
	}
	//Reply false if log doesn't contain an entry at prevLogIndex
	//whose term matches prevLogTerm
	if args.PrevLogIndex < 0 {
	} else if len(rf.log) <= args.PrevLogIndex {
		reply.Term = rf.currenTerm
		reply.Success = false
		return
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.log = rf.log[:args.PrevLogIndex]
		reply.Term = rf.currenTerm
		reply.Success = false
		return
	}
	if args.Term >= rf.currenTerm {
		if rf.state == candidate {
			rf.becomeFollower <- true
			rf.state = follower
		}
		if args.Term > rf.currenTerm && rf.state == leader {
			rf.becomeFollower <- true
			rf.state = follower
		}
		rf.currenTerm = args.Term
		if args.Entries != nil {
			rf.log = append(rf.log, args.Entries[0])
		}
		reply.Term = rf.currenTerm
		reply.Success = true
	}
	if args.LeaderCommit > rf.commitIndex {
		lastCommit := rf.commitIndex
		min := func(x, y int) int {
			if x < y {
				return x
			}
			return y
		}
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		for i := lastCommit + 1; i <= rf.commitIndex; i++ {
			//fmt.Println("follower ", rf.me, " commit ", i, rf.log[i])
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
	if args.Entries != nil {
		fmt.Println("term", rf.currenTerm, rf.me, "receives", args, "log:", rf.log)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
