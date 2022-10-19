package raft

import "fmt"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term        int //candidate’s term
	CandidateId int //candidate requesting vote

	//only vote for candidate who is at least as up-to-date as receiver’s log
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).'
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.RLock()
	fmt.Println(args)
	if rf.currenTerm < args.Term {
		rf.muForHeartbeat.Lock()
		rf.heartbeatExist = true
		rf.muForHeartbeat.Unlock()

		fmt.Println(rf.me, " vote for ", args.CandidateId, " in term ", args.Term)
		rf.currenTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currenTerm
	} else {
		fmt.Println(rf.me, " refuse ", args.CandidateId, " in term ", args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currenTerm
	}
	rf.mu.RUnlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}