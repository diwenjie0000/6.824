package raft

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

// RequestVote  RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currenTerm < args.Term && rf.state != follower {
		rf.becomeFollower <- true
	}
	if (rf.currenTerm < args.Term || (rf.currenTerm == args.Term && rf.votedFor == args.CandidateId)) &&
		(len(rf.log) == 0 || isAtLeastUpToDate(args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Term, len(rf.log)-1)) {
		rf.currenTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.state = follower
		reply.VoteGranted = true
		reply.Term = rf.currenTerm
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currenTerm
		return
	}

}

// if a is at least up-to-date b
func isAtLeastUpToDate(aTerm, aIdx, bTerm, bIdx int) bool {
	return aTerm > bTerm || aTerm == bTerm && aIdx >= bIdx
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
