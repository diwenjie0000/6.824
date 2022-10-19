package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const heartbeatTimeout = 100 * time.Millisecond
const electionTimeout = 500 * time.Millisecond
const (
	follower = iota
	candidate
	leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	muState sync.Mutex
	state   int //state of server

	muForHeartbeat sync.Mutex
	heartbeatExist bool

	//stop candidate
	stopCandidate chan bool
	becomeLeader  chan bool

	//Persistent state
	currenTerm int        //latest term server has seen
	votedFor   int        //candidateId that received vote in current term
	log        []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader

	//Volatile state
	commitIndex int //index of highest log entry known to be committed
	lastApplied int //index of highest log entry applied to state machine

	//Leader only volatile state
	nextIndex  int //for each server, index of the next log entry to send to that server
	matchIndex int //for each server, index of highest log entry known to be replicated on server
}

type LogEntry struct {
	term    int
	command Command
}

type Command struct {
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	var term int
	if rf.currenTerm == 0 {
		term = 1
	} else {
		term = rf.currenTerm
	}

	isLeader := rf.state == leader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currenTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) candidateState(thisTerm int) {
	rf.muState.Lock()
	rf.state = candidate
	rf.muState.Unlock()

	rf.mu.Lock()
	rf.currenTerm = thisTerm + 1
	fmt.Println("term ", rf.currenTerm, " ", rf.me, " start election")
	rf.votedFor = rf.me
	rf.stopCandidate = make(chan bool)
	rf.becomeLeader = make(chan bool)
	rf.mu.Unlock()

	vote := make(chan bool, len(rf.peers))
	for key, _ := range rf.peers {
		if key != rf.me {
			go func(key int) {
				rf.mu.RLock()
				defer rf.mu.RUnlock()
				args := RequestVoteArgs{
					Term:         rf.currenTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log),
					LastLogTerm:  0,
				}
				if len(rf.log) > 0 {
					args.LastLogTerm = rf.log[len(rf.log)-1].term
				}
				reply := RequestVoteReply{
					Term:        0,
					VoteGranted: false,
				}
				fmt.Println("term ", rf.currenTerm, " ", "candidate ", rf.me, " request a vote from ", key)
				rf.sendRequestVote(key, &args, &reply)
				if reply.VoteGranted {
					vote <- true
					fmt.Println("term ", rf.currenTerm, " ", "candidate ", rf.me, " receives 1 vote")
				} else {
					vote <- false
					fmt.Println("term ", rf.currenTerm, " ", "candidate ", rf.me, " receives 1 refuse")
				}
			}(key)
		}
	}

	go func(vote chan bool) {
		num := float32(rand.Intn(10))*0.1 + 1 // 1~2 times basic timeout
		select {
		case <-rf.stopCandidate: //receive stop candidate
			close(vote)
			go rf.followerState()
		case <-rf.becomeLeader:
		case <-time.After(time.Duration(num) * electionTimeout): // set election timeout to close vote chan
			close(vote)
			rf.mu.RLock()
			go rf.candidateState(rf.currenTerm - 1)
			rf.mu.RUnlock()
		}
	}(vote)

	//count the result of voting, and the chan guarantee the sync
	voteGrantedCnt := 1 // vote for itself
	needVote := float32(len(rf.peers)) / 2
	voteCnt := 1
	for result := range vote {
		if result == true {
			voteGrantedCnt++
		}
		if float32(voteGrantedCnt) > needVote {
			rf.becomeLeader <- true
			go rf.leaderState()
			return
		}
		voteCnt++
		if voteCnt == len(rf.peers) {
			close(vote)
		}
	}

}

func (rf *Raft) followerState() {
	rf.mu.Lock()
	rf.state = follower
	rf.mu.Unlock()
	go rf.ticker()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.heartbeatExist = true
	for rf.killed() == false {
		num := float32(rand.Intn(10))*0.1 + 1 // 1~2 times basic timeout
		time.Sleep(time.Duration(num) * electionTimeout)
		rf.muForHeartbeat.Lock()
		if rf.heartbeatExist == false {
			go rf.candidateState(rf.currenTerm)
			break
		}
		rf.heartbeatExist = false
		rf.muForHeartbeat.Unlock()
	}
}

func (rf *Raft) leaderState() {
	fmt.Println("term ", rf.currenTerm, " ", rf.me, " become leader")
	rf.mu.Lock()
	rf.state = leader
	rf.mu.Unlock()
	go rf.sendHeartbeat()
}

func (rf *Raft) sendHeartbeat() {
	for rf.killed() == false {
		//check if the server a leader
		rf.muState.Lock()
		if rf.state != leader {
			rf.muState.Unlock()
			return
		}
		rf.muState.Unlock()

		//send heartbeat to all peers
		for key, _ := range rf.peers {
			if key != rf.me {
				go func(key int) {
					args := AppendEntriesArgs{
						Term:         rf.currenTerm,
						LeaderId:     rf.me,
						PrevLogIndex: len(rf.log),
						PrevLogTerm:  rf.log[len(rf.log)-1].term,
						Entries:      nil,
						LeaderCommit: 0,
					}
					reply := AppendEntriesReply{
						Term:    0,
						Success: false,
					}
					rf.sendAppendEntries(key, &args, &reply)
				}(key)
			}
		}
		time.Sleep(heartbeatTimeout)
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:             sync.RWMutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		muState:        sync.Mutex{},
		state:          follower,
		muForHeartbeat: sync.Mutex{},
		heartbeatExist: true,
		stopCandidate:  nil,
		becomeLeader:   nil,
		currenTerm:     0,
		votedFor:       0,
		log:            nil,
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      0,
		matchIndex:     0,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.followerState()

	return rf
}
