package raft

import "sync"
import "math/rand"
import "time"
import "fmt"

//
// Leader Election
// Log Replication
// Membership Changes
// Log Compaction
// Snapshot

// Note
// Figure8: commit 4 indirectly commit 2, will cannot be overwrite.
// If commit 2, then commit 4, server crash between them, committed 2 will be overwrite

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

import "labrpc"

// import "bytes"
// import "labgob"

// finish leader in 5s, the tester limits you to 10 heartbeats per second
const ELECTION_TIMEOUT_MIN int32 = 800     // ms
const ELECTION_TIMEOUT_MAX int32 = 1200    // ms
const HEARTBEAT_PERIOD time.Duration = 150 // ms

func getElectionTimeout() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Duration(r.Int31n(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN)
}

// ApplyMsg ...
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry ...
//
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// Raft ...
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	majorityNum int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int // init -1, means null
	log         []LogEntry

	commitIndex int
	lastApplied int

	// fields used in leader only
	nextIndex  []int
	matchIndex []int

	applyChan chan ApplyMsg

	heartbeatTicker   *time.Ticker
	heartbeatTickerCh chan bool // for close
	heartbeatTickerMu sync.Mutex
	voteTicker        *time.Ticker // election
	voteTickerCh      chan bool    // for close
	voteTickerMu      sync.Mutex

	exitCh chan bool // system exit, close it to notify all routines to exit
}

// AppendEntriesArgs ...
type AppendEntriesArgs struct {
	LeaderTerm int
	LeaderID   int

	PreLogIndex int
	PreLogTerm  int
	LogEntries  []LogEntry

	LeaderCommitIndex int
}

// AppendEntriesReply ...
type AppendEntriesReply struct {
	CurrentTerm int
	Success     bool
}

// RequestVoteArgs ...
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int
	CandidateID   int
	LastLogIndex  int
	LastLogTerm   int
}

// RequestVoteReply ...
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int
	VoteGranted bool
}

// GetState ...
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.votedFor == rf.me
	rf.debug("GetState: term %d, voted for %d\n", term, rf.votedFor)
	return term, isleader
}

// persist ...
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

func (rf *Raft) closeHeartBeatTicker() {
	// rf.heartbeatTickerMu.Lock()
	if rf.heartbeatTickerCh != nil {
		close(rf.heartbeatTickerCh)
		rf.heartbeatTickerCh = nil
	}
	// rf.heartbeatTickerMu.Unlock()
}

func (rf *Raft) closeVoteTicker() {
	// rf.voteTickerMu.Lock()
	if rf.voteTickerCh != nil {
		close(rf.voteTickerCh)
		rf.voteTickerCh = nil
	}
	// rf.voteTickerMu.Unlock()
}

func (rf *Raft) debug(format string, a ...interface{}) {
	DPrintf("Raft-%d: %s", rf.me, fmt.Sprintf(format, a...))
}

func (rf *Raft) updateLocalCommitIndex(leaderCommitIndex int) {
	if leaderCommitIndex > rf.commitIndex {
		if leaderCommitIndex < len(rf.log) {
			rf.commitIndex = leaderCommitIndex
		} else {
			rf.commitIndex = len(rf.log)
		}
	}
}

func (rf *Raft) scheduleVoteService() {
	rf.voteTickerMu.Lock()
	defer rf.voteTickerMu.Unlock()

	rf.closeVoteTicker()
	// restart a new timer
	rf.voteTicker = time.NewTicker(time.Millisecond * getElectionTimeout())
	rf.voteTickerCh = make(chan bool)
	rf.debug("create vote ticker %p", rf.voteTickerCh)
	go func() {
		for {
			select {
			case <-rf.voteTicker.C:
				rf.becomeCandidate()
			case <-rf.voteTickerCh:
				rf.debug("close vote ticker %p", rf.voteTickerCh)
				return
			case <-rf.exitCh:
				// rf.debug("Vote routine exit as raft(%p) exit", rf)
				rf.debug("close vote ticker %p", rf.voteTickerCh)
				return
			}
		}
	}()
}

func (rf *Raft) scheduleHeartBeatService() {
	rf.closeHeartBeatTicker()
	rf.heartbeatTicker = time.NewTicker(time.Millisecond * HEARTBEAT_PERIOD)
	rf.heartbeatTickerCh = make(chan bool)
	go func() {
		for {
			select {
			case <-rf.heartbeatTicker.C:
				ok := rf.broadcastHeartBeat()
				if !ok {
					rf.closeHeartBeatTicker()
					rf.becomeFollower()
					return
				}
			case <-rf.heartbeatTickerCh:
				return
			case <-rf.exitCh:
				// rf.debug("HearBeat routine exit as raft exit")
				return
			}
		}
	}()
}

func (rf *Raft) becomeLeader() {
	rf.debug("become leader\n")
	if rf.broadcastHeartBeat() {
		rf.scheduleHeartBeatService()
	} else {
		rf.becomeFollower()
	}
}

func (rf *Raft) becomeFollower() {
	rf.debug("become follower\n")
	rf.scheduleVoteService()
}

func (rf *Raft) becomeCandidate() {
	rf.debug("become candidate\n")
	if rf.broadcastVote() {
		rf.voteTickerMu.Lock()
		defer rf.voteTickerMu.Unlock()
		rf.closeVoteTicker()
		rf.becomeLeader()
	}
}

// broadcastVote ...
// begin vote
func (rf *Raft) broadcastVote() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// GetState test Leader by votedFor, avoid wrong adjudgement here
	// if leader disconnect, its votedFor always self
	rf.votedFor = -1

	lastindex := 0
	lastterm := 0
	if len(rf.log) > 0 {
		lastindex = rf.log[len(rf.log)-1].Index
		lastterm = rf.log[len(rf.log)-1].Term
	}
	rf.currentTerm++
	args := RequestVoteArgs{CandidateTerm: rf.currentTerm, CandidateID: rf.me,
		LastLogIndex: lastindex, LastLogTerm: lastterm}

	sumGrantedVote := 0
	// need reset vote ticker? NO
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			sumGrantedVote++
			continue
		}

		var reply RequestVoteReply
		if rf.sendRequestVote(i, &args, &reply) {
			rf.debug("send vote to %d, granted %t\n", i, reply.VoteGranted)
			if reply.VoteGranted {
				sumGrantedVote++
			}
		} else {
			rf.debug("failed to receive response of sendRequestVote from %d", i)
		}
	}

	// become leader
	if sumGrantedVote >= rf.majorityNum {
		// assign here to keep GetState() result right
		rf.votedFor = rf.me
		return true
	}

	return false
}

func (rf *Raft) broadcastHeartBeat() bool {
	sumOKHeart := 0
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		arg := AppendEntriesArgs{LeaderTerm: rf.currentTerm, LeaderID: rf.me, LeaderCommitIndex: rf.commitIndex}
		reply := AppendEntriesReply{}

		if rf.sendAppendEntries(i, &arg, &reply) {
			rf.debug("send heartbeat to %d, success %t\n", i, reply.Success)
			if reply.Success {
				sumOKHeart++
			} else {
				if reply.CurrentTerm > rf.currentTerm {
					rf.debug("heartbeat: receive larger term: %d, current term: %d", reply.CurrentTerm, rf.currentTerm)
					rf.currentTerm = reply.CurrentTerm
					return false
				}
			}
		} else {
			rf.debug("failed to receive response of sendAppendEntries from %d", i)
		}
	}

	if sumOKHeart < rf.majorityNum {
		return false
	}

	return true
}

// RequestVote ...
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.debug("Handle RequestVote")
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	grantVote := false
	if args.CandidateTerm < rf.currentTerm {
		grantVote = false
	} else if args.CandidateTerm == rf.currentTerm { // keep grant once for each term
		if args.CandidateID == rf.votedFor {
			grantVote = true
		} else {
			grantVote = false
		}
	} else {
		if rf.votedFor < 0 {
			grantVote = true
		} else {
			logLen := len(rf.log)
			if logLen > 0 {
				if rf.log[logLen-1].Term < args.LastLogTerm {
					grantVote = true
				} else if rf.log[logLen-1].Term == args.LastLogTerm {
					grantVote = rf.log[logLen-1].Index <= args.LastLogIndex
				} else {
					grantVote = false
				}
			} else {
				grantVote = true
			}
		}
	}

	if grantVote {
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.CandidateTerm
		reply.VoteGranted = true
		reply.CurrentTerm = rf.currentTerm
		rf.voteTickerMu.Lock()
		defer rf.voteTickerMu.Unlock()
		rf.closeVoteTicker()
		rf.becomeFollower()
	} else {
		reply.VoteGranted = false
		reply.CurrentTerm = rf.currentTerm
	}
}

// sendRequestVote ...
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// MatchLog ...
func (rf *Raft) MatchLog(term int, index int) bool {
	if len(rf.log) < index {
		return false
	}

	return rf.log[index-1].Term == term
}

// AppendEntries ...
// handler to appendEntry
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.debug("AppendEntries: LeaderID %d, LeaderTerm %d", args.LeaderID, args.LeaderTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.LeaderTerm < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		reply.Success = false
		return
	}

	// heartbeat
	if len(args.LogEntries) == 0 {
		rf.scheduleVoteService()
		reply.CurrentTerm = rf.currentTerm
		reply.Success = true
		rf.updateLocalCommitIndex(args.LeaderCommitIndex)
		return
	}

	// append entry
	if !rf.MatchLog(args.PreLogTerm, args.PreLogIndex) {
		reply.CurrentTerm = rf.currentTerm
		reply.Success = false
		rf.scheduleVoteService()
		return
	}

	irf := args.PreLogIndex
	iarg := 0
	for iarg < len(args.LogEntries) {
		if irf < len(rf.log) {
			if args.LogEntries[iarg].Index != rf.log[irf].Index {
				panic("log index in args must match in raft log")
			}
			if args.LogEntries[iarg].Term != rf.log[irf].Term {
				rf.log = rf.log[:irf] // delete inconsistency log
				continue
			}
		}

		if irf < len(rf.log) {
			rf.log[irf] = args.LogEntries[iarg]
		} else {
			rf.log = append(rf.log, args.LogEntries[iarg])
		}
		iarg++
		irf++
	}

	// update commit index
	rf.updateLocalCommitIndex(args.LeaderCommitIndex)
	rf.scheduleVoteService()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start ...
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill ...
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.debug("Kill\n")
	close(rf.exitCh)
}

// Make ...
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.majorityNum = len(peers)/2 + 1
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0, 1000)
	rf.commitIndex = 0
	rf.lastApplied = 0

	clientsNumber := len(peers)
	rf.nextIndex = make([]int, clientsNumber, clientsNumber)
	rf.matchIndex = make([]int, clientsNumber, clientsNumber)

	rf.applyChan = applyCh

	rf.exitCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.becomeFollower()
	return rf
}
