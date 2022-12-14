package raft

type LogEntry struct {
    Term int
    Command interface{}
}

type AppendEntriesArgs struct {
    Term int
    LeaderId int
    PrevLogTerm int
    PrevLogIndex int
    Entries []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term int
    Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm

    if args.Term < rf.currentTerm {
        reply.Success = false
        return
    }

    if rf.state != FOLLOWER {
        rf.becomeFollowerL(args.Term)
    }

    // Debug("%v receive heartbeat from %v\n", rf.me, args.LeaderId)
    rf.setElectionTimeL()
    if args.PrevLogIndex != 0 && (args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
        reply.Success = false
        return
    }

    reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) processAppendReply(args *AppendEntriesArgs, reply *AppendEntriesReply, server int) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.currentTerm < reply.Term {
        rf.becomeFollowerL(reply.Term)
        return
    }

    if reply.Success {
        myNextIndex := rf.nextIndex[server] + len(args.Entries) + 1
        myMatchIndex := rf.nextIndex[server] + len(args.Entries)
        if myNextIndex > rf.nextIndex[server] {
            rf.nextIndex[server] = myNextIndex
        }
        if myMatchIndex > rf.matchIndex[server] {
            rf.matchIndex[server] = myMatchIndex
        }
    } else {
        rf.nextIndex[server]--
    }
}

func (rf *Raft) sendAppend(server int, heartbeat bool) {
    rf.mu.Lock()
    if rf.nextIndex[server] >= len(rf.log) {
        rf.nextIndex[server] = len(rf.log) - 1
    }

    next := rf.nextIndex[server]
    args := &AppendEntriesArgs{}
    args.Term = rf.currentTerm
    args.LeaderId = rf.me
    args.PrevLogTerm = 0
    args.PrevLogIndex = 0
    if !heartbeat {
        args.Entries = make([]LogEntry, len(rf.log) - next)
        copy(args.Entries, rf.log[next :])
    }
    args.LeaderCommit = rf.commitIndex

    if next > 0 {
        args.PrevLogIndex = next - 1
        args.PrevLogTerm = rf.log[next - 1].Term
    }

    rf.mu.Unlock()

    reply := &AppendEntriesReply{}
    ok := rf.sendAppendEntries(server, args, reply)

    if ok {
        rf.processAppendReply(args, reply, server)
    }
}

func (rf *Raft) sendAppendsL(heartbeat bool) {
    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me && (len(rf.log) - 1 > rf.nextIndex[i] || heartbeat) {
            go rf.sendAppend(i, heartbeat)
        }
    }
}

//
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
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.state != LEADER {
        return -1, rf.currentTerm, false
    }

    e := LogEntry{rf.currentTerm, command}
    rf.log = append(rf.log, e)
    rf.sendAppendsL(false)

    return len(rf.log) - 1, rf.currentTerm, true
}
