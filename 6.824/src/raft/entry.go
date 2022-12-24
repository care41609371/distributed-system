package raft

type AppendEntriesArgs struct {
    Term int
    LeaderId int
    PrevLogTerm int
    PrevLogIndex int
    Entries []Entry
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

    rf.setElectionTimeL()
    if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
        reply.Success = false
        return
    }

    if len(args.Entries) > 0 {
        Debug("%v reveive entries index %v - %v\n", rf.me, args.PrevLogIndex + 1, args.PrevLogIndex + len(args.Entries))
        rf.log.append(args.PrevLogIndex, args.Entries...)
    }

    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = args.LeaderCommit
    }
    if rf.commitIndex > rf.log.lastIndex() {
        rf.commitIndex = rf.log.lastIndex()
    }
    rf.applyCond.Broadcast()

    reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) leaderCommitL() {
    for i := rf.commitIndex + 1; i <= rf.log.lastIndex(); i++ {
        if rf.log.at(i).Term != rf.currentTerm {
            continue
        }

        count := 1
        for j := 0; j < len(rf.peers); j++ {
            if j != rf.me && rf.matchIndex[j] >= i {
                count++
            }
        }

        if count > len(rf.peers) / 2 {
            Debug("%v mojority\n", count)
            rf.commitIndex = i
        }
    }

    rf.applyCond.Broadcast()
}

func (rf *Raft) processAppendReply(args *AppendEntriesArgs, reply *AppendEntriesReply, server int) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.currentTerm < reply.Term {
        rf.becomeFollowerL(reply.Term)
        return
    }

    if rf.state != LEADER {
        return
    }

    if reply.Success {
        rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
        rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
        rf.leaderCommitL()
    } else {
        rf.nextIndex[server]--
        index := rf.nextIndex[server]
        for index > rf.log.firstIndex() && rf.log.at(index - 1).Term != reply.Term {
            index--
        }
        if rf.nextIndex[server] > index {
            rf.nextIndex[server] = index
        }
    }
}

func (rf *Raft) sendAppend(server int) {
    rf.mu.Lock()

    next := rf.nextIndex[server]
    args := &AppendEntriesArgs{}
    args.Term = rf.currentTerm
    args.LeaderId = rf.me
    if rf.nextIndex[server] <= rf.log.lastIndex() {
        args.Entries = make([]Entry, rf.log.lastIndex() + 1 - next)
        copy(args.Entries, rf.log.slice(next, rf.log.lastIndex() + 1))
    }
    args.LeaderCommit = rf.commitIndex
    args.PrevLogIndex = next - 1
    args.PrevLogTerm = rf.log.at(next - 1).Term

    rf.mu.Unlock()

    reply := &AppendEntriesReply{}
    ok := rf.sendAppendEntries(server, args, reply)

    if ok {
        rf.processAppendReply(args, reply, server)
    }
}

func (rf *Raft) sendAppendsL() {
    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me {
            go rf.sendAppend(i)
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

    Debug("leader receive entry\n")
    e := Entry{rf.currentTerm, command}
    rf.log.append(rf.log.lastIndex(), e)
    // rf.sendAppendsL(false)

    return rf.log.lastIndex(), rf.currentTerm, true
}

func (rf *Raft) applier() {
    rf.mu.Lock()
    for !rf.killed() {
        if rf.lastApplied + 1 <= rf.commitIndex && rf.lastApplied + 1 <= rf.log.lastIndex() {
            rf.lastApplied++
            am := ApplyMsg{}
            am.CommandValid = true
            am.Command = rf.log.at(rf.lastApplied).Command
            am.CommandIndex = rf.lastApplied
            rf.mu.Unlock()
            rf.applyCh <- am
            rf.mu.Lock()
            Debug("%v applied %v\n", rf.me, rf.lastApplied)
        } else {
            rf.applyCond.Wait()
        }
    }
}
