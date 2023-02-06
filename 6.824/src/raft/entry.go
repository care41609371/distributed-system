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
    ConflictTerm int
    ConflictIndex int
    Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }

    if rf.currentTerm < args.Term {
        rf.becomeFollowerL(args.Term)
    }

    rf.setElectionTimeL()
    reply.Term = rf.currentTerm

    if args.PrevLogIndex < rf.log.LastIncludedIndex {
        reply.Success = false
        reply.ConflictTerm = -1
        reply.ConflictIndex = -1
        return
    }

    if args.PrevLogIndex > rf.log.lastIndex() {
        reply.Success = false
        reply.ConflictTerm = -1
        reply.ConflictIndex = rf.log.lastIndex() + 1
        return
    }

    if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
        reply.Success = false
        reply.ConflictTerm = rf.log.at(args.PrevLogIndex).Term

        index := args.PrevLogIndex
        for index > rf.log.LastIncludedIndex && rf.log.at(index).Term == reply.ConflictTerm {
            index--
        }
        reply.ConflictIndex = index + 1

        rf.log.cutBack(args.PrevLogIndex)
        if rf.commitIndex > rf.log.lastIndex() {
            rf.commitIndex = rf.log.lastIndex()
        }

        return
    }

    if len(args.Entries) > 0 {
        rf.log.append(args.PrevLogIndex, args.Entries...)
        DPrintf("[entry] %v %v", rf.me, rf.log.lastIndex())
        rf.persist()
    }

    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = min(args.LeaderCommit, rf.log.lastIndex())
        rf.applyCond.Broadcast()
    }

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
            rf.commitIndex = i
        }
    }

    rf.applyCond.Broadcast()
}

func (rf *Raft) processAppendReply(args *AppendEntriesArgs, reply *AppendEntriesReply, server int) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term != rf.currentTerm {
        return
    }

    if rf.state != LEADER {
        return
    }

    if rf.currentTerm < reply.Term {
        rf.becomeFollowerL(reply.Term)
        return
    }

    if reply.Success {
        rf.nextIndex[server] = max(rf.nextIndex[server], args.PrevLogIndex + len(args.Entries) + 1)
        rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex + len(args.Entries))
        rf.leaderCommitL()
    } else {
        if rf.nextIndex[server] != args.PrevLogIndex + 1 {
            return
        }

        if reply.ConflictTerm != -1 {
            index := -1
            for i := reply.ConflictIndex; i > rf.log.LastIncludedIndex; i-- {
                if rf.log.at(i).Term == reply.ConflictTerm {
                    index = i
                    break
                }
            }

            if index == -1 {
                rf.nextIndex[server] = reply.ConflictIndex
            } else {
                rf.nextIndex[server] = index
            }
        } else {
            if reply.ConflictIndex != -1 {
                rf.nextIndex[server] = reply.ConflictIndex
            }
        }
    }
}

func (rf *Raft) sendAppend(server int) {
    rf.mu.Lock()

    if rf.state != LEADER {
        rf.mu.Unlock()
        return
    }

    next := min(rf.nextIndex[server], rf.log.lastIndex() + 1)

    if next <= rf.log.LastIncludedIndex {
        rf.mu.Unlock()
        rf.sendSnapshot(server)
        return
    }


    args := &AppendEntriesArgs{
        Term : rf.currentTerm,
        LeaderId : rf.me,
        LeaderCommit : rf.commitIndex,
        PrevLogIndex : next - 1,
        PrevLogTerm : rf.log.at(next - 1).Term,
    }

    if next <= rf.log.lastIndex() {
        args.Entries = make([]Entry, rf.log.lastIndex() + 1 - next)
        copy(args.Entries, rf.log.slice(next))
    }

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

    e := Entry{rf.currentTerm, command}
    rf.log.append(rf.log.lastIndex(), e)
    rf.persist()
    rf.sendAppendsL()

    return rf.log.lastIndex(), rf.currentTerm, true
}

func (rf *Raft) applier() {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    rf.lastApplied = max(rf.lastApplied, rf.log.LastIncludedIndex)
    rf.commitIndex = max(rf.commitIndex, rf.log.LastIncludedIndex)

    for !rf.killed() {
        if rf.lastApplied + 1 <= rf.commitIndex && rf.lastApplied + 1 <= rf.log.lastIndex() {
            rf.lastApplied++
            am := ApplyMsg{
                CommandValid : true,
                Command : rf.log.at(rf.lastApplied).Command,
                CommandIndex : rf.lastApplied,
            }

            DPrintf("[apply command] %v index:%v\n", rf.me, rf.lastApplied)

            rf.mu.Unlock()
            rf.applyCh <- am
            rf.mu.Lock()
        } else if rf.waitingSnapshot != nil {
            am := ApplyMsg{
                SnapshotValid : true,
                CommandValid : false,
                Snapshot : rf.waitingSnapshot,
                SnapshotTerm : rf.waitingSnapshotTerm,
                SnapshotIndex : rf.waitingSnapshotIndex,
            }
            rf.waitingSnapshot = nil

            DPrintf("[apply snapshot] %v index:%v\n", rf.me, am.SnapshotIndex)

            rf.mu.Unlock()
            rf.applyCh <- am
            rf.mu.Lock()
        } else {
            rf.applyCond.Wait()
        }
    }
}
