package raft

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//

type InstallSnapshotArgs struct {
    Term int
    LeaderId int
    LastIncludedIndex int
    LastIncludedTerm int
    Data []byte
}

type InstallSnapshotReply struct {
    Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        return
    }

    if args.Term > rf.currentTerm {
        rf.becomeFollowerL(args.Term)
    }

    rf.setElectionTimeL()
    reply.Term = rf.currentTerm

    if args.LastIncludedIndex <= rf.log.LastIncludedIndex || args.LastIncludedIndex <= rf.commitIndex {
        return
    }

    rf.waitingSnapshotIndex = args.LastIncludedIndex
    rf.waitingSnapshotTerm = args.LastIncludedTerm
    rf.waitingSnapshot = args.Data

    rf.applyCond.Broadcast()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
    return ok
}

func (rf *Raft) sendSnapshot(server int) {
    rf.mu.Lock()
    args := &InstallSnapshotArgs{
        Term : rf.currentTerm,
        LeaderId : rf.me,
        LastIncludedIndex : rf.log.LastIncludedIndex,
        LastIncludedTerm : rf.log.LastIncludedTerm,
        Data : rf.persister.ReadSnapshot(),
    }
    rf.mu.Unlock()

    reply := &InstallSnapshotReply{}
    ok := rf.sendInstallSnapshot(server, args, reply)

    if ok {
        rf.mu.Lock()
        defer rf.mu.Unlock()

        if args.Term != rf.currentTerm {
            return
        }

        if reply.Term > rf.currentTerm {
            rf.becomeFollowerL(reply.Term)
            return
        }

        rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex + 1)
        rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
    }
}

func (rf *Raft) CondInstallSnapshot(snapshotTerm int, snapshotIndex int, snapshot []byte) bool {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if snapshotIndex <= rf.log.LastIncludedIndex || snapshotIndex <= rf.commitIndex {
        return false
    }

    rf.log.rebuild(snapshotIndex, snapshotTerm)
    rf.lastApplied = snapshotIndex
    rf.commitIndex = snapshotIndex
    rf.persistStateAndSnapshot(snapshot)

    return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if index <= rf.log.LastIncludedIndex || index > rf.log.lastIndex() {
        return
    }

    rf.log.rebuild(index, rf.log.at(index).Term)
    rf.persistStateAndSnapshot(snapshot)
}

