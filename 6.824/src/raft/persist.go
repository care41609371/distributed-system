package raft

import (
    "bytes"
    "6.824/labgob"
)

func (rf *Raft) persistedState() []byte {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    data := w.Bytes()

    return data
}

//
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    rf.persister.SaveRaftState(rf.persistedState())
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
    rf.persister.SaveStateAndSnapshot(rf.persistedState(), snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var currentTerm, votedFor int
    var log logEntry
    if d.Decode(&currentTerm) != nil ||
        d.Decode(&votedFor) != nil ||
        d.Decode(&log) != nil {
        // error...
    } else {
        rf.currentTerm = currentTerm
        rf.votedFor = votedFor
        rf.log = log
    }
}

