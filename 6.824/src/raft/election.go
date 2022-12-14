package raft

import (
    "time"
    "math/rand"
)

const electionTimeout = 1 * time.Second

func (rf *Raft) setElectionTimeL() {
    t := time.Now()
    t = t.Add(electionTimeout)
    ms := rand.Int63() % 300
    t = t.Add(time.Duration(ms) * time.Millisecond)
    rf.electionTime = t
}

func (rf *Raft) becomeFollowerL(term int) {
    rf.state = FOLLOWER
    rf.currentTerm = term
    rf.votedFor = -1
}

func (rf *Raft) becomeLeaderL() {
    Debug("%v in term %v become leader ------------\n", rf.me, rf.currentTerm)
    rf.state = LEADER
    for i := 0; i < len(rf.nextIndex); i++ {
        rf.nextIndex[i] = len(rf.log)
    }
    rf.sendAppendsL(true)
}

func (rf *Raft) collectVote(server int, args *RequestVoteArgs, votes *int) {
    reply := &RequestVoteReply{}
    ok := rf.sendRequestVote(server, args, reply)

    if ok {
        rf.mu.Lock()
        if reply.VoteGranted {
            *votes++
            Debug("%v receive %v vote in term %v   totol votes : %v\n", rf.me, server, rf.currentTerm, *votes)
            if *votes > len(rf.peers) / 2 && rf.state == CANDIDATE && rf.currentTerm == args.Term {
                rf.becomeLeaderL()
            }
        }
        if reply.Term > rf.currentTerm {
            rf.becomeFollowerL(reply.Term)
        }
        rf.mu.Unlock()
    }
}

func (rf *Raft) startElectionL() {
    votes := 1
    rf.state = CANDIDATE
    rf.votedFor = rf.me
    rf.currentTerm++

    args := &RequestVoteArgs{rf.currentTerm, rf.me, 0, 0}
    if len(rf.log) > 0 {
        args.LastLogIndex = len(rf.log) - 1
        args.LastLogTerm = rf.log[len(rf.log) - 1].Term
    }

    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me {
            go rf.collectVote(i, args, &votes)
        }
    }
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
    for rf.killed() == false {
        // Your code here to check if a leader election should
        // be started and to randomize sleeping time using
        rf.mu.Lock()

        if rf.state == LEADER {
            rf.setElectionTimeL()
            rf.sendAppendsL(true)
        }

        if time.Now().After(rf.electionTime) {
            rf.setElectionTimeL()
            Debug("%v in term %v start eletion\n", rf.me, rf.currentTerm + 1)
            rf.startElectionL()
        }

        rf.mu.Unlock()
        time.Sleep(50 * time.Millisecond)
    }
}
