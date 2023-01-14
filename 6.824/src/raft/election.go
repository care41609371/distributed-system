package raft

import (
    "time"
    "math/rand"
)

const electionTimeout = 1000 * time.Millisecond

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
    rf.persist()
}

func (rf *Raft) becomeLeaderL() {
    rf.state = LEADER
    for i := 0; i < len(rf.nextIndex); i++ {
        rf.nextIndex[i] = rf.log.lastIndex() + 1
        rf.matchIndex[i] = 0
    }
    rf.sendAppendsL()
}

func (rf *Raft) collectVote(server int, args *RequestVoteArgs, votes *int) {
    reply := &RequestVoteReply{}
    ok := rf.sendRequestVote(server, args, reply)

    if ok {
        rf.mu.Lock()
        if reply.Term < args.Term {
            rf.mu.Unlock()
            return
        }
        if reply.VoteGranted {
            *votes++
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
    rf.persist()

    args := &RequestVoteArgs{
        rf.currentTerm,
        rf.me,
        rf.log.lastIndex(),
        rf.log.at(rf.log.lastIndex()).Term,
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
            rf.sendAppendsL()
        } else if time.Now().After(rf.electionTime) {
            rf.setElectionTimeL()
            rf.startElectionL()
        }

        rf.mu.Unlock()
        time.Sleep(40 * time.Millisecond)
    }
}
