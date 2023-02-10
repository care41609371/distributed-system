package raft

import (
    "reflect"
    "fmt"
)

type Entry struct {
    Term int
    Command interface{}
}

type logEntry struct {
    Entries []Entry
    LastIncludedIndex int
    LastIncludedTerm int
}

func makeLog() logEntry {
    l := logEntry{}
    l.LastIncludedIndex = 0
    l.LastIncludedTerm = 0
    l.Entries = []Entry{}
    return l
}

func (l *logEntry) lastIndex() int {
    return l.LastIncludedIndex + len(l.Entries)
}

func (l *logEntry) rebuild(snapshotIndex, snapshotTerm int) {
    if snapshotIndex >= l.lastIndex() {
        l.Entries = []Entry{}
        l.LastIncludedIndex = snapshotIndex
        l.LastIncludedTerm = snapshotTerm
    } else {
        l.Entries = l.Entries[snapshotIndex - l.LastIncludedIndex : ]
        l.LastIncludedIndex = snapshotIndex
        l.LastIncludedTerm = snapshotTerm
    }
}

func (l *logEntry) at(index int) *Entry {
    if index > l.LastIncludedIndex && index <= l.lastIndex() {
        return &l.Entries[index - l.LastIncludedIndex - 1]
    } else if index == l.LastIncludedIndex {
        return &Entry{l.LastIncludedTerm, nil}
    } else {
        s := fmt.Sprintf("%v log index out of bound! %v-%v\n", index, l.LastIncludedIndex, l.lastIndex())
        panic(s)
    }
}

func (l *logEntry) cutBack(index int) {
    if index > l.lastIndex() {
        return
    }
    l.Entries = l.Entries[ : index - l.LastIncludedIndex - 1]
}

func (l *logEntry) append(prevLogIndex int, es ...Entry) {
    ok := true

    if prevLogIndex + len(es) <= l.lastIndex() {
        for i, j := prevLogIndex + 1, 0; ; i++ {
            if i > prevLogIndex + len(es) {
                ok = false
                break
            }
            if !reflect.DeepEqual(l.Entries[i - l.LastIncludedIndex - 1], es[j]) {
                break
            }
            j++
        }
    }

    if ok {
        l.cutBack(prevLogIndex + 1)
        l.Entries = append(l.Entries, es...)
    }
}

func (l *logEntry) slice(begin int) []Entry {
    return l.Entries[begin - l.LastIncludedIndex - 1 : ]
}
