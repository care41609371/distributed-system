package raft

type Entry struct {
    Term int
    Command interface{}
}

type logEntry struct {
    entries []Entry
    index0 int
}

func makeLog() logEntry {
    l := logEntry{}
    l.index0 = 0
    l.entries = make([]Entry, 1)
    l.entries[0] = Entry{0, nil}
    return l
}

func (l *logEntry) firstIndex() int {
    return l.index0
}

func (l *logEntry) lastIndex() int {
    return l.index0 + len(l.entries) - 1
}

func (l *logEntry) at(index int) *Entry {
    index -= l.index0
    if index >= l.firstIndex() && index <= l.lastIndex() {
       return &l.entries[index]
    } else {
        return &Entry{-1, nil}
    }
}

func (l *logEntry) append(prevLogIndex int, es ...Entry) {
    for i, j := prevLogIndex + 1, 0; j < len(es); j++ {
        if i <= l.lastIndex() {
            l.entries[i - l.index0] = es[j]
        } else {
            l.entries = append(l.entries, es[j])
        }
        i++
    }
}

func (l *logEntry) slice(begin int, end int) []Entry {
    return l.entries[begin - l.index0 : end - l.index0]
}
