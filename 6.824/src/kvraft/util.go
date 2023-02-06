package kvraft

import "log"

const Debug = false

func DPrintf(format string, a ...interface{}) (n, int, err error) {
    if Debug {
        log.SetFlags(log.Lmicroseconds)
        log.Printf(format, a...)
    }
    return
}

func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
