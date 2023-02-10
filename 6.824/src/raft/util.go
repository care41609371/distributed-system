package raft

import "log"

// Debugging
const Debugging = true

func DPrintf(format string, a ...interface{}) {
	if Debugging {
        log.SetFlags(log.Lmicroseconds)
		log.Printf(format, a...)
	}
}

func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

func min(a, b int) int {
    if a > b {
        return b
    }
    return a
}
