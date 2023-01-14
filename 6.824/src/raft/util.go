package raft

import "log"

// Debugging
// const Debugging = true
const Debugging = false

func Debug(format string, a ...interface{}) {
	if Debugging {
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
