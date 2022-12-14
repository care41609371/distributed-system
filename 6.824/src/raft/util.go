package raft

import "log"

// Debugging
const Debugging = false

func Debug(format string, a ...interface{}) {
	if Debugging {
		log.Printf(format, a...)
	}
}
