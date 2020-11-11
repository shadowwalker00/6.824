package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId int
}

// Add your RPC definitions here.
type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	TaskObj Task
}

type ReportTaskArgs struct {
	Done     bool
	WorkerId int
	TaskObj     Task
}

type ReportTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
