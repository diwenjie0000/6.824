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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}
type TaskApply struct {
	WorkerId int
}
type MapTaskComplete struct {
	WorkerId int
	TaskId   int
}
type ReduceTaskComplete struct {
	WorkerId int
	TaskId   int
}
type TaskDistribute struct {
	TaskId   int
	TaskType int
	FileName string
}
type PingArgs struct {
	WorkerId int
}
type PingReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
