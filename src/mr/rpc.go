package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType int
	TaskId   int
	FileName string
	NReduce  int
	NMap     int
}

type ReportTaskArgs struct {
	TaskId   int
	TaskType int
}

type ReportTaskReply struct {
}
