package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type Args struct{}

type WorkerRegisterReq struct {
	EntryPoint string
}

type WorkerRegisterResp struct {
	WorkerId int
	Success  bool
}

type MapTask struct {
	Id          int
	FilePath    string
	Parallelism int
}

type MapTaskResp struct {
	Id           int
	Success      bool
	ShuffleFiles []string
}

type ReduceTask struct {
	Id             int
	MapParallelism int
}

type ReduceTaskResp struct {
	Id      int
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func workerSock(workerId int) string {
	s := "/var/tmp/5840-mr-worker-" + strconv.Itoa(workerId)
	return s
}
