package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	TASK_IDLE        = "TASK_IDLE"
	TASK_IN_PROGRESS = "TASK_IN_PROGRESS"
	TASK_COMPLETED   = "TASK_COMPLETED"

	TASK_TYPE_MAP    = "TASK_TYPE_MAP"
	TASK_TYPE_REDUCE = "TASK_TYPE_REDUCE"
)

type MRTask struct {
	taskType string // task type: map or reduce
	status   string // task status
	file     string // file name
	worker   int    // -1 if unassigned to any worker
}

type Coordinator struct {
	// Your definitions here.
	mapTasks    []MRTask
	reduceTasks []MRTask

	// Lock
	lock sync.RWMutex

	// TODO: need to track list of workers?
	// Have all workers ping coordinator upon startup
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Find unassigned task
	for i := 0; i < len(c.mapTasks); i++ {
		if c.mapTasks[i].status == TASK_IDLE {
			reply.FileName = c.mapTasks[i].file
			reply.TaskType = TASK_TYPE_MAP

			c.mapTasks[i].status = TASK_IN_PROGRESS
			c.mapTasks[i].worker = 0 // TODO: how to identify worker
			return nil
		}
	}

	for i := 0; i < len(c.reduceTasks); i++ {
		if c.reduceTasks[i].status == TASK_IDLE {
			reply.FileName = c.reduceTasks[i].file
			reply.TaskType = TASK_TYPE_REDUCE

			c.reduceTasks[i].status = TASK_IN_PROGRESS
			c.reduceTasks[i].worker = 0 // TODO: how to identify worker
			return nil
		}
	}

	// If we get here, we have no unassigned tasks
	reply.FileName = ""
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTasks = make([]MRTask, len(files))
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = MRTask{
			taskType: TASK_TYPE_MAP,
			status:   TASK_IDLE,
			file:     files[i],
			worker:   -1,
		}
	}

	c.reduceTasks = make([]MRTask, nReduce)
	for i := 0; i < len(files); i++ {
		c.reduceTasks[i] = MRTask{
			taskType: TASK_TYPE_REDUCE,
			status:   TASK_IDLE,
			file:     "",
			worker:   -1,
		}
	}

	c.server()
	return &c
}
