package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	IDLE        = "IDLE"        // waiting to be assigned
	IN_PROGRESS = "IN_PROGRESS" // already assigned to a worker
	COMPLETED   = "COMPLETED"   // completed

	TASK_TYPE_MAP    = "TASK_TYPE_MAP"
	TASK_TYPE_REDUCE = "TASK_TYPE_REDUCE"
	TASK_TYPE_IDLE   = "TASK_TYPE_IDLE" // informs the worker to wait
	TASK_TYPE_KILL   = "TASK_TYPE_KILL" // informs the worker to terminate because all tasks are completed
)

type TaskInfo struct {
	taskType  string    // task type
	status    string    // task status
	startTime time.Time // task start time (when it was assigned to a worker)
	fileName  string    // file name
}

type Coordinator struct {
	// Your definitions here.
	tasks []TaskInfo
	lock  sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// Assign a task to a worker machine:
// (1) If there are idle tasks, assign a task to the worker.
// (2) If there are in-progress tasks but no idle tasks, tell the worker to wait for future tasks.
// (3) If all tasks are completed, the worker may terminate.
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Find unassigned task for worker
	allCompleted := true
	for i := 0; i < len(c.tasks); i++ {
		if c.tasks[i].status == IDLE {
			// Found unassigned task
			reply.FileName = c.tasks[i].fileName
			reply.TaskType = c.tasks[i].taskType

			c.tasks[i].status = IN_PROGRESS
			c.tasks[i].startTime = time.Now()

			fmt.Printf("[Coordinator] Assigns task %v (%v) to worker, with file %v and start time %v\n", i, reply.TaskType, reply.FileName, c.tasks[i].startTime)
			return nil
		}

		if c.tasks[i].status != COMPLETED {
			allCompleted = false
		}
	}

	if allCompleted == false {
		// Found uncompleted task: worker cannot terminate yet
		reply.TaskType = TASK_TYPE_IDLE
	} else {
		// All tasks are completed: worker can terminate
		reply.TaskType = TASK_TYPE_KILL
	}

	fmt.Printf("[Coordinator] Assigns task of type %v to worker\n", reply.TaskType)
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

	// Set up Map tasks
	c.tasks = make([]TaskInfo, 0)
	for i := 0; i < len(files); i++ {
		// For every input file, create a Map task
		c.tasks = append(c.tasks, TaskInfo{
			taskType:  TASK_TYPE_MAP,
			status:    IDLE,
			fileName:  files[i],
			startTime: time.Now(),
		})
	}

	c.server()
	return &c
}
