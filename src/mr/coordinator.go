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
	// Phases
	MAP_PHASE = "MAP_PHASE"
	REDUCE_PHASE = "REDUCE_PHASE"

	// Task states
	IDLE        = "IDLE"        // waiting to be assigned
	IN_PROGRESS = "IN_PROGRESS" // already assigned to a worker
	COMPLETED   = "COMPLETED"   // completed

	// Task types returned to the workers via RPC
	TASK_TYPE_MAP    = "TASK_TYPE_MAP"
	TASK_TYPE_REDUCE = "TASK_TYPE_REDUCE"
	TASK_TYPE_IDLE   = "TASK_TYPE_IDLE" // informs the worker to wait
	TASK_TYPE_KILL   = "TASK_TYPE_KILL" // informs the worker to terminate because all tasks are completed
)

// Defines a task
type TaskInfo struct {
	TaskId    int		// task id
	taskType  string    // task type
	status    string    // task status
	startTime time.Time // task start time (when it was assigned to a worker)
	fileName  string    // file name
}

// Defines the coordinator
type Coordinator struct {
	nMap				int			// total number of map tasks
	nReduce				int			// total number of reduce tasks
	nMapCompleted		int			// number of completed map tasks
	nReduceCompleted	int			// number of completed reduce tasks

	currentPhase 	string			// current phase of the system: MAP_PHASE or REDUCE_PHASE

	tasks 			[]TaskInfo		// list of all pending tasks
	lock  			sync.RWMutex	// lock
}

// Assign task to worker
func (c *Coordinator) assignTaskInternal(reply *AssignTaskReply) error {
	// Find idle task for worker
	for i := 0; i < len(c.tasks); i++ {
		if c.tasks[i].status == IDLE {
			// Found idle task; respond to worker
			reply.TaskId = c.tasks[i].taskId
			reply.FileName = c.tasks[i].fileName
			reply.TaskType = c.tasks[i].taskType
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap

			c.tasks[i].status = IN_PROGRESS
			c.tasks[i].startTime = time.Now()

			fmt.Printf("[Coordinator] Assigns task %v (%v) to worker, with file %v and start time %v\n", reply.TaskId, reply.TaskType, reply.FileName, c.tasks[i].startTime)
			return nil
		}
	}
	// No idle task was found; tell worker to wait
	reply.TaskType = TASK_TYPE_IDLE

	fmt.Printf("[Coordinator] Assigns task of type %v to worker\n", reply.TaskType)
	return nil
}

// Assign a task to a worker machine:
// (a) If in MAP_PHASE:
//     1. If there is an idle map task, assign the task to the worker.
//     2. If there is no idle map task, tell the worker to wait.
//     3. If all map tasks are completed, set phase to REDUCE_PHASE and initialize reduce tasks, then assign a task to worker.
// (b) If in REDUCE_PHASE:
//     1. If there is an idle reduce task, assign the task to the worker.
//     2. If there is no idle reduce task, tell the worker to wait.
//     3. If all reduce tasks are completed, tell the worker to terminate.
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// (a) If in MAP_PHASE:
	if c.currentPhase = MAP_PHASE {
		if c.nMapCompleted < c.nMap {
			// Assign map task to worker
			return c.assignTaskInternal(reply)
		} else if c.nMapCompleted == c.nMap {
			// All map tasks are completed; enter REDUCE_PHASE
			c.phase = REDUCE_PHASE

			// Initialize reduce tasks
			c.tasks = make([]TaskInfo, 0)
			for i := 0; i < c.nReduce; i++ {
				c.tasks = append(c.tasks, TaskInfo{
					taskId:	   i,
					taskType:  TASK_TYPE_REDUCE,
					status:    IDLE,
					fileName:  "",
					startTime: time.Now(),
				})
			}
			// Assign reduce task to worker
			return c.assignTaskInternal(reply)
		} else {
			// Error
			log.fatalf("Incorrect parameters: nMapCompleted=%v, nMap=%v", nMapCompleted, nMap)
		}
	} else if c.currentPhase = REDUCE_PHASE {
		if c.nReduceCompleted < c.nReduce {
			// Assign reduce task to worker
			return c.assignTaskInternal(reply)
		} else if c.nReduceCompleted == c.nReduce {
			// All reduce tasks are completed; tell worker to terminate
			reply.taskType = TASK_TYPE_KILL
			
			fmt.Printf("[Coordinator] Assigns task of type %v to worker\n", reply.TaskType)
			return nil
		} else {
			// Error
			log.fatalf("Incorrect parameters: nReduceCompleted=%v, nReduce=%v", nReduceCompleted, nReduce)
		}
	}
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
	// Initialize coordinator
	c := Coordinator{}
	c.currentPhase = MAP_PHASE
	c.nReduce = nReduce
	c.nMap = len(files)
	c.nMapCompleted = 0
	c.nReduceCompleted = 0

	// Set up Map tasks
	c.tasks = make([]TaskInfo, 0)
	for i := 0; i < len(files); i++ {
		// For every input file, create a Map task
		c.tasks = append(c.tasks, TaskInfo{
			taskId:	   i,
			taskType:  TASK_TYPE_MAP,
			status:    IDLE,
			fileName:  files[i],
			startTime: time.Now(),
		})
	}

	c.server()
	return &c
}
