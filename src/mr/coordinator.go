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
	DONE_PHASE = "DONE_PHASE"

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
	taskId    int		// task id
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
func (c *Coordinator) assignTaskInternal(args *AssignTaskArgs, reply *AssignTaskReply) error {
	// Find idle task for worker
	for i := 0; i < len(c.tasks); i++ {
		if c.tasks[i].status == IDLE {
			// Found idle task; assign to worker
			reply.TaskId = c.tasks[i].taskId
			reply.FileName = c.tasks[i].fileName
			reply.TaskType = c.tasks[i].taskType
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap

			c.tasks[i].status = IN_PROGRESS
			c.tasks[i].startTime = time.Now()

			fmt.Printf("[Coordinator] Assigns task %v (%v) to worker, with file %v and start time %v\n", reply.TaskId, reply.TaskType, reply.FileName, c.tasks[i].startTime)
			return nil
		} else if c.tasks[i].status == IN_PROGRESS {
			// If the task is in progress, check if it has timed out
			// Timeout is set to 10 seconds according to the lab spec
			duration := time.Now().Sub(c.tasks[i].startTime)
			if duration.Seconds() > 10 {
				// Has timed out; assign to worker
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
	}

	// No idle task was found; tell worker to wait
	reply.TaskType = TASK_TYPE_IDLE

	fmt.Printf("[Coordinator] Assigns task of type %v to worker\n", reply.TaskType)
	return nil
}

// Assign a task to a worker machine
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch c.currentPhase {
	case MAP_PHASE:
		return c.assignTaskInternal(args, reply)
	case REDUCE_PHASE:
		return c.assignTaskInternal(args, reply)
	case DONE_PHASE:
		// All tasks are completed; tell worker to terminate
		reply.TaskType = TASK_TYPE_KILL
		fmt.Printf("[Coordinator] Notifies worker to terminate\n")
	}

	return nil
}

// Mark a task as completed by a worker.
func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	completedTaskId := args.CompletedTaskId
	completedTaskType := args.CompletedTaskType
	fmt.Printf("[Coordinator] Receives a completed task: type=%v, id=%v\n", completedTaskType, completedTaskId)

	// Sanity check
	if completedTaskType != TASK_TYPE_MAP && completedTaskType != TASK_TYPE_REDUCE {
		log.Fatalf("[Coordinator] Receives a completed task of type %v", completedTaskType)
	}

	switch c.currentPhase {
	case MAP_PHASE:
		if completedTaskType == TASK_TYPE_REDUCE {
			// Error
			log.Fatalf("System is in MAP_PHASE, but received a completed task of type %v", completedTaskType)
		}

		if c.tasks[completedTaskId].status == COMPLETED {
			// Disregard current update
			return nil
		}

		// Mark map task as completed
		c.tasks[completedTaskId].status = COMPLETED
		c.nMapCompleted++

		// Check if we are ready to move into REDUCE_PHASE
		if c.nMapCompleted == c.nMap {
			c.currentPhase = REDUCE_PHASE

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
		} else if c.nMapCompleted > c.nMap {
			// Error
			log.Fatalf("Invalid nMapCompleted value: nMapCompleted=%v, nMap=%v", c.nMapCompleted, c.nMap)
		}
	case REDUCE_PHASE:
		if completedTaskType == TASK_TYPE_MAP {
			// We are already in REDUCE_PHASE; the completed map task is irrelevant
			return nil
		}

		if c.tasks[completedTaskId].status == COMPLETED {
			// Disregard current update
			return nil
		}

		// Mark map task as completed
		c.tasks[completedTaskId].status = COMPLETED
		c.nReduceCompleted++

		// Check if we are ready to move into DONE_PHASE
		if c.nReduceCompleted == c.nReduce {
			c.currentPhase = DONE_PHASE
		}
	}

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
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.currentPhase == DONE_PHASE
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
