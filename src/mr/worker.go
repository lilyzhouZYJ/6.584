package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		// Send an RPC to the coordinator asking for a task.
		assignTaskReply := CallAssignTask()
		switch assignTaskReply.TaskType {
		case TASK_TYPE_MAP:
			// Open the file
			fileName := assignTaskReply.FileName
			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("cannot open %v", fileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", fileName)
			}
			file.Close()

			// Call Map function
			mapResult := mapf(fileName, string(content))

			// TODO: process mapResult and store to intermediate files
		case TASK_TYPE_REDUCE:
			// TODO: perform Reduce task
		case TASK_TYPE_IDLE:
			// Wait 3 seconds before asking again
			time.Sleep(3 * time.Second)
		case TASK_TYPE_KILL:
			// TODO: terminate current worker
		}
	}
}

// Sends an RPC to request a task from the coordinator.
func CallAssignTask() *AssignTaskReply {
	args := AssignTaskArgs{}
	reply := AssignTaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("reply.FileName: %v, reply.TaskType: %v\n", reply.FileName, reply.TaskType)
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
