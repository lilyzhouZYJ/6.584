package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
	"encoding/json"
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
// Takes the map and reduce functions as arguments.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// Send an RPC to the coordinator asking for a task
		assignTaskReply := CallAssignTask()

		switch assignTaskReply.TaskType {
		case TASK_TYPE_MAP:
			// Open the file
			fileName := assignTaskReply.FileName
			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("Cannot open %v", fileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Cannot read %v", fileName)
			}
			file.Close()

			// Call Map function; returns []KeyValue
			mapResult := mapf(fileName, string(content))

			// Process mapResult and store to intermediate files
			/*
			The map phase should divide the intermediate keys into buckets 
			for nReduce reduce tasks, where nReduce is the number of reduce 
			tasks -- the argument that main/mrcoordinator.go passes to 
			MakeCoordinator(). Each mapper should create nReduce intermediate
			files for consumption by the reduce tasks.
			*/

			nReduce := assignTaskReply.NReduce
			mapResultBuckets := make([][]KeyValue, nReduce)

			for _, kv := range mapResult {
				// For each KeyValue pair, determine which reduce task it is assigned to (using hash)
				key := kv.Key

				reduceTaskId := ihash(key) % nReduce
				mapResultBuckets[reduceTaskId] = append(mapResultBuckets[reduceTaskId], kv)
			}

			// Store to intermediate files
			// TODO: need atomic rename
			mapTaskId := assignTaskReply.TaskId
			for reduceTaskId, keyValuePairs := range mapResultBuckets {
				// Create file: mr-x-y, where x is map task id and y is reduce task id
				fileName := fmt.Sprintf("mr-%v-%v", mapTaskId, reduceTaskId)
				file, err := os.Create(fileName)
				if err != nil {
					log.Fatalf("Create intermediate file %v failed", file)
				}

				enc := json.NewEncoder(file)
				for _, kv := range keyValuePairs {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("Encoding KV %v into intermediate file %v failed", kv, fileName)
					}
				}
			}

			// Notify the coordinator that the task is completed
			completeTaskReply := CallCompleteTask(assignTaskReply.TaskId, assignTaskReply.TaskType)
			// TODO: What to do if error?
			if completeTaskReply != nil {
				log.Fatalf("Error in CallCompleteTask")
			}

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

// Sends an RPC to notify the coordinator that a task is completed.
func CallCompleteTask(completedTaskId int, completedTaskType string) *CompleteTaskReply {
	args := CompleteTaskArgs{CompletedTaskId: completedTaskId, CompletedTaskType: completedTaskType}
	reply := CompleteTaskReply{}

	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
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
