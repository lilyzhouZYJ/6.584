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
	"sort"
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

// A map task has been received; process it
func processMapTask(mapf func(string, string) []KeyValue, taskId int, fileName string, nReduce int) {
	fmt.Printf("[Worker %v] Receives MAP task: taskId=%v, fileName=%v\n", os.Getpid(), taskId, fileName)

	// Open the file
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("[Worker %v] Cannot open %v", os.Getpid(), fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[Worker %v] Cannot read %v", os.Getpid(), fileName)
	}
	file.Close()

	// Call Map function; returns []KeyValue
	fmt.Printf("[Worker %v] Calls map function\n", os.Getpid())
	mapResult := mapf(fileName, string(content))

	// Process mapResult and store to intermediate files
	/*
	The map phase should divide the intermediate keys into buckets 
	for nReduce reduce tasks, where nReduce is the number of reduce 
	tasks -- the argument that main/mrcoordinator.go passes to 
	MakeCoordinator(). Each mapper should create nReduce intermediate
	files for consumption by the reduce tasks.
	*/

	mapResultBuckets := make([][]KeyValue, nReduce)

	for _, kv := range mapResult {
		// For each KeyValue pair, determine which reduce task it is assigned to (using hash)
		key := kv.Key

		reduceTaskId := ihash(key) % nReduce
		mapResultBuckets[reduceTaskId] = append(mapResultBuckets[reduceTaskId], kv)
	}

	// Store to intermediate files
	// Note: some intermediate files may be empty, as some reduce tasks may get assigned no KV pairs
	for reduceTaskId, keyValuePairs := range mapResultBuckets {
		// Create temp intermediate file
		file, err := os.CreateTemp("./", "temp*")
		if err != nil {
			log.Fatalf("[Worker %v] Create temp intermediate file failed", os.Getpid())
		}
		fmt.Printf("[Worker %v] Temp intermediate file created: %v\n", os.Getpid(), file.Name())

		// Write to temp file
		enc := json.NewEncoder(file)
		for _, kv := range keyValuePairs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("[Worker %v] Encoding KV %v into temp intermediate file %v failed", os.Getpid(), kv, file.Name())
			}
		}

		// Atomic rename to actual name: mr-x-y, where x is map task id and y is reduce task id
		fileName := fmt.Sprintf("mr-%v-%v", taskId, reduceTaskId)
		err = os.Rename(file.Name(), fileName)
		if err != nil {
			log.Fatalf("[Worker %v] Atomic rename of temp intermediate file to %v failed", os.Getpid(), fileName)
		}
		fmt.Printf("[Worker %v] Intermediate file atomically renamed to %v\n", os.Getpid(), fileName)
	}

	// Notify the coordinator that the task is completed
	fmt.Printf("[Worker %v] Calls coordinator to notify the completion of MAP task: taskId=%v\n", os.Getpid(), taskId)
	completeTaskReply := CallCompleteTask(taskId, TASK_TYPE_MAP)
	// TODO: What to do if error?
	if completeTaskReply == nil {
		log.Fatalf("Error in CallCompleteTask")
	}
}

// A reduce task has been received; process it
func processReduceTask(reducef func(string, []string) string, taskId int, nMap int) {
	fmt.Printf("[Worker %v] Receives REDUCe task: taskId=%v\n", os.Getpid(), taskId)

	// Read each KV pair from each intermediate file into list
	kvList := make([]KeyValue, 0)
	for mapTaskId := 0; mapTaskId < nMap; mapTaskId++ {
		fileName := fmt.Sprintf("mr-%v-%v", mapTaskId, taskId)

		// Open the file
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("[Worker %v] Cannot open %v", os.Getpid(), fileName)
		}

		// Read file into list
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvList = append(kvList, kv)
		}
	}

	// Sort the KV list
	sort.Slice(kvList, func(i, j int) bool {
		return kvList[i].Key > kvList[j].Key
	})

	// Shuffle the KV list: combine into a list of (key, [list of values])
	shuffledKvList := make(map[string][]string)
	for _, kv := range kvList {
		shuffledKvList[kv.Key] = append(shuffledKvList[kv.Key], kv.Value)
	}

	// Create temp file that will become the final output file of this reduce task
	file, err := os.CreateTemp("./", "temp*")
	if err != nil {
		log.Fatalf("[Worker %v] Create temp reduce output file failed", os.Getpid())
	}
	fmt.Printf("[Worker %v] Temp reduce output file created: %v\n", os.Getpid(), file.Name())

	// Feed each (key, [list of values]) into reduce function
	for k, vList := range shuffledKvList {
		reduceResult := reducef(k, vList)
		// Write reduce result to file
		fmt.Fprintf(file, "%v %v\n", k, reduceResult)
	}

	// Atomic rename to mr-out-x, where x is the reduce task id
	fileName := fmt.Sprintf("mr-out-%v", taskId)
	err = os.Rename(file.Name(), fileName)
	if err != nil {
		log.Fatalf("[Worker %v] Atomic rename of temp reduce output file to %v failed", os.Getpid(), fileName)
	}

	// Notify the coordinator that the task is completed
	fmt.Printf("[Worker %v] Calls coordinator to notify the completion of REDUCE task: taskId=%v\n", os.Getpid(), taskId)
	completeTaskReply := CallCompleteTask(taskId, TASK_TYPE_REDUCE)
	// TODO: What to do if error?
	if completeTaskReply == nil {
		log.Fatalf("Error in CallCompleteTask")
	}
}

// main/mrworker.go calls this function.
// Takes the map and reduce functions as arguments.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	fmt.Printf("[Worker %v] Starting\n", os.Getpid())

	for {
		// Send an RPC to the coordinator asking for a task
		fmt.Printf("[Worker %v] Calls coordinator to ask for task\n", os.Getpid())
		assignTaskReply := CallAssignTask()

		switch assignTaskReply.TaskType {
		case TASK_TYPE_MAP:
			processMapTask(mapf, assignTaskReply.TaskId, assignTaskReply.FileName, assignTaskReply.NReduce)
		case TASK_TYPE_REDUCE:
			processReduceTask(reducef, assignTaskReply.TaskId, assignTaskReply.NMap)
		case TASK_TYPE_IDLE:
			// Wait 3 seconds before asking again
			time.Sleep(3 * time.Second)
		case TASK_TYPE_KILL:
			// Terminate current worker
			fmt.Printf("[Worker %v] Terminates upon receipt of TASK_TYPE_KILL\n", os.Getpid())
			os.Exit(0)
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
