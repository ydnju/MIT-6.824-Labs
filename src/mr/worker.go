package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	PollTask()
}

func PollTask() {

	// declare an argument structure.
	canExit := false
	for canExit != true {
		args := MRJobArgs{}

		// declare a reply structure.
		reply := MRJobReply{}

		// send the RPC request, wait for the reply.
		call("Coordinator.AssignJob", &args, &reply)

		fmt.Printf("Job Type :%v\n", reply.Type)
		switch reply.Type {
		case CanExit:
			canExit = true
		case Wait:
			time.Sleep(100 * time.Millisecond)
		case MapJob:
			fmt.Printf("Do Map Job with File %v\n", reply.File)
			// Do Real Map Job
			NotifyMapJobDone(reply.Id)
		default:
			fmt.Printf("Unknown Job Type")
		}
	}

	fmt.Println("All Jobs Done, Exit Now!")
}

func NotifyMapJobDone(id int) {
	args := MRJobArgs{Id: id, Type: MapJob}

	// declare a reply structure.
	reply := MRJobReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.SubmitJob", &args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
