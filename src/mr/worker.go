package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	PollTask(mapf, reducef)
}

func PollTask(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// declare an argument structure.
	canExit := false
	for canExit != true {
		args := MRJobArgs{}

		// declare a reply structure.
		reply := MRJobReply{}

		// send the RPC request, wait for the reply.
		call("Coordinator.AssignJob", &args, &reply)

		switch reply.Type {
		case CanExit:
			canExit = true
		case Wait:
			time.Sleep(100 * time.Millisecond)
		case MapJob:
			DoMap(reply.FileIndex, reply.Files, reply.NReduce, mapf)
			NotifyJobDone(reply.Id, MapJob)
		case ReduceJob:
			DoReduce(reply.Partition, reply.NReduce, reply.Files, reducef)
			NotifyJobDone(reply.Id, ReduceJob)
		default:
			fmt.Printf("Unknown Job Type")
		}
	}

	fmt.Println("All Jobs Done, Exit Now!")
}

func NotifyJobDone(id int, JobType string) {
	args := MRJobArgs{Id: id, Type: JobType}

	// declare a reply structure.
	reply := MRJobReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.SubmitJob", &args, &reply)
}

func DoJobMock() {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(15) // n will be between 0 and 10
	fmt.Printf("Sleeping %d seconds...\n", n)
	time.Sleep(time.Duration(n) * time.Second)
}

func DoMap(fileIndex int, files []string, nReduce int, mapf func(string, string) []KeyValue) {
	filename := files[fileIndex]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate := []KeyValue{}
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	partitions := make([][]KeyValue, nReduce)
	for j := 0; j < nReduce; j++ {
		partitions[j] = []KeyValue{}
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		part := ihash(intermediate[i].Key) % nReduce
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		for k := i; k < j; k++ {
			partitions[part] = append(partitions[part], intermediate[k])
		}

		i = j
	}

	for i := 0; i < nReduce; i++ {
		tmpfile, err := ioutil.TempFile("", "example")
		if err != nil {
			log.Fatal(err)
		}

		enc := json.NewEncoder(tmpfile)
		for _, kv := range partitions[i] {
			enc.Encode(&kv)
		}

		tmpfile.Close()
		oname := getIntermediateFileName(fileIndex, i)
		os.Rename(tmpfile.Name(), oname)
	}
}

func DoReduce(partition int, nReduce int, files []string, reducef func(string, []string) string) {
	kva := []KeyValue{}

	for idx := range files {
		filename := getIntermediateFileName(idx, partition)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	tmpfile, err := ioutil.TempFile("", "example")
	if err != nil {
		log.Fatal(err)
	}

	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	tmpfile.Close()
	oname := fmt.Sprintf("mr-out-%v", partition)
	os.Rename(tmpfile.Name(), oname)
}

func getIntermediateFileName(mapId int, partition int) string {
	return fmt.Sprintf("mr-%v-%v", mapId, partition)
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
