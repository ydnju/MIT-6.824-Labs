package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskDone    = "done"
	TaskStarted = "started"
)

type Task struct {
	Id        int
	StartTime time.Time
	Status    string
}

type Coordinator struct {
	// Your definitions here.
	TaskCount int
	Files     []string
	MapTasks  map[string]*Task
	NReduce   int
	Lock      sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignJob(args *MRJobArgs, reply *MRJobReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	unfinishedMapTask := 0
	for _, f := range c.Files {
		task, ok := c.MapTasks[f]
		if ok {
			if task.Status == TaskDone {
				continue
			} else if task.Status == TaskStarted {
				unfinishedMapTask++
				duration := time.Since(task.StartTime)
				if duration.Seconds() > 10.0 {
					// Consider the task failed, abort and restart
					c.TaskCount++
					c.MapTasks[f] = &Task{Id: c.TaskCount, StartTime: time.Now(), Status: TaskStarted}
					reply.File = f
					reply.Type = MapJob
					reply.Id = c.TaskCount
					return nil
				}
			}
		} else {
			c.TaskCount++
			c.MapTasks[f] = &Task{Id: c.TaskCount, StartTime: time.Now(), Status: TaskStarted}
			reply.File = f
			reply.Type = MapJob
			reply.Id = c.TaskCount
			return nil
		}
	}

	if unfinishedMapTask > 0 {
		reply.Type = Wait
		return nil
	}

	// TODO: Add reduce job logic
	reply.Type = CanExit
	return nil
}

func (c *Coordinator) SubmitJob(args *MRJobArgs, reply *MRJobReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	switch args.Type {
	case MapJob:
		for _, v := range c.MapTasks {
			if v.Id == args.Id {
				duration := time.Since(v.StartTime)
				if duration.Seconds() < 10.0 {
					v.Status = TaskDone
				}
			}
		}
		return nil
	default:
		return nil
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{Files: files, NReduce: nReduce}
	c.MapTasks = make(map[string]*Task)

	// Your code here.

	c.server()
	return &c
}
