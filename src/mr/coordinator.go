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
	TaskCount   int
	Files       []string
	MapTasks    map[string]*Task
	ReduceTasks map[int]*Task
	NReduce     int
	Lock        sync.Mutex
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
	for idx, f := range c.Files {
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
					reply.FileIndex = idx
					reply.Type = MapJob
					reply.Id = c.TaskCount
					reply.NReduce = c.NReduce
					reply.Files = c.Files
					return nil
				}
			}
		} else {
			c.TaskCount++
			c.MapTasks[f] = &Task{Id: c.TaskCount, StartTime: time.Now(), Status: TaskStarted}
			reply.FileIndex = idx
			reply.Type = MapJob
			reply.Id = c.TaskCount
			reply.NReduce = c.NReduce
			reply.Files = c.Files
			return nil
		}
	}

	if unfinishedMapTask > 0 {
		reply.Type = Wait
		return nil
	}

	unfinishedReduceTasks := 0
	for i := 0; i < c.NReduce; i++ {
		task, ok := c.ReduceTasks[i]
		if ok {
			if task.Status == TaskDone {
				continue
			} else if task.Status == TaskStarted {
				unfinishedReduceTasks++
				duration := time.Since(task.StartTime)
				if duration.Seconds() > 10.0 {
					// Consider the task failed, abort and restart

					c.TaskCount++
					c.ReduceTasks[i] = &Task{Id: c.TaskCount, StartTime: time.Now(), Status: TaskStarted}
					reply.Partition = i
					reply.Type = ReduceJob
					reply.Id = c.TaskCount
					reply.NReduce = c.NReduce
					reply.Files = c.Files
					return nil
				}
			}
		} else {
			c.TaskCount++
			c.ReduceTasks[i] = &Task{Id: c.TaskCount, StartTime: time.Now(), Status: TaskStarted}
			reply.Partition = i
			reply.Type = ReduceJob
			reply.Id = c.TaskCount
			reply.NReduce = c.NReduce
			reply.Files = c.Files
			return nil
		}
	}

	if unfinishedReduceTasks > 0 {
		reply.Type = Wait
		return nil
	}

	reply.Type = CanExit
	return nil
}

func (c *Coordinator) SubmitJob(args *MRJobArgs, reply *MRJobReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	switch args.Type {
	case MapJob:
		for _, v := range c.MapTasks {
			v.AcceptTaskOrNot(args.Id)
		}
		return nil
	case ReduceJob:
		for _, v := range c.ReduceTasks {
			v.AcceptTaskOrNot(args.Id)
		}
		return nil
	default:
		return nil
	}
}

func (task *Task) AcceptTaskOrNot(expectedTaskId int) {
	if task.Id == expectedTaskId {
		duration := time.Since(task.StartTime)
		if duration.Seconds() < 10.0 {
			task.Status = TaskDone
		} else {
		}
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
	c.Lock.Lock()
	defer c.Lock.Unlock()

	// Your code here.
	for i := 0; i < c.NReduce; i++ {
		if task, ok := c.ReduceTasks[i]; ok {
			if task.Status != TaskDone {
				return false
			}
		} else {
			return false
		}
	}

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{Files: files, NReduce: nReduce}

	// Your code here.
	c.MapTasks = make(map[string]*Task)
	c.ReduceTasks = make(map[int]*Task)

	c.server()
	return &c
}
