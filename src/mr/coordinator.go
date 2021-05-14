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
	TaskNotStarted = "not started"
	TaskDone       = "done"
	TaskStarted    = "started"
)

type Task struct {
	Id        int
	Sequence  int
	StartTime time.Time
	Status    string
}

type Coordinator struct {
	TaskSequences int
	Files         []string
	MapTasks      []*Task
	ReduceTasks   []*Task
	NReduce       int
	Lock          sync.Mutex
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (reply *MRJobReply) Setup(taskId int, t string, nReduce int, files []string, sequence int) {
	reply.TaskId = taskId
	reply.Type = t
	reply.NReduce = nReduce
	reply.Files = files
	reply.Sequence = sequence
}

func PollAndCheckMapJobsAllDone(c *Coordinator, args *MRJobArgs, reply *MRJobReply) bool {
	unfinishedMapTask := 0
	for idx := range c.Files {
		task := c.MapTasks[idx]
		if task.Status == TaskDone {
			continue
		} else if task.Status == TaskStarted {
			unfinishedMapTask++
			duration := time.Since(task.StartTime)
			if duration.Seconds() > 10.0 {
				c.TaskSequences++
				c.MapTasks[idx] = &Task{Sequence: c.TaskSequences, StartTime: time.Now(), Status: TaskStarted}
				reply.Setup(idx, MapJob, c.NReduce, c.Files, c.TaskSequences)
				return false
			}
		} else {
			c.TaskSequences++
			c.MapTasks[idx] = &Task{Sequence: c.TaskSequences, StartTime: time.Now(), Status: TaskStarted}
			reply.Setup(idx, MapJob, c.NReduce, c.Files, c.TaskSequences)
			return false
		}
	}

	if unfinishedMapTask > 0 {
		reply.Type = Wait
		return false
	} else {
		return true
	}
}

func PollAndCheckJobsAllDone(c *Coordinator, jobType string, tasks []*Task, reply *MRJobReply) bool {
	unfinishedReduceTasks := 0
	for taskId, task := range tasks {
		if task.Status == TaskDone {
			continue
		} else if task.Status == TaskStarted {
			unfinishedReduceTasks++
			duration := time.Since(task.StartTime)
			if duration.Seconds() > 10.0 {
				c.TaskSequences++
				tasks[taskId] = &Task{Sequence: c.TaskSequences, StartTime: time.Now(), Status: TaskStarted}
				reply.Setup(taskId, jobType, c.NReduce, c.Files, c.TaskSequences)
				return false
			}
		} else {
			c.TaskSequences++
			tasks[taskId] = &Task{Sequence: c.TaskSequences, StartTime: time.Now(), Status: TaskStarted}
			reply.Setup(taskId, jobType, c.NReduce, c.Files, c.TaskSequences)
			return false
		}
	}

	if unfinishedReduceTasks > 0 {
		reply.Type = Wait
		return false
	} else {
		return true
	}
}

func (c *Coordinator) AssignJob(args *MRJobArgs, reply *MRJobReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	if mapJobsAllDone := PollAndCheckJobsAllDone(c, MapJob, c.MapTasks, reply); !mapJobsAllDone {
		return nil
	}

	if reduceJobsAllDone := PollAndCheckJobsAllDone(c, ReduceJob, c.ReduceTasks, reply); !reduceJobsAllDone {
		return nil
	}

	reply.Type = CanExit
	return nil
}

func (c *Coordinator) SubmitJob(args *SubmitMRJobArgs, reply *SubmitMRJobReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	switch args.Type {
	case MapJob:
		task := c.MapTasks[args.Id]
		task.AcceptTask()
		return nil
	case ReduceJob:
		task := c.ReduceTasks[args.Id]
		task.AcceptTask()

		return nil
	default:
		return nil
	}
}

func (task *Task) AcceptTask() {
	// Only submit job with same id, if ids are not equal, the task
	// has been assigned to another worker, and this submission can be discarded
	if time.Since(task.StartTime).Seconds() < 10.0 {
		task.Status = TaskDone
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
	for _, reduceTask := range c.ReduceTasks {
		if reduceTask.Status != TaskDone {
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

	c.MapTasks = make([]*Task, len(files))
	for i := range c.MapTasks {
		c.MapTasks[i] = &Task{Status: TaskNotStarted}
	}
	c.ReduceTasks = make([]*Task, nReduce)
	for i := range c.ReduceTasks {
		c.ReduceTasks[i] = &Task{Status: TaskNotStarted}
	}

	c.server()
	return &c
}
