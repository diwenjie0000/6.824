package mr

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	idle = iota
	inProgress
	completed
)
const (
	mapT = iota
	reduceT
	faultT
)

const NReduce = 10

type Task struct {
	taskId    int
	taskType  int            //mapT, reduceT
	filenames map[int]string //if taskType==mapT, len(filenames)==1
	state     int            //idle, inProgress, completed
	wid       int
}

type Coordinator struct {
	// Your definitions here.
	totalMapTasks int
	//distributedMapTasks int
	finishedMapTasks int

	totalReduceTasks int
	//distributedReduceTasks int
	finishedReduceTasks int

	taskDistributedChan chan Task
	//taskDoneChan        chan Task
	//interTaskChan       chan Task

	mapTasks    []Task
	reduceTasks []Task

	done bool

	lock sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) isTaskCompleted(tid int, ttype int) {
	time.Sleep(10 * time.Second)
	c.lock.Lock()
	defer c.lock.Unlock()
	if ttype == mapT {
		if c.mapTasks[tid].state != completed {
			c.mapTasks[tid].state = idle
			c.taskDistributedChan <- c.mapTasks[tid]
		}
	} else {
		if c.reduceTasks[tid].state != completed {
			c.reduceTasks[tid].state = idle
			c.taskDistributedChan <- c.reduceTasks[tid]
		}
	}
}

func (c *Coordinator) TaskDistribute(args *TaskApply, reply *TaskDistribute) error {
	t := <-c.taskDistributedChan
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.done {
		return errors.New("end")
	}
	if t.taskType == mapT {
		idx := t.taskId
		reply.TaskType = mapT
		reply.FileName = ""
		reply.TaskId = t.taskId
		for _, value := range c.mapTasks[idx].filenames {
			reply.FileName += value
		}
		c.mapTasks[idx].state = inProgress
		c.mapTasks[idx].wid = args.WorkerId
		go c.isTaskCompleted(t.taskId, t.taskType)
		return nil
	} else {
		idx := t.taskId
		reply.TaskType = reduceT
		reply.FileName = ""
		reply.TaskId = t.taskId
		for _, value := range c.reduceTasks[idx].filenames {
			reply.FileName += value + " "
		}
		c.reduceTasks[idx].state = inProgress
		c.reduceTasks[idx].wid = args.WorkerId
		go c.isTaskCompleted(t.taskId, t.taskType)
		return nil
	}
	return errors.New("all tasks completed")
}

func (c *Coordinator) CompleteMapTask(args *MapTaskComplete, reply *TaskDistribute) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.WorkerId != c.mapTasks[args.TaskId].wid {
		return errors.New(strconv.Itoa(args.TaskId) + "already is re-distributed")
	}
	taskId := args.TaskId
	c.mapTasks[taskId].state = completed
	c.finishedMapTasks++
	//fmt.Printf("%v %v\n", args.WorkerId, c.finishedMapTasks)
	if c.finishedMapTasks == c.totalMapTasks {
		for i := 0; i < NReduce; i++ {
			for j := 0; j < c.totalMapTasks; j++ {
				c.reduceTasks[i].filenames[c.mapTasks[j].taskId] = "mr-inter-" + strconv.Itoa(c.mapTasks[j].wid) + "-" + strconv.Itoa(c.mapTasks[j].taskId) + "-" + strconv.Itoa(i)
			}
		}

		//fmt.Printf("all map task finished\n")
		for i := 0; i < c.totalReduceTasks; i++ {
			c.taskDistributedChan <- c.reduceTasks[i]
		}
	}
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *ReduceTaskComplete, reply *TaskDistribute) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.WorkerId != c.reduceTasks[args.TaskId].wid {
		return errors.New(strconv.Itoa(args.TaskId) + "already is re-distributed")
	}
	taskId := args.TaskId
	c.reduceTasks[taskId].state = completed
	c.finishedReduceTasks++
	if c.totalReduceTasks == c.finishedReduceTasks {
		c.done = true
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
		//log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	ret := false
	if c.done {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		totalMapTasks: len(files),
		//distributedMapTasks:         0,
		finishedMapTasks: 0,
		totalReduceTasks: nReduce,
		done:             false,
		//distributedReduceTasks:      0,
		finishedReduceTasks: 0,
		taskDistributedChan: make(chan Task, len(files)+nReduce),
		mapTasks:            make([]Task, len(files)),
		reduceTasks:         make([]Task, nReduce),
	}

	for i := 0; i < c.totalReduceTasks; i++ {
		c.reduceTasks[i].filenames = make(map[int]string, c.totalReduceTasks)
	}

	for i := 0; i < c.totalMapTasks; i++ {
		c.mapTasks[i] = Task{taskId: i, taskType: mapT, filenames: make(map[int]string), state: idle}
		c.mapTasks[i].filenames[0] = files[i]
		c.taskDistributedChan <- c.mapTasks[i]
	}
	for i := 0; i < c.totalReduceTasks; i++ {
		c.reduceTasks[i] = Task{taskId: i, taskType: reduceT, filenames: make(map[int]string), state: idle}
	}
	c.server()
	return &c
}
