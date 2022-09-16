package mr

import (
	"errors"
	"fmt"
	"log"
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
)

const NReduce = 10

type Task struct {
	filename string
	state    int
	wid      int
}

type Coordinator struct {
	// Your definitions here.
	totalMapTasks       int
	finishedMapTasks    int
	totalReduceTasks    int
	finishedReduceTasks int

	mapTasks    []Task
	reduceTasks [][]Task

	workerDistributedMapTask    map[int]int //i->j worker_i distributed map tasks
	workerDistributedReduceTask map[int]int
	workerConn                  map[int]time.Time

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

func (c *Coordinator) TaskDistribute(args *TaskApply, reply *TaskDistribute) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.totalMapTasks > c.finishedMapTasks {
		for i := 0; i < c.totalMapTasks; i++ {
			if c.mapTasks[i].state == idle {
				c.workerConn[args.WorkerId] = time.Now()
				reply.TaskType = mapT
				reply.FileName = c.mapTasks[i].filename
				if c.mapTasks[i].filename == "" {
					println(i)
				}
				c.mapTasks[i].state = inProgress
				c.mapTasks[i].wid = args.WorkerId
				c.workerDistributedMapTask[args.WorkerId] = i
				return nil
			}
		}
		return errors.New("mapTasks still in progress")
	} else if c.totalReduceTasks > c.finishedReduceTasks {
		for i := 0; i < c.totalReduceTasks; i++ {
			if c.reduceTasks[i][0].state == idle { //reduceTasks[i][:]一次性分配
				c.workerConn[args.WorkerId] = time.Now()
				reply.TaskType = reduceT
				reply.FileName = ""
				fileNames := make(map[string]int)
				for index, value := range c.reduceTasks[i] {
					fileNames[value.filename] = 1
					c.reduceTasks[i][index].state = inProgress
					c.reduceTasks[i][index].wid = args.WorkerId
				}
				for key := range fileNames {
					reply.FileName += key + " "
				}
				c.workerDistributedReduceTask[args.WorkerId] = i
				return nil
			}
		}
		return errors.New("reduceTasks still in progress")
	}
	return errors.New("")
	//return errors.New("failed to distribute tasks ")
}

func (c *Coordinator) CompleteMapTask(args *MapTaskComplete, reply *TaskDistribute) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.workerConn[args.WorkerId]
	if !ok {
		return errors.New(strconv.Itoa(args.WorkerId) + "already is failed")
	}
	taskId := c.workerDistributedMapTask[args.WorkerId]
	c.mapTasks[taskId].state = completed
	delete(c.workerDistributedMapTask, args.WorkerId)

	for i := 0; i < NReduce; i++ {
		c.reduceTasks[i][taskId].filename = "mr-inter-" + strconv.Itoa(args.WorkerId) + "-" + strconv.Itoa(i)
	}
	c.finishedMapTasks++
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *ReduceTaskComplete, reply *TaskDistribute) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	taskId := c.workerDistributedReduceTask[args.WorkerId]
	for i := 0; i < c.totalMapTasks; i++ {
		c.reduceTasks[taskId][i].state = completed
	}
	delete(c.workerDistributedReduceTask, args.WorkerId)
	c.finishedReduceTasks++
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
	go c.CheckConn()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	ret := false
	if c.finishedMapTasks == c.totalMapTasks && c.finishedReduceTasks == c.totalReduceTasks {
		ret = true
	}
	return ret
}

func (c *Coordinator) HandlePing(args *PingArgs, reply *PingReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.workerConn[args.WorkerId] = time.Now()
	return nil
}

func (c *Coordinator) CheckConn() {
	for {
		c.lock.Lock()
		timeNow := time.Now()
		for key, value := range c.workerConn {
			if timeNow.Sub(value).Seconds() > 3 {
				fmt.Printf("%v failed\n", key)
				distributedMapTask, ok1 := c.workerDistributedMapTask[key]
				distributedReduceTask, ok2 := c.workerDistributedReduceTask[key]
				if ok1 {
					if c.mapTasks[distributedMapTask].state == completed {
						for i := 0; i < c.totalReduceTasks; i++ {
							c.reduceTasks[i][distributedReduceTask] = Task{}
						}
						c.finishedMapTasks--
					}
					c.mapTasks[distributedMapTask].state = idle
				}
				if ok2 {
					if c.reduceTasks[distributedReduceTask][0].state == inProgress {
						c.reduceTasks[distributedReduceTask][0].state = idle
					}
				}
				delete(c.workerConn, key)
			} else {
				fmt.Printf("%v alive\n", key)
			}
		}
		c.lock.Unlock()
		time.Sleep(1 * time.Second)
	}
}

//func callClient(clientId int, rpcname string, args interface{}, reply interface{}) bool {
//	sockname := "/var/tmp/824-mr-" + strconv.Itoa(clientId)
//	c, err := rpc.DialHTTP("unix", sockname)
//	if err != nil {
//		log.Fatal("dialing:", err)
//	}
//	defer c.Close()
//
//	err = c.Call(rpcname, args, reply)
//	if err == nil {
//		return true
//	}
//	return false
//}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		totalMapTasks:               len(files),
		finishedMapTasks:            0,
		totalReduceTasks:            nReduce,
		finishedReduceTasks:         0,
		mapTasks:                    make([]Task, len(files)),
		reduceTasks:                 make([][]Task, nReduce),
		workerDistributedMapTask:    make(map[int]int),
		workerDistributedReduceTask: make(map[int]int),
		workerConn:                  make(map[int]time.Time),
	}
	for i := 0; i < c.totalReduceTasks; i++ {
		c.reduceTasks[i] = make([]Task, c.totalMapTasks)
	}
	// Your code here.
	for i := 0; i < c.totalMapTasks; i++ {
		c.mapTasks[i].filename = files[i]
	}
	c.server()
	return &c
}
