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
	totalMapTasks       int
	distributedMapTasks int
	finishedMapTasks    int

	totalReduceTasks       int
	distributedReduceTasks int
	finishedReduceTasks    int

	mapTaskChan    chan int
	reduceTaskChan chan int

	mapTasks    []Task
	reduceTasks []Task

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
	println(122212121)
	c.lock.Lock()
	if c.totalMapTasks > c.finishedMapTasks {
		println(000000000)
		if c.distributedMapTasks == c.totalMapTasks {
			reply.TaskType = faultT
			c.lock.Unlock()
			return nil
		}
		println(11111111111)
		c.distributedMapTasks++
		c.lock.Unlock()
		println(2222222222)
		idx := <-c.mapTaskChan
		c.lock.Lock()
		c.workerConn[args.WorkerId] = time.Now()
		reply.TaskType = mapT
		reply.FileName = ""
		for _, value := range c.mapTasks[idx].filenames {
			reply.FileName += value
		}
		c.mapTasks[idx].state = inProgress
		c.mapTasks[idx].wid = args.WorkerId
		c.workerDistributedMapTask[args.WorkerId] = c.mapTasks[idx].taskId
		c.lock.Unlock()
		return nil
	} else if c.totalReduceTasks > c.finishedReduceTasks {
		c.lock.Lock()
		if c.distributedReduceTasks == c.totalReduceTasks {
			reply.TaskType = faultT
			c.lock.Unlock()
			return nil
		}
		c.distributedReduceTasks++
		c.lock.Unlock()
		idx := <-c.reduceTaskChan
		c.lock.Lock()
		c.workerConn[args.WorkerId] = time.Now()
		reply.TaskType = reduceT
		reply.FileName = ""
		for _, value := range c.reduceTasks[idx].filenames {
			reply.FileName += value + " "
		}
		c.reduceTasks[idx].state = inProgress
		c.reduceTasks[idx].wid = args.WorkerId
		c.workerDistributedReduceTask[args.WorkerId] = c.reduceTasks[idx].taskId
		c.lock.Unlock()
		return nil
	}
	return errors.New("all tasks completed")
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

	// N个reduce任务的文件名都增加上tid产生的中间文件
	for i := 0; i < NReduce; i++ {
		c.reduceTasks[i].filenames[args.WorkerId] = "mr-inter-" + strconv.Itoa(args.WorkerId) + "-" + strconv.Itoa(i)
	}
	c.finishedMapTasks++
	fmt.Printf("%v %v\n", args.WorkerId, c.finishedMapTasks)
	if c.finishedMapTasks == c.totalMapTasks {
		fmt.Printf("all finished\n")
		for i := 0; i < c.totalReduceTasks; i++ {
			c.reduceTaskChan <- i
		}
	}
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *ReduceTaskComplete, reply *TaskDistribute) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	taskId := c.workerDistributedReduceTask[args.WorkerId]
	c.reduceTasks[taskId].state = completed
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
	c.lock.Lock()
	defer c.lock.Unlock()
	for {
		timeNow := time.Now()
		for key, value := range c.workerConn {
			if timeNow.Sub(value).Seconds() > 3 {
				fmt.Printf("%v failed\n", key)
				distributedMapTask, ok1 := c.workerDistributedMapTask[key]
				distributedReduceTask, ok2 := c.workerDistributedReduceTask[key]
				if ok1 {
					if c.mapTasks[distributedMapTask].state == completed {
						for i := 0; i < c.totalReduceTasks; i++ {
							delete(c.reduceTasks[i].filenames, key)
						}
						c.finishedMapTasks--
						c.mapTaskChan <- distributedMapTask
					}
					c.mapTasks[distributedMapTask].state = idle
					c.distributedMapTasks--
				}
				if ok2 {
					if c.reduceTasks[distributedReduceTask].state == inProgress {
						c.reduceTasks[distributedReduceTask].state = idle
						c.reduceTaskChan <- distributedReduceTask
						c.distributedReduceTasks--
					}
				}
				delete(c.workerConn, key)
			} else {
				fmt.Printf("%v alive\n", key)
			}
		}
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
		distributedMapTasks:         0,
		finishedMapTasks:            0,
		totalReduceTasks:            nReduce,
		distributedReduceTasks:      0,
		finishedReduceTasks:         0,
		mapTaskChan:                 make(chan int, len(files)),
		reduceTaskChan:              make(chan int, nReduce),
		mapTasks:                    make([]Task, len(files)),
		reduceTasks:                 make([]Task, nReduce),
		workerDistributedMapTask:    make(map[int]int),
		workerDistributedReduceTask: make(map[int]int),
		workerConn:                  make(map[int]time.Time),
	}

	for i := 0; i < c.totalReduceTasks; i++ {
		c.reduceTasks[i].filenames = make(map[int]string, c.totalReduceTasks)
	}

	for i := 0; i < c.totalMapTasks; i++ {
		c.mapTasks[i] = Task{taskId: i, taskType: mapT, filenames: make(map[int]string), state: idle}
		c.mapTasks[i].filenames[0] = files[i]
		c.mapTaskChan <- i
	}
	c.server()
	return &c
}
