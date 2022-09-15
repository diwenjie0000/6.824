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

type Coordinator struct {
	// Your definitions here.
	totalMapTasks       int
	finishedMapTasks    int
	totalReduceTasks    int
	finishedReduceTasks int

	mapTasksFileName    []string         //mapTask_i's filename
	reduceTasksFileName []map[int]string //reduceTask_i's filename join with " ", it can use strings.Fields to parse

	mapTasksState    []int
	reduceTasksState []int

	mapTaskDistributeToWorker    []int //i->j mapTask_i distribute to worker_j
	reduceTaskDistributeToWorker []int

	workerDistributedMapTask    map[int]int //i->j worker_i distributed map tasks
	workerDistributedReduceTask map[int]int

	workerConn map[int]time.Time

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
			fmt.Printf("map task %v %v\n", i, c.mapTasksState[i])
			if c.mapTasksState[i] == idle {
				c.workerConn[args.WorkerId] = time.Now()
				reply.TaskType = mapT
				reply.FileName = c.mapTasksFileName[i]
				if c.mapTasksFileName[i] == "" {
					println(i)
				}
				c.mapTasksState[i] = inProgress
				c.mapTaskDistributeToWorker[i] = args.WorkerId
				c.workerDistributedMapTask[args.WorkerId] = i
				return nil
			}
		}
	} else if c.totalReduceTasks > c.finishedReduceTasks {
		for i := 0; i < c.totalReduceTasks; i++ {
			fmt.Printf("reduce task %v %v\n", i, c.reduceTasksState[i])
			if c.reduceTasksState[i] == idle {
				println("111")
				c.workerConn[args.WorkerId] = time.Now()
				reply.TaskType = reduceT
				reply.FileName = ""
				for _, value := range c.reduceTasksFileName[i] {
					reply.FileName += value + " "
				}
				c.reduceTasksState[i] = inProgress
				c.reduceTaskDistributeToWorker[i] = args.WorkerId
				c.workerDistributedReduceTask[args.WorkerId] = i
				return nil
			}
		}
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
	c.mapTasksState[taskId] = completed
	delete(c.workerDistributedMapTask, args.WorkerId)
	for key, value := range args.FileNames { //一个map任务最多产生R个中间文件
		c.reduceTasksFileName[key][args.WorkerId] = value
	}
	c.finishedMapTasks++
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *ReduceTaskComplete, reply *TaskDistribute) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	taskId := c.workerDistributedReduceTask[args.WorkerId]
	c.reduceTasksState[taskId] = completed
	delete(c.workerDistributedReduceTask, args.WorkerId)
	c.finishedReduceTasks++
	fmt.Printf("reduce task %v completed\n", taskId)
	for key, value := range c.reduceTasksFileName[taskId] {
		fmt.Printf("file from %v %v\n", key, value)
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
			if timeNow.Sub(value).Seconds() > 10 {
				fmt.Printf("%v failed\n", key)
				//mapTasksState    []int
				//reduceTasksState []int
				//
				//mapTaskDistributeToWorker    []int //i->j mapTask_i distribute to worker_j
				//reduceTaskDistributeToWorker []int
				//
				//workerDistributedMapTask    map[int]int //i->j worker_i distributed map tasks
				//workerDistributedReduceTask map[int]int
				distributedMapTask, ok1 := c.workerDistributedMapTask[key]
				distributedReduceTask, ok2 := c.workerDistributedReduceTask[key]
				if ok1 {
					if c.mapTasksState[distributedMapTask] == completed {
						c.finishedMapTasks--
					}
					c.mapTasksState[distributedMapTask] = idle
					fmt.Printf("delete %v %v", key, distributedMapTask)
					for _, valueMap := range c.reduceTasksFileName {
						delete(valueMap, key)
					}
				}
				if ok2 {
					if c.reduceTasksState[distributedReduceTask] == inProgress {
						c.reduceTasksState[distributedReduceTask] = idle
					}
				}
				delete(c.workerConn, key)
			} else {
				fmt.Printf("%v alive\n", key)
			}
		}
		c.lock.Unlock()
		time.Sleep(10 * time.Second)
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
		totalMapTasks:                len(files),
		finishedMapTasks:             0,
		totalReduceTasks:             nReduce,
		finishedReduceTasks:          0,
		mapTasksFileName:             files,
		reduceTasksFileName:          make([]map[int]string, nReduce),
		mapTasksState:                make([]int, len(files)),
		reduceTasksState:             make([]int, nReduce),
		mapTaskDistributeToWorker:    make([]int, len(files)),
		reduceTaskDistributeToWorker: make([]int, nReduce),
		workerDistributedMapTask:     make(map[int]int),
		workerDistributedReduceTask:  make(map[int]int),
		workerConn:                   make(map[int]time.Time),
	}
	for i := 0; i < len(c.reduceTasksFileName); i++ {
		c.reduceTasksFileName[i] = make(map[int]string)
	}
	// Your code here.

	c.server()
	return &c
}
