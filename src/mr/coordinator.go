package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
	reduceTasksFileName []map[string]int //reduceTask_i's filename join with " ", it can use strings.Fields to parse

	mapTasksState    []int
	reduceTasksState []int

	mapTaskDistributeToWorker    []int //i->j mapTask_i distribute to worker_j
	reduceTaskDistributeToWorker []int

	workerDistributedMapTask    map[int]int //i->j worker_i distributed map tasks
	workerDistributedReduceTask map[int]int

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
			if c.mapTasksState[i] == idle {
				reply.TaskType = mapT
				reply.FileName = c.mapTasksFileName[i]
				c.mapTasksState[i] = inProgress
				c.mapTaskDistributeToWorker[i] = args.WorkerId
				c.workerDistributedMapTask[args.WorkerId] = i
				return nil
			}
		}
	} else if c.totalReduceTasks > c.finishedReduceTasks {
		for i := 0; i < c.totalReduceTasks; i++ {
			if c.reduceTasksState[i] == idle {
				reply.TaskType = reduceT
				reply.FileName = ""
				for key := range c.reduceTasksFileName[i] {
					reply.FileName += key + " "
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

	taskId := c.workerDistributedMapTask[args.WorkerId]
	c.mapTasksState[taskId] = completed
	delete(c.workerDistributedMapTask, args.WorkerId)
	for key, value := range args.FileNames { //一个map任务最多产生R个中间文件
		c.reduceTasksFileName[key][value] = 1
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
		reduceTasksFileName:          make([]map[string]int, nReduce),
		mapTasksState:                make([]int, len(files)),
		reduceTasksState:             make([]int, nReduce),
		mapTaskDistributeToWorker:    make([]int, len(files)),
		reduceTaskDistributeToWorker: make([]int, nReduce),
		workerDistributedMapTask:     make(map[int]int),
		workerDistributedReduceTask:  make(map[int]int),
	}
	for i := 0; i < len(c.reduceTasksFileName); i++ {
		c.reduceTasksFileName[i] = make(map[string]int)
	}
	// Your code here.

	c.server()
	return &c
}
