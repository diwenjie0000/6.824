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

	workerDistributedMapTask    map[int]map[int]int //i->j worker_i distributed map tasks
	workerDistributedReduceTask map[int]map[int]int

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
		if c.mapTasks[tid].taskType != completed {
			c.mapTasks[tid].state = idle
			c.taskDistributedChan <- c.mapTasks[tid]
			delete(c.workerDistributedMapTask[c.mapTasks[tid].wid], tid)
		}
	} else {
		if c.reduceTasks[tid].taskType != completed {
			c.reduceTasks[tid].state = idle
			c.taskDistributedChan <- c.reduceTasks[tid]
			delete(c.workerDistributedReduceTask[c.mapTasks[tid].wid], tid)
		}
	}
}

func (c *Coordinator) TaskDistribute(args *TaskApply, reply *TaskDistribute) error {
	t := <-c.taskDistributedChan
	c.lock.Lock()
	defer c.lock.Unlock()
	if t.taskType == mapT {
		//c.distributedMapTasks++
		idx := t.taskId
		reply.TaskType = mapT
		reply.FileName = ""
		reply.TaskId = t.taskId
		for _, value := range c.mapTasks[idx].filenames {
			reply.FileName += value
		}
		c.mapTasks[idx].state = inProgress
		c.mapTasks[idx].wid = args.WorkerId
		_, ok := c.workerDistributedMapTask[args.WorkerId]
		if !ok {
			c.workerDistributedMapTask[args.WorkerId] = make(map[int]int)
		}
		c.workerDistributedMapTask[args.WorkerId][c.mapTasks[idx].taskId] = 1
		go c.isTaskCompleted(t.taskId, t.taskType)
		return nil
	} else {
		//c.distributedReduceTasks++
		idx := t.taskId
		reply.TaskType = reduceT
		reply.FileName = ""
		reply.TaskId = t.taskId
		for _, value := range c.reduceTasks[idx].filenames {
			reply.FileName += value + " "
		}
		c.reduceTasks[idx].state = inProgress
		c.reduceTasks[idx].wid = args.WorkerId
		_, ok := c.workerDistributedReduceTask[args.WorkerId]
		if !ok {
			c.workerDistributedReduceTask[args.WorkerId] = make(map[int]int)
		}
		c.workerDistributedReduceTask[args.WorkerId][c.reduceTasks[idx].taskId] = 1
		go c.isTaskCompleted(t.taskId, t.taskType)
		return nil
	}
	return errors.New("all tasks completed")
}

func (c *Coordinator) CompleteMapTask(args *MapTaskComplete, reply *TaskDistribute) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.workerDistributedMapTask[args.WorkerId][args.TaskId]
	if !ok {
		return errors.New(strconv.Itoa(args.TaskId) + "already is re-distributed")
	}
	taskId := args.TaskId
	c.mapTasks[taskId].state = completed
	//delete(c.workerDistributedMapTask, args.WorkerId)

	// N个reduce任务的文件名都增加上tid产生的中间文件
	for i := 0; i < NReduce; i++ {
		c.reduceTasks[i].filenames[args.TaskId] = "mr-inter-" + strconv.Itoa(args.WorkerId) + "-" + strconv.Itoa(args.TaskId) + "-" + strconv.Itoa(i)
	}
	c.finishedMapTasks++
	fmt.Printf("%v %v\n", args.WorkerId, c.finishedMapTasks)
	if c.finishedMapTasks == c.totalMapTasks {
		fmt.Printf("all finished\n")
		for i := 0; i < c.totalReduceTasks; i++ {
			c.taskDistributedChan <- c.reduceTasks[i]
		}
	}
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *ReduceTaskComplete, reply *TaskDistribute) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, ok := c.workerDistributedReduceTask[args.WorkerId][args.TaskId]
	if !ok {
		return errors.New(strconv.Itoa(args.TaskId) + "already is re-distributed")
	}
	taskId := args.TaskId
	c.reduceTasks[taskId].state = completed
	//delete(c.workerDistributedReduceTask, args.WorkerId)
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
	c.lock.RLock()
	defer c.lock.RUnlock()
	ret := false
	if c.finishedMapTasks == c.totalMapTasks && c.finishedReduceTasks == c.totalReduceTasks {
		ret = true
	}
	return ret
}

//func (c *Coordinator) HandlePing(args *PingArgs, reply *PingReply) error {
//	c.lock.Lock()
//	defer c.lock.Unlock()
//	c.workerConn[args.WorkerId] = time.Now()
//	return nil
//}

//func (c *Coordinator) CheckConn() {
//	for {
//		c.lock.Lock()
//		timeNow := time.Now()
//		for key, value := range c.workerConn {
//			if timeNow.Sub(value).Seconds() > 10 {
//				distributedMapTasks, ok1 := c.workerDistributedMapTask[key]
//				distributedReduceTasks, ok2 := c.workerDistributedReduceTask[key]
//				fmt.Printf("%v failed\n", key)
//				if ok1 && c.finishedMapTasks < c.totalMapTasks {
//					for i := 0; i < c.totalReduceTasks; i++ {
//						delete(c.reduceTasks[i].filenames, key)
//					}
//					for idx := range distributedMapTasks {
//						if c.mapTasks[idx].state == completed {
//							c.finishedMapTasks--
//						}
//						println("re-distribute map task:", c.mapTasks[idx].taskId)
//						c.mapTasks[idx].state = idle
//						c.taskDistributedChan <- c.mapTasks[idx]
//						//c.distributedMapTasks--
//					}
//					delete(c.workerDistributedMapTask, key)
//				}
//				if ok2 && c.finishedReduceTasks < c.totalReduceTasks {
//					for idx := range distributedReduceTasks {
//						fmt.Printf("leave reduce task %v\n", idx)
//						if c.reduceTasks[idx].state == inProgress {
//							c.reduceTasks[idx].state = idle
//							c.taskDistributedChan <- c.reduceTasks[idx]
//							//c.distributedReduceTasks--
//						}
//					}
//					delete(c.workerDistributedReduceTask, key)
//				}
//				delete(c.workerConn, key)
//			} else {
//				fmt.Printf("%v alive\n", key)
//			}
//		}
//		c.lock.Unlock()
//		time.Sleep(5 * time.Second)
//
//	}
//}

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
		totalMapTasks: len(files),
		//distributedMapTasks:         0,
		finishedMapTasks: 0,
		totalReduceTasks: nReduce,
		//distributedReduceTasks:      0,
		finishedReduceTasks:         0,
		taskDistributedChan:         make(chan Task, len(files)+nReduce),
		mapTasks:                    make([]Task, len(files)),
		reduceTasks:                 make([]Task, nReduce),
		workerDistributedMapTask:    make(map[int]map[int]int),
		workerDistributedReduceTask: make(map[int]map[int]int),
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
