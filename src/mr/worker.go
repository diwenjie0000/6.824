//go:build (darwin && cgo) || linux
// +build darwin,cgo linux

// go:build (darwin && cgo) || linux
package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapFunc func(string, string) []KeyValue,
	reduceFunc func(string, []string) string) {
	for {
		taskType, fileName, err := CallForTask()
		if err != nil {
			return
		}
		if taskType == mapT {
			err := doMapTasks(fileName, mapFunc)
			if err != nil {
				return
			}
		} else {
			err := doReduceTasks(strings.Fields(fileName), reduceFunc)
			if err != nil {
				return
			}
		}

	}

}
func doMapTasks(fileName string, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
		return err
	}
	file.Close()
	intermediate := mapf(fileName, string(content))

	sort.Sort(ByKey(intermediate))

	fileNameMap := make(map[int]string)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		oName := "mr-inter-" + strconv.Itoa(os.Getuid()) + "-" + strconv.Itoa(ihash(intermediate[i].Key)%NReduce)
		fileNameMap[ihash(intermediate[i].Key)%NReduce] = oName
		oFile, _ := os.OpenFile(oName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModeAppend|os.ModePerm)
		for k := i; k < j; k++ {
			fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, intermediate[i].Value)
		}
		oFile.Close()
		i = j
	}
	CallForCompleteMapTask(fileNameMap)
	return nil
}
func doReduceTasks(fileNames []string, reducef func(string, []string) string) error {
	intermediate := ByKey{}
	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
			return err
		}
		fileScanner := bufio.NewScanner(file)

		for fileScanner.Scan() {
			context := strings.Fields(fileScanner.Text())
			intermediate = append(intermediate, KeyValue{context[0], context[1]})
		}

		if err := fileScanner.Err(); err != nil {
			log.Fatalf("Error while reading file: %s", err)
		}
		file.Close()
	}

	sort.Sort(intermediate)
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}

		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		oName := "mr-out-" + strconv.Itoa(ihash(intermediate[i].Key)%NReduce)
		oFile, _ := os.OpenFile(oName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModeAppend|os.ModePerm)
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)
		oFile.Close()
		i = j
	}
	CallForCompleteReduceTask()
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument
// and reply types are defined in rpc.go
func CallForCompleteMapTask(fileNameMap map[int]string) {
	args := MapTaskComplete{os.Getuid(), fileNameMap}
	reply := TaskDistribute{}
	ok := call("Coordinator.CompleteMapTask", &args, &reply)
	if !ok {
		//fmt.Printf("call complete map task failed!\n")
	}
}
func CallForCompleteReduceTask() {
	args := ReduceTaskComplete{os.Getuid()}
	reply := TaskDistribute{}
	ok := call("Coordinator.CompleteReduceTask", &args, &reply)
	if !ok {
		//fmt.Printf("call complete reduce task failed!\n")
	}
}
func CallForTask() (int, string, error) {
	args := TaskApply{os.Getuid()}
	reply := TaskDistribute{}
	ok := call("Coordinator.TaskDistribute", &args, &reply)
	if ok {
		return reply.TaskType, reply.FileName, nil
	} else {
		//fmt.Printf("call for tasks failed\n")
	}
	return 0, "", errors.New("分配任务失败")
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
