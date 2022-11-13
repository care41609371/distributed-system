package mr

import (
    "fmt"
    "log"
    "net/rpc"
    "hash/fnv"
    "os"
    "io/ioutil"
    "encoding/json"
    "sort"
    "time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readFileContent(filename string) string {
    file, _ := os.Open(filename)
    content, _ := ioutil.ReadAll(file)
    file.Close()
    return string(content)
}

func doMap(filename string, taskId int, nReduce int, mapf func(string, string) []KeyValue) {
    kva := mapf(filename, readFileContent(filename))

    tempfiles := make([]*os.File, nReduce)
    encoders := make([]*json.Encoder, nReduce)
    for i := 0; i < nReduce; i++ {
        tempfiles[i], _ = ioutil.TempFile("", "")
        encoders[i] = json.NewEncoder(tempfiles[i])
    }

    for _, o := range kva {
        id := ihash(o.Key) % nReduce
        encoders[id].Encode(&o)
    }

    for i := range tempfiles {
        tempfiles[i].Close()
        name := fmt.Sprintf("mr-%d-%d", taskId, i)
        os.Rename(tempfiles[i].Name(), name)
    }

    args := &SubmitTaskArgs{}
    args.TaskName = "map"
    args.TaskId = taskId
    call("Coordinator.SubmitTask", args, &SubmitTaskReply{})
}

func doReduce (taskId int, nMap int, reducef func (string, []string) string) {
    kva := []KeyValue{}

    for i := 0; i < nMap; i++ {
        filename := fmt.Sprintf("mr-%d-%d", i, taskId)
        file, _ := os.Open(filename)
        decoder := json.NewDecoder(file)

        for {
            var kv KeyValue
            if err := decoder.Decode(&kv); err != nil {
                break
            }
            kva = append(kva, kv)
        }

        file.Close()
    }

    sort.Slice(kva, func(i, j int) bool {
        return kva[i].Key < kva[j].Key
    })

    tempfile, _ := ioutil.TempFile("", "")

    i := 0
    for i < len(kva) {
        values := []string{}
        values = append(values, kva[i].Value)

        j := i + 1
        for j < len(kva) && kva[j].Key == kva[i].Key {
            values = append(values, kva[j].Value)
            j++
        }

        fmt.Fprintf(tempfile, "%v %v\n", kva[i].Key, reducef(kva[i].Key, values))

        i = j
    }

    tempfile.Close()
    name := fmt.Sprintf("mr-out-%d", taskId)
    os.Rename(tempfile.Name(), name)

    args := &SubmitTaskArgs{}
    args.TaskName = "reduce"
    args.TaskId = taskId
    call("Coordinator.SubmitTask", args, &SubmitTaskReply{})
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
    for {
        args := &GetTaskArgs{}
        reply := &GetTaskReply{}
        call("Coordinator.GetTask", args, reply)

        if reply.TaskName == "map" {
            doMap(reply.MapFilename, reply.TaskId, reply.NReduce, mapf)
        } else if reply.TaskName == "reduce" {
            doReduce(reply.TaskId, reply.NMap, reducef)
        } else if reply.TaskName == "sleep" {
            time.Sleep(time.Second)
        } else if reply.TaskName == "finish" {
            os.Exit(1)
        }
    }
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

