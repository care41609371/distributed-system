package mr

import (
    "log"
    "net"
    "os"
    "net/rpc"
    "net/http"
    "sync"
    "time"
)

// ongoing finished timeout undone
type mapTask struct {
    taskId int
    filename string
    status string
}

type reduceTask struct {
    taskId int
    status string
}

type Coordinator struct {
    mapTasks []mapTask
    reduceTasks []reduceTask
    nMap int
    nReduce int
    mapTasksFinished bool
    reduceTasksFinished bool
    mu sync.Mutex
}

func (c *Coordinator) handleTimeout(taskName string, taskId int) {
    time.Sleep(10 * time.Second)

	c.mu.Lock()
    defer c.mu.Unlock()

    if taskName == "map" {
        if c.mapTasks[taskId].status == "ongoing" {
            c.mapTasks[taskId].status = "timeout"
        }
    } else if taskName == "reduce" {
        if c.reduceTasks[taskId].status == "ongoing" {
            c.reduceTasks[taskId].status = "timeout"
        }
    }
}

func (c *Coordinator) assignMapTask() int {
    cnt := 0
    for i := range c.mapTasks {
        if c.mapTasks[i].status == "undone" || c.mapTasks[i].status == "timeout" {
            return i
        } else if c.mapTasks[i].status == "finished" {
            cnt++
        }
    }

    if cnt == c.nMap {
        c.mapTasksFinished = true
    }

    return -1
}

func (c *Coordinator) assignReduceTask() int {
    cnt := 0
    for i := range c.reduceTasks {
        if c.reduceTasks[i].status == "undone" || c.reduceTasks[i].status == "timeout" {
            return i
        } else if c.reduceTasks[i].status == "finished" {
            cnt++
        }
    }

    if cnt == c.nReduce {
        c.reduceTasksFinished = true
    }

    return -1
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if !c.mapTasksFinished {
        id := c.assignMapTask()

        if id == -1 {
            reply.TaskName = "sleep"
        } else {
            c.mapTasks[id].status = "ongoing"
            reply.TaskName = "map"
            reply.TaskId = id
            reply.NReduce = c.nReduce
            reply.MapFilename = c.mapTasks[id].filename
            go c.handleTimeout("map", id)
        }
    } else if !c.reduceTasksFinished {
        id := c.assignReduceTask()

        if id == -1 {
            reply.TaskName = "sleep"
        } else {
            c.reduceTasks[id].status = "ongoing"
            reply.TaskName = "reduce"
            reply.TaskId = id
            reply.NMap = c.nMap
            go c.handleTimeout("reduce", id)
        }
    } else {
        reply.TaskName = "finish"
    }

	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
    taskName := args.TaskName
    taskId := args.TaskId

    c.mu.Lock()
    defer c.mu.Unlock()
    if taskName == "map" {
        c.mapTasks[taskId].status = "finished"
    } else if taskName == "reduce" {
        c.reduceTasks[taskId].status = "finished"
    }

	return nil
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
    c.mu.Lock()
    defer c.mu.Unlock()
	return c.mapTasksFinished && c.reduceTasksFinished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
    c.nReduce = nReduce
    c.nMap = len(files)
    c.mapTasks = make([]mapTask, len(files))
    for i, o := range files {
        c.mapTasks[i].status = "undone"
        c.mapTasks[i].filename = o
    }

    c.reduceTasks = make([]reduceTask, nReduce)
    for i := 0; i < nReduce; i++ {
        c.reduceTasks[i].status = "undone"
    }

	c.server()
	return &c
}

