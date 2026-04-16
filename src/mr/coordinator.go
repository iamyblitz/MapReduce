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

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []TaskInfo
	reduceTasks []TaskInfo
	nReduce     int
	nMap        int
	phase       int
}

type TaskInfo struct {
	Status    int
	FileName  string
	TaskId    int
	StartTime time.Time
}

func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.reduceTasks {
		if task.Status != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) tickle() {
	for {
		time.Sleep(500 * time.Millisecond)
		c.mu.Lock()

		for i, task := range c.mapTasks {
			if task.Status == 1 && time.Since(task.StartTime) > 10*time.Second {
				c.mapTasks[i].Status = 0
			}
		}

		for i, task := range c.reduceTasks {
			if task.Status == 1 && time.Since(task.StartTime) > 10*time.Second {
				c.reduceTasks[i].Status = 0
			}
		}

		c.mu.Unlock()
	}
}

func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.nMap = len(files)
	c.phase = 0

	for i, filename := range files {
		task := TaskInfo{
			Status:   0,
			FileName: filename,
			TaskId:   i,
		}
		c.mapTasks = append(c.mapTasks, task)
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, TaskInfo{Status: 0, TaskId: i})
	}

	c.server(sockname)
	go c.tickle()
	return &c
}

func (c *Coordinator) GetTask(ars *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case 0:
		for i, task := range c.mapTasks {
			if task.Status == 1 && time.Since(task.StartTime) > 10*time.Second {
				c.mapTasks[i].Status = 0
			}
		}
	case 1:
		for i, task := range c.reduceTasks {
			if task.Status == 1 && time.Since(task.StartTime) > 10*time.Second {
				c.reduceTasks[i].Status = 0
			}
		}
	}

	if c.phase == 0 {
		for i, task := range c.mapTasks {
			if task.Status == 0 {
				c.mapTasks[i].Status = 1
				c.mapTasks[i].StartTime = time.Now()
				reply.FileName = task.FileName
				reply.TaskId = i
				reply.TaskType = 0
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
		}

		allMapDone := true
		for _, task := range c.mapTasks {
			if task.Status != 2 {
				allMapDone = false
				break
			}
		}

		if !allMapDone {
			reply.TaskType = 2
			return nil
		}

		c.phase = 1
	}

	if c.phase == 1 {
		for i, task := range c.reduceTasks {
			if task.Status == 0 {
				c.reduceTasks[i].Status = 1
				c.reduceTasks[i].StartTime = time.Now()
				reply.TaskId = i
				reply.TaskType = 1
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
		}

		allReduceDone := true
		for _, task := range c.reduceTasks {
			if task.Status != 2 {
				allReduceDone = false
				break
			}
		}

		if allReduceDone {
			reply.TaskType = 3
			return nil
		}

		reply.TaskType = 2
		return nil
	}

	reply.TaskType = 2
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	taskId := args.TaskId

	switch args.TaskType {
	case 0: //Map
		for i, task := range c.mapTasks {
			if task.TaskId == taskId {
				c.mapTasks[i].Status = 2
			}
		}
	case 1: // Reduce
		for i, task := range c.reduceTasks {
			if task.TaskId == taskId {
				c.reduceTasks[i].Status = 2
			}
		}
	}
	return nil
}
