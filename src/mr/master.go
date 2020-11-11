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

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
)

type State int

const (
	Ready State = iota
	Queued
	Running
	Done
	Corrupt
)

type TaskStats struct {
	filename  string
	taskState State
	startTime time.Time
}

type Task struct {
	FileName string
	NReduce  int
	NMaps    int
	WorkerId int
	TaskType Phase
	Alive    bool
}

type Master struct {
	// Your definitions here.
	mu           sync.Mutex
	Seq          int
	nMap         int
	nReduce      int
	phase        Phase
	taskList     []TaskStats
	fileNames    []string
	timeout      float64
	taskCh       chan Task
	mapAllFinish bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

// request a task, called by the worker
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {

	task := <-m.taskCh
	reply.TaskObj = task

	m.taskList[task.WorkerId].taskState = Running
	m.taskList[task.WorkerId].startTime = time.Now()
	log.Printf("Worker %d is requesting task %v", task.WorkerId, m.taskList[task.WorkerId].filename)
	return nil
}

// report Task
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	if args.Done {
		m.taskList[args.WorkerId].taskState = Done
	} else {
		m.taskList[args.WorkerId].taskState = Corrupt
	}

	return nil
}

// register worker
func (m *Master) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	reply.WorkerId = m.Seq
	m.Seq = m.Seq + 1
	log.Printf("register worker called in master %d", reply.WorkerId)
	return nil
}

func (m *Master) createTask(index int) Task {
	task := Task{
		TaskType: m.phase,
		FileName: "",
		WorkerId: index,
		NReduce:  m.nReduce,
		NMaps:    len(m.fileNames),
		Alive:    true,
	}
	if m.phase == MapPhase {
		task.FileName = m.fileNames[index]
	}
	return task
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Done() {
		return
	}

	mapAllFinish := true
	for index, t := range m.taskList {
		switch t.taskState {
		case Ready:
			tmp := m.createTask(index)
			m.taskCh <- tmp
			m.taskList[index].taskState = Queued
			mapAllFinish = false
		case Running:
			if time.Now().Sub(t.startTime).Seconds() >= m.timeout {
				m.taskCh <- m.createTask(index)
				m.taskList[index].taskState = Queued
			}
			mapAllFinish = false
		case Corrupt:
			m.taskCh <- m.createTask(index)
			m.taskList[index].taskState = Queued
			m.mapAllFinish = false
			log.Printf("task %d is corrupted", index)
		case Queued:
			mapAllFinish = false
		case Done:
			log.Printf("task %d is done", index)
		}


	}
	if mapAllFinish {
		log.Print("=======Stepping into Reduce Phase=======")
		m.phase = ReducePhase
		m.taskList = make([]TaskStats, m.nReduce)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	if m.phase == MapPhase {
		return false
	}

	for i := 0; i < m.nMap; i++ {
		if m.taskList[i].taskState != Done {
			return false
		}
	}

	return true
}

// create a Master.
// main/mrmaster.go calls this funcmethod "Done" has 1 input parameters; needs exactly threetion.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mu = sync.Mutex{}
	m.Seq = 0
	m.phase = MapPhase
	m.nReduce = nReduce
	m.nMap = len(files)
	m.timeout = 10
	m.fileNames = files
	m.taskList = make([]TaskStats, len(files))

	channelSize := Max(m.nReduce, len(m.fileNames))
	m.taskCh = make(chan Task, channelSize)
	m.server()

	// start to schedule
	for !m.Done() {
		go m.schedule()
		time.Sleep(time.Millisecond * 500)
	}

	return &m
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

