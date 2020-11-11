package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

type WorkObj struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := WorkObj{}
	worker.mapf = mapf
	worker.reducef = reducef

	// register worker
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Master.RegisterWorker", &args, &reply); !ok {
		log.Fatal("register worker failed")
	}
	worker.id = reply.WorkerId

	worker.run()
}

func (w *WorkObj) run() {
	// if reqTask conn fail, worker exit
	for {
		task := w.requireTask()
		if !task.Alive {
			log.Printf("worker get task not alive, exit")
			return
		}
		w.doTask(task)
	}
}

func (w *WorkObj) doTask(task Task) {
	switch task.TaskType {
	case MapPhase:
		w.doMapTask(task)
	case ReducePhase:
		w.doReduceTask(task)
	}
	log.Printf("worker %d done %v Task", task.WorkerId, func() string {
		if task.TaskType == 0 {
			return "Map"
		} else {
			return "Reduce"
		}
	}())
}

func (w *WorkObj) doMapTask(task Task) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
		w.reportTask(task, false)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
		w.reportTask(task, false)
	}
	file.Close()
	kva := w.mapf(task.FileName, string(content))

	// determine key X drops into bucket Y
	bucketsKV := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		bucketsKV[idx] = append(bucketsKV[idx], kv)
	}

	// flush each bucket into files
	for reduceId := 0; reduceId < len(bucketsKV); reduceId++ {
		mapFileName := w.intermediate_name(task.WorkerId, reduceId)
		ofile, err := os.Create(mapFileName)
		log.Printf("iter file : %v", mapFileName)
		if err != nil {
			log.Fatalf("cannot create intermediate file %v", mapFileName)
			w.reportTask(task, false)
		}
		enc := json.NewEncoder(ofile)

		for _, kv := range bucketsKV[reduceId] {
			// exception handling
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(task, false)
				log.Printf("encode json error: %v", err)
			}
			//log.Printf("writing %v %v", kv.Key, kv.Value)
		}
		if err := ofile.Close(); err != nil {
			log.Printf("close intermediate file error: %v", err)
			w.reportTask(task, false)
		}
	}

	//report map task done
	w.reportTask(task, true)
}

func (w *WorkObj) doReduceTask(task Task) {
	reduceId := task.WorkerId

	// load intermediate from disk for each mr-X-Y
	// TODO: need to change
	intermediateMapping := make(map[string][]string)

	for mapId := 0; mapId < task.NMaps; mapId++ {
		mapFileName := w.intermediate_name(mapId, reduceId)
		interFile, _ := os.Open(mapFileName)
		dec := json.NewDecoder(interFile)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			//log.Printf("loading %v %v", kv.Key, kv.Value)
			intermediateMapping[kv.Key] = append(intermediateMapping[kv.Key], kv.Value)
		}
		interFile.Close()
	}

	// call reducef and flush each output
	oname := w.output_name(reduceId)
	ofile, _ := os.Create(oname)

	for key_str, values := range intermediateMapping {
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", key_str, w.reducef(key_str, values))
	}
	ofile.Close()

	//report reduce task done
	w.reportTask(task, true)
}

// Utils
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

func (w *WorkObj) requireTask() Task {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	args.WorkerId = w.id

	if ok := call("Master.RequestTask", &args, &reply); !ok {
		log.Fatal("request worker failed")
	}
	log.Printf("worker %d is requesting task %v", reply.TaskObj.WorkerId, reply.TaskObj.FileName)

	return reply.TaskObj
}

func (w *WorkObj) reportTask(task Task, done bool) {
	args := ReportTaskArgs{}
	reply := ReportTaskReply{}
	args.TaskObj = task
	args.Done = done
	args.WorkerId = task.WorkerId

	if ok := call("Master.ReportTask", &args, &reply); !ok {
		log.Fatal("report task failed")
	}
}

func (w *WorkObj) intermediate_name(mapID int, reduceID int) string {
	return fmt.Sprintf("mr-%d-%d", mapID, reduceID)
}

func (w *WorkObj) output_name(reduceID int) string {
	return fmt.Sprintf("mr-out-%d", reduceID)
}
