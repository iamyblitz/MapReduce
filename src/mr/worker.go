package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}

		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			fmt.Println("Coordinator doesn't respond, exiting...")
			return
		}

		switch reply.TaskType {
		case 0:
			fmt.Printf("It's MAP task, babe -  №%d для файла %s\n", reply.TaskId,
				reply.FileName)
			content, err := os.ReadFile(reply.FileName)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			kva := mapf(reply.FileName, string(content))

			files := make([]*os.File, reply.NReduce)
			encoders := make([]*json.Encoder, reply.NReduce)
			tempFilenames := make([]string, reply.NReduce)

			for i := 0; i < reply.NReduce; i++ {
				tempFile, err := os.CreateTemp("", "mr-tmp-*")
				if err != nil {
					log.Fatalf("cannot create temp file: %v", err)
				}
				files[i] = tempFile
				tempFilenames[i] = tempFile.Name()
				encoders[i] = json.NewEncoder(tempFile)
			}

			for _, kv := range kva {
				bucket := ihash(kv.Key) % reply.NReduce
				err := encoders[bucket].Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode %v", kv)
				}
			}

			// Close and rename temp files to final names
			for i := 0; i < reply.NReduce; i++ {
				files[i].Close()
				finalName := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
				os.Rename(tempFilenames[i], finalName)
			}

			reportArgs := ReportTaskArgs{
				TaskId:   reply.TaskId,
				TaskType: 0, // Map
			}
			reportReply := ReportTaskReply{}
			call("Coordinator.ReportTask", &reportArgs, &reportReply)

		case 1:
			fmt.Printf("It's REDUCE task №%d\n", reply.TaskId)
			// Collect all intermediate files for this reduce task
			var kva []KeyValue
			for i := 0; i < reply.NMap; i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskId)
				file, err := os.Open(filename)
				if err != nil {
					continue // file might not exist
				}
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

			// Sort by key
			sort.Sort(ByKey(kva))

			// Write output to temp file
			tempFile, err := os.CreateTemp("", "mr-tmp-*")
			if err != nil {
				log.Fatalf("cannot create temp file: %v", err)
			}
			tempFilename := tempFile.Name()

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			tempFile.Close()

			// Rename temp file to final name
			finalName := fmt.Sprintf("mr-out-%d", reply.TaskId)
			os.Rename(tempFilename, finalName)

			reportArgs := ReportTaskArgs{
				TaskId:   reply.TaskId,
				TaskType: 1, // Reduce
			}
			reportReply := ReportTaskReply{}
			call("Coordinator.ReportTask", &reportArgs, &reportReply)

		case 2:
			time.Sleep(time.Second)

		case 3:
			// Exit signal from coordinator
			fmt.Println("All tasks done, exiting...")
			return

		default:
			fmt.Printf("Unknown type of task: %d\n", reply.TaskType)
		}
	} // Your worker implementation here.
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
