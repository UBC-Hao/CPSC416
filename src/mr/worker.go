package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"

	"encoding/json"
	"io/ioutil"
	"os"
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

func handleMapWork(mapf func(string, string) []KeyValue, work *Work, nReduce int) (error){
	filename := work.filename
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil{
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil{
		return err
	}
	kva := mapf(filename, string(content))
	
	kva_a := make([][]KeyValue, nReduce)
	id:= work.ID
	for i:=0;i<nReduce;i++{
		kva_a[i] = make([]KeyValue, 0)
	}
	// create multiple files for reduce task, mr-id-0 to mr-id-(nReduce-1)
	for _,keyvalue := range(kva){
		tnum := ihash(keyvalue.Key) % nReduce
		kva_a[tnum] = append(kva_a[tnum], keyvalue)
	}

	//write out files
	for i:=0;i<nReduce;i++{
		json,err:=json.Marshal(kva_a[i])
		if err != nil{
			return err
		}
		f,_ := os.Create(fmt.Sprintf("mr-%d-%d", id, i))
		f.Write(json)
		f.Close()
	}

	return nil
}


// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		//request work every 1 second
		nReduce := getNumReduce()
		ok, work :=requestWork()
		if !ok{ continue;} // no work to do right now, continue
		if work.Type==MapWork{
			//MapWork
			err := handleMapWork(mapf, work, nReduce)
			if err != nil{
				fmt.Printf("Error when reading the file, absort current workload\n")
				continue
			}
			//tell the coordinator, I'm done!
			finishWork(work)
		}else{
			//ReduceWork

		}
	}

}


func getNumReduce() int{
	send := Packet{Type: GetNumReduce}
	ret := Packet{}
	ok := call("Coordinator.SendRequest", &send, &ret)
	if ok == false{
		fmt.Printf("Call Failed \n")
	}
	return ret.Msg0
}

func finishWork(work *Work)  {
	send := Packet{Type: FinishedWork, Msg0: work.ID}
	ret	 := Packet{}
	ok	 := call("Coordinator.SendRequest", &send, &ret)
	if ok == false{
		fmt.Printf("Call Failed \n")
	}
	return
}

func requestWork() (bool, *Work) {
	send := Packet{Type: RequestWork}
	ret	 := Packet{}
	ok	 := call("Coordinator.SendRequest", &send, &ret)
	if ok == false{
		fmt.Printf("Call Failed \n")
	}
	success := ret.Type!=Failed
	work := &Work{}
	if success{
		work.Type = ret.Type
		work.ID = ret.Msg0
		work.filename = ret.Msg1
	}

	return success,work
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
