package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

func handleReduceWork(reducef func(string, []string) string, work *Work, nMap int){
	j := work.ID
	reduce_id := j - nMap //reduce id , not the work id
	// this reduce task will tackle mr-0-reduce_id,...,mr-(nMap-1)-reduce_id
	// first, we read all the files and append to a big kva
	intermediate := make([]KeyValue, 0)
	for i:=0;i<nMap;i++{
		file,err := os.Open(fmt.Sprintf("mr-%d-%d",i,reduce_id))
		if err!=nil{
			fmt.Print("Error when trying to read the file")
			return 
		}
		var kva []KeyValue
		content, _ := ioutil.ReadAll(file)
		err = json.Unmarshal(content, &kva)
		if err!=nil{
			fmt.Print("Error when unmarshalling the file")
			return 
		}
		intermediate = append(intermediate,kva... )
		file.Close()
	}
	//below, copied from mrsequential
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", reduce_id)
	ofile, _ := os.Create(oname)

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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		//request work every 1 second
		nMap := getNumMap()
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
			handleReduceWork(reducef, work, nMap)
			finishWork(work)
		}
	}

}



func getNumMap() int{
	send := Packet{Type: GetNumMap}
	ret := Packet{}
	ok := call("Coordinator.SendRequest", &send, &ret)
	if ok == false{
		fmt.Printf("Call Failed \n")
	}
	return ret.Msg0
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
