package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"os"
)

type Coordinator struct {
	// Your definitions here.

	// Boolean to indicate all jobs done, no need for atmoic read
	AllDone bool 

	// channels to hand out work
	workload chan *Work

	//mutex for reading parameters in Coordinator
	mu sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.


// send a request to the coordinator, the reply will be in the packet reply
func (c *Coordinator) SendRequest(request *Packet, reply *Packet) error{
	// handle the request
	switch request.Type{
		case RequestWork:
			select{
				case work := <- c.workload:
					reply.Type = work.Type
					reply.Msg0 = work.ID
					reply.Msg1 = work.filename
				default:
					//no work to do
					reply.Type = Failed
			}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	ret := c.AllDone
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	maxWorkLoad := nReduce + len(files) + 3
	c := Coordinator{
		workload: make(chan *Work, maxWorkLoad),
	}

	// Your code here.

	c.server()
	return &c
}
