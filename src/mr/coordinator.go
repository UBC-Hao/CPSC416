package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.


	// number of reduce tasks, no changes to nReduce, no need for mutex
	nReduce int

	// Boolean to indicate all jobs done, no need for atmoic read
	AllDone bool 

	// channels to hand out work
	workload chan *Work

	// works array to record work history
	timestamps []*time.Time
	finished []bool

	//mutex for reading parameters in Coordinator
	mu sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.


// send a request to the coordinator, the reply will be in the packet reply
func (c *Coordinator) SendRequest(request *Packet, reply *Packet) error{
	// handle the request
	switch request.Type{
		case GetNumReduce:
			reply.Msg0 = c.nReduce
		case RequestWork:
			select{
				case work := <- c.workload:
					reply.Type = work.Type
					reply.Msg0 = work.ID
					reply.Msg1 = work.filename
					c.mu.Lock()
					tnow := time.Now()
					c.timestamps[work.ID] = &tnow
					c.mu.Unlock()
				default:
					//no work to do rightnow
					reply.Type = Failed
			}
		case FinishedWork:
			id := request.Msg0
			c.mu.Lock()
			c.finished[id] = true
			c.timestamps[id] = nil
			c.mu.Unlock()
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
	lenfiles := len(files)
	maxWorkLoad := nReduce + lenfiles + 3
	c := Coordinator{
		workload: make(chan *Work, maxWorkLoad),
		timestamps: make([]*time.Time,maxWorkLoad),
		finished: make([]bool, maxWorkLoad),
		nReduce: nReduce,
	}

	// handout workload to workers
	for i:=0;i<lenfiles;i++{
		filename := files[i]
		work := &Work{
			Type: MapWork,
			ID: i,
			filename: filename,
		}
		c.workload <- work
	}

	// check if all the work is finished, if not, resend the workload
	for {
		time.Sleep(time.Second)
		c.mu.RLock()
		allDone:=true
		for i:=0;i<lenfiles;i++{
			if c.finished[i] == false{
				allDone = false
				if c.timestamps[i] != nil && time.Now().After(c.timestamps[i].Add(10*time.Second)){
					// timeout, reschedule the taks
					work := &Work{
						Type: MapWork,
						ID: i,
						filename: files[i],
					}
					c.workload <- work
				}
			}
		}
		c.mu.RUnlock()
		if allDone { break }
	}
	fmt.Printf("Successfully handled map workload ! \n")
	//reset finished to all False
	
	

	c.server()
	return &c
}
