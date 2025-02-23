package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	parallelism       int
	avaliableWorkers  chan *worker
	mapParallelism    int
	files             []string
	workerIdGenerator int32
	mapTaskChannel    chan MapTask
	reduceTaskChannel chan ReduceTask
	workerGuard       sync.Cond
	allDone           bool
}

type worker struct {
	id         int
	entryPoint string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RegisterWorker(req *WorkerRegisterReq, reply *WorkerRegisterResp) error {
	if c.allDone {
		reply.Success = false
		return nil
	}
	log.Println("Receive worker register request.")
	workerId := atomic.AddInt32(&c.workerIdGenerator, 1)
	reply.WorkerId = int(workerId)
	reply.Success = true
	go func(reply *WorkerRegisterResp) {
		worker := &worker{
			id:         int(reply.WorkerId),
			entryPoint: req.EntryPoint,
		}
		c.workerGuard.L.Lock()
		defer c.workerGuard.L.Unlock()
		log.Println("Add new worker" + strconv.Itoa(int(reply.WorkerId)) + " to queue.")
		c.avaliableWorkers <- worker
		c.workerGuard.Signal()
	}(reply)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
	return c.allDone
}

func (c *Coordinator) shutdown() {
	var wg sync.WaitGroup
	close(c.avaliableWorkers)
	for worker := range c.avaliableWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.callWithTimeout("WorkerEntry.Done", &Args{}, &Args{})
			log.Println("Close worker entryPoint: " + worker.entryPoint)
		}()
	}
	wg.Wait()
	log.Println("Close all workers finished.")
	c.allDone = true
}

// send an RPC request to the worker, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func (worker *worker) callWithTimeout(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := worker.entryPoint
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("dialing:", err)
	}
	defer c.Close()

	done := make(chan error, 1)
	go func() {
		err = c.Call(rpcname, args, reply)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			return true
		}
		log.Printf("RPC call occurs an error: %v\n", err)
	case <-time.After(10 * time.Second):
		log.Printf("RPC call time out.")
	}
	return false
}

func (c *Coordinator) start() error {
	// wait until there are any workers registered
	c.waitWorkersOrExit()
	defer close(c.mapTaskChannel)
	defer close(c.reduceTaskChannel)

	log.Println("Begin to dispatch Map task...")
	go func() {
		for idx, file := range c.files {
			c.mapTaskChannel <- MapTask{
				Id:          idx,
				FilePath:    file,
				Parallelism: c.parallelism,
			}
		}
	}()

	var mapFinishes int32
	mapDone := make(chan int32)
	for {
		select {
		case mapTask := <-c.mapTaskChannel:
			go func() {
				worker := c.pickWorker()
				log.Printf("Dispatch map task%v to worker%v", mapTask.Id, worker.id)
				mapTaskResp := MapTaskResp{}
				if worker.callWithTimeout("WorkerEntry.HandleMapTask", &mapTask, &mapTaskResp) || mapTaskResp.Success {
					cnt := atomic.AddInt32(&mapFinishes, 1)
					log.Printf("Send cnt: %v", cnt)
					mapDone <- cnt
					c.avaliableWorkers <- worker
				} else {
					c.mapTaskChannel <- mapTask
					mapDone <- atomic.LoadInt32(&mapFinishes)
				}
			}()
		case cnt := <-mapDone:
			if int(cnt) == len(c.files) {
				goto MapFinish
			}
		}
	}
MapFinish:
	log.Println("All Map Task finished.")
	log.Println("Start to dispatch Reduce task...")
	go func() {
		for id := 0; id < c.parallelism; id++ {
			c.reduceTaskChannel <- ReduceTask{
				Id:             id,
				MapParallelism: c.mapParallelism,
			}
		}
	}()
	var reduceFinishes int32
	reduceDone := make(chan int32)
	for {
		select {
		case reduceTask := <-c.reduceTaskChannel:
			go func() {
				worker := c.pickWorker()
				log.Printf("Dispatch reduce task%v to worker%v", reduceTask.Id, worker.id)
				reduceTaskResp := ReduceTaskResp{}
				if worker.callWithTimeout("WorkerEntry.HandleReduceTask", &reduceTask, &reduceTaskResp) || reduceTaskResp.Success {
					cnt := atomic.AddInt32(&reduceFinishes, 1)
					log.Printf("Send reduce cnt: %v", cnt)
					reduceDone <- cnt
					c.avaliableWorkers <- worker
				} else {
					reduceDone <- atomic.LoadInt32(&reduceFinishes)
					c.reduceTaskChannel <- reduceTask
				}
			}()
		case cnt := <-reduceDone:
			log.Println("All Reduce Task finished.")
			if int(cnt) == c.parallelism {
				return nil
			}
		}
	}
}

func (c *Coordinator) pickWorker() *worker {
	return <-c.avaliableWorkers
}

func (c *Coordinator) waitWorkersOrExit() {
	c.workerGuard.L.Lock()
	log.Println("wait for workers...")
	if len(c.avaliableWorkers) > 0 {
		c.workerGuard.L.Unlock()
		return
	}
	c.workerGuard.L.Unlock()
	select {
	case <-time.After(10 * time.Second):
		{
			log.Fatal("Can't detect any alive workers in 10 seconds, exit.")
		}
	case <-func(wg *sync.Cond) chan struct{} {
		ch := make(chan struct{})
		go func() {
			wg.L.Lock()
			wg.Wait()
			wg.L.Unlock()
			close(ch)
		}()
		return ch
	}(&c.workerGuard):
		{
			log.Println("Found new worker")
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		parallelism:       nReduce,
		files:             files,
		mapParallelism:    len(files),
		mapTaskChannel:    make(chan MapTask, len(files)),
		reduceTaskChannel: make(chan ReduceTask, nReduce),
		avaliableWorkers:  make(chan *worker, nReduce),
		workerGuard:       *sync.NewCond(new(sync.RWMutex)),
	}
	// Your code here.
	log.Println("Bootstrap coordinator server...")
	c.server()
	c.start()
	c.shutdown()
	return &c
}
