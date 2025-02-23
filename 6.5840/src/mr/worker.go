package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerEntry struct {
	id         int
	entryPoint string
	done       chan struct{}
	mapf       func(string, string) []KeyValue
	reducef    func(string, []string) string
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	workerEntry := newWorkerEntry(mapf, reducef)
	if err := workerEntry.serve(); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
	if err := workerEntry.registerToCoordinator(); err != nil {
		log.Fatalf("Failed to register to coordinator: %v", err)
	}
	workerEntry.setupSignalHandler()
	log.Println("Worker id: " + strconv.Itoa(int(workerEntry.id)) + ", entrypint: " + workerEntry.entryPoint)

	<-workerEntry.done
	workerEntry.shutdown()
}

func newWorkerEntry(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) *WorkerEntry {
	return &WorkerEntry{
		entryPoint: workerSock(rand.Intn(101)),
		mapf:       mapf,
		reducef:    reducef,
		done:       make(chan struct{}, 1),
	}
}

func (w *WorkerEntry) serve() error {
	rpc.Register(w)
	rpc.HandleHTTP()
	os.Remove(w.entryPoint)
	l, e := net.Listen("unix", w.entryPoint)
	if e != nil {
		return fmt.Errorf("listen error: %v", e)
	}
	go http.Serve(l, nil)
	return nil
}

func (w *WorkerEntry) registerToCoordinator() error {
	workerRegisterReq := WorkerRegisterReq{w.entryPoint}
	workerRegisterResp := WorkerRegisterResp{}
	if !call("Coordinator.RegisterWorker", &workerRegisterReq, &workerRegisterResp) || !workerRegisterResp.Success {
		return fmt.Errorf("register to coordinatorn failed")
	}
	w.id = workerRegisterResp.WorkerId
	return nil
}

func (w *WorkerEntry) setupSignalHandler() {
	// Set up a channel to receive OS signals
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	// Goroutine to handle the signal
	go func() {
		<-c
		w.shutdown()
		os.Exit(1)
	}()
}

func (w *WorkerEntry) shutdown() {
	log.Printf("Worker%v shutdown.\n", w.id)
	time.Sleep(100 * time.Millisecond)
	os.Remove(w.entryPoint)
}

func (w *WorkerEntry) HandleMapTask(task *MapTask, resp *MapTaskResp) error {
	log.SetPrefix("Worker" + strconv.Itoa(w.id))
	log.Printf("Receive map task %v.\n", task)
	w.doHanddleMapTask(task, resp)
	return nil
}

func (w *WorkerEntry) doHanddleMapTask(task *MapTask, resp *MapTaskResp) error {
	log.SetPrefix("Worker" + strconv.Itoa(w.id))
	log.Printf("Worker%v Start to handle task, id: %v, file: %v, parallelism: %v",
		w.id,
		strconv.Itoa(int(task.Id)),
		task.FilePath,
		task.Parallelism)
	file, err := os.Open(task.FilePath)
	if err != nil {
		log.Fatalf("cannot open %v", file)
	}
	defer file.Close()
	shuffleFiles := [10]*json.Encoder{}
	for i := 0; i < task.Parallelism; i++ {
		shuffleFileName := fmt.Sprintf("mr-%v-%v", task.Id, i)
		shuffleFile, err := os.Create(shuffleFileName)
		if err != nil {
			log.Fatalf("cannot create shuffle file: %v", shuffleFileName)
			resp.Success = false
			return nil
		}
		defer shuffleFile.Close()
		shuffleFiles[i] = json.NewEncoder(shuffleFile)
		resp.ShuffleFiles = append(resp.ShuffleFiles, shuffleFileName)
	}

	if content, err := io.ReadAll(bufio.NewReader(file)); err == io.EOF {
		log.Printf("MapTask%v finished.\n", task.Id)
	} else if err != nil {
		log.Fatalf("Error readling line: %v\n", err)
	} else {
		kva := w.mapf(file.Name(), string(content))
		for _, kv := range kva {
			shuffleFiles[ihash(kv.Key)%len(shuffleFiles)].Encode(&kv)
		}
	}

	resp.Id = task.Id
	resp.Success = true
	return nil
}

func (w *WorkerEntry) HandleReduceTask(task *ReduceTask, resp *ReduceTaskResp) error {
	log.SetPrefix("Worker" + strconv.Itoa(w.id))
	log.Printf("Receive reduce task %v.\n", task.Id)
	w.doHanddleReduceTask(task, resp)
	return nil
}

func (w *WorkerEntry) doHanddleReduceTask(task *ReduceTask, resp *ReduceTaskResp) error {
	log.SetPrefix("Worker" + strconv.Itoa(w.id))
	resp.Id = task.Id
	oname := fmt.Sprintf("mr-out-%v", task.Id)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	writer := bufio.NewWriter(ofile)
	defer writer.Flush()
	files := []*json.Decoder{}
	for i := 0; i < task.MapParallelism; i++ {
		f, err := os.Open(fmt.Sprintf("mr-%v-%v", i, task.Id))
		if err != nil {
			log.Printf("Open file %v failed, err: %v\n", f.Name(), err)
			resp.Success = false
			return nil
		}
		defer f.Close()
		reader := json.NewDecoder(bufio.NewReader(f))
		files = append(files, reader)
	}
	intermediate := []KeyValue{}
	for _, decoder := range files {
		var prev KeyValue
		for {
			if err := decoder.Decode(&prev); err == io.EOF {
				break
			} else if err == nil {
				intermediate = append(intermediate, prev)
			} else {
				log.Printf("Handle shuffle file failed, err: %v", err)
				resp.Success = false
				return nil
			}
		}
	}
	sort.Sort(ByKey(intermediate))
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
		output := w.reducef(intermediate[i].Key, values)
		fmt.Fprintf(writer, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	resp.Success = true
	return nil
}

func (w *WorkerEntry) Done(req, resp *Args) error {
	w.done <- struct{}{}
	return nil
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
