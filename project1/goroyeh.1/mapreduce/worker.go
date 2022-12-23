package mapreduce

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Worker is a server waiting for DoJob or Shutdown RPCs

type Worker struct {
	name   string
	Reduce func(string, []string) string
	Map    func(string) []KeyValue
	nRPC   int
	nJobs  int
	l      net.Listener
	fprob  float64 // probability of network failure
	mu     sync.Mutex
}

// The master sent us a job
func (wk *Worker) DoJob(arg *DoJobArgs, res *DoJobReply) error {
	fmt.Printf("Dojob %s job %d file %s operation %v N %d\n",
		wk.name, arg.JobNumber, arg.File, arg.Operation,
		arg.NumOtherPhase)
	switch arg.Operation {
	case Map:
		DoMap(arg.JobNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case Reduce:
		DoReduce(arg.JobNumber, arg.File, arg.NumOtherPhase, wk.Reduce)
	}
	res.OK = true
	return nil
}

// The master is telling us to shutdown. Report the number of Jobs we
// have processed.
func (wk *Worker) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
	wk.mu.Lock()
	defer wk.mu.Unlock()

	DPrintf("Shutdown %s\n", wk.name)
	res.Njobs = wk.nJobs
	res.OK = true
	wk.nRPC = 1 // OK, because the same thread reads nRPC
	wk.nJobs--  // Don't count the shutdown RPC
	return nil
}

// Tell the master we exist and ready to work
func Register(master string, me string) {
	args := &RegisterArgs{}
	args.Worker = me
	var reply RegisterReply
	ok := call(master, "MapReduce.Register", args, &reply)
	if ok == false {
		fmt.Printf("Register: RPC %s register error\n", master)
	}
}

// Set up a connection with the master, register with the master,
// and wait for jobs from the master
func RunWorker(MasterAddress string, me string, MapFunc func(string) []KeyValue,
	ReduceFunc func(string, []string) string, nRPC int, fprob float64, straggler bool) {
	DPrintf("RunWorker %s\n", me)

	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	wk.fprob = fprob

	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me) // only needed for "unix"

	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l
	Register(MasterAddress, me)

	wk.mu.Lock()
	defer wk.mu.Unlock()
	for wk.nRPC != 0 {
		wk.mu.Unlock()
		conn, err := wk.l.Accept()
		wk.mu.Lock()
		if err == nil {
			if (rand.Int63() % 1000) < int64(wk.fprob*1000) {
				// discard the request
				conn.Close()
			} else {
				if straggler {
					time.Sleep(10 * time.Second)
				}
				wk.nRPC -= 1
				go rpcs.ServeConn(conn)
				wk.nJobs += 1
			}
		} else {
			break
		}
	}
	wk.l.Close()
	DPrintf("RunWorker %s exit\n", me)
}
