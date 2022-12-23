package mapreduce

// import (
// 	"fmt"
// 

// any additional state that you want to add to type WorkerInfo
type WorkerInfoImpl struct {
}

//
// run the MapReduce job across all the workers
//

// Get the number of workers the master can use
// at the beginning
func CheckRegisteredWorkers(mr *MapReduce) {
	for true {
		worker_address := <-mr.registerChannel

		// add this information to mr.Workers
		worker_info := &WorkerInfo{worker_address, WorkerInfoImpl{}}
		mr.Workers[worker_address] = worker_info

		// push to mr.impl.freeWorker
		mr.mu.Lock()
		mr.impl.freeWorkers = append(mr.impl.freeWorkers, worker_address)	
		mr.mu.Unlock()
		mr.impl.HasWorkerChannel <- true
	}
}

// Use RPC call to assign job for the worker
// JobType: "Map" or "Reduce"
func AssignJobs(mr *MapReduce, JobType_ JobType, nNumOtherPhase int, JobNumber int) {

	args := &DoJobArgs{
		mr.file,
		JobType_,
		JobNumber, // index
		nNumOtherPhase}
	var reply DoJobReply

	// If I have a new worker
	<-mr.impl.HasWorkerChannel
	mr.mu.Lock()
	free_worker_addr := mr.impl.freeWorkers[0]
	// pop this worker (busy now)
	mr.impl.freeWorkers = mr.impl.freeWorkers[1:]
	mr.mu.Unlock()

	ok := call(free_worker_addr, "Worker.DoJob", args, &reply)

	mr.mu.Lock()
	mr.impl.freeWorkers = append(mr.impl.freeWorkers, free_worker_addr)
	mr.mu.Unlock()
	mr.impl.HasWorkerChannel <- true	

	if ok == false {
		// Fail : Master should re-assign this task to other free worker!
		go AssignJobs(mr, JobType_, nNumOtherPhase, JobNumber)

	} else {

		mr.mu.Lock()
		mr.impl.numDoneTasks = mr.impl.numDoneTasks + 1
		if mr.impl.numDoneTasks == mr.nMap || mr.impl.numDoneTasks == mr.nReduce+mr.nMap {
			mr.impl.JobDoneChannel <- true
		}
		mr.mu.Unlock()
	}
}

func (mr *MapReduce) RunMasterImpl() {

	// 1. Check the number of workers you have in total
	// Runs infinite loop inside this thread...
	go CheckRegisteredWorkers(mr)

	// 2. Map Jobs
	for i := 0; i < mr.nMap; i++ {
		go AssignJobs(mr, "Map", mr.nReduce, i)
	}

	// 3. Waiting for all Map jobs are done
	<-mr.impl.JobDoneChannel

	// 4. Reduce Jobs
	for i := 0; i < mr.nReduce; i++ {
		go AssignJobs(mr, "Reduce", mr.nMap, i)
	}

	// Waiting for all Reduce jobs are done
	<-mr.impl.JobDoneChannel
}
