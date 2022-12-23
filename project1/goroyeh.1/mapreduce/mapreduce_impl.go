package mapreduce


// any additional state that you want to add to type MapReduce
type MapReduceImpl struct {
	numDoneTasks int
	freeWorkers []string // a slice of string
	JobDoneChannel chan bool
	HasWorkerChannel chan bool

}

func max(x, y int) int{
	if x>y {
		return x
	} else{
		return y
	}
}
// additional initialization of mr.* state beyond that in InitMapReduce
func (mr *MapReduce) InitMapReduceImpl(nmap int, nreduce int,
	file string, master string) {

	mr.nMap = nmap
	mr.nReduce = nreduce
	mr.file = file
	mr.MasterAddress = master

	// // Map of registered workers that you need to keep up to date
	mr.Workers = make(map[string]*WorkerInfo)
	mr.impl = MapReduceImpl{0, []string{}, make(chan bool), make(chan bool, max(mr.nMap, mr.nReduce))} 

}
