package main

import (
	"fmt"
	"os"
	"strconv"

	"umich.edu/eecs491/proj1/mapreduce"
)

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 failure_probability &)
func main() {
	if len(os.Args) == 4 && os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			//nMap, nReduce, file, Map ,Reduce
			mapreduce.RunSingle(5, 3, os.Args[2], mapreduce.WCMap, mapreduce.WCReduce)
		} else {
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else if len(os.Args) == 5 && os.Args[1] == "worker" {
		fprob, err := strconv.ParseFloat(os.Args[4], 64)
		if err != nil {
			mapreduce.RunWorker(os.Args[2], os.Args[3], mapreduce.WCMap, mapreduce.WCReduce, 100, fprob, false)
		} else {
			fmt.Printf("Unable to parse failure probability parameter: %s\n", os.Args[4])
		}
	} else {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	}
}
