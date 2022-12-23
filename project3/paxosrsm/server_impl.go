package paxosrsm

import (
	"time"

	"umich.edu/eecs491/proj3/paxos"
)

const InitialSleep = 10 * time.Millisecond

// additions to PaxosRSM state
type PaxosRSMImpl struct {
	Seq int // sequence number
}

// initialize rsm.impl.*
func (rsm *PaxosRSM) InitRSMImpl() {
	rsm.impl.Seq = 0
}

var sleepTime = InitialSleep

// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
func (rsm *PaxosRSM) AddOp(v interface{}) {

	/*
		This PaxosRSM call px.Start(seq)
		Wait until Paxos reach agreement!

	*/

	var sleepTime = InitialSleep
	for {
		println("PaxosRSM using instance", rsm.impl.Seq, "on", rsm.me)
		rsm.px.Start(rsm.impl.Seq, v)
		time.Sleep(sleepTime) //performance issue
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
		// get agreed value from paxos peers
		// fmt.Printf("		[KVPaxos %d rsm try seq %d op: %v\n", rsm.me, rsm.impl.Seq, v)
		status, value := rsm.px.Status(rsm.impl.Seq)
		if status != paxos.Pending {
			println("Applying update", rsm.impl.Seq, "on", rsm.me)
			rsm.applyOp(value)
			rsm.px.Done(rsm.impl.Seq)
			rsm.impl.Seq++
			if value == v {
				break
			} else {
				status = paxos.Pending   // reset status
				sleepTime = InitialSleep // reset sleepTime
			}
		}

	}

}
