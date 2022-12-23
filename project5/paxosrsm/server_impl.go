package paxosrsm

import (
	"time"

	"umich.edu/eecs491/proj5/paxos"
)

const InitialSleep = 10 * time.Millisecond

// additions to PaxosRSM state
type PaxosRSMImpl struct {
	CurrentInstance int
}

// initialize rsm.impl.*
func (rsm *PaxosRSM) InitRSMImpl() {
	rsm.impl = PaxosRSMImpl{
		CurrentInstance: 0,
	}
}

// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
func (rsm *PaxosRSM) AddOp(v interface{}) {
	var sleepTime = InitialSleep
	for {
		// println("PaxosRSM using instance", rsm.impl.CurrentInstance, "on", rsm.me)
		rsm.px.Start(rsm.impl.CurrentInstance, v)
		time.Sleep(sleepTime) //performance issue
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
		var status, result = rsm.px.Status(rsm.impl.CurrentInstance)
		if status != paxos.Pending {
			println("Applying update seq:", rsm.impl.CurrentInstance, "on rsm of server", rsm.me)
			rsm.applyOp(result)
			rsm.px.Done(rsm.impl.CurrentInstance)
			rsm.impl.CurrentInstance++

			if rsm.equals(result, v) {
				break
			} else {
				status = paxos.Pending
				sleepTime = InitialSleep
			}
		}
	}
}
