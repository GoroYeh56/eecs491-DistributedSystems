package paxos

import (
	"umich.edu/eecs491/proj3/common"
)

// In all data types that represent RPC arguments/reply, field names
// must start with capital letters, otherwise RPC will break.

const (
	OK     = "OK"
	Reject = "Reject"
)

type Response string

type DoneArgs struct {
	Source   int
	LastDone int
}

type Proposal struct {
	Number uint // proposed np
	Value  interface{}
}

func (p *Proposal) Some() bool {
	return p.Number > 0
}

type PrepareArgs struct {
	Seq       int
	ProposedN uint
}

type PrepareReply struct {
	Response Response
	Log      InstanceLog
}

type AcceptArgs struct {
	Seq      int
	Proposal Proposal
}

type AcceptReply struct {
	Response Response
}

type DecidedArgs struct {
	Seq      int
	Decision Proposal
	Done     DoneArgs
}

type DecidedReply struct {
	Response Response
	LastDone int
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	if !px.isdead() {
		px.mu.Lock()
		// println("Prepare on", px.me, "for", args.Instance, "with Proposal", args.Proposal)
		var log = px.getLog(args.Seq)
		if args.ProposedN > log.Instance.PromisedN && log.Fate == Pending {
			// println("Promise on", px.me, "for", args.Instance, "with Proposal", args.Proposal)
			log.Instance.PromisedN = args.ProposedN
			px.impl.Log[args.Seq] = log
			reply.Response = OK
		} else {
			reply.Response = Reject
		}
		px.mu.Unlock()
		reply.Log = log
	} else {
		reply.Response = Reject
	}
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	if !px.isdead() {
		px.mu.Lock()
		// println("Accept on", px.me, "for", args.Instance, "with Proposal", args.Proposal.ID)
		var log = px.getLog(args.Seq)
		if args.Proposal.Number >= log.Instance.PromisedN && log.Fate == Pending {
			log.Instance.PromisedN = args.Proposal.Number
			log.Instance.Accepted = args.Proposal
			// println("Accepts Proposal", args.Proposal.ID, "on", px.me)
			px.impl.Log[args.Seq] = log
			reply.Response = OK
		} else {
			reply.Response = Reject
		}
		px.mu.Unlock()
	} else {
		reply.Response = Reject
	}
	return nil
}

func (px *Paxos) Learn(args *DecidedArgs, reply *DecidedReply) error {
	var lastDone = px.getDone(px.me)
	reply.LastDone = lastDone
	if !px.isdead() {
		px.mu.Lock()
		// println("Learn on", px.me, "for", args.Instance, "with Proposal", args.Decision.ID)
		var log = px.getLog(args.Seq)
		px.setDone(args.Done)
		if log.Fate == Pending {
			// println("Learns Proposal", args.Decision.ID, "on", px.me)
			log.Fate = Decided
			log.Instance.Accepted = args.Decision
			if log.Instance.PromisedN < args.Decision.Number {
				log.Instance.PromisedN = args.Decision.Number
			}
			px.impl.Log[args.Seq] = log
			reply.Response = OK
		} else {
			reply.Response = Reject
		}
		if px.impl.GCState.Updates > uint(len(px.peers)) {
			px.collectGarbage()
		}
		px.mu.Unlock()
	} else {
		reply.Response = Reject
	}
	return nil
}

//
// add RPC handlers for any RPCs you introduce.
//

type PrepareBroadcast struct {
	Consensus     bool
	Decided       bool
	NextProposedN uint
	Proposal      Proposal
}

func (px *Paxos) proposerPrepare(args *PrepareArgs, result *PrepareBroadcast) {
	result.Consensus = false
	result.Decided = false
	var accepted = uint(0)
	var promises = uint(0)
	var highest_promise = args.ProposedN
	// iterate all peers and calculate the number of acceptance
	for i := range px.peers {
		var ok = false
		var reply = PrepareReply{}
		var fate, _ = px.Status(args.Seq)
		if px.isdead() || fate != Pending || result.Consensus || result.Decided {
			break
		} else if i == px.me {
			px.Prepare(args, &reply)
			ok = true
		} else {
			ok = common.Call(px.peers[i], "Paxos.Prepare", args, &reply)
		}
		if ok {

			// Update highest_promise
			if reply.Log.Instance.PromisedN > highest_promise {
				highest_promise = reply.Log.Instance.PromisedN
			}
			// Update
			if reply.Log.Instance.Accepted.Number > result.Proposal.Number {
				result.Proposal = reply.Log.Instance.Accepted
				accepted = 1
			} else if result.Proposal.Some() && reply.Log.Instance.Accepted.Number == result.Proposal.Number {
				accepted++
				result.Decided = px.majority(accepted)
			}

			if reply.Log.Fate != Pending {
				result.Decided = true
				result.Proposal = reply.Log.Instance.Accepted
			}

			if reply.Response == OK {
				promises++
				result.Consensus = px.majority(promises)
			}
		}
	}
	result.NextProposedN = px.proposeHigher(highest_promise)
}

func (px *Paxos) proposerAccept(args *AcceptArgs) bool {
	var accepted = uint(0)
	for i := range px.peers {
		var ok = false
		var reply = AcceptReply{}
		var fate, _ = px.Status(args.Seq)
		if px.isdead() || fate != Pending || px.majority(accepted) {
			break
		} else if i == px.me {
			px.Accept(args, &reply)
			ok = true
		} else {
			ok = common.Call(px.peers[i], "Paxos.Accept", args, &reply)
		}
		if ok && reply.Response == OK {
			accepted++
		}
	}
	return px.majority(accepted)
}

func (px *Paxos) proposerLearn(args *DecidedArgs) {
	var done = make(chan DoneArgs, len(px.peers))
	for i := range px.peers {
		var reply = DecidedReply{}
		if px.isdead() {
			break
		} else if i == px.me {
			px.Learn(args, &reply)
		} else if common.Call(px.peers[i], "Paxos.Learn", args, &reply) {
			done <- DoneArgs{i, reply.LastDone}
		}
	}
	px.mu.Lock()
	defer px.mu.Unlock()
	for len(done) > 0 {
		px.setDone(<-done)
	}
	px.collectGarbage()
}
