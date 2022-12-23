package paxos

import (
	"umich.edu/eecs491/proj5/common"
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
	ID    uint
	Value interface{}
}

func (p *Proposal) Some() bool {
	return p.ID > 0
}

type PrepareArgs struct {
	Instance int
	Proposal uint
}

type PrepareReply struct {
	Response Response
	Log      InstanceLog
}

type AcceptArgs struct {
	Instance int
	Proposal Proposal
}

type AcceptReply struct {
	Response Response
}

type DecidedArgs struct {
	Instance int
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
		var log = px.getLog(args.Instance)
		if args.Proposal > log.Instance.Promised && log.Fate == Pending {
			// println("Promise on", px.me, "for", args.Instance, "with Proposal", args.Proposal)
			log.Instance.Promised = args.Proposal
			px.impl.Log[args.Instance] = log
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
		var log = px.getLog(args.Instance)
		if args.Proposal.ID >= log.Instance.Promised && log.Fate == Pending {
			log.Instance.Promised = args.Proposal.ID
			log.Instance.Accepted = args.Proposal
			// println("Accepts Proposal", args.Proposal.ID, "on", px.me)
			px.impl.Log[args.Instance] = log
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
	// var lastDone = px.getDone(px.me)
	// reply.LastDone = lastDone
	if !px.isdead() {
		px.mu.Lock()
		var lastDone = px.getDone(px.me)
		reply.LastDone = lastDone
		// println("Learn on", px.me, "for", args.Instance, "with Proposal", args.Decision.ID)
		var log = px.getLog(args.Instance)
		px.setDone(args.Done)
		if log.Fate == Pending {
			// println("Learns Proposal", args.Decision.ID, "on", px.me)
			log.Fate = Decided
			log.Instance.Accepted = args.Decision
			if log.Instance.Promised < args.Decision.ID {
				log.Instance.Promised = args.Decision.ID
			}
			px.impl.Log[args.Instance] = log
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
	Consensus bool
	Decided   bool
	NextID    uint
	Proposal  Proposal
}

func (px *Paxos) proposerPrepare(args *PrepareArgs, result *PrepareBroadcast) {
	result.Consensus = false
	result.Decided = false
	var accepted = uint(0)
	var promises = uint(0)
	var highest_promise = args.Proposal
	for i := range px.peers {
		var ok = false
		var reply = PrepareReply{}
		var fate, _ = px.Status(args.Instance)
		if px.isdead() || fate != Pending || result.Consensus || result.Decided {
			break
		} else if i == px.me {
			px.Prepare(args, &reply)
			ok = true
		} else {
			ok = common.Call(px.peers[i], "Paxos.Prepare", args, &reply)
		}
		if ok {
			if reply.Log.Instance.Promised > highest_promise {
				highest_promise = reply.Log.Instance.Promised
			}
			if reply.Log.Instance.Accepted.ID > result.Proposal.ID {
				result.Proposal = reply.Log.Instance.Accepted
				accepted = 1
			} else if result.Proposal.Some() && reply.Log.Instance.Accepted.ID == result.Proposal.ID {
				accepted++
				result.Decided = px.consensus(accepted)
			}
			if reply.Log.Fate != Pending {
				result.Decided = true
				result.Proposal = reply.Log.Instance.Accepted
			}
			if reply.Response == OK {
				promises++
				result.Consensus = px.consensus(promises)
			}
		}
	}
	result.NextID = px.proposeHigher(highest_promise)
}

func (px *Paxos) proposerAccept(args *AcceptArgs) bool {
	var accepted = uint(0)
	for i := range px.peers {
		var ok = false
		var reply = AcceptReply{}
		var fate, _ = px.Status(args.Instance)
		if px.isdead() || fate != Pending || px.consensus(accepted) {
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
	return px.consensus(accepted)
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
