package paxos

import (
	"sync/atomic"
	"time"

	"umich.edu/eecs491/proj3/common"
)

const BackoffMin = 250
const BackoffMax = 450

type Range struct {
	Lower int64
	Upper int64
}

type Instance struct {
	PromisedN uint
	Accepted  Proposal
}

type InstanceLog struct {
	Fate     Fate
	Instance Instance
}

type GarbageCollector struct {
	LastDone []int64
	Updates  uint
}

func (gc *GarbageCollector) init() {
	for i := range gc.LastDone {
		gc.LastDone[i] = -1
	}
}

func (px *Paxos) updateBound() int {
	var result = px.getDone(0)
	for i := range px.impl.GCState.LastDone {
		var done = px.getDone(i)
		if done < result {
			result = done
		}
	}
	px.impl.GCState.Updates = 0
	return result + 1
}

// additions to Paxos state.
type PaxosImpl struct {
	Log      map[int]InstanceLog
	LogRange Range
	GCState  GarbageCollector
}

func (px *Paxos) initialProposal() uint {
	return uint(px.me) + 1
}

func (px *Paxos) majority(votes uint) bool {
	return votes >= uint(len(px.peers)+1)/2
}

func (px *Paxos) proposeHigher(p uint) uint {
	var servers = uint(len(px.peers))
	var r, pe = p / servers, p % servers
	if pe > uint(px.me) {
		r++
	}
	return px.initialProposal() + r*servers
}

// requires Locked Environment
func (px *Paxos) getLog(instanceInt int) InstanceLog {
	var instance = int64(instanceInt)
	if instance < px.impl.LogRange.Lower {
		return InstanceLog{Forgotten, Instance{}}
	} else if v, has := px.impl.Log[instanceInt]; has {
		return v
	} else {
		if instance > px.impl.LogRange.Upper {
			px.impl.LogRange.Upper = instance
		}
		var log = InstanceLog{Pending, Instance{}}
		px.impl.Log[instanceInt] = log
		return log
	}
}

func (px *Paxos) getDone(pxID int) int {
	// return px.impl.GCState.getDone(p)
	return int(atomic.LoadInt64(&(px.impl.GCState.LastDone[pxID])))
}

func (px *Paxos) setDone(args DoneArgs) {
	// px.impl.GCState.setDone(args)
	var l = px.getDone(px.me)
	if l < args.LastDone {
		px.impl.GCState.LastDone[args.Source] = int64(args.LastDone)
		px.impl.GCState.Updates++
	}

}

func (px *Paxos) collectGarbage() {
	var result = int64(px.updateBound())
	if result > px.impl.LogRange.Lower {
		px.impl.LogRange.Lower = result
		// scan from last min to new min, not for all over map
		for k := range px.impl.Log {
			if int64(k) < px.impl.LogRange.Lower {
				delete(px.impl.Log, k)
			}
		}
	}
}

func randomDuration(min int, max int, unit time.Duration) time.Duration {
	var length = min + int(common.Nrand())%(max-min)
	return time.Duration(length) * unit
}

// your px.impl.* initializations here.
func (px *Paxos) initImpl() {
	px.impl = PaxosImpl{
		Log:      make(map[int]InstanceLog),
		LogRange: Range{-1, -1},
		GCState: GarbageCollector{
			make([]int64, len(px.peers)),
			0,
		},
	}
	px.impl.GCState.init()
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	go func() {
		px.mu.Lock()
		var log = px.getLog(seq)
		px.mu.Unlock()
		var statistics = PrepareBroadcast{
			Consensus:     false,
			Decided:       false,
			NextProposedN: px.initialProposal(),
			Proposal:      log.Instance.Accepted,
		}
		for !px.isdead() && log.Fate == Pending {
			var proposal = Proposal{statistics.NextProposedN, v}
			var prepare_args = PrepareArgs{
				Seq:       seq,
				ProposedN: statistics.NextProposedN,
			}
			px.proposerPrepare(&prepare_args, &statistics)
			var accept_consensus = false
			if statistics.Decided {
				proposal = statistics.Proposal
			} else if statistics.Consensus {
				if statistics.Proposal.Some() {
					proposal.Value = statistics.Proposal.Value
				}
				var accept_args = AcceptArgs{
					Seq:      seq,
					Proposal: proposal,
				}
				accept_consensus = px.proposerAccept(&accept_args)
			}
			if (statistics.Decided || accept_consensus) && proposal.Some() {
				// println("Broadcasting decided instance", seq, "with Proposal", proposal.ID)
				var decision = DecidedArgs{
					seq,
					proposal,
					DoneArgs{px.me, px.getDone(px.me)},
				}
				px.proposerLearn(&decision)
				break
			}
			time.Sleep(randomDuration(BackoffMin, BackoffMax+1, time.Millisecond))
			px.mu.Lock()
			log = px.getLog(seq)
			px.mu.Unlock()
		}
	}()
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.setDone(DoneArgs{px.me, seq})
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	return int(atomic.LoadInt64(&(px.impl.LogRange.Upper)))
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min does not reflect another peer's Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers' Min()s will not increase
// even if all reachable peers call Done(). The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.collectGarbage()
	return int(px.impl.LogRange.Lower) // anything below Min() is garbagecollect
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so, what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	var log = px.getLog(seq)
	if log.Instance.Accepted.Some() {
		return log.Fate, log.Instance.Accepted.Value
	} else {
		return log.Fate, nil
	}
}
