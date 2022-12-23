package pbservice

import (
	"fmt"
)

// additions to Clerk state.
type ClerkImpl struct {
	// Cache local primary!
	name    int64
	primary string
	OpID    int
}

// your ck.impl.* initializations here.
func (ck *Clerk) initImpl() {
	ck.impl.name = nrand()
	ck.impl.primary = "" // initially: empty primary
	ck.impl.OpID = 0
}

// fetch a key's value from the current primary;
// if the key has never been set, return "".
// Get() must keep trying until either the
// primary replies with the value or the primary
// says the key doesn't exist, i.e., has never been Put().
func (ck *Clerk) Get(key string) string {

	// For initial cache primary
	if ck.impl.primary == "" {
		cur_view, _ := ck.vs.Get()
		ck.impl.primary = cur_view.Primary
	}

	args := &GetArgs{}
	args.Key = key
	args.Impl = GetArgsImpl{ck.impl.name, ck.impl.OpID}
	reply := GetReply{}

	// send an RPC request to primary, wait for the reply.
	fmt.Printf("\n\nclient %d send Get(%s) RPC to primary: %s (OpID: %d)!\n", ck.impl.name, key, ck.impl.primary, ck.impl.OpID)
	ok := call(ck.impl.primary, "PBServer.Get", args, &reply)
	if ok == false || reply.Err != OK {

		//// Should not reply if RPC keep failing
		for ok == false || reply.Err != OK {

			// fmt.Printf("Client Get() RPC Fail! Should ask for a new primary!\n")
			new_primary := ck.vs.Primary()
			fmt.Printf("		New primary: %s\n ", new_primary)
			ck.impl.primary = new_primary
			// Send request again
			fmt.Printf("Retry ... client %d send Get RPC to primary: %s (OpID: %d)!\n", ck.impl.name, ck.impl.primary, ck.impl.OpID)
			ok = call(ck.impl.primary, "PBServer.Get", args, &reply)
		}
	}
	fmt.Printf("client %d recv reply from Get(%s) : %s \n", ck.impl.name, key, reply.Value)
	ck.impl.OpID++
	return reply.Value
}

// send a Put() or Append() RPC
// must keep trying until it succeeds.
func (ck *Clerk) PutAppend(key string, value string, op string) {

	if ck.impl.primary == "" {
		cur_view, _ := ck.vs.Get()
		ck.impl.primary = cur_view.Primary
	}

	args := &PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Impl = PutAppendArgsImpl{ck.impl.name, ck.impl.OpID, op}
	reply := PutAppendReply{}

	fmt.Printf("\n\nclient %d send PutAppend RPC (%s) to primary: %s (OpID: %d)!\n", ck.impl.name, args.Impl.Op, ck.impl.primary, ck.impl.OpID)
	// send an RPC request to primary, wait for the reply.
	ok := call(ck.impl.primary, "PBServer.PutAppend", args, &reply)
	if ok == false || reply.Err != OK {
		// Retry until its request is meet

		for ok == false || reply.Err != OK {
			// fmt.Printf("Client PutAppend() RPC Fail! Should ask for a new primary!\n")
			new_primary := ck.vs.Primary()
			// fmt.Printf("		New primary: %s\n ", new_primary)
			ck.impl.primary = new_primary
			// Send request again
			// fmt.Printf("Retry ... client %d send PutAppend RPC to primary: %s! (OpID: %d)\n", ck.impl.name, ck.impl.primary, ck.impl.OpID)
			ok = call(ck.impl.primary, "PBServer.PutAppend", args, &reply)
		}
	}
	fmt.Printf("client %d recv reply from Put(%s, %s) \n", ck.impl.name, key, value)
	// For next request!
	ck.impl.OpID++
}
