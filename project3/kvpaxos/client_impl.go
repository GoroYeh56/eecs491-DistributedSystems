package kvpaxos

import (
	"fmt"

	"umich.edu/eecs491/proj3/common"
)

// any additions to Clerk state
type ClerkImpl struct {
	Self ClientInfo // name(int64) and request ID (int)
}

// initialize ck.impl state
func (ck *Clerk) InitImpl() {
	ck.impl.Self.Name = common.Nrand()
	fmt.Printf("Initialize Client : %d\n", ck.impl.Self.Name)
	ck.impl.Self.ReqID = 0
}

func (ck *Clerk) randServerID() int {
	return int(common.Nrand()) % len(ck.servers)
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	var args = GetArgs{
		Key: key,
		Impl: GetArgsImpl{
			Client: ck.impl.Self,
		},
	}
	ck.impl.Self.ReqID++
	var reply = GetReply{
		Err:   "",
		Value: "",
	}
	var success = false
	for !success {
		success = common.Call(ck.servers[ck.randServerID()], "KVPaxos.Get", &args, &reply)
		success = success && (reply.Err == OK || reply.Err == ErrNoKey)
	}
	return reply.Value
}

// shared by Put and Append; op is either "Put" or "Append"
func (ck *Clerk) PutAppend(key string, value string, op string) {
	var args = PutAppendArgs{
		Op:    op,
		Key:   key,
		Value: value,
		Impl: PutAppendArgsImpl{
			Client: ck.impl.Self,
		},
	}
	ck.impl.Self.ReqID++
	var reply = PutAppendReply{
		Err: "",
	}
	var success = false
	for !success {
		success = common.Call(ck.servers[ck.randServerID()], "KVPaxos.PutAppend", &args, &reply)
		success = success && reply.Err == OK
	}
}
