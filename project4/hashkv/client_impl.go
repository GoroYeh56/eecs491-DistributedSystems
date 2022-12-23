package hashkv

import (
	"fmt"
	"time"

	"umich.edu/eecs491/proj4/common"
)

// additions to Clerk state
type ClerkImpl struct {
	Self ClientInfo
}

// initialize ck.impl.*
func (ck *Clerk) InitImpl() {
	ck.impl = ClerkImpl{
		Self: ClientInfo{
			Name:  common.Nrand(),
			ReqID: 0,
		},
	}
}

func (ck *Clerk) randServerID() int {
	return int(common.Nrand()) % len(ck.servers)
}

// fetch the current value for a key.
// return "" if the key does not exist.
// keep retrying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {

	/*
		For-loop to try different server
		If Err = ErrorWrongServer: try next server
		Else:  return string
	*/

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
		server := ck.servers[ck.randServerID()]
		// for _, server := range ck.servers {
		// fmt.Printf("	[Try Get:] Client %d send Get(%s) request to %s \n", ck.impl.Self.Name, key, server)
		// fmt.Printf("	key to Shard: %d \n", common.Key2Shard(key))
		success = common.Call(server, "HashKV.Get", &args, &reply)
		success = success && (reply.Err == OK || reply.Err == ErrNoKey)
		if success {
			fmt.Printf("	Get Success: client %d Get(%s) : %s\n", ck.impl.Self.Name, key, reply.Value)
		}
		if !success {
			time.Sleep(100 * time.Millisecond)
		}
		// }
	}
	return reply.Value
}

// send a Put or Append request.
// keep retrying forever until success.
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
		// for _, server := range ck.servers {
		server := ck.servers[ck.randServerID()]
		// fmt.Printf("Client %d send Put(%s, %s) request to %s \n", ck.impl.Self.Name, key, value, server)
		// fmt.Printf("	key to Shard: %d \n", common.Key2Shard(key))
		success = common.Call(server, "HashKV.PutAppend", &args, &reply)
		success = success && reply.Err == OK
		// if success {
		// fmt.Printf("	Success! Client %d send Put(%s, %s) request to %s \n", ck.impl.Self.Name, key, value, server)
		// }
		// sleep between retry
		if !success {
			time.Sleep(100 * time.Millisecond)
		}
		// }
	}
	fmt.Printf("	Success! Client %d send Put(%s, %s)\n", ck.impl.Self.Name, key, value)

}
