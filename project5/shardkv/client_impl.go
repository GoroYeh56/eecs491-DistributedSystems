package shardkv

import (
	"umich.edu/eecs491/proj5/common"
)

// additions to Clerk state
type ClerkImpl struct {
	Self common.ClientInfo
}

// initialize ck.impl.*
func (ck *Clerk) InitImpl() {

	name := common.Nrand()
	// -1 is reserved for ShardMaster!
	for name == -1 {
		name = common.Nrand()
	}

	ck.impl = ClerkImpl{
		Self: common.ClientInfo{
			Name:  name,
			ReqID: 0,
		},
	}
}

func randServerID(servers []string) int {
	return int(common.Nrand()) % len(servers)
}

// fetch the current value for a key.
// return "" if the key does not exist.
// keep retrying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {

	// Get current configuration
	config := ck.sm.Query(-1)
	// Get target shard
	shard := common.Key2Shard(key)
	// Find the group that handles this shard
	servers := config.Groups[config.Shards[shard]]

	// iterage servers and sent Get (try propose by Paxos)
	var args = GetArgs{
		Key: key,
		Impl: GetArgsImpl{
			ClientInfo: ck.impl.Self,
		},
	}
	ck.impl.Self.ReqID++
	var reply = GetReply{
		Err:   "",
		Value: "",
	}
	println("Client", ck.impl.Self.Name, " send Get(", key, ") to shard", shard)
	println("	servers:")
	for _, srv := range servers {
		println("	", srv)
	}

	var success = false
	for !success {
		idx := randServerID(servers)
		srv := servers[idx]
		println("Client", ck.impl.Self.Name, "send Get(", key, ") to srv", srv)
		reply = GetReply{
			Err:   "",
			Value: "",
		}
		success = common.Call(srv, "ShardKV.Get", &args, &reply)
		success = success && (reply.Err == OK || reply.Err == ErrNoKey)
		if reply.Err == ErrWrongGroup {
			// get a new config:
			config = ck.sm.Query(-1)
			servers = config.Groups[config.Shards[shard]]
		}
	}
	println("	Client", ck.impl.Self.Name, " done Get(", key, ")")
	return reply.Value

}

// send a Put or Append request.
// keep retrying forever until success.
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Get current configuration
	config := ck.sm.Query(-1)
	// Get target shard
	shard := common.Key2Shard(key)
	// Find the group that handles this shard
	servers := config.Groups[config.Shards[shard]]

	// iterage servers and sent Get (try propose by Paxos)
	var args = PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Impl: PutAppendArgsImpl{
			ClientInfo: ck.impl.Self,
		},
	}
	ck.impl.Self.ReqID++
	var reply = PutAppendReply{
		Err: "",
	}

	println("Client", ck.impl.Self.Name, " send ", op, "(", key, ",", value, ") to shard", shard)
	println("	servers:")
	for _, srv := range servers {
		println("	", srv)
	}
	var success = false
	for !success {
		idx := randServerID(servers)
		srv := servers[idx]
		reply = PutAppendReply{
			Err: "",
		}
		println("Client", ck.impl.Self.Name, "send PutAppend(", key, ":", value, ") to srv", srv)
		success = common.Call(srv, "ShardKV.PutAppend", &args, &reply)
		success = success && reply.Err == OK
		if reply.Err == ErrWrongGroup {
			println("Client PutAppend(", key, ",", value, ") get ErrWrongGroup!")
			// get a new config:
			config = ck.sm.Query(-1)
			servers = config.Groups[config.Shards[shard]]
			// Remember to increase your ReqID if the config has changed!
			// And, update argument!
			args = PutAppendArgs{
				Key:   key,
				Value: value,
				Op:    op,
				Impl: PutAppendArgsImpl{
					ClientInfo: ck.impl.Self,
				},
			}
			ck.impl.Self.ReqID++
		}
	}

	println("	Client", ck.impl.Self.Name, " done PutAppend")
}
