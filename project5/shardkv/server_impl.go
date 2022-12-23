package shardkv

import "umich.edu/eecs491/proj5/common"

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters
type Op struct {
	Op     string
	Key    string
	Value  string
	Client common.ClientInfo
	// For UpdateShard
	Shard              int
	Database           map[string]string // what database we need to copy from
	ClientLatestReqIDs map[common.ReqKey]int
}

// Method used by PaxosRSM to determine if two Op values are identical
func equals(v1 interface{}, v2 interface{}) bool {
	// Note: comparing Op
	/*
		Op     string


		Key    string
		Value  string
		Client ClientInfo
		Shard    int
		Database map[string]string // what database we need to copy from
	*/
	op1 := v1.(Op)
	op2 := v2.(Op)
	// println("	kv.equals()...")
	// println("	op1.Op", op1.Op, " op2.Op ", op2.Op)
	if op1.Op == op2.Op {
		switch op1.Op {
		case "Get":
			// Key and Client are the same
			return op1.Client == op2.Client && op1.Key == op2.Key
		case "Put":
			// println("	op1.Client", op1.Client.Name, ",", op1.Client.ReqID, " op2.Client", op2.Client.Name, ",", op2.Client.ReqID)
			// println("	op1.K,V", op1.Key, ",", op1.Value, " op2.K,V", op2.Key, ",", op2.Value)
			return op1.Client == op2.Client && op1.Key == op2.Key && op1.Value == op2.Value
		case "Append":
			return op1.Client == op2.Client && op1.Key == op2.Key && op1.Value == op2.Value
		case "PackShard":
			return op1.Client == op2.Client && op1.Shard == op2.Shard
		case "UpdateShard":
			if op1.Client == op2.Client && op1.Shard == op2.Shard {
				db1 := op1.Database
				db2 := op2.Database
				for k, v := range db1 {
					v2, existed := db2[k]
					if existed == false || v2 != v {
						return false
					}
				}
				return true
			} else {
				return false
			}
		}
	}
	return false
}

// Need a state variable to determine the [from, to] range
// of this servers
type Range struct {
	from int // from this hash number
	to   int // to this hash number
}

// additions to ShardKV state
type ShardKVImpl struct {
	Shards             map[int]bool          // the set of shards this server(group) handles
	ClientLatestReqIDs map[common.ReqKey]int // map client (int64) -> it's request ID
	Database           map[string]string     // stores key/value pairs
}

// initialize kv.impl.*
func (kv *ShardKV) InitImpl() {
	kv.impl.Shards = make(map[int]bool)
	kv.impl.ClientLatestReqIDs = make(map[common.ReqKey]int)
	kv.impl.Database = make(map[string]string)
}

// RPC handler for client Get requests
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	if !kv.isdead() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		println("	kv server", kv.me, " Add Get(", args.Key, ") to rsm")
		kv.rsm.AddOp(args.asOp())
		if kv.valid(args.Key) {
			val, existed := kv.impl.Database[args.Key]
			if existed {
				reply.Err = OK
				reply.Value = val
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongGroup
		}
		println("Get reply.Err: ", reply.Err, "Done Get for", args.Key, "on", kv.me)
		// println("Done Get for", args.Key, "on", kv.me)
	} else {
		println("[GET] kv", kv.me, " is dead")
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	if !kv.isdead() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if !kv.updateClientHistory(args.Impl.ClientInfo, args.Op) {
			println("	kv server", kv.me, " Add ", args.Op, "(", args.Key, ",", args.Value, ") to rsm")
			kv.rsm.AddOp(args.asOp())
			// if kv.valid(args.Key) {
			// 	reply.Err = OK
			// } else {
			// 	reply.Err = ErrWrongGroup
			// }
		}
		if kv.valid(args.Key) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
	} else {
		println("[PutAppend] kv", kv.me, " is dead")
	}
	return nil
}

// Execute operation encoded in decided value v and update local state
func (kv *ShardKV) ApplyOp(v interface{}) {
	var op = v.(Op)
	// println(" ApplyOp: op", op.Op, "Key", op.Key, "Value", op.Value)
	println("KV Srv", kv.me, " [ApplyOp]")
	switch op.Op {
	case "Get":
		println("[ApplyOp] srv", kv.me, " Get", op.Key, ", return!")
	case "Put":
		println("[ApplyOp] srv", kv.me, " Put", op.Key, ":", op.Value)
		kv.impl.Database[op.Key] = op.Value
	case "Append":
		println("[ApplyOp] srv", kv.me, " Append", op.Key, ":", op.Value)
		var value = kv.impl.Database[op.Key]
		kv.impl.Database[op.Key] = value + op.Value
	// ignore "Get", "PackShard", "Invalidate Shard"
	case "PackShard":
		println("case PackShard: just return")
		println("[PackShard] delete ", op.Shard, "from server", kv.me)
		delete(kv.impl.Shards, op.Shard)
		println("Done Pack and Invalidate Shard", op.Shard, "on", kv.me)

	case "InvalidateShard":
		println("[ApplyOp] Invalidate: delete ", op.Shard, "from server", kv.me)
		delete(kv.impl.Shards, op.Shard)
		println("Done invalidate Shard", op.Shard, "on", kv.me)
	// just return, like "Get"
	case "UpdateShard":
		// Add this shard to my Shards.
		println("[ApplyOp] UpdateShard: srv", kv.me, " Add Shard", op.Shard, "")
		kv.impl.Shards[op.Shard] = true
		// copy op.Database to my local storage
		for key, val := range op.Database {
			println("	[ApplyOp] UpdateShard: Add (", key, ",", val, ") to server", kv.me)
			kv.impl.Database[key] = val
		}

		// copy op.ClientLatestReqIDs to my local ClientLatestReqIDs
		for key, val := range op.ClientLatestReqIDs {
			println("	[ApplyOp] UpdateShard: Add ClientReqID (", key.ClientName, ",", key.Op, ":", val, ") to server", kv.me)
			kv.impl.ClientLatestReqIDs[key] = val
		}
	}
	kv.updateClientHistory(op.Client, op.Op)
}

// Add RPC handlers for any other RPCs you introduce

func (kv *ShardKV) valid(key string) bool {
	shard := common.Key2Shard(key)
	_, existed := kv.impl.Shards[shard]
	return existed
}
func (kv *ShardKV) updateClientHistory(client common.ClientInfo, op string) bool {
	reqkey := common.ReqKey{
		ClientName: client.Name,
		Op:         op}
	var last, has = kv.impl.ClientLatestReqIDs[reqkey]
	if !has || client.ReqID > last {
		println("[UpdateClientHistory] Srv", kv.me, "add client", client.Name, "op", op, " ReqID:", client.ReqID)
		kv.impl.ClientLatestReqIDs[reqkey] = client.ReqID
		return false
	} else {
		return true
	}
}

func (g *GetArgs) asOp() Op {
	return Op{
		Op:       "Get",
		Key:      g.Key,
		Value:    "",
		Client:   g.Impl.ClientInfo,
		Database: make(map[string]string),
	}
}

func (p *PutAppendArgs) asOp() Op {
	return Op{
		Op:       p.Op,
		Key:      p.Key,
		Value:    p.Value,
		Client:   p.Impl.ClientInfo,
		Database: make(map[string]string),
	}
}

func AsOpPackShardArgs(p *common.PackShardArgs) Op {
	return Op{
		Op:                 "PackShard",
		Key:                "",
		Value:              "",
		Client:             p.Impl.ClientInfo,
		Shard:              p.Shard,
		Database:           make(map[string]string),
		ClientLatestReqIDs: make(map[common.ReqKey]int),
	}
}

func AsOpInvalidateShardArgs(i *common.InvalidateShardArgs) Op {
	return Op{
		Op:                 "InvalidateShard",
		Key:                "",
		Value:              "",
		Client:             i.ClientInfo,
		Shard:              i.Shard,
		Database:           make(map[string]string),
		ClientLatestReqIDs: make(map[common.ReqKey]int),
	}
}

func AsOpUpdateShardArgs(u *common.UpdateShardArgs) Op {
	return Op{
		Op:                 "UpdateShard",
		Key:                "",
		Value:              "",
		Client:             u.Impl.ClientInfo,
		Shard:              u.Shard,
		Database:           u.Database,
		ClientLatestReqIDs: u.ClientLatestReqIDs,
	}
}

// For shard senders: Copy self k/v that belongs to args.shards to reply.DB
// Copy self "ClientLatestReq" to reply.ClientLatestReq
func (kv *ShardKV) PackShard(args *common.PackShardArgs, reply *common.PackShardReply) error {

	if !kv.isdead() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		println("Pack shard", args.Shard, "on", kv.me)
		println("	kv server", kv.me, " Add PackShard ", args.Shard, ") to rsm")
		kv.rsm.AddOp(AsOpPackShardArgs(args))
		// Pack shard here
		reply.Database = make(map[string]string)
		for key, val := range kv.impl.Database {
			if common.Key2Shard(key) == args.Shard {
				println("	[PackShard]: server", kv.me, " Pack (", key, ",", val, ") to reply.Databse")
				reply.Database[key] = val
			}
		}
		println("	Pack ClientLatestReqIDs")
		// Pack ClientReq here
		reply.ClientLatestReqIDs = make(map[common.ReqKey]int)
		for key, val := range kv.impl.ClientLatestReqIDs {
			println("		client:", key.ClientName, " op", key.Op, ": ", val)
			reply.ClientLatestReqIDs[key] = val
		}

		reply.Err = OK
		// // delete this shard from valid
		// println("[PackShard] delete ", args.Shard, "from server", kv.me)
		// delete(kv.impl.Shards, args.Shard)
		// println("Done Pack and Invalidate Shard", args.Shard, "on", kv.me)
	} else {
		println("[PackShard] srv", kv.me, "is dead")
	}
	// Add this operation to Paxos Log to handle Concurrent Get/PutAppend
	return nil
}

// For shards receivers: Copy args.DB to locel Database storage
func (kv *ShardKV) UpdateShard(args *common.UpdateShardArgs, reply *common.UpdateShardReply) error {
	if !kv.isdead() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if !kv.updateClientHistory(args.Impl.ClientInfo, "UpdateShard") {
			// Add this operation to Paxos Log to handle Concurrent Get/PutAppend
			println("	kv server", kv.me, " Add UpdateShard ", args.Shard, "to rsm")
			kv.rsm.AddOp(AsOpUpdateShardArgs(args))
		}
		reply.Err = OK
	} else {
		println("[UpdateShard] srv", kv.me, "is dead")
	}
	return nil

}
