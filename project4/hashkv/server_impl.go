package hashkv

import (
	"fmt"

	"umich.edu/eecs491/proj4/common"
)

// Need a state variable to determine the [from, to] range
// of this servers
type Range struct {
	from int // from this hash number
	to   int // to this hash number
}

type ReqKey struct {
	ClientName int64
	Key        string
}

// additions to HashKV state
type HashKVImpl struct {
	active             bool
	Range              Range             // [from, to] hashkey
	ClientLatestReqIDs map[ReqKey]int    // map client (int64) -> it's request ID
	Database           map[string]string // stores key/value pairs
}

type HashCollisionMsg struct {
	ActiveServer   string
	ConflictServer string
	Collision      bool
}

func (kv *HashKV) HashCollision(otherID string) bool {
	return common.Key2Shard(otherID) == common.Key2Shard(kv.ID)
}

func (kv *HashKV) IsActive() HashCollisionMsg {
	result := HashCollisionMsg{}
	if kv.pred != kv.me && kv.HashCollision(kv.predID) {
		result.Collision = true
		if kv.predID < kv.ID {
			result.ActiveServer = kv.pred
			result.ConflictServer = kv.me
		} else {
			result.ActiveServer = kv.me
			result.ConflictServer = kv.pred
		}
	} else if kv.succ != kv.me && kv.HashCollision(kv.succID) {
		result.Collision = true
		if kv.succID < kv.ID {
			result.ActiveServer = kv.succ
			result.ConflictServer = kv.me
		} else {
			result.ActiveServer = kv.me
			result.ConflictServer = kv.succ
		}
	} else {
		result.Collision = false
	}
	return result
}

func (kv *HashKV) UpdateDB(Database map[string]string) {
	db := Database
	for key, val := range db {
		kv.impl.Database[key] = val
		fmt.Printf("	Add (%s, %s) to %s DB\n", key, val, kv.me)
	}
}

func (kv *HashKV) UpdateClientLatestReqID(ClientLatestReqID map[ReqKey]int) {

	for key, val := range ClientLatestReqID {
		kv.impl.ClientLatestReqIDs[key] = val
		fmt.Printf("	Add (%v, %d) to %s ClientLatestReqID\n", key, val, kv.me)
	}
}

func (kv *HashKV) UpdatePredecessor(pred string, succ string, succID string) bool {
	update_args := &UpdateSuccArgs{}
	update_args.Succ = succ
	update_args.SuccID = succID
	update_reply := &UpdateSuccReply{}
	ok := common.Call(pred, "HashKV.UpdateSucc", update_args, update_reply)
	return ok
}

func (kv *HashKV) UpdateSuccessor(succ string, pred string, predID string) bool {
	update_pred_args := &UpdatePredArgs{}
	update_pred_args.Pred = pred
	update_pred_args.PredID = predID
	update_pred_reply := &UpdatePredReply{}
	ok := common.Call(succ, "HashKV.UpdatePred", update_pred_args, update_pred_reply)
	return ok
}

// initialize kv.impl.*
func (kv *HashKV) InitImpl() {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// When Transferring shards: need mutex!
	kv.impl.Range = Range{from: common.Key2Shard(kv.predID), to: common.Key2Shard(kv.ID)}
	kv.impl.ClientLatestReqIDs = make(map[ReqKey]int)
	kv.impl.Database = make(map[string]string)
	result := kv.IsActive()

	fmt.Printf("Initialize HashKV %s,ID %s \n	Pred: %s Succ:%s\n", kv.me, kv.ID, kv.pred, kv.succ)

	if result.Collision {
		if result.ActiveServer == kv.me {
			// Need to transfer data from pred of succ
			if result.ConflictServer == kv.pred {
				fmt.Printf("	server %s is active with ID:%s\n", kv.me, kv.ID)
				fmt.Printf("	Transfering DB from its predecessor %s\n", kv.pred)
				fmt.Printf("		(pred%s set to inactive\n", kv.pred)
				// Transfering DB from pred to me
				args := &TransferDBArgs{}
				reply := &TransferDBReply{}
				// Ask kv.pred to pack all its database to reply.Database
				ok := common.Call(kv.pred, "HashKV.TransferDB", args, reply)
				for !ok {
					fmt.Printf(" RPC fail for %s to take data from pred %s\n", kv.me, kv.pred)
					ok = common.Call(kv.pred, "HashKV.TransferDB", args, reply)
				}
				// server Update local db with reply DB
				kv.UpdateDB(reply.Database)
				kv.UpdateClientLatestReqID(reply.ClientHistory)

			} else if result.ConflictServer == kv.succ {
				// RPC call for transferring data from successor!
				fmt.Printf("	server %s is active with ID:%s\n", kv.me, kv.ID)
				fmt.Printf("	Transfering DB from its successor %s\n", kv.succ)
				fmt.Printf("		(succ %s set to inactive\n", kv.succ)
				args := &TransferDBArgs{}
				reply := &TransferDBReply{}
				ok := common.Call(kv.succ, "HashKV.TransferDB", args, reply)
				for !ok {
					fmt.Printf(" RPC fail for %s to take data from succ %s\n", kv.me, kv.succ)
					ok = common.Call(kv.succ, "HashKV.TransferDB", args, reply)
				}
				kv.UpdateDB(reply.Database)
				kv.UpdateClientLatestReqID(reply.ClientHistory)
			}
			kv.impl.active = true
		} else {
			kv.impl.active = false
		}
	} else {
		kv.impl.active = true
		if kv.succ != kv.me {
			args := &GetFromSuccArgs{
				NewServerFrom: kv.impl.Range.from,
				NewServerTo:   kv.impl.Range.to,
				NewServer:     kv.me,
				NewServerID:   kv.ID,
			}
			reply := &GetFromSuccReply{}
			ok := common.Call(kv.succ, "HashKV.GetFromSucc", args, reply)
			for !ok {
				fmt.Printf(" RPC fail for %s to take data from succ %s. Retry...\n", kv.me, kv.succ)
				ok = common.Call(kv.succ, "HashKV.GetFromSucc", args, reply)
			}

			kv.UpdateDB(reply.Database)
			kv.UpdateClientLatestReqID(reply.ClientHistory)
		}
	}

	fmt.Printf("	Range: (%d, %d]\n	Active: %v\n", kv.impl.Range.from, kv.impl.Range.to, kv.impl.active)

	if kv.pred != kv.me {
		// Update predeccesor's successor
		ok := kv.UpdatePredecessor(kv.pred, kv.me, kv.ID)
		for !ok {
			fmt.Printf("	%s Fail from RPC UpdatePred, retry...\n", kv.me)
			ok = kv.UpdatePredecessor(kv.pred, kv.me, kv.ID)
		}
	}

	if kv.succ != kv.me {
		// Update succ's pred and Range
		ok := kv.UpdateSuccessor(kv.succ, kv.me, kv.ID)
		for !ok {
			fmt.Printf("	%s Fail from RPC UpdateSucc, retry...\n", kv.me)
			ok = kv.UpdateSuccessor(kv.succ, kv.me, kv.ID)
		}
	}
}

func (kv *HashKV) PackDBandClientHistory() SendSuccArgs {
	args := SendSuccArgs{
		Pred:          kv.pred,
		PredID:        kv.predID,
		Database:      make(map[string]string),
		ClientHistory: make(map[ReqKey]int),
	}

	for key, val := range kv.impl.Database {
		if kv.valid(key) {
			args.Database[key] = val
		}
		// delete it!
		// TODO: Send Delete after correctly transferring data!
		// delete(kv.impl.Database, key)
	}
	for k, v := range kv.impl.ClientLatestReqIDs {
		args.ClientHistory[k] = v
	}
	return args
}

func (kv *HashKV) clear() {
	for k, _ := range kv.impl.Database {
		delete(kv.impl.Database, k)
	}
	for k, _ := range kv.impl.ClientLatestReqIDs {
		delete(kv.impl.ClientLatestReqIDs, k)
	}
}

// hand off shards before shutting down
func (kv *HashKV) KillImpl() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Printf("\nServer %s is leaving\n", kv.me)
	fmt.Printf("    Range: (%d,%d]  \n", kv.impl.Range.from, kv.impl.Range.to)
	fmt.Printf("	pred: %s\n", kv.pred)
	fmt.Printf("	succ: %s\n", kv.succ)

	// Pass Databse to kv.succ
	args := kv.PackDBandClientHistory()
	reply := &SendSuccReply{}
	// Update successor's database
	// Update succ's predecessor to be kv.pred
	ok := common.Call(kv.succ, "HashKV.SendSucc", &args, reply)
	for !ok {
		fmt.Printf("	RPC fail for HashKV.SendSucc! Error in KillImpl!\n")
		ok = common.Call(kv.succ, "HashKV.SendSucc", &args, reply)
	}
	fmt.Printf("	Successfully passed %s (ID %s) databse to successor %s (ID:%s) \n", kv.me, kv.ID, kv.succ, kv.succID)

	// Delete my local DB
	// kv.clear()

	// Update predecessor's successor to be kv.succ
	ok = kv.UpdatePredecessor(kv.pred, kv.succ, kv.succID)
	for !ok {
		fmt.Printf("	RPC UpdateSucc failed\n")
		ok = kv.UpdatePredecessor(kv.pred, kv.succ, kv.succID)
	}

}

// S0: (15, 2]  ID : 0
// S1: (2 , 2]  ID : 1
// S2: (2,  2]  ID : 2
// S3: (2, 15]

// Get(key=>2) => S1

// When called valid => This kv server is ACTIVE!
func (kv *HashKV) valid(key string) bool {
	// if only one server
	shard := common.Key2Shard(key)

	// only one server
	if kv.pred == kv.me && kv.succ == kv.me {
		return true
	}

	// only two servers:
	if kv.pred == kv.succ {
		if kv.impl.Range.from >= kv.impl.Range.to {
			return shard > kv.impl.Range.from || shard <= kv.impl.Range.to
		} else {
			return shard > kv.impl.Range.from && shard <= kv.impl.Range.to
		}
	}

	// if collision with pred:
	if kv.HashCollision(kv.predID) {
		if kv.predID < kv.ID {
			// I am inactive
			return false
		} else {
			if kv.impl.Range.from >= kv.impl.Range.to {
				return shard > kv.impl.Range.from || shard <= kv.impl.Range.to
			} else {
				return shard > kv.impl.Range.from && shard <= kv.impl.Range.to
			}
		}
	}
	// if collision with succ:
	if kv.HashCollision(kv.succID) {
		if kv.succID < kv.ID {
			return false
		} else {
			if kv.impl.Range.from >= kv.impl.Range.to {
				return shard > kv.impl.Range.from || shard <= kv.impl.Range.to
			} else {
				return shard > kv.impl.Range.from && shard <= kv.impl.Range.to
			}
		}
	}

	// If it's tail: (from, to] => (pred, ID]
	if kv.impl.Range.from > kv.impl.Range.to {
		return shard > kv.impl.Range.from || shard <= kv.impl.Range.to
	} else {
		return shard > kv.impl.Range.from && shard <= kv.impl.Range.to
	}
}

func (kv *HashKV) ValidForNewServer(from, to int, key string) bool {
	shard := common.Key2Shard(key)
	if from > to {
		return shard > from || shard <= to
	} else {
		return shard > from && shard <= to
	}
}

// At-most-once Semantic
// Note: if the shard of this request should be handed off => copy that to the new server!
func (kv *HashKV) updateClientHistory(client ClientInfo, key string) bool {
	reqkey := ReqKey{client.Name, key}
	var last, has = kv.impl.ClientLatestReqIDs[reqkey]
	if !has || client.ReqID > last {
		kv.impl.ClientLatestReqIDs[reqkey] = client.ReqID
		// fmt.Printf("	server %s recv NEW Req: client %d wtth ReqID %d\n", kv.me, client.Name, client.ReqID)
		return false
	} else {
		return true
	}
}

// RPC handler for client Get requests
func (kv *HashKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// if args.Key not this shard: return ErrWrongServer
	if !kv.isdead() && kv.impl.active {
		if kv.valid(args.Key) {
			// fmt.Printf("server %s Range: (%d, %d]\n", kv.me, kv.impl.Range.from, kv.impl.Range.to)
			// fmt.Printf("Client %d send Get(%s) request to %s \n", args.Impl.Client.Name, args.Key, kv.me)
			// fmt.Printf("	Sever %s Handle Get(%s) in Shard: %d \n", kv.me, args.Key, common.Key2Shard(args.Key))
			val, existed := kv.impl.Database[args.Key]
			if existed {
				reply.Err = OK
				reply.Value = val
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongServer
		}
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *HashKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.isdead() && kv.impl.active {
		if kv.valid(args.Key) {
			// fmt.Printf("server %s Range: (%d, %d]\n", kv.me, kv.impl.Range.from, kv.impl.Range.to)
			// fmt.Printf("Client %d send Put(%s, %s) request to %s \n", args.Impl.Client.Name, args.Key, args.Value, kv.me)
			// fmt.Printf("	server %s Handle Put(%s, %s) Shard: %d \n", kv.me, args.Key, args.Value, common.Key2Shard(args.Key))
			if !kv.updateClientHistory(args.Impl.Client, args.Key) {
				switch args.Op {
				case "Put":
					// fmt.Printf("	server %s Put (%s, %s)\n", kv.me, args.Key, args.Value)
					kv.impl.Database[args.Key] = args.Value
				case "Append":
					var value = kv.impl.Database[args.Key]
					kv.impl.Database[args.Key] = value + args.Value
				}
				kv.updateClientHistory(args.Impl.Client, args.Key)
			}
			reply.Err = OK
		} else {
			reply.Err = ErrWrongServer
		}
	}
	return nil

}

//
// Add RPC handlers for any other RPCs you introduce
//

// This kv: Successor. Pack data to reply.Database. Only pack if valid(k) and valid for new server
func (kv *HashKV) GetFromSucc(args *GetFromSuccArgs, reply *GetFromSuccReply) error {
	// Successor: pack reply database with shards from kv.predID to args.ID
	// Delete (k,v) in my impl.database
	// return!
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Printf("\n[RPC] Successor %s preparing k/v pairs to new Server: %s\n", kv.me, args.NewServer)
	reply.Database = make(map[string]string)
	reply.ClientHistory = make(map[ReqKey]int)
	for k, v := range kv.impl.Database {
		// if valid for NewServer.range
		if kv.valid(k) {
			if kv.ValidForNewServer(args.NewServerFrom, args.NewServerTo, k) {
				fmt.Printf("	Transfering (%s, %s) shard %d to new server %s\n", k, v, common.Key2Shard(k), args.NewServer)
				reply.Database[k] = v
				// delete(kv.impl.Database, k)
			}
		}
	}

	// key: ReqKey !
	for k, v := range kv.impl.ClientLatestReqIDs {
		// if kv.ValidForNewServer(args.NewServerFrom, args.NewServerTo, k.Key) {
		reply.ClientHistory[k] = v
		// }
	}

	fmt.Printf("	Done scanning %s Database. Now update kv.pred to new server %s\n", kv.me, args.NewServer)
	// update kv's predecessor information!
	// kv.pred = args.NewServer
	// kv.predID = args.NewServerID
	// kv.impl.Range.from = common.Key2Shard(kv.predID)
	// fmt.Printf("	server %s update Range.from: %d\n", kv.me, kv.impl.Range.from)
	return nil
}

func (kv *HashKV) SendSucc(args *SendSuccArgs, reply *SendSuccReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Printf("\n[RPC] %s get k/v pairs from dying predecessor %s\n", kv.me, kv.pred)
	// Successor's view: copy db and update new pred, predID, Range!
	for k, v := range args.Database {
		// Key: when pred being killed, only copy its data that I didn't own.
		// fmt.Printf("	Adding (%s, %s) to %s database\n", k, v, kv.me)
		// Key : need to see if this operation is 'Later'!
		// if _, existed := kv.impl.Database[k]; !existed {
		fmt.Printf("	Adding (%s, %s) to %s database\n", k, v, kv.me)
		kv.impl.Database[k] = v
		// }
	}

	for k, v := range args.ClientHistory {
		// if _, existed := kv.impl.ClientLatestReqIDs[k]; !existed {
		fmt.Printf("	Adding (%v, %d) to %s ClientLatestReqID\n", k, v, kv.me)
		kv.impl.ClientLatestReqIDs[k] = v
		// }
	}

	fmt.Printf("	Done copying %s Database & Client history. Now update kv.pred to be %s\n", kv.me, args.Pred)

	// Update predecessor and my range
	kv.impl.active = true
	kv.pred = args.Pred
	kv.predID = args.PredID
	kv.impl.Range.from = common.Key2Shard(kv.predID)
	fmt.Printf("	Update successors %s pred: %s\n	    Range: (%d,%d]\n", kv.me, kv.pred, kv.impl.Range.from, kv.impl.Range.to)
	return nil
}

// Transfer all DB in this kv to the newly joined server.
// Note: only transfer if the key hash to a valid shard
func (kv *HashKV) TransferDB(args *TransferDBArgs, reply *TransferDBReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Pack kv's DB to reply
	reply.Database = make(map[string]string)
	reply.ClientHistory = make(map[ReqKey]int)
	for k, v := range kv.impl.Database {
		if kv.valid(k) {
			reply.Database[k] = v
		}
	}
	for k, v := range kv.impl.ClientLatestReqIDs {
		reply.ClientHistory[k] = v
	}

	kv.impl.active = false
	return nil
}

// Predecessor: update it's successor
func (kv *HashKV) UpdateSucc(args *UpdateSuccArgs, reply *UpdateSuccReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.succ = args.Succ
	kv.succID = args.SuccID
	fmt.Printf("	Update server %s successor => %s\n", kv.me, args.Succ)
	return nil
}

// Successor: update it's predecessor & range
func (kv *HashKV) UpdatePred(args *UpdatePredArgs, reply *UpdatePredReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.pred = args.Pred
	kv.predID = args.PredID
	kv.impl.Range.from = common.Key2Shard(args.PredID)
	fmt.Printf("	Update server %s predecessor => %s\n", kv.me, args.Pred)
	fmt.Printf("	Update server %s Range: (%d, %d]\n", kv.me, kv.impl.Range.from, kv.impl.Range.to)
	return nil
}

func (kv *HashKV) GarbageCollect(args *GarbageCollectArgs, reply *GarbageCollectReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// kv delete data that shard from args.from ~ args.to
	fmt.Printf("\n[RPC] %s garbage collecting...\n", kv.me)
	for k, v := range kv.impl.Database {
		// if valid for NewServer.range
		if kv.ValidForNewServer(args.From, args.To, k) {
			fmt.Printf("	Deleting (%s, %s) shard %d in server %s\n", k, v, common.Key2Shard(k), kv.me)
			delete(kv.impl.Database, k)
		}
	}

	for k, v := range kv.impl.ClientLatestReqIDs {
		// if valid for NewServer.range
		if kv.ValidForNewServer(args.From, args.To, k.Key) {
			fmt.Printf("	Deleting ClientHistory (%v, %d) server %s\n", k, v, kv.me)
			delete(kv.impl.ClientLatestReqIDs, k)
		}
	}

	// fmt.Printf("	Done garbage collecting %s Database. \n", kv.me)
	// update kv's predecessor information!
	return nil
}
