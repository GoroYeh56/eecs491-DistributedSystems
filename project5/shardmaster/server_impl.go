package shardmaster

import (
	"fmt"

	"umich.edu/eecs491/proj5/common"
	// "umich.edu/eecs491/proj5/shardkv"
)

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters.
type Op struct {
	OpName    string
	GID       int64
	Servers   []string
	Shard     int
	ConfigNum int
	ReqID     int
}

// Method used by PaxosRSM to determine if two Op values are identical
func equals(v1 interface{}, v2 interface{}) bool {
	op1 := v1.(Op)
	op2 := v2.(Op)
	if op1.ReqID == op2.ReqID {
		if op1.OpName == op2.OpName {
			switch op1.OpName {
			case "Join":
				// Note: consider Client ID? Name & ReqID
				// compare GID and Servers
				if op1.GID == op2.GID {
					if len(op1.Servers) == len(op2.Servers) {
						for i := 0; i < len(op1.Servers); i++ {
							if op1.Servers[i] != op2.Servers[i] {
								return false
							}
						}
						return true
					} else {
						return false
					}
				} else {
					return false
				}

			case "Leave":
				return op1.GID == op2.GID
			case "Move":
				return op1.GID == op2.GID && op1.Shard == op2.Shard
			case "Query":
				return op1.ConfigNum == op2.ConfigNum

			}
		}
	}
	return false
}

// Shard master: is a cluster of servers using Paoxs to decided the configuration at each config number

// additions to ShardMaster state
type ShardMasterImpl struct {
	config_num       int
	shards           [common.NShards]int64 // map shard number -> group ID (shard 'idx' -> maps to group shards[idx])
	groups           map[int64][]string    // maps GID -> the set of servers
	configs          map[int]Config        // maps config number -> Config
	num_group        int                   // number of server groups
	shards_per_gp    map[int64]int         // number of shards handled per group
	gp_shards        map[int64][]int       // map GID to a set of shards
	shardmaster_name int64                 // name for ClientInfo.Name int64. Set as -1
	ReqID            int                   // unique request id for shardkv to updateClientHistory

}

// initialize sm.impl.*
func (sm *ShardMaster) InitImpl() {
	sm.impl.config_num = 0

	for i := 0; i < common.NShards; i++ {
		sm.impl.shards[i] = 0 // initially, all shards map to GID 0
		// sm.impl.gp_shards[0] = append(sm.impl.gp_shards[0], i)
	}
	sm.impl.groups = make(map[int64][]string)
	sm.impl.configs = make(map[int]Config)
	sm.impl.num_group = 0
	sm.impl.shards_per_gp = make(map[int64]int) // map GID -> number of shards this group serves
	sm.impl.gp_shards = make(map[int64][]int)   // map GID -> the set of shards this group serves

	// make a new map for group
	newGroups := CopyMap(sm.impl.groups)
	sm.impl.configs[0] = Config{
		Num:    sm.impl.config_num,
		Shards: sm.impl.shards,
		Groups: newGroups,
	}
	fmt.Printf("Init sm.impl.gropus: %d\n", len(sm.impl.groups))

	sm.impl.shardmaster_name = -1
	sm.impl.ReqID = 0
}

// We have total 16 shards
// [16]int: shard 0 -> GID 2, ..., shard 15 -> GID 6?

// RPC handlers for Join, Leave, Move, and Query RPCs
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {

	if !sm.isdead() {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		sm.rsm.AddOp(args.asOp())
	}
	return nil

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	if !sm.isdead() {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		sm.rsm.AddOp(args.asOp())
	}
	return nil

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {

	if !sm.isdead() {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		sm.rsm.AddOp(args.asOp())
	}
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {

	// Note: this one should be GET: return reply.Config

	if !sm.isdead() {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		println("Query config num", args.Num, "on sm", sm.me)
		sm.rsm.AddOp(args.asOp())
		// var value, has = sm.impl.Storage[args.Key]
		if args.Num == -1 || args.Num > sm.impl.config_num {
			// return latest config
			fmt.Printf("	Query latest config: %d \n", sm.impl.config_num)
			reply.Config = sm.impl.configs[sm.impl.config_num]
			return nil
		} else {
			// return history config
			fmt.Printf("	Query historical config: %d \n", args.Num)
			reply.Config = sm.impl.configs[args.Num]
			println("	Num:", reply.Config.Num)
			println("	Num Groups:", len(reply.Config.Groups))
			// println("	Num:", reply.Config.Num)
			return nil
		}
		// println("Done Get for", args.Key, "on", sm.me)
	}
	return nil
}

// / Operations may be: Join/Leave/Move/Query
// Execute operation encoded in decided value v and update local state
func (sm *ShardMaster) ApplyOp(v interface{}) {

	value := v.(Op)

	switch value.OpName {
	case "Join":

		newGID := value.GID
		servers := value.Servers
		println("Join GID", newGID, " w/ servers")
		for _, srv := range servers {
			println("	", srv)
		}

		// if already having this group, return!
		_, existed := sm.impl.groups[newGID]
		if existed {
			println("	GID", newGID, "already existed! Skip!\n")
			return
		}

		// Compute shards per server
		println("	num_group: ", sm.impl.num_group)
		sm.impl.num_group += 1
		println("	after +1, num_group: ", sm.impl.num_group)
		shard_per_server := common.NShards / sm.impl.num_group
		println("	Shard per server:", shard_per_server)

		// For the first group joined:
		if sm.impl.num_group == 1 {
			for sh := 0; sh < common.NShards; sh++ {
				sm.impl.shards[sh] = newGID
				sm.impl.gp_shards[newGID] = append(sm.impl.gp_shards[newGID], sh)

				////////////// Shard Master to servers //////////////
				// Pack Shard and Empty DB
				db := make(map[string]string)
				// Hand off this shard to the new group of servers
				success := false
				for !success {
					for _, srv := range servers {
						success = sm.SendUpdateShard(srv, sh, db, make(map[common.ReqKey]int))
						// for !success {
						// 	success = sm.SendUpdateShard(srv, sh, db, make(map[common.ReqKey]int))
						// }
						if success {
							break
						}
					}
				}
				/////////////////////////////////////////////////////

			}
			sm.impl.shards_per_gp[newGID] = common.NShards
		} else {
			println("	Shard per server:", shard_per_server)

			sm.impl.shards_per_gp[newGID] = 0
			for sm.impl.shards_per_gp[newGID] < shard_per_server {
				group := sm.find_Group_With_Max_Shards(newGID)
				// Move one shard from group to newGID
				shard := sm.impl.gp_shards[group][len(sm.impl.gp_shards[group])-1]
				sm.impl.gp_shards[group] = sm.impl.gp_shards[group][:len(sm.impl.gp_shards[group])-1]

				////////////// Shard Master to servers //////////////
				// Get server
				srvs_to_handoff := sm.impl.groups[group]
				pack_reply := common.PackShardReply{}
				success := false
				for !success {
					for _, srv := range srvs_to_handoff {
						pack_reply, success = sm.SendPackShard(srv, shard)
						if success {
							break
						}
					}
				}

				// TODO: how to invalidate?
				// PackShard success. Now, send "InvalidateShard" to all servers
				// i_reply := common.InvalidateShardReply{}
				// for _, srv := range srvs_to_handoff {
				// 	sm.SendInvalidate(srv, shard)
				// 	// println(" i_reply.Err: ", i_reply.Err)
				// 	println("successfully delete shard", shard, " in server ", srv)
				// }

				db_to_handoff := pack_reply.Database
				clnt_history := pack_reply.ClientLatestReqIDs
				// Hand off this shard to the new group of servers
				// Note: all servers have to success!
				println("Update newGID", newGID, " Add shard", shard, "to it")
				success = false
				for !success {
					for _, srv := range servers {
						println("SendUpdateShard to server", srv)
						success = sm.SendUpdateShard(srv, shard, db_to_handoff, clnt_history)
						if success {
							break
						}
					}
				}
				/////////////////////////////////////////////////////

				sm.impl.shards_per_gp[group] -= 1
				fmt.Printf("	Move shard %d from GID %d to new Group %d\n", shard, group, newGID)
				sm.impl.gp_shards[newGID] = append(sm.impl.gp_shards[newGID], shard)
				sm.impl.shards_per_gp[newGID] += 1
				// Update this shard
				sm.impl.shards[shard] = newGID
			}
		}
		// Add this Group to sm.impl.group
		sm.impl.groups[newGID] = servers

		println("After Join, num_groups:", len(sm.impl.groups))

		// Update latest config num
		sm.impl.config_num += 1

		// make a new map for group
		newGroups := CopyMap(sm.impl.groups)
		new_config := Config{
			Num:    sm.impl.config_num,
			Shards: sm.impl.shards,
			Groups: newGroups,
		}
		// Insert a new config
		fmt.Printf("	Insert new Config on config_num %d\n", sm.impl.config_num)
		sm.impl.configs[sm.impl.config_num] = new_config

	case "Leave":
		GID := value.GID
		println("===\nLeave", GID)

		// if already left, ignore!
		_, existed := sm.impl.groups[GID]
		if existed == false {
			println("	GID", GID, "already left!Skip\n")
			return
		}

		// Handoff group GID to the rest groups
		// Trying to fill the gap
		// Feed shard to the server who has the min number of shards
		sh_idx := 0 // shard idx
		for sh_idx < len(sm.impl.gp_shards[GID]) {

			group := sm.find_Group_With_Min_Shards(GID)
			cur_shard := sm.impl.gp_shards[GID][sh_idx]
			sm.impl.gp_shards[group] = append(sm.impl.gp_shards[group], cur_shard)

			////////////// Shard Master to servers //////////////
			println("Send PackShard to", GID, " pack shard", cur_shard)
			srvs_to_handoff := sm.impl.groups[GID]
			// Get server
			pack_reply := common.PackShardReply{}
			success := false
			for !success {
				for _, srv := range srvs_to_handoff {
					pack_reply, success = sm.SendPackShard(srv, cur_shard)
					if success {
						break
					}
				}
			}
			db_to_handoff := pack_reply.Database
			clnt_history := pack_reply.ClientLatestReqIDs

			// PackShard success. Now, send "InvalidateShard" to all servers
			// for _, srv := range srvs_to_handoff {
			// 	sm.SendInvalidate(srv, cur_shard)
			// 	println("successfully delete shard", cur_shard, " in server ", srv)
			// }

			// Hand off this shard to the new group of servers
			println("Update newGID", group, " Add shard", cur_shard, "to it")
			servers_to_recv := sm.impl.groups[group]
			success = false
			for !success {
				for _, srv := range servers_to_recv {
					success = sm.SendUpdateShard(srv, cur_shard, db_to_handoff, clnt_history)
					if success {
						break
					}
				}
			}

			/////////////////////////////////////////////////////

			sm.impl.shards_per_gp[group] += 1
			println("	Hand off shard", cur_shard, "to GID", group)
			sm.impl.shards[sm.impl.gp_shards[GID][sh_idx]] = group
			sh_idx++
		}
		delete(sm.impl.shards_per_gp, GID)
		delete(sm.impl.gp_shards, GID)

		delete(sm.impl.groups, GID)
		sm.impl.num_group -= 1
		println("After Leave, num_groups:", len(sm.impl.groups))

		// make a new map for group
		newGroups := CopyMap(sm.impl.groups)
		////////// Update latest config num
		sm.impl.config_num += 1
		new_config := Config{
			Num:    sm.impl.config_num,
			Shards: sm.impl.shards,
			Groups: newGroups,
		}
		fmt.Printf("	Insert new Config on config_num %d\n", sm.impl.config_num)
		// Update new config in the configs log
		sm.impl.configs[sm.impl.config_num] = new_config

	case "Move":
		// Move shard 'Shard' to group 'GID'
		Shard := value.Shard
		newGID := value.GID
		println("Move Shard", Shard, "from", sm.impl.shards[Shard], " to GID", newGID)

		oldGID := sm.impl.shards[Shard]
		for idx, sh := range sm.impl.gp_shards[oldGID] {
			if sh == Shard {
				sm.impl.gp_shards[oldGID] = append(sm.impl.gp_shards[oldGID][:idx], sm.impl.gp_shards[oldGID][idx+1:]...)
				sm.impl.shards_per_gp[oldGID]--

				////////////// Shard Master to servers //////////////
				srvs_to_handoff := sm.impl.groups[oldGID]
				pack_reply := common.PackShardReply{}
				success := false
				for !success {
					for _, srv := range srvs_to_handoff {
						pack_reply, success = sm.SendPackShard(srv, Shard)
						if success {
							break
						}
					}
				}

				db_to_handoff := pack_reply.Database
				clnt_history := pack_reply.ClientLatestReqIDs

				// PackShard success. Now, send "InvalidateShard" to all servers

				// for _, srv := range srvs_to_handoff {
				// 	sm.SendInvalidate(srv, Shard)
				// 	println("successfully delete shard", Shard, " in server ", srv)
				// }
				// Hand off this shard to the new group of servers
				success = false
				servers_to_recv := sm.impl.groups[newGID]
				for !success {
					for _, srv := range servers_to_recv {
						success = sm.SendUpdateShard(srv, Shard, db_to_handoff, clnt_history)
					}
					if success {
						break
					}
				}
				/////////////////////////////////////////////////////

				sm.impl.gp_shards[newGID] = append(sm.impl.gp_shards[newGID], sh)
				sm.impl.shards_per_gp[newGID]++
				sm.impl.shards[sh] = newGID
				break
			}
		}

		////////// Update new Config
		sm.impl.config_num += 1
		newGroups := CopyMap(sm.impl.groups)
		new_config := Config{
			Num:    sm.impl.config_num,
			Shards: sm.impl.shards,
			Groups: newGroups,
		}
		fmt.Printf("	Insert new Config on config_num %d\n", sm.impl.config_num)
		// Update new config in the configs log
		sm.impl.configs[sm.impl.config_num] = new_config

	}

}

// ////// Functions Defined by me /////////////
func (j *JoinArgs) asOp() Op {
	return Op{
		OpName:    "Join",
		GID:       j.GID,
		Servers:   j.Servers,
		Shard:     -1,
		ConfigNum: -1,
		ReqID:     int(common.Nrand()),
	}
}

func (l *LeaveArgs) asOp() Op {
	return Op{
		OpName:    "Leave",
		GID:       l.GID,
		Servers:   nil,
		Shard:     -1,
		ConfigNum: -1,
		ReqID:     int(common.Nrand()),
	}
}

func (m *MoveArgs) asOp() Op {
	return Op{
		OpName:    "Move",
		GID:       m.GID,
		Servers:   nil,
		Shard:     m.Shard,
		ConfigNum: -1,
		ReqID:     int(common.Nrand()),
	}
}

func (q *QueryArgs) asOp() Op {
	return Op{
		OpName:    "Query",
		GID:       0,
		Servers:   nil,
		Shard:     -1,
		ConfigNum: q.Num,
		ReqID:     int(common.Nrand()),
	}
}

func CopyMap(FromMap map[int64][]string) map[int64][]string {
	ToMap := make(map[int64][]string)
	for key, val := range FromMap {
		ToMap[key] = val
	}
	return ToMap
}

func (sm *ShardMaster) find_Group_With_Max_Shards(except_gid int64) int64 {
	var group int64
	max_num_shards := 0
	for gp, num_shards := range sm.impl.shards_per_gp {
		if num_shards > max_num_shards && gp != except_gid {
			max_num_shards = num_shards
			group = gp
		}
	}
	println("group with max shard", group)
	return group
}

func (sm *ShardMaster) find_Group_With_Min_Shards(except_gid int64) int64 {
	var group int64
	min_num_shards := 100 // TODO: A large number
	for gp, num_shards := range sm.impl.shards_per_gp {
		if num_shards < min_num_shards && gp != except_gid {
			min_num_shards = num_shards
			group = gp
		}
	}
	println("group with min shard", group)
	return group
}

// TODO: Only return "success" for RPC, don't care what "reply" is
// Send UpdateShard to a server
func (sm *ShardMaster) SendUpdateShard(srv string, shard int, db map[string]string, clienthistory map[common.ReqKey]int) bool {
	// Send "UpdateShard" RPC to server 'srv'
	sm.impl.ReqID++
	var args = common.UpdateShardArgs{
		Shard:              shard,
		Database:           db,
		ClientLatestReqIDs: clienthistory,
		Impl: common.UpdateShardArgsImpl{
			ClientInfo: common.ClientInfo{Name: sm.impl.shardmaster_name, ReqID: sm.impl.ReqID},
		},
	}
	var reply = common.UpdateShardReply{
		Err: "",
	}
	var success = false
	// println("[SendUpdateShard] send ", srv, "Update Shard", shard)
	println("[SendUpdateShard] send ", srv, "Update Shard", shard)
	success = common.Call(srv, "ShardKV.UpdateShard", &args, &reply)
	// if !success {
	// 	time.Sleep(100 * time.Millisecond)
	// }
	// println("Success in Send UpdateShard RPC to ", srv, " Add Shard", shard)
	return success
}

// Send PackShard to get Database of a shard from a server
// TODO: Get only one succeed! No need for three success!
func (sm *ShardMaster) SendPackShard(srv string, shard int) (common.PackShardReply, bool) {
	// Send "PackShard" RPC to server 'srv'
	sm.impl.ReqID++
	var args = common.PackShardArgs{
		Shard: shard,
		Impl: common.PackShardArgsImpl{
			ClientInfo: common.ClientInfo{Name: sm.impl.shardmaster_name, ReqID: sm.impl.ReqID},
		},
	}
	var reply = common.PackShardReply{
		Err:                "",
		Database:           make(map[string]string),
		ClientLatestReqIDs: make(map[common.ReqKey]int),
	}
	println("[SendPackShard] send ", srv, " Pack Shard", shard)
	var success = false
	success = common.Call(srv, "ShardKV.PackShard", &args, &reply)
	// success = success && reply.Err == common.OK
	// println("Success in Send PackShard RPC to ", srv, " Pack Shard", shard)
	return reply, success
}

func (sm *ShardMaster) SendInvalidate(srv string, shard int) {
	var args = common.InvalidateShardArgs{
		Shard:      shard,
		ClientInfo: common.ClientInfo{},
	}
	var reply = common.InvalidateShardReply{
		Err: "",
	}
	success := false
	for !success {
		println("[SendInvalidate] send ", srv, " Invalidate Shard", shard)
		success = common.Call(srv, "ShardKV.InvalidateShard", &args, &reply)
	}
}
