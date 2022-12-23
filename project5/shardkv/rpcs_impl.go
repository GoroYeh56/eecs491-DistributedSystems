package shardkv

import "umich.edu/eecs491/proj5/common"

// Field names must start with capital letters,
// otherwise RPC will break.

// additional state to include in arguments to PutAppend RPC.
type PutAppendArgsImpl struct {
	ClientInfo common.ClientInfo
}

// additional state to include in arguments to Get RPC.
type GetArgsImpl struct {
	ClientInfo common.ClientInfo
}

// for new RPCs that you add, declare types for arguments and reply
//

// type PackShardArgs struct {
// 	Shard int // which shard to copy from
// 	Impl  common.PackShardArgsImpl
// }

// type PackShardReply struct {
// 	Err      Err
// 	Database map[string]string
// }

// type UpdateShardArgs struct {
// 	Shard    int
// 	Database map[string]string
// 	Impl     common.UpdateShardArgsImpl
// }

// type UpdateShardReply struct {
// 	Err Err
// }
