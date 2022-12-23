package kvpaxos

// additional state to include in arguments to PutAppend RPC.
// Field names must start with capital letters,
// otherwise RPC will break.
type ClientInfo struct {
	Name  int64
	ReqID uint
}

type PutAppendArgsImpl struct {
	Client ClientInfo
}

// additional state to include in arguments to Get RPC.
type GetArgsImpl struct {
	Client ClientInfo
}

//
// for new RPCs that you add, declare types for arguments and reply
//
