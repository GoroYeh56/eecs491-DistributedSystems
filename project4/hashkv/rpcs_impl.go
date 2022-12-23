package hashkv

// Field names must start with capital letters,
// otherwise RPC will break.

// additional state to include in arguments to PutAppend RPC.
type PutAppendArgsImpl struct {
	Client ClientInfo
}

// additional state to include in arguments to Get RPC.
type GetArgsImpl struct {
	Client ClientInfo
}

// for new RPCs that you add, declare types for arguments and reply
type ClientInfo struct {
	Name  int64
	ReqID int
}

// InitImpl: Get Database from predecessor shards
type GetFromSuccArgs struct {
	NewServerFrom int
	NewServerTo   int
	NewServer     string
	NewServerID   string
}

type GetFromSuccReply struct {
	Database      map[string]string
	ClientHistory map[ReqKey]int
}

// KillImpl: Send Database to my successor

type SendSuccArgs struct {
	Pred          string
	PredID        string
	Database      map[string]string
	ClientHistory map[ReqKey]int
}

type SendSuccReply struct {
	Err Err
}

type UpdateSuccArgs struct {
	Succ   string
	SuccID string
}

type UpdateSuccReply struct {
}

type UpdatePredArgs struct {
	Pred   string
	PredID string
}

type UpdatePredReply struct {
}

type TransferDBArgs struct {
}

type TransferDBReply struct {
	Database      map[string]string
	ClientHistory map[ReqKey]int
}

type GarbageCollectArgs struct {
	From int
	To   int
}

type GarbageCollectReply struct {
}
