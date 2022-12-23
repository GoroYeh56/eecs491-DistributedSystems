package common

// define here any data types that you need to access in two packages without
// creating circular dependencies
const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrDead       = "ErrDead"
)

type Err string

type ReqKey struct {
	ClientName int64
	Op         string
}
type ClientInfo struct {
	Name  int64
	ReqID int
}

type PackShardArgs struct {
	Shard int // which shard to copy from
	Impl  PackShardArgsImpl
}
type PackShardArgsImpl struct {
	ClientInfo ClientInfo
}

type PackShardReply struct {
	Err                Err
	Database           map[string]string
	ClientLatestReqIDs map[ReqKey]int
}

type InvalidateShardArgs struct {
	Shard      int // which shard we would invalidate
	ClientInfo ClientInfo
}
type InvalidateShardReply struct {
	Err Err
}

type UpdateShardArgs struct {
	Shard              int
	Database           map[string]string
	ClientLatestReqIDs map[ReqKey]int
	Impl               UpdateShardArgsImpl
}

// additional state to include in arguments to Get RPC.
type UpdateShardArgsImpl struct {
	ClientInfo ClientInfo
}
type UpdateShardReply struct {
	Err Err
}
