package pbservice

// In all data types that represent arguments to RPCs, field names
// must start with capital letters, otherwise RPC will break.

// additional state to include in arguments to PutAppend RPC.
type PutAppendArgsImpl struct {
	Client_name int64
	OpID        int    // To know if it is an duplicated request (with the same OpID)
	Op          string // Put or Append

}

// additional state to include in arguments to Get RPC.
type GetArgsImpl struct {
	Client_name int64
	OpID        int // To know if it is an duplicated request (with the same OpID)
}

//
// for new RPCs that you add, declare types for arguments and reply.
//

// Put or Append
type BootStrapArgs struct {
	Client_name int64
	OpID        int
	Req         requestOps
	ck_requests map[client_op_id]bool
}

type BootStrapReply struct {
	Err Err
}

// Put or Append
type ForwardDBArgs struct {
	Ck_requests map[client_op_id]bool
	Database    map[string]string
}

type ForwardDBReply struct {
}
