package kvpaxos

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters,
// otherwise RPC will break.
type Op struct {
	Op     string // Get, Put, Append
	Key    string
	Value  string
	Client ClientInfo
}

func (g *GetArgs) asOp() Op {
	return Op{
		Op:     "Get",
		Key:    g.Key,
		Value:  "",
		Client: g.Impl.Client,
	}
}

func (p *PutAppendArgs) asOp() Op {
	return Op{
		Op:     p.Op,
		Key:    p.Key,
		Value:  p.Value,
		Client: p.Impl.Client,
	}
}

// additions to KVPaxos state
type KVPaxosImpl struct {
	Database        map[string]string // map key -> value
	ClientLatestReq map[int64]uint    // map client.impl.Self.Name => latest ReqID
}

// initialize kv.impl.*
func (kv *KVPaxos) InitImpl() {
	kv.impl.Database = make(map[string]string, 100)
	kv.impl.ClientLatestReq = make(map[int64]uint, 100) // whether thie Operation (Key: ReqID) has been executed
}

func (kv *KVPaxos) updateClientHistory(client ClientInfo) bool {
	var last, has = kv.impl.ClientLatestReq[client.Name]
	if !has || client.ReqID > last {
		kv.impl.ClientLatestReq[client.Name] = client.ReqID
		return false
	} else {
		return true
	}
}

// Handler for Get RPCs
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	if !kv.isdead() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		println("Get for", args.Key, "on", kv.me)
		kv.rsm.AddOp(args.asOp())
		var value, has = kv.impl.Database[args.Key]
		if has {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		println("Done Get for", args.Key, "on", kv.me)
	}
	return nil
}

// Handler for Put and Append RPCs
func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	if !kv.isdead() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if !kv.updateClientHistory(args.Impl.Client) {
			kv.rsm.AddOp(args.asOp())
		}
		reply.Err = OK
	}
	return nil
}

// Execute operation encoded in decided value v and update local state
func (kv *KVPaxos) ApplyOp(v interface{}) {
	var instance = v.(Op)
	switch instance.Op {
	case "Put":
		kv.impl.Database[instance.Key] = instance.Value
	case "Append":
		var value = kv.impl.Database[instance.Key]
		kv.impl.Database[instance.Key] = value + instance.Value
	}
	kv.updateClientHistory(instance.Client)
}
