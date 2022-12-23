package pbservice

import (
	"fmt"

	"umich.edu/eecs491/proj2/viewservice"
)

type client_op_id struct {
	Client_name int64
	OpID        int
}

type requestOps struct {
	Op    string
	Key   string
	Value string
}

// additions to PBServer state.
type PBServerImpl struct {
	view      viewservice.View
	isPrimary bool
	isBackup  bool

	ck_requests map[client_op_id]bool // a map to map client(name, OpID) -> Is his req done in Primary?
	database    map[string]string     // a map to store key-value pairs
	last_ack    int
}

// your pb.impl.* initializations here.
func (pb *PBServer) initImpl() {
	pb.impl.view = viewservice.View{0, "", ""} // initialize to be zero
	pb.impl.isPrimary = false
	pb.impl.isBackup = false

	pb.impl.ck_requests = make(map[client_op_id]bool)
	pb.impl.database = make(map[string]string)
	pb.impl.last_ack = 0
}

// server Get() RPC handler.
func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	//  If pb.isPrimary: before executing, bootstrap Ops to the backup!
	//  args:  GetArgs,    b_args : BootStrapArgs
	//  reply: GetReply,   b_reply: BootStrapReply
	if pb.isdead() {
		return nil
	}
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// if executed before, return nil
	ck_op_key := client_op_id{args.Impl.Client_name, args.Impl.OpID}
	_, insert := pb.impl.ck_requests[ck_op_key]
	if insert == false {
		fmt.Println("Hasn't execute this operation!")
	} else {
		fmt.Println("Executed! Return!")
		reply.Value = pb.impl.database[args.Key]
		reply.Err = OK
		return nil
	}

	if pb.impl.isPrimary {

		// cur_view, _ := pb.vs.Get()
		if pb.impl.view.Backup != "" {
			// BootStrap Get() to backup
			b_args := &BootStrapArgs{}
			b_args.Client_name = args.Impl.Client_name
			b_args.Req = requestOps{"Get", args.Key, ""}
			b_args.ck_requests = pb.impl.ck_requests
			b_reply := BootStrapReply{}

			// send an RPC request to primary, wait for the reply.
			ok := call(pb.impl.view.Backup, "PBServer.BootStrap", b_args, &b_reply)

			if ok == false || b_reply.Err != OK {
				fmt.Println("Bootstrap RPC Error. Primary -> Backup fail, Network issue or Backup might die")

				// Retry and ask for new_view to check if we have a new backup...
				for ok == false || b_reply.Err != OK {
					cur_view, _ := pb.vs.Get()
					if cur_view.Backup == "" {
						fmt.Printf("No Backup! Primary execute and reply to client.\n")
						break
					} else {
						if cur_view.Backup != pb.impl.view.Backup {
							// Copy database to this nes backup & send RPC to this new backup!
							success := pb.CopyDBtoNewBackup(cur_view.Backup)
							// TODO: Copy might fail?
							for success == false {
								success = pb.CopyDBtoNewBackup(cur_view.Backup)
							}
						}
						pb.impl.view = cur_view // update pb's cached view
						ok = call(pb.impl.view.Backup, "PBServer.BootStrap", b_args, &b_reply)
					}
				}
			}
		} // end if currrent backup is not empty

		// receive ack from Backup
		fmt.Printf("Primary execture Get(%s)\n", args.Key)
		// Execture Get!
		value, ok := pb.impl.database[args.Key]
		if ok == true {
			reply.Value = value
			reply.Err = OK
		} else {
			fmt.Printf("Key %s hasn't been pushed!\n", args.Key)
			reply.Value = ""
			reply.Err = OK
		}

		pb.impl.ck_requests[ck_op_key] = true

	} else if pb.impl.isBackup {
		// execute it
		fmt.Printf("Backup %s executing Get() \n", pb.me)
		value, ok := pb.impl.database[args.Key]
		if ok == true {
			reply.Value = value
			reply.Err = OK
		} else {
			fmt.Printf("Key %s hasn't been pushed!\n", args.Key)
			reply.Value = ""
			reply.Err = OK
		}
	} else {

		fmt.Println("Neither Primary nor Backup. Reject client's request!")
		reply.Value = ""
		reply.Err = ErrWrongServer
	}

	return nil
}

// server PutAppend() RPC handler.
func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	//  If pb.isPrimary: before executing, bootstrap Ops to the backup!
	//  args:  PutAppendArgs,    b_args : BootStrapArgs
	//  reply: PutAppendReply,   b_reply: BootStrapReply

	if pb.isdead() {
		return nil
	}
	// Add mutex here
	pb.mu.Lock()
	defer pb.mu.Unlock()

	ck_op_key := client_op_id{args.Impl.Client_name, args.Impl.OpID}
	_, existed := pb.impl.ck_requests[ck_op_key]
	if existed == false {
		fmt.Printf("I am %s. The ck_op_key first time insert! Should forward req and execute!", pb.me)
	} else {
		fmt.Printf("Already %s, return\n", args.Impl.Op)
		reply.Err = OK
		return nil
	}

	if pb.impl.isPrimary {

		// Backup might me out of date hear!
		if pb.impl.view.Backup != "" {
			// Send RPC to Backup and wait for its ACK
			// Put or Append
			b_args := &BootStrapArgs{}
			b_args.Client_name = args.Impl.Client_name
			b_args.Req = requestOps{args.Impl.Op, args.Key, args.Value}
			b_args.ck_requests = pb.impl.ck_requests
			b_reply := BootStrapReply{}

			fmt.Printf("Primary send BootStrap RPC to Backup\n")
			// send an RPC request to primary, wait for the reply.
			ok := call(pb.impl.view.Backup, "PBServer.BootStrap", b_args, &b_reply)

			// If network fails or backup dies:
			if ok == false || b_reply.Err != OK {
				fmt.Println("Bootstrap RPC Error. Primary -> Backup fail, Network issue or Backup might die")

				// Retry and ask for new_view to check if we have a new backup...
				for ok == false || b_reply.Err != OK {
					cur_view, _ := pb.vs.Get()
					if cur_view.Backup == "" {
						fmt.Printf("No Backup, so Primary execute PutAppend itself!\n")
						// fmt.Printf("No Backup! Primary execute and reply to client.\n")
						break
					} else {
						if cur_view.Backup != pb.impl.view.Backup {
							// Copy database to this nes backup & send RPC to this new backup!
							success := pb.CopyDBtoNewBackup(cur_view.Backup)
							// TODO: Copy might fail?
							for success == false {
								success = pb.CopyDBtoNewBackup(cur_view.Backup)
							}
						}
						pb.impl.view = cur_view // update pb's cached view
						fmt.Printf("%s Retry... forward req to backup %s \n", pb.me, pb.impl.view.Backup)
						ok = call(pb.impl.view.Backup, "PBServer.BootStrap", b_args, &b_reply)
					}
				}
				fmt.Printf("ok==true && b_reply.Err==OK\n")
			}
		}
		// fmt.Printf("Primary %s recv ack_num %d from backup %s\n", pb.me, pb.impl.last_ack, pb.impl.view.Backup)
		fmt.Printf("Primary %s execute PutAppend! %s %s -> %s \n", pb.me, args.Impl.Op, args.Key, args.Value)
		// Primary Execture Get!
		op := args.Impl.Op
		if op == "Put" {
			fmt.Printf("	Put key %s -> %s \n", args.Key, args.Value)
			pb.impl.database[args.Key] = args.Value
		} else {
			// Append
			str, ok := pb.impl.database[args.Key]
			if ok == true {
				fmt.Printf("	Append key %s -> %s \n", args.Key, str+args.Value)
				// key existed :=> Append the string 'value' after string 'str'!
				pb.impl.database[args.Key] = str + args.Value
			} else {
				// key first appear :=> Put
				fmt.Printf("	Append key (first put) %s -> %s \n", args.Key, args.Value)
				pb.impl.database[args.Key] = args.Value
			}

		}
		reply.Err = OK
		pb.impl.ck_requests[ck_op_key] = true

	} else if pb.impl.isBackup {
		// execute it
		fmt.Printf("Backup %s execute PutAppend!\n", pb.me)
		op := args.Impl.Op
		if op == "Put" {
			fmt.Printf("	Put key %s -> %s \n", args.Key, args.Value)
			pb.impl.database[args.Key] = args.Value
		} else {
			// Append
			str, ok := pb.impl.database[args.Key]
			if ok == true {
				fmt.Printf("	Append key %s -> %s \n", args.Key, str+args.Value)
				// key existed :=> Append the string 'value' after string 'str'!
				pb.impl.database[args.Key] = str + args.Value
			} else {
				// key first appear :=> Put
				fmt.Printf("	Append key (first put)%s -> %s \n", args.Key, args.Value)
				pb.impl.database[args.Key] = args.Value
			}

		}
		reply.Err = OK

	} else {
		fmt.Printf("%s is neither Primary nor Backup. Reject client's request!\n", pb.me)
		reply.Err = ErrWrongServer
	}

	return nil

}

// ping the viewserver periodically.
// if view changed:
//
//	transition to new view.
//	manage transfer of state from primary to new backup.
//
// /// NOTE: This could be run on either Primary, Backup, or IDLE machine!
func (pb *PBServer) tick() {

	if pb.isdead() {
		// pb.impl.view.Viewnum = 0 // reset
		pb.impl.isPrimary = false
		pb.impl.isBackup = false
		return
	}

	// Add mutex here
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// ping ViewService for the current view
	cur_view, err := pb.vs.Ping(pb.impl.view.Viewnum)
	fmt.Printf("[PBServer %s tick()]  : view:(%d,%s,%s) \n ", pb.me, cur_view.Viewnum, cur_view.Primary, cur_view.Backup)

	if err != nil {
		fmt.Printf("error!\n")
	}

	primary := cur_view.Primary
	backup := cur_view.Backup

	if pb.me == primary {
		fmt.Printf("%s is primary!\n", pb.me)
		pb.impl.isPrimary = true
		pb.impl.isBackup = false
		// Does the primary have to forward its database to a new Backup?s
		if backup != pb.impl.view.Backup {
			pb.CopyDBtoNewBackup(backup)
		}
	} else if pb.me == backup {
		pb.impl.isBackup = true
		pb.impl.isPrimary = false
		fmt.Printf("%s is backup\n", pb.me)
	} else {
		pb.impl.isPrimary = false
		pb.impl.isBackup = false
		fmt.Printf("%s is an idle server\n", pb.me)
	}

	// update pb.impl.view
	pb.impl.view = cur_view

}

//
// add RPC handlers for any new RPCs that you include in your design.
//

func (pb *PBServer) CopyDBtoNewBackup(backup string) bool { // true: sucess, false: fail
	// Primary should forward database!
	forwarddb_args := &ForwardDBArgs{}
	forwarddb_args.Database = pb.impl.database
	f_reply := ForwardDBReply{}
	// send an RPC request to primary, wait for the reply.
	ok := call(backup, "PBServer.ForwardDB", forwarddb_args, &f_reply)
	if ok == false {
		fmt.Println("ForwardDB RPC Error. Primary -> Backup fail, Backup might die")
		return false
	} else {
		fmt.Printf("Successfully copy primary's %s states to the new backup %s!\n", pb.me, backup)
		return true
	}
}

// server Bootstrap RPC handler.
// Primary try to forward requestOps to the Backup
func (pb *PBServer) BootStrap(args *BootStrapArgs, reply *BootStrapReply) error {

	///// Cause DeadLock!
	// Add mutex here
	// pb.mu.Lock()
	// defer pb.mu.Unlock()

	if pb.isdead() {
		return nil
	}

	// call pb.Get / PutAppend
	req := args.Req // type requestOps
	if req.Op == "Get" {
		g_args := &GetArgs{}
		g_args.Key = args.Req.Key
		g_args.Impl.Client_name = args.Client_name
		g_args.Impl.OpID = args.OpID
		g_reply := GetReply{}
		err := pb.Get(g_args, &g_reply)
		reply.Err = g_reply.Err
		if err != nil {
			fmt.Println("Backup BootStrap Failed")
		}
	} else if req.Op == "Put" {

		p_args := &PutAppendArgs{}
		p_args.Key = args.Req.Key
		p_args.Value = args.Req.Value
		p_args.Impl.Client_name = args.Client_name
		p_args.Impl.OpID = args.OpID

		p_args.Impl.Op = "Put"
		p_reply := PutAppendReply{}
		err := pb.PutAppend(p_args, &p_reply)
		reply.Err = p_reply.Err
		if err != nil {
			fmt.Println("Backup BootStrap Failed")
		}

	} else { // Append request

		p_args := &PutAppendArgs{}
		p_args.Key = args.Req.Key
		p_args.Value = args.Req.Value
		p_args.Impl.Client_name = args.Client_name
		p_args.Impl.OpID = args.OpID
		p_args.Impl.Op = "Append"
		p_reply := PutAppendReply{}
		err := pb.PutAppend(p_args, &p_reply)
		reply.Err = p_reply.Err
		if err != nil {
			fmt.Println("Backup BootStrap Failed")
		}

	}

	// Upon return,
	return nil
}

func (pb *PBServer) ForwardDB(args *ForwardDBArgs, reply *ForwardDBReply) error {
	// Add mutex here
	// pb.mu.Lock()
	// defer pb.mu.Unlock()

	if pb.isdead() {
		return nil

	}

	for key, value := range args.Ck_requests {
		pb.impl.ck_requests[key] = value
	}

	for key, value := range args.Database {
		pb.impl.database[key] = value
	}

	// KEY: From IDLE to BACKUP
	// I am now the new backup!
	pb.impl.isPrimary = false
	pb.impl.isBackup = true

	return nil
}
