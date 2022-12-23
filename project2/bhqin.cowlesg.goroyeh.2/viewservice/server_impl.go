package viewservice

//
// additions to ViewServer state.
//

/*

1. tick number : int
2. current view: View
3. an idle map : map address to servers
map[address] => servers' last ping : int

4. a channel to check if primary acknowledges

5. a structure for primary, backmap
a tuple < <p_last_tick, p _view _num   > , <  >  >

6. promting: just push one from the idle map
   iterate through the idle hashmap
7. lock: protect the "tick" of ViewService

struct (for primary & backup)
1. last ping tick :int
2. last view number: int




*/

/*
09/25 Question:

Fail on View Service wait for primary to ACK:

Q1.
I have only one place to send "true" to the primary_ack_channel:
which is when we receive Ping RPC from the primary server

However, if I change the buffer size to 0, 1, 10, or 50.
I'll get the view service spin forever and I don't know why.
(I have all the <-vs.impl.p_ack_chan commet out so none of them will be waiting for this channel)

Q2.
Currently, there are two functions that could change the view number of the view service.
They are Ping() and tick() in server_impl.go
Is this valid or is this not recommended?
Since I saw the comment for tick() and it said we should determine whether to change view in tick()

Q3.
The spec said all Ping RPC would be threads running concurrently,
however, they share the same View Service.
Thus, should I use a mutext lock to protect some components in View Service?
But it seems that I should block all things inside 'Ping()' inorder to maintain consistency across threads.
Is there anything wrong in my understanding?
Thank you so much!

*/

import (
	"fmt"
)

// a data structure for each idle server
type idle_server struct {
	name      string // the name of this idle server
	alive     bool   // if this machine is alive
	view      View   // current view (num, primary, backup)
	last_ping int
}

type p_and_b struct {
	p_alive     bool // if primary is alive
	p_last_ping int
	b_alive     bool // if backup is alive
	b_last_ping int
}

type ViewServerImpl struct {
	tick         int                    // current tick number
	view         View                   // current view (view_num, primary, backup) in common.go
	p_and_b_     p_and_b                // a structure for primary's last ping tick and  backup's last ping tick
	idle_servers map[string]idle_server // a map
	// idle_servers [] idle_server				// a slice of idle_servers

	recv_p_ack bool // whether the view server receive an acknowledgement from the primary

	p_ack_chan chan bool // a channel to make sure the primary acknoledges current view

}

// func (vs *ViewServer) RemoveIdle(index int){
//     vs.impl.idle_servers = append(vs.impl.idle_servers[:index], vs.impl.idle_servers[index+1:]...)
// }

// your vs.impl.* initializations here.
func (vs *ViewServer) initImpl() {

	vs.impl.tick = 0
	vs.impl.view = View{0, "", ""}
	vs.impl.p_and_b_ = p_and_b{false, 0, false, 0}
	vs.impl.idle_servers = make(map[string]idle_server)
	vs.impl.p_ack_chan = make(chan bool, 100)
	vs.impl.recv_p_ack = false
}

//
// server Ping() RPC handler.
//
// TODO: whenever the primary pings, if view_num from primary == vs.impl.view.Viewnum => send a true(ready) to p_ack_channel
// 		 So the view service can proceed to the new view!

func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	/*
		PingArgs:
			Me      string // "host:port"
			Viewnum uint   // caller's notion of current view #
	*/

	if vs.isdead() {
		return nil
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	name := args.Me
	view_num := args.Viewnum

	fmt.Printf("\n\n[PING!] view_num from server %s: %d\n", name, view_num)

	// Special case: (1) First time the server ping (2) directly restart server
	//               (2) if p/b not empty and == name: restart!
	if view_num == 0 {
		if name == vs.impl.view.Primary {
			// drop it, and place it to the idle
			// first, add to idle
			vs.impl.idle_servers[name] = idle_server{name, true, vs.impl.view, vs.impl.tick}

			if vs.impl.view.Backup == "" {
				vs.impl.view.Primary = ""
				// vs.impl.recv_p_ack = false

				// at least one idle server, since we push last primary first!
				for _, server := range vs.impl.idle_servers {
					vs.impl.view.Primary = server.name
					// key! needs to delete this machine from idle_servers!
					delete(vs.impl.idle_servers, server.name)
					// <- vs.impl.p_ack_chan
					vs.impl.view.Viewnum++
					vs.impl.recv_p_ack = false
					fmt.Printf("Direct restart primary %s, push to idle and %s becomes new Primary!\n", name, server.name)
					break // only take the first key-value in idle map
				}
			} else {
				vs.impl.view.Primary = vs.impl.view.Backup

				vs.impl.recv_p_ack = false

				// promote a new backup
				// at least one idle server so no need to check
				for _, server := range vs.impl.idle_servers {
					vs.impl.view.Backup = server.name
					// key! needs to delete this machine from idle_servers!
					delete(vs.impl.idle_servers, server.name)
					//<- vs.impl.p_ack_chan
					vs.impl.view.Viewnum++
					vs.impl.recv_p_ack = false
					fmt.Printf("Direct restart primary %s, push to idle and %s becomes new Backup\n", name, server.name)
					break // only take the first key-value in idle map
				}
			}
		} else if name == vs.impl.view.Backup {
			// directly restart the backup machine:
			// (1) drop backup & promote a new backup (if has idle machine)
			// (2) add the backup to the idlemap!
			vs.impl.idle_servers[name] = idle_server{name, true, vs.impl.view, vs.impl.tick}

			for _, server := range vs.impl.idle_servers {
				vs.impl.view.Backup = server.name
				delete(vs.impl.idle_servers, server.name)
				//<- vs.impl.p_ack_chan
				vs.impl.view.Viewnum++
				vs.impl.recv_p_ack = false
				break // only take the first key-value in idle map
			}
		} else { // name NOT primary & backup:
			/*
				1. If Primary is empty ("")
				   set it to be primary
				2. P exist, Backup is empty:
				   set it to be backup
				3. Just add to idle

			*/
			if vs.impl.view.Primary == "" {
				vs.impl.view.Primary = name
				vs.impl.p_and_b_.p_alive = true
				vs.impl.p_and_b_.p_last_ping = vs.impl.tick
				fmt.Printf("Previously no primary. Set primary to be %s\n", name)
				///// Problem: Doesn't have primary before? Spin forever? //////
				//<- vs.impl.p_ack_chan // Problem: spin forever!
				vs.impl.view.Viewnum++
				vs.impl.recv_p_ack = false
			} else if vs.impl.view.Backup == "" {
				// case: previously don't have any idle server!
				vs.impl.view.Backup = name
				vs.impl.p_and_b_.b_alive = true
				vs.impl.p_and_b_.b_last_ping = vs.impl.tick
				// wait for p to acknowledge vew num:
				//<- vs.impl.p_ack_chan
				vs.impl.view.Viewnum++
				vs.impl.recv_p_ack = false
				fmt.Printf("Previously no backup. Set %s to be new Backup\n", name)

			} else {
				// Just add it to the idle_servers. No view change needed
				vs.impl.idle_servers[name] = idle_server{name, true, vs.impl.view, vs.impl.tick}
				fmt.Printf("Add server %s to idle_servers!\n", name)
			}
		}

		fmt.Printf("[End Ping()] View_num %d Primary: %s \n", vs.impl.view.Viewnum, vs.impl.view.Primary)
		fmt.Printf("receive primary ack: %v\n", vs.impl.recv_p_ack)
		reply.View = vs.impl.view
		return nil
	}

	// view_num NOT zero:
	// If ping from Primary: send ACK to primary_ack_channel!
	if name == vs.impl.view.Primary {
		vs.impl.p_and_b_.p_alive = true
		vs.impl.p_and_b_.p_last_ping = vs.impl.tick
		fmt.Println("Primary ping, alive!")
		// vs.impl.p_ack_chan <- true
		fmt.Println("[P ACK] Primary ack on current view! Set recv_p_ack to true")
		if view_num == vs.impl.view.Viewnum {
			vs.impl.recv_p_ack = true
		}
	} else if name == vs.impl.view.Backup {
		vs.impl.p_and_b_.b_alive = true
		vs.impl.p_and_b_.b_last_ping = vs.impl.tick
		fmt.Println("Backup ping, alive!")
	} else {
		// Do we have this key(idle server) before?
		server, _ := vs.impl.idle_servers[name]
		// Yes:
		// if ok == true {
		server.alive = true
		server.last_ping = vs.impl.tick
		fmt.Println("server ping, alive")
		// No: add the new server to the idle server.
		//// Impossible! /////
		// } else {
		// 	fmt.Println("Impossible! idle server first added with non-zero view number.\nSomething went wrong!")
		// }
	}

	fmt.Printf("[End Ping()] View_num %d Primary: %s \n", vs.impl.view.Viewnum, vs.impl.view.Primary)
	fmt.Printf("receive primary ack: %v\n", vs.impl.recv_p_ack)
	reply.View = vs.impl.view
	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	if vs.isdead() {
		return nil
	}
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.impl.view
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//

/*

for ViewServer to go through each server
If the primary fails => change View
go routine:

*/

/*


 */

func (vs *ViewServer) tick() {

	// Add mutex here
	// vs.mu.Lock()
	// defer vs.mu.Unlock()
	// go through its primary, backup, and idle_servers to check if any machine dies
	if vs.isdead() {
		return
	}
	vs.mu.Lock()
	// defer vs.mu.Unlock()

	fmt.Printf("\n\n=======\n tick number: %d\n", vs.impl.tick)
	fmt.Printf("View: (%d, %s, %s) \n", vs.impl.view.Viewnum, vs.impl.view.Primary, vs.impl.view.Backup)

	// check if primary is dead

	tick_diff := vs.impl.tick - vs.impl.p_and_b_.p_last_ping
	if vs.impl.p_and_b_.p_alive && tick_diff >= DeadPings {
		// bring backup to primary!
		fmt.Println("Primary dead. Promte one backup to be the new Primary!")

		if vs.impl.recv_p_ack == true {
			fmt.Println("Recv primary ack. Can proceed to new view!")
			vs.impl.p_and_b_.p_alive = false

			if vs.impl.view.Backup != "" {
				vs.impl.view.Primary = vs.impl.view.Backup
				vs.impl.p_and_b_.p_alive = true
				vs.impl.recv_p_ack = false // reset primary ack flag

				if len(vs.impl.idle_servers) > 0 {
					for _, server := range vs.impl.idle_servers {
						vs.impl.view.Backup = server.name
						delete(vs.impl.idle_servers, server.name)
						break
					}
					vs.impl.view.Viewnum++
					vs.impl.recv_p_ack = false
					fmt.Printf("Promte %s to be the new Backup. View change: (%d, %s, %s)\n", vs.impl.view.Backup, vs.impl.view.Viewnum, vs.impl.view.Primary, vs.impl.view.Backup)

				} else {
					vs.impl.view.Backup = ""
					vs.impl.view.Viewnum++
					vs.impl.recv_p_ack = false
					fmt.Printf("No idle server so Backup is empty. View change: (%d, %s, %s)\n", vs.impl.view.Viewnum, vs.impl.view.Primary, vs.impl.view.Backup)

				}
			} else {
				vs.impl.view.Primary = ""
				vs.impl.view.Viewnum++
				vs.impl.recv_p_ack = false
				fmt.Printf("No backup, new view: (%d, %s, %s)\n", vs.impl.view.Viewnum, vs.impl.view.Primary, vs.impl.view.Backup)
			}
		} else {
			fmt.Println("Primary hasn't acknoledge current view. No view change!")
		}
	}

	// check if backup is dead
	tick_diff = vs.impl.tick - vs.impl.p_and_b_.b_last_ping
	if vs.impl.p_and_b_.b_alive && tick_diff >= DeadPings {
		// bring backup to primary!
		fmt.Println("Backup dead. Promte one backup!")
		vs.impl.p_and_b_.b_alive = false

		if len(vs.impl.idle_servers) > 0 {
			for _, server := range vs.impl.idle_servers {
				vs.impl.view.Backup = server.name
				delete(vs.impl.idle_servers, server.name)
				// TODO: wait for p ack
				// <- vs.impl.p_ack_chan
				vs.impl.view.Viewnum++
				vs.impl.recv_p_ack = false
				fmt.Printf("Promte %s to be the new Backup. View change: (%d, %s, %s)\n", vs.impl.view.Backup, vs.impl.view.Viewnum, vs.impl.view.Primary, vs.impl.view.Backup)
				break
			}
		} else {
			vs.impl.view.Backup = ""
			// TODO: wait for p ack to proceed to next view
			// <- vs.impl.p_ack_chan
			vs.impl.view.Viewnum++
			vs.impl.recv_p_ack = false
			fmt.Printf("No idle server to replace backup.")
		}

	}

	vs.impl.tick += 1
	fmt.Printf("tick: %d\n", vs.impl.tick)
	vs.mu.Unlock()
}
