package hashkv

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"

	// "sync"
	"testing"
	"time"

	"umich.edu/eecs491/proj4/common"
)

const NKeys = 20

type Server struct {
	ID   string
	port string
}

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "skv-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func setup(t *testing.T, tag string, nservers int) []Server {
	servers := make([]Server, nservers)
	for i := 0; i < nservers; i++ {
		servers[i].ID = strconv.FormatUint(uint64(common.Nrand()), 10)
		servers[i].port = port(tag, i)
	}
	return servers
}

func check(t *testing.T, servers []Server, kvmap map[string]string, unassigned bool) {
	compare := func(i, j int) bool {
		h1 := common.Key2Shard(servers[i].ID)
		h2 := common.Key2Shard(servers[j].ID)
		return (h1 < h2) || ((h1 == h2) && (servers[i].ID < servers[j].ID))
	}
	// Sort slice in increasing shards order
	sort.Slice(servers, compare)

	done := make(chan error, 3*NKeys)
	for key, val := range kvmap {
		j := 0
		for ; j < len(servers); j++ {
			if common.Key2Shard(key) <= common.Key2Shard(servers[j].ID) {
				break
			}
		}
		assigned := j % len(servers)

		// fmt.Printf("	Checking: Expect key %s -> val %s \n", key, val)
		go checkAssign(key, val, servers[assigned].port, true, done)
		if unassigned {
			pred := (assigned + len(servers) - 1) % len(servers)
			succ := (assigned + len(servers) + 1) % len(servers)
			go checkAssign(key, val, servers[pred].port, false, done)
			go checkAssign(key, val, servers[succ].port, false, done)
		}
	}

	drainOne := func(done <-chan error) {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < NKeys; i++ {
		drainOne(done)
		if unassigned {
			drainOne(done)
			drainOne(done)
		}
	}
}

func checkAssign(key string, value string, s string, assigned bool, done chan error) {
	ck := MakeClerk([]string{s})

	ch := make(chan string)
	go func() {
		// fmt.Printf("	ck send Get(%s) req\n", key)
		v := ck.Get(key)
		// fmt.Printf("	Get %s : %s\n", key, v)
		ch <- v
		close(ch)
	}()
	// fmt.Println("Recv valud from channel ch")
	select {
	case v := <-ch:
		if !assigned {
			done <- fmt.Errorf("Server %s served request on key %s in shard %d, which is not assigned to it", s, key, common.Key2Shard(key))
			return
		}
		if v != value {
			fmt.Printf("Error! Expected value: %s, got %s\n", value, v)
			done <- fmt.Errorf("Server %s returned incorrect value for key %s in shard %d", s, key, common.Key2Shard(key))
			return
		}
	case <-time.After(2 * time.Second):
		if assigned {
			done <- fmt.Errorf("Server %s did not serve request on key %s in shard %d, which is assigned to it", s, key, common.Key2Shard(key))
			return
		}
	}

	done <- nil
}

func findNeighbors(s Server, servers []Server) (Server, Server) {
	compare := func(i, j int) bool {
		h1 := common.Key2Shard(servers[i].ID)
		h2 := common.Key2Shard(servers[j].ID)
		return (h1 < h2) || ((h1 == h2) && (servers[i].ID < servers[j].ID))
	}
	sort.Slice(servers, compare)

	var i int
	for i = 0; i < len(servers); i++ {
		if servers[i] == s {
			break
		}
	}

	return servers[(i+len(servers)-1)%len(servers)], servers[(i+len(servers)+1)%len(servers)]
}

func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4 * NKeys)

	nservers := 3
	s := setup(t, "basic", nservers)
	hashkv_servers := make([]*HashKV, 0)

	fmt.Printf("Test: Basic joins ...\n")

	hashkv_servers = append(hashkv_servers, StartServer([]string{s[0].port, s[0].port, s[0].port}, []string{s[0].ID, s[0].ID, s[0].ID}))

	ports := make([]string, 0)
	ports = append(ports, s[0].port)
	ck := MakeClerk(ports)
	vals := make(map[string]string)
	rr := rand.New(rand.NewSource(int64(os.Getpid())))
	for j := 0; j < NKeys; j++ {
		key := strconv.Itoa(rr.Int())
		vals[key] = strconv.Itoa(rr.Int())
		ck.Put(key, vals[key])
	}

	for i := 1; i < nservers; i++ {
		curr_s := make([]Server, i+1)
		copy(curr_s, s[:i+1])
		pred, succ := findNeighbors(s[i], curr_s)
		hashkv_servers = append(hashkv_servers, StartServer([]string{pred.port, s[i].port, succ.port}, []string{pred.ID, s[i].ID, succ.ID}))
		check(t, curr_s, vals, false)

		ports = append(ports, s[i].port)
		ck = MakeClerk(ports)
		for key, _ := range vals {
			vals[key] = strconv.Itoa(rr.Int())
			ck.Put(key, vals[key])
		}
	}
	curr_s := make([]Server, nservers)
	copy(curr_s, s)
	check(t, curr_s, vals, true)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Basic leaves ...\n")

	for i := 0; i < nservers-1; i++ {
		hashkv_servers[0].kill()
		hashkv_servers = hashkv_servers[1:]
		curr_s := make([]Server, nservers-1-i)
		copy(curr_s, s[i+1:])
		check(t, curr_s, vals, false)

		ports = ports[1:]
		ck = MakeClerk(ports)
		for key, _ := range vals {
			vals[key] = strconv.Itoa(rr.Int())
			ck.Put(key, vals[key])
		}
	}

	fmt.Printf("  ... Passed\n")
}

// Test : Unreliable
func TestUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4 * NKeys)

	nservers := 3
	s := setup(t, "basic", nservers)
	hashkv_servers := make([]*HashKV, 0)

	fmt.Printf("Test: Basic joins Unreliable ...\n")

	hashkv_servers = append(hashkv_servers, StartServer([]string{s[0].port, s[0].port, s[0].port}, []string{s[0].ID, s[0].ID, s[0].ID}))

	var pxa []*HashKV = make([]*HashKV, nservers)
	// var pxh []string = make([]string, npaxos)
	// defer cleanup(pxa)

	ports := make([]string, 0)
	ports = append(ports, s[0].port)
	ck := MakeClerk(ports)
	vals := make(map[string]string)
	rr := rand.New(rand.NewSource(int64(os.Getpid())))
	for j := 0; j < NKeys; j++ {
		key := strconv.Itoa(rr.Int())
		vals[key] = strconv.Itoa(rr.Int())
		ck.Put(key, vals[key])
	}

	for i := 1; i < nservers; i++ {
		curr_s := make([]Server, i+1)
		copy(curr_s, s[:i+1])
		pred, succ := findNeighbors(s[i], curr_s)
		pxa[i] = StartServer([]string{pred.port, s[i].port, succ.port}, []string{pred.ID, s[i].ID, succ.ID})
		pxa[i].Setunreliable(true)

		hashkv_servers = append(hashkv_servers, pxa[i])
		check(t, curr_s, vals, false)
		ports = append(ports, s[i].port)
		ck = MakeClerk(ports)
		for key, _ := range vals {
			vals[key] = strconv.Itoa(rr.Int())
			ck.Put(key, vals[key])
		}
	}

	curr_s := make([]Server, nservers)
	copy(curr_s, s)
	check(t, curr_s, vals, true)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Basic leaves Unreliable...\n")

	for i := 0; i < nservers-1; i++ {
		hashkv_servers[0].kill()
		hashkv_servers = hashkv_servers[1:]
		curr_s := make([]Server, nservers-1-i)
		copy(curr_s, s[i+1:])
		check(t, curr_s, vals, false)

		ports = ports[1:]
		ck = MakeClerk(ports)
		for key, _ := range vals {
			vals[key] = strconv.Itoa(rr.Int())
			ck.Put(key, vals[key])
		}
	}

	fmt.Printf("  ... Passed\n")
}

func cleanup(pxa []*HashKV) {
	for i := 0; i < len(pxa); i++ {
		if pxa[i] != nil {
			pxa[i].kill()
		}
	}
}

/*

Linearizability: Seems to be a Single Server!
Multiple clients send requests to Nkeys:
If only one server => blocking!
How to set groundtruth?
*/

func RandomClerk(n_clerks int) int {
	return int(common.Nrand()) % n_clerks
}
func (ck *Clerk) SendPutReq(key, val string, key_mutex map[string]*sync.Mutex, t *testing.T, done_chan chan string) {
	key_mutex[key].Lock()
	defer key_mutex[key].Unlock()
	fmt.Printf("	Client %d acquire mutex for key %s\n", ck.impl.Self.Name, key)
	ck.Put(key, val)
	fmt.Printf("	Client %d try Get(%s) : \n", ck.impl.Self.Name, key)
	if ck.Get(key) != val {
		err := fmt.Errorf("Error! Get(%s) expect %s but got %s\n", key, val, ck.Get(key))
		t.Fatal(err)
	}
	done_chan <- "Put " + key + "," + val
	fmt.Printf("		Client %d release lock for key %s\n", ck.impl.Self.Name, key)
}
func TestLinearizability(t *testing.T) {
	runtime.GOMAXPROCS(4 * NKeys)

	fmt.Printf("Test: Linearizability ...\n")

	nservers := 10
	n_clerks := nservers

	s := setup(t, "basic", nservers)
	hashkv_servers := make([]*HashKV, 0)
	// hashkv_servers = append(hashkv_servers, StartServer([]string{s[0].port, s[0].port, s[0].port}, []string{s[0].ID, s[0].ID, s[0].ID}))
	ports := make([]string, 0)
	// ports = append(ports, s[0].port)
	// vals := make(map[string]string)
	rr := rand.New(rand.NewSource(int64(os.Getpid())))

	// Initialize n_clerks,
	cks := make([]*Clerk, 0)
	// for j := 0; j < n_clerks; j++ {
	// 	cks = append(cks, MakeClerk(ports))
	// }

	// a map of [string] sync.Mutex, map key to each mutex
	key_mutex := make(map[string]*sync.Mutex, NKeys)
	done_chan := make(chan string)

	var ground_truth_key string
	var ground_truth_val string

	pxa := make([]*HashKV, nservers)
	for i := 0; i < nservers; i++ {

		// Join server
		fmt.Printf("	Add server %d\n", i)
		curr_s := make([]Server, i+1)
		copy(curr_s, s[:i+1])
		pred, succ := findNeighbors(s[i], curr_s)

		pxa[i] = StartServer([]string{pred.port, s[i].port, succ.port}, []string{pred.ID, s[i].ID, succ.ID})
		pxa[i].Setunreliable(true)

		hashkv_servers = append(hashkv_servers, pxa[i])
		// hashkv_servers = append(hashkv_servers, StartServer([]string{pred.port, s[i].port, succ.port}, []string{pred.ID, s[i].ID, succ.ID}))
		// check(t, curr_s, vals, false)
		ports = append(ports, s[i].port)
		///// Note: should update cks's server to be 'port'
		for j := 0; j < i; j++ {
			cks[j].servers = ports
		}
		// Add client
		cks = append(cks, MakeClerk(ports))

		key := strconv.Itoa(rr.Int())
		_, existed := key_mutex[key]
		if !existed {
			key_mutex[key] = &sync.Mutex{}
		}
		val := strconv.Itoa(rr.Int())

		// Assigning ground truth for later check (when Killing servers)
		if i == 0 {
			ground_truth_key = key
			ground_truth_val = val
		}

		fmt.Printf("\n===== Spawn client %d, Key: %s =======\n", i, key)
		go cks[i].SendPutReq(key, val, key_mutex, t, done_chan)

	}

	fmt.Printf("Done spawning go routines. Now wait for success ......\n")

	for i := 0; i < n_clerks; i++ {
		val := <-done_chan
		fmt.Printf("Get value: %s\n", val)
	}

	fmt.Printf("Test leaves ...\n")
	for i := 0; i < nservers-1; i++ {
		hashkv_servers[0].kill()
		hashkv_servers = hashkv_servers[1:]
		curr_s := make([]Server, nservers-1-i)
		copy(curr_s, s[i+1:])
		ports = ports[1:]

		// Update ports
		for j := 0; j < n_clerks; j++ {
			cks[j].servers = ports
		}

		// Try to Get key
		fmt.Printf("	Test Get(%s) \n", ground_truth_key)
		val := cks[RandomClerk(n_clerks)].Get(ground_truth_key)
		if val != ground_truth_val {
			err := fmt.Errorf("Error! Get(%s) expect %s but got %s\n", ground_truth_key, ground_truth_val, val)
			t.Fatal(err)
		}
		fmt.Printf("		Correct! Get(%s) returns %s\n", ground_truth_key, ground_truth_val)

	}

	fmt.Printf("  ... Passed\n")

}
