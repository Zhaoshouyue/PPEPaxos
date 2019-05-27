package main

import (
	"flag"
	"fmt"
	"ppgenericsmrproto"
	"log"
	"masterproto"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

var portnum *int = flag.Int("port", 7087, "Port # to listen on. Defaults to 7087")
var numNodes *int = flag.Int("N", 3, "Number of replicas. Defaults to 3.")

//modified by Luna in 2018.12.20
const MAX_COM = 15000

type Master struct {
	N        int
	nodeList []string
	addrList []string
	portList []int
	lock     *sync.Mutex
	nodes    []*rpc.Client
	leader   []bool
	alive    []bool
}

//modified by Luna in 2018.12.20
//to calculate the priority of each allocating key
type comPriority struct {
	scDif    int //the difference factor
	variance float64 //the variance of replica command
}
type keyAdapter struct {
	key              int64 //the key No
	infoElem         []float64 //the key allocation factor of each replica 
	relateElem       []float64 //the related key factor of each replica 
	isAllocate       bool //if the key has been allocate
	comNo            int //the total command number of the key
	allocatePriority comPriority //for decide which key to allocate first
}

func main() {
	flag.Parse()

	log.Printf("Master starting on port %d\n", *portnum)
	log.Printf("...waiting for %d replicas\n", *numNodes)

	master := &Master{*numNodes,
		make([]string, 0, *numNodes),
		make([]string, 0, *numNodes),
		make([]int, 0, *numNodes),
		new(sync.Mutex),
		make([]*rpc.Client, *numNodes),
		make([]bool, *numNodes),
		make([]bool, *numNodes)}

	rpc.Register(master)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if err != nil {
		log.Fatal("Master listen error:", err)
	}

	go master.run()

	http.Serve(l, nil)
}

func (master *Master) run() {
	for true {
		master.lock.Lock()
		if len(master.nodeList) == master.N {
			master.lock.Unlock()
			break
		}
		master.lock.Unlock()
		time.Sleep(100000000)
	}
	time.Sleep(2000000000)

	// connect to SMR servers
	for i := 0; i < master.N; i++ {
		var err error
		addr := fmt.Sprintf("%s:%d", master.addrList[i], master.portList[i]+1000)
		master.nodes[i], err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("Error connecting to replica %d\n", i)
		}
		master.leader[i] = false
	}
	master.leader[0] = true

	//Modified by Luna in 2018.12.20
	//if true, the key need to do partion processing; if false, the key don't need to
	oldKToR := make([]bool, ppgenericsmrproto.KEY_NUM)
	for i := 0; i < ppgenericsmrproto.KEY_NUM; i++ {
		oldKToR[i] = false
	}

	for true {
		time.Sleep(3000 * 1000 * 1000)//3 seconds
		//Modified by Luna in 2018.12.20
		keyInfoFromR := make([]ppgenericsmrproto.EDealInfoReply, master.N)//reserve the information from replica
		//get the information from replica
		for i := 0; i < master.N; i++ {
			if master.alive[i] {
				var rReply ppgenericsmrproto.EDealInfoReply
				err := master.nodes[i].Call("Replica.EDealInfo", new(ppgenericsmrproto.EDealInfoArgs), &rReply)
				if err == nil {
					keyInfoFromR[i] = rReply
					/*if keyInfoFromR[i].KeyInfo[42].Total != 0 {
						log.Printf("key 42 has %d slow commands of %d total commands.\n", keyInfoFromR[i].KeyInfo[42].Slow, keyInfoFromR[i].KeyInfo[42].Total)
					}*/
				} else {
					log.Printf("Can't get replica %d key information.\n", i)
				}
			}
		}
		total := make([]int, master.N) //record each replica deal with how much commands
		for i := 0; i < master.N; i++ {
			total[i] = 0
		}
		var totalSlow int//for each key
		var totalNum int//for each key
		var maxTotalNum int//the max num for each key
		var maxTotalSlow int//the max slow num for each key
		allKeyAdapter := make([]*keyAdapter, ppgenericsmrproto.KEY_NUM) //reserve the adapter for each key
		var keyNo int//record the length of allKeyAdapter
		keyNo = 0
		//judge if need to do partion execution. 
		for i := 0; i < ppgenericsmrproto.KEY_NUM; i++ {
			totalSlow = 0
			totalNum = 0
			maxTotalNum = 0
			maxTotalSlow = 0

			//For each key, calculate its totalSlow and totalNum
			for j := 0; j < master.N; j++ {
				if master.alive[j] {
					totalSlow = totalSlow + keyInfoFromR[j].KeyInfo[i].Slow
					totalNum = totalNum + keyInfoFromR[j].KeyInfo[i].Total
					if keyInfoFromR[j].KeyInfo[i].Total > maxTotalNum {
						maxTotalNum = keyInfoFromR[j].KeyInfo[i].Total
					}
					if keyInfoFromR[j].KeyInfo[i].Slow > maxTotalSlow {
						maxTotalSlow = keyInfoFromR[j].KeyInfo[i].Slow
					}
				}
			}
			if oldKToR[i] { //the key has already done the partition processing, estimate the conflict number
				if maxTotalSlow > (totalSlow - maxTotalSlow) {
					totalSlow = 2*(totalSlow - maxTotalSlow)
				}
			}
			//if need to do partion execution,satisfy the condition
			if totalSlow*(master.N/2+1) > totalNum {//judge if need to do partion execution
				//add to allKeyAdapter
				elemInfo := make([]float64, master.N)//to calculate the replica allocation index,the client commands factor
				elemRelate := make([]float64, master.N)//to calculate the replica allocation index,the related key allocation factor
				var priority comPriority //for calculate priority 
				var average_m int //the total command number of all replicas for each key
				var average float64 //the average command number of each replica, for calculate variance
				var number int32 //the number of replica for each key
				average_m = 0
				average = 0
				number = 0
				//for calculating which replica to choose
				for p := 0; p < master.N; p++ {
					if master.alive[p] {
						elemInfo[p] = 0.8 * ( float64(keyInfoFromR[p].KeyInfo[i].Total) / float64(maxTotalNum) )
						elemRelate[p] = 0
						average_m = average_m + keyInfoFromR[p].KeyInfo[i].Total
						number++
					}
				}
				//for calculating the priority, if the number of command i is less than MAX_COM
				if average_m <= MAX_COM {
					commandNo := average_m //the total commands number of the key
					average = float64(average_m)/float64(number)
					priority.variance = 0
					for p := 0; p < master.N; p++ {
						if master.alive[p] {
							priority.variance = priority.variance + (float64(keyInfoFromR[p].KeyInfo[i].Total) - average) * (float64(keyInfoFromR[p].KeyInfo[i].Total) - average)
						}
					}
					priority.scDif = totalSlow*(master.N/2+1) - totalNum
					priority.variance = priority.variance/float64(number)
					//add to allKeyAdapter
					allKeyAdapter[keyNo] = &keyAdapter{int64(i), elemInfo, elemRelate, false, commandNo, priority}
					keyNo = keyNo + 1
				}	
			} else {
				//if don't need to allocate, calculate the dealt command number for each replica
				for k := 0; k < master.N; k++ {
					if master.alive[k] {
						total[k] = total[k] + keyInfoFromR[k].KeyInfo[i].Total //calculate the total number of commands which don't need to partion execution
					}
				}
			}
		}
		// update oldKToR
		for i := 0; i < ppgenericsmrproto.KEY_NUM; i++ {
			oldKToR[i] = false
		}
		keyToReplica := make([]ppgenericsmrproto.AllocateResult, keyNo) //reserve which key belongs to which replica

		//calculate key priority, and allKeyAdapter is ordered by priority.
		for i := 0; i < keyNo-1; i++ {
			minIndex := i
			for j := i+1; j < keyNo; j++ {
				if allKeyAdapter[j].allocatePriority.scDif > allKeyAdapter[minIndex].allocatePriority.scDif {
					minIndex = j
					/*middleResult = allKeyAdapter[j].allocatePriority.scDif
					middleVariance = allKeyAdapter[j].allocatePriority.variance
					middleResultIndex = j*/
				} else if allKeyAdapter[j].allocatePriority.scDif == allKeyAdapter[minIndex].allocatePriority.scDif && allKeyAdapter[j].allocatePriority.variance < allKeyAdapter[minIndex].allocatePriority.variance {
					/*middleVariance = allKeyAdapter[j].allocatePriority.variance
					middleResultIndex = j*/
					minIndex = j
				}
			}
			if minIndex != i {
				tmp := allKeyAdapter[i]
				allKeyAdapter[i] = allKeyAdapter[minIndex]
				allKeyAdapter[minIndex] = tmp
			}
		}

		//if there exists key to be partioned, allocate key to replica
		var transKeyNo int
		transKeyNo = 0
		if keyNo != 0 {
			//our strategy is to find the max adapter number in allKeyAdapter in order of priority
			var maxAdapter float64 //the max adapter NO for each key
			var comNum int //the total number of the command in maxAdapter
			var replicaNo int32 //the replica number of max adapter NO for each key
			
			for i := 0; i < keyNo; i++ { //the index of keyToReplica 
				maxAdapter, comNum, replicaNo = -1, -1, -1
				//if not allocated
				if !allKeyAdapter[i].isAllocate {
					for q := int32(0); q < int32(master.N); q++ { // for each replica
						if master.alive[q] && ((total[q]+allKeyAdapter[i].comNo) <= MAX_COM) { // the load factor
							qadapter := allKeyAdapter[i].infoElem[q]+allKeyAdapter[i].relateElem[q]
							if qadapter >  maxAdapter { //has the greater adapter
								maxAdapter = qadapter
								comNum = keyInfoFromR[q].KeyInfo[allKeyAdapter[i].key].Total
								replicaNo = q
							}else if qadapter == maxAdapter && keyInfoFromR[q].KeyInfo[allKeyAdapter[i].key].Total > comNum { //when the adapter is equal, choose the replica that has the greater command number
								comNum = keyInfoFromR[q].KeyInfo[allKeyAdapter[i].key].Total
								replicaNo = q
							}
						}
					}
					//allocate the key
					if replicaNo != -1 {
						keyToReplica[transKeyNo].Key = allKeyAdapter[i].key
						keyToReplica[transKeyNo].Replica = replicaNo
						allKeyAdapter[i].isAllocate = true
						total[replicaNo] = total[replicaNo] + allKeyAdapter[i].comNo
						//update oldKTOR
						oldKToR[keyToReplica[transKeyNo].Key] = true
						//update allKeyAdapter
						master.updateAdapter(allKeyAdapter, keyNo, keyToReplica[transKeyNo].Key, replicaNo)	
						transKeyNo++
						log.Printf("key %d need to transport commands to replica %d.\n", keyToReplica[transKeyNo-1].Key, keyToReplica[transKeyNo-1].Replica)
					} else { //all replica is overload
						for j := 0; j < master.N; j++ {
							total[j] = total[j] + keyInfoFromR[j].KeyInfo[allKeyAdapter[i].key].Total
						}
					}
				}	
			}
		}
		//inform replica
		for i := 0; i < master.N; i++ {
			if master.alive[i] {
				sendArgs := &ppgenericsmrproto.UpdateKeyAllocateArgs{transKeyNo, keyToReplica}
				err := master.nodes[i].Call("Replica.UpdateKeyAllocate", sendArgs, new(ppgenericsmrproto.UpdateKeyAllocateReply))
				if err != nil {
					log.Printf("Failed send key allocate information to replica %d.\n", i)
				}
			}
		}
		//log.Printf("Success inform all replica.\n")
		
		new_leader := false
		for i, node := range master.nodes {
			err := node.Call("Replica.Ping", new(ppgenericsmrproto.PingArgs), new(ppgenericsmrproto.PingReply))
			if err != nil {
				//log.Printf("Replica %d has failed to reply\n", i)
				master.alive[i] = false
				if master.leader[i] {
					// neet to choose a new leader
					new_leader = true
					master.leader[i] = false
				}
			} else {
				master.alive[i] = true
			}
		}
		if !new_leader {
			continue
		}
		for i, new_master := range master.nodes {
			if master.alive[i] {
				err := new_master.Call("Replica.BeTheLeader", new(ppgenericsmrproto.BeTheLeaderArgs), new(ppgenericsmrproto.BeTheLeaderReply))
				if err == nil {
					master.leader[i] = true
					log.Printf("Replica %d is the new leader.", i)
					break
				}
			}
		}
	}
}

//update the allKeyAdapter
func (master *Master) updateAdapter(adapterValue []*keyAdapter, keyNo int ,key int64, rep int32) {
	for i := 0; i < keyNo; i++ {
		if !adapterValue[i].isAllocate {
			if adapterValue[i].key-key <= 1 && adapterValue[i].key-key >= -1{ //1 represents the related degree,which could be changed into 2 or 3
				adapterValue[i].relateElem[rep] = 0.2
			}
		}
	}
}

func (master *Master) Register(args *masterproto.RegisterArgs, reply *masterproto.RegisterReply) error {

	master.lock.Lock()
	defer master.lock.Unlock()

	nlen := len(master.nodeList)
	index := nlen

	addrPort := fmt.Sprintf("%s:%d", args.Addr, args.Port)

	for i, ap := range master.nodeList {
		if addrPort == ap {
			index = i
			break
		}
	}

	if index == nlen {
		master.nodeList = master.nodeList[0 : nlen+1]
		master.nodeList[nlen] = addrPort
		master.addrList = master.addrList[0 : nlen+1]
		master.addrList[nlen] = args.Addr
		master.portList = master.portList[0 : nlen+1]
		master.portList[nlen] = args.Port
		nlen++
	}

	if nlen == master.N {
		reply.Ready = true
		reply.ReplicaId = index
		reply.NodeList = master.nodeList
	} else {
		reply.Ready = false
	}

	return nil
}

func (master *Master) GetLeader(args *masterproto.GetLeaderArgs, reply *masterproto.GetLeaderReply) error {
	time.Sleep(4 * 1000 * 1000)
	for i, l := range master.leader {
		if l {
			*reply = masterproto.GetLeaderReply{i}
			break
		}
	}
	return nil
}

func (master *Master) GetReplicaList(args *masterproto.GetReplicaListArgs, reply *masterproto.GetReplicaListReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()

	if len(master.nodeList) == master.N {
		reply.ReplicaList = master.nodeList
		reply.Ready = true
	} else {
		reply.Ready = false
	}
	return nil
}
