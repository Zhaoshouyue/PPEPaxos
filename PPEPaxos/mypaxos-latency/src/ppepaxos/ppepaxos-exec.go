package ppepaxos

import (
	"ppgenericsmr"
	//"log"
	"ppepaxosproto"
	"ppgenericsmrproto"
	"sort"
	"time"
)

const (
	WHITE int8 = iota
	GRAY
	BLACK
)

type Exec struct {
	r *Replica
}

type SCComponent struct {
	nodes []*Instance
	color int8
}

func (e *Exec) executeCommand(replica int32, instance int32) bool {
	if e.r.InstanceSpace[replica][instance] == nil {
		return false
	}
	inst := e.r.InstanceSpace[replica][instance]
	if inst.Status == ppepaxosproto.EXECUTED {
		return true
	}
	if inst.Status != ppepaxosproto.COMMITTED {
		return false
	}

	if !e.findSCC(inst) {
		return false
	}

	return true
}

var stack []*Instance = make([]*Instance, 0, 100)

func (e *Exec) findSCC(root *Instance) bool {
	index := 1
	//find SCCs using Tarjan's algorithm
	stack = stack[0:0]
	return e.strongconnect(root, &index)
}

func (e *Exec) strongconnect(v *Instance, index *int) bool {
	v.Index = *index
	v.Lowlink = *index
	*index = *index + 1

	l := len(stack)
	if l == cap(stack) {
		newSlice := make([]*Instance, l, 2*l)
		copy(newSlice, stack)
		stack = newSlice
	}
	stack = stack[0 : l+1]
	stack[l] = v

	for q := int32(0); q < int32(e.r.N); q++ {
		inst := v.Deps[q]
		for i := e.r.ExecedUpTo[q] + 1; i <= inst; i++ {
			for e.r.InstanceSpace[q][i] == nil || e.r.InstanceSpace[q][i].Cmds == nil || v.Cmds == nil {
				time.Sleep(1000 * 1000)
			}
			/*        if !state.Conflict(v.Command, e.r.InstanceSpace[q][i].Command) {
			          continue
			          }
			*/
			if e.r.InstanceSpace[q][i].Status == ppepaxosproto.EXECUTED {
				continue
			}
			for e.r.InstanceSpace[q][i].Status != ppepaxosproto.COMMITTED {
				time.Sleep(1000 * 1000)
			}
			w := e.r.InstanceSpace[q][i]

			if w.Index == 0 {
				//e.strongconnect(w, index)
				if !e.strongconnect(w, index) {
					for j := l; j < len(stack); j++ {
						stack[j].Index = 0
					}
					stack = stack[0:l]
					return false
				}
				if w.Lowlink < v.Lowlink {
					v.Lowlink = w.Lowlink
				}
			} else { //if e.inStack(w)  //<- probably unnecessary condition, saves a linear search
				if w.Index < v.Lowlink {
					v.Lowlink = w.Index
				}
			}
		}
	}

	if v.Lowlink == v.Index {
		//found SCC
		list := stack[l:len(stack)]

		//execute commands in the increasing order of the Seq field
		sort.Sort(nodeArray(list))
		for _, w := range list {
			for w.Cmds == nil {
				time.Sleep(1000 * 1000)
			}
			for idx := 0; idx < len(w.Cmds); idx++ {
				val := w.Cmds[idx].Execute(e.r.State)
				if e.r.Dreply && w.lb != nil && w.lb.clientProposals != nil {
					//modified by Luna in 2018.12.20
					//the command is from a client
					if w.lb.clientProposals[idx].IsRepli == int32(-1) {
						ppgenericsmr.ReplyProposeTS(
                            e.r.Replica,
							&ppgenericsmrproto.ProposeReplyTS{
								TRUE,
								w.lb.clientProposals[idx].CommandId,
								val,
								w.lb.clientProposals[idx].Timestamp},
							e.r.ClientConn[w.lb.clientProposals[idx].Reply])
					} else { // the command is from a replica
						//log.Printf("exec:replica is %d,rpcCode is %d.\n", w.lb.clientProposals[idx].IsRepli, e.r.repliProposeReplyRPC)
						args := ppepaxosproto.RepliProposeReply{TRUE, w.lb.clientProposals[idx].CommandId, w.Cmds[idx].K, val, w.lb.clientProposals[idx].Timestamp, w.lb.clientProposals[idx].Reply}
						w := e.r.PeerWriters[w.lb.clientProposals[idx].IsRepli]
						w.WriteByte(e.r.repliProposeReplyRPC)
						args.Marshal(w)
						w.Flush()
					}
				}
			}
			w.Status = ppepaxosproto.EXECUTED
		}
		stack = stack[0:l]
	}
	return true
}

func (e *Exec) inStack(w *Instance) bool {
	for _, u := range stack {
		if w == u {
			return true
		}
	}
	return false
}

type nodeArray []*Instance

func (na nodeArray) Len() int {
	return len(na)
}

func (na nodeArray) Less(i, j int) bool {
	return na[i].Seq < na[j].Seq
}

func (na nodeArray) Swap(i, j int) {
	na[i], na[j] = na[j], na[i]
}
