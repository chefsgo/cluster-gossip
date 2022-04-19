package cluster_gossip

import (
	"github.com/chefsgo/chef"
	"github.com/hashicorp/memberlist"
)

const (
	cmdAdd = "add"
	cmdDel = "del"
)

type update struct {
	Data chef.ClusterData `json:"d"`
}

type broadcast struct {
	msg    []byte
	notify chan<- struct{}
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

func getMemberState(state memberlist.NodeStateType) string {
	if state == memberlist.StateAlive {
		return "alive"
	} else if state == memberlist.StateSuspect {
		return "suspect"
	} else if state == memberlist.StateLeft {
		return "left"
	} else if state == memberlist.StateDead {
		return "dead"
	} else {
		return "unknown"
	}
}
