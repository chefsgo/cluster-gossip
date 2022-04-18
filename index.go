package cluster_gossip

import (
	"github.com/chef-go/chef"
)

func Driver() chef.ClusterDriver {
	return &gossipClusterDriver{}
}

func init() {
	chef.Register("gossip", Driver())
}
