package cluster_gossip

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/chefsgo/chef"

	// "github.com/chefsgo/util"

	"github.com/hashicorp/memberlist"
)

type (
	gossipClusterDriver struct {
	}
	gossipClusterConnect struct {
		mutex   sync.RWMutex
		config  chef.ClusterConfig
		setting gossipClusterSetting

		local  string
		writed bool

		data chef.ClusterData

		// hashring *util.HashRing

		client     *memberlist.Memberlist
		broadcasts *memberlist.TransmitLimitedQueue
	}
	gossipClusterSetting struct {
		Mode string
	}
	gossipClusterMutex struct {
		Expiry time.Time
	}
)

func (driver *gossipClusterDriver) Connect(config chef.ClusterConfig) (chef.ClusterConnect, error) {

	setting := gossipClusterSetting{
		Mode: "lan",
	}

	if mode, ok := config.Setting["mode"].(string); ok {
		setting.Mode = mode
	}

	return &gossipClusterConnect{
		config: config, setting: setting,
		data: make(chef.ClusterData, 0),
	}, nil
}

//打开连接
func (connect *gossipClusterConnect) Open() error {
	// weights := make(map[string]int, 0)
	// connect.hashring = util.NewHashRing(weights)

	var cfg *memberlist.Config
	if connect.setting.Mode == "wan" {
		cfg = memberlist.DefaultWANConfig()
	} else if connect.setting.Mode == "lan" {
		cfg = memberlist.DefaultLANConfig()
	} else {
		cfg = memberlist.DefaultLocalConfig()
	}

	cfg.Name = connect.config.Name
	cfg.BindPort = connect.config.Port
	if connect.config.Host != "" {
		cfg.BindAddr = connect.config.Host
	}
	cfg.Events = connect
	cfg.Delegate = connect
	cfg.LogOutput = io.Discard

	client, err := memberlist.Create(cfg)
	if err != nil {
		return err
	}

	if connect.config.Join != nil && len(connect.config.Join) > 0 {
		_, err := client.Join(connect.config.Join)
		if err != nil {
			return err
		}
	}

	connect.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return client.NumMembers()
		},
		RetransmitMult: 3,
	}

	connect.client = client

	return nil
}

//关闭连接
func (connect *gossipClusterConnect) Close() error {
	if connect.client != nil {
		connect.client.Leave(time.Second)
	}
	return nil
}

func (connect *gossipClusterConnect) Locate(key string) bool {
	// connect.mutex.RLock()
	// defer connect.mutex.RUnlock()

	// locate := connect.hashring.Locate(key)
	// if locate == connect.local {
	// 	return true
	// }

	return false
}

func (connect *gossipClusterConnect) Read(key string) ([]byte, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()

	if val, ok := connect.data[key]; ok {
		return val, nil
	}

	return nil, errors.New("read error")
}
func (connect *gossipClusterConnect) Write(key string, val []byte) error {
	data := chef.ClusterData{key: val}

	// 待处理，优化成框架内置的JSON方法
	// bytes, err := chef.JsonEncode(&data)
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	//通知自己，因为广播的时候，不包括自己
	connect.NotifyMsg(bytes)

	connect.broadcasts.QueueBroadcast(&broadcast{
		msg: bytes, notify: nil,
	})

	return nil
}
func (connect *gossipClusterConnect) Delete(key string) error {
	data := chef.ClusterData{key: nil}

	// 待处理，优化成框架内置的JSON方法
	// bytes, err := chef.JsonEncode(&data)
	bytes, err := json.Marshal(&data)
	if err != nil {
		return err
	}

	//通知自己
	connect.NotifyMsg(bytes)

	connect.broadcasts.QueueBroadcast(&broadcast{
		msg: bytes, notify: nil,
	})

	return nil
}

// Clear 待优化，成只发送命令，各节点自行处理
func (connect *gossipClusterConnect) Clear(prefix string) error {
	data := make(chef.ClusterData, 0)

	for key, _ := range connect.data {
		if strings.HasPrefix(key, prefix) {
			data[key] = nil
		}
	}

	// 待处理，优化成框架内置的JSON方法
	// bytes, err := chef.JsonEncode(&data)
	bytes, err := json.Marshal(&data)
	if err != nil {
		return err
	}

	//通知自己
	connect.NotifyMsg(bytes)
	connect.broadcasts.QueueBroadcast(&broadcast{
		msg: bytes, notify: nil,
	})

	return nil
}

func (connect *gossipClusterConnect) Batch(data chef.ClusterData) error {
	//待处理
	// bytes, err := chef.JsonEncode(&data)
	bytes, err := json.Marshal(&data)
	if err != nil {
		return err
	}

	//通知自己
	connect.NotifyMsg(bytes)
	connect.broadcasts.QueueBroadcast(&broadcast{
		msg: bytes, notify: nil,
	})

	return nil
}
func (connect *gossipClusterConnect) Fetch(prefix string) (chef.ClusterData, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()

	data := make(chef.ClusterData, 0)

	for key, val := range connect.data {
		if strings.HasPrefix(key, prefix) {
			data[key] = val
		}
	}

	return data, nil
}

func (connect *gossipClusterConnect) Peers() []chef.ClusterPeer {
	peers := make([]chef.ClusterPeer, 0)

	members := connect.client.Members()
	for _, member := range members {
		peers = append(peers, chef.ClusterPeer{
			Name:  member.Name,
			Host:  member.Addr.String(),
			Port:  int(member.Port),
			Meta:  member.Meta,
			State: getMemberState(member.State),
		})
	}

	return peers
}

//----以上为事件
func (connect *gossipClusterConnect) NotifyJoin(node *memberlist.Node) {
	// group := getNodeGroup(node.Name)
	// if connect.config.Group == group {
	// 	connect.mutex.Lock()
	// 	connect.hashring.Append(node.String(), 1)
	// 	connect.mutex.Unlock()
	// }

	chef.Info(node.Name, node.Address(), "加入集群")
}

func (connect *gossipClusterConnect) NotifyLeave(node *memberlist.Node) {
	// group := getNodeGroup(node.Name)
	// if connect.config.Group == group {
	// 	connect.mutex.Lock()
	// 	connect.hashring.Remove(node.Name)
	// 	connect.mutex.Unlock()
	// }

	chef.Info(node.Name, node.Address(), "离开集群")
}

func (connect *gossipClusterConnect) NotifyUpdate(node *memberlist.Node) {
	chef.Info(node.Name, node.Address(), "节点更新")
}

//-------以下为代理方法
func (connect *gossipClusterConnect) NodeMeta(limit int) []byte {
	return []byte{}
}

func (connect *gossipClusterConnect) NotifyMsg(b []byte) {
	if len(b) == 0 {
		return
	}

	var data chef.ClusterData
	//待处理，优化成统一的JSON方法
	// if err := chef.JsonDecode(b, &data); err != nil {
	if err := json.Unmarshal(b, &data); err != nil {
		return
	}

	connect.mutex.Lock()
	for k, v := range connect.data {
		if v == nil {
			delete(connect.data, k)
		} else {
			connect.data[k] = v
		}
	}
	connect.mutex.Unlock()
}

func (connect *gossipClusterConnect) GetBroadcasts(overhead, limit int) [][]byte {
	return connect.broadcasts.GetBroadcasts(overhead, limit)
}
func (connect *gossipClusterConnect) LocalState(join bool) []byte {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()

	//待处理
	// bytes, _ := chef.JsonEncode(data)
	bytes, err := json.Marshal(connect.data)
	if err != nil {
		return []byte{}
	}

	return bytes
}
func (connect *gossipClusterConnect) MergeRemoteState(buf []byte, join bool) {
	if false == join || nil == buf || 0 == len(buf) {
		return
	}

	//直接通知本地
	connect.NotifyMsg(buf)
}
