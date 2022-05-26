package cluster

import (
	"context"
	"fmt"
	"github.com/jolestar/go-commons-pool/v2"
	"godis/config"
	"godis/dataStruct/dict"
	database2 "godis/database"
	"godis/interface/database"
	"godis/interface/redis"
	"godis/lib/consistenthash"
	"godis/lib/idgenerator"
	"godis/lib/logger"
	"godis/redis/protocol"
	"runtime/debug"
	"strings"
)

type PeerPicker interface {
	AddNode(keys ...string)
	PickNode(key string) string
}

// Cluster represents a node of godis cluster
// it holds part of data and coordinates other nodes to finish transactions
type Cluster struct {
	self string

	nodes          []string
	peerPicker     PeerPicker
	peerConnection map[string]*pool.ObjectPool

	db           database.EmbedDB
	transactions *dict.SimpleDict // id -> Transaction

	idGenerator *idgenerator.IDGenerator
	// use a variable to allow injecting stub for testing
	relayImpl func(cluster *Cluster, node string, c redis.Connection, cmdLine CmdLine) redis.Reply
}

const (
	replicas = 4
)

// if only one node involved in a transaction, just execute the command don't apply tcc procedure
var allowFastTransaction = true

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply

// MakeCluster creates and starts a node of cluster
func MakeCluster() *Cluster {
	cluster := &Cluster{
		self: config.Properties.Self,

		db:             database2.NewStandaloneServer(),
		transactions:   dict.MakeSimple(),
		peerPicker:     consistenthash.New(replicas, nil),
		peerConnection: make(map[string]*pool.ObjectPool),

		idGenerator: idgenerator.MakeGenerator(config.Properties.Self),
		relayImpl:   defaultRelayImpl,
	}
	contains := make(map[string]struct{})
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers {
		if _, ok := contains[peer]; ok {
			continue
		}
		contains[peer] = struct{}{}
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	cluster.peerPicker.AddNode(nodes...)
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	cluster.nodes = nodes
	return cluster
}

// Close stops current node of cluster
func (cluster *Cluster) Close() {
	cluster.db.Close()
}

var router = makeRouter()

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

// Exec executes command on cluster
func (cluster *Cluster) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "auth" {
		return database2.Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}

	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return database2.StartMulti(c)
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return database2.DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(cluster, c, nil)
	} else if cmdName == "select" {
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execSelect(c, cmdLine)
	}
	if c != nil && c.InMultiState() {
		return database2.EnqueueCmd(c, cmdLine)
	}
	cmdFunc, ok := router[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, c, cmdLine)
	return
}

// AfterClientClose does some clean after client close connection
func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	cluster.db.AfterClientClose(c)
}
