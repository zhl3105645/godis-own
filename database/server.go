package database

import (
	"fmt"
	"godis/aof"
	"godis/config"
	"godis/constant"
	"godis/interface/database"
	"godis/interface/redis"
	"godis/lib/logger"
	"godis/lib/utils"
	"godis/pubsub"
	"godis/redis/reply"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// MultiDB is a set of multiple database set
type MultiDB struct {
	dbSet []*DB

	// handle publish/subscribe
	hub *pubsub.Hub
	// handle aof persistence
	aofHandler *aof.Handler
}

func NewStandaloneServer() *MultiDB {
	mdb := &MultiDB{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}
	mdb.hub = pubsub.MakeHub()
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(mdb, func() database.EmbedDB {
			return MakeBasicMultiDB()
		})
		if err != nil {
			panic(err)
		}
		mdb.aofHandler = aofHandler
		for _, db := range mdb.dbSet {
			// avoid closure
			singleDB := db
			singleDB.addAof = func(line CmdLine) {
				mdb.aofHandler.AddAof(singleDB.index, line)
			}
		}
	}
	return mdb
}

// MakeBasicMultiDB create a MultiDB only with basic abilities for aof rewrite and other usages
func MakeBasicMultiDB() *MultiDB {
	mdb := &MultiDB{}
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		mdb.dbSet[i] = makeBasicDB()
	}
	return mdb
}

// Exec executes command
func (m *MultiDB) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	// authenticate
	if cmdName == constant.Auth {
		return Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return reply.MakeErrReply("NOAUTH Authentication required")
	}

	// special commands
	if cmdName == constant.Subscribe {
		if len(cmdLine) < 2 {
			return reply.MakeArgNumErrReply(constant.Subscribe)
		}
		return pubsub.Subscribe(m.hub, c, cmdLine[1:])
	} else if cmdName == constant.Publish {
		return pubsub.Publish(m.hub, cmdLine[1:])
	} else if cmdName == constant.UnSubscribe {
		return pubsub.UnSubscribe(m.hub, c, cmdLine[1:])
	} else if cmdName == constant.BgRewriteAof {
		//TODO import问题
		return BGRewriteAOF(m, cmdLine[1:])
	} else if cmdName == constant.RewriteAof {
		return RewriteAOF(m, cmdLine[1:])
	} else if cmdName == constant.FlushAll {
		return m.flushAll()
	} else if cmdName == constant.Select {
		if c != nil && c.InMultiState() {
			return reply.MakeErrReply("cannot select database within multi")
		}
		if len(cmdLine) != 2 {
			return reply.MakeArgNumErrReply(constant.Select)
		}
		return execSelect(c, m, cmdLine[1:])
	}
	// TODO: support multi database transaction

	// normal command
	dbIndex := c.GetDBIndex()
	if dbIndex >= len(m.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	selectedDB := m.dbSet[dbIndex]
	return selectedDB.Exec(c, cmdLine)
}

// AfterClientClose does some clean after client close connection
func (m *MultiDB) AfterClientClose(c redis.Connection) {
	pubsub.UnsubscribeAll(m.hub, c)
}

// Close shutdown database
func (m *MultiDB) Close() {
	if m.aofHandler != nil {
		m.aofHandler.Close()
	}
}

// ExecWithLock executes normal commands, invoker should provide locks
func (m *MultiDB) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	if conn.GetDBIndex() >= len(m.dbSet) {
		panic("ERR DB index is out of range")
	}
	db := m.dbSet[conn.GetDBIndex()]
	return db.execWithLock(cmdLine)
}

// ExecMulti executes multi commands transaction Atomically and Isolated
func (m *MultiDB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []database.CmdLine) redis.Reply {
	if conn.GetDBIndex() >= len(m.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	db := m.dbSet[conn.GetDBIndex()]
	return db.ExecMulti(conn, watching, cmdLines)
}

// GetUndoLogs return rollback commands
func (m *MultiDB) GetUndoLogs(dbIndex int, cmdLine [][]byte) []database.CmdLine {
	if dbIndex >= len(m.dbSet) {
		panic("ERR DB index is out of range")
	}
	db := m.dbSet[dbIndex]
	return db.GetUndoLogs(cmdLine)
}

// ForEach traverses all the keys in the given database
func (m *MultiDB) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	if dbIndex >= len(m.dbSet) {
		return
	}
	db := m.dbSet[dbIndex]
	db.ForEach(cb)
}

// RWLocks lock keys for writing and reading
func (m *MultiDB) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	if dbIndex >= len(m.dbSet) {
		panic("ERR DB index is out of range")
	}
	db := m.dbSet[dbIndex]
	db.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (m *MultiDB) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	if dbIndex >= len(m.dbSet) {
		panic("ERR DB index is out of range")
	}
	db := m.dbSet[dbIndex]
	db.RWUnLocks(writeKeys, readKeys)
}

func (m *MultiDB) flushAll() redis.Reply {
	for _, db := range m.dbSet {
		db.Flush()
	}
	if m.aofHandler != nil {
		m.aofHandler.AddAof(0, utils.ToCmdLine("FlushAll"))
	}
	return &reply.OkReply{}
}

func execSelect(c redis.Connection, mdb *MultiDB, args [][]byte) redis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}

// BGRewriteAOF asynchronously rewrites Append-Only-File
func BGRewriteAOF(db *MultiDB, args [][]byte) redis.Reply {
	go db.aofHandler.Rewrite()
	return reply.MakeStatusReply("Background append only file rewriting started")
}

// RewriteAOF start Append-Only-File rewriting and blocked until it finished
func RewriteAOF(db *MultiDB, args [][]byte) redis.Reply {
	db.aofHandler.Rewrite()
	return reply.MakeStatusReply("Background append only file rewriting started")
}
