package aof

import (
	"godis/config"
	"godis/constant"
	"godis/interface/database"
	"godis/lib/logger"
	"godis/lib/utils"
	"godis/redis/connection"
	"godis/redis/parser"
	"godis/redis/protocol"
	"io"
	"os"
	"strconv"
	"sync"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const aofQueueSize = 1 << 16

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

// Handler receive msgs from channel and write to AOF file
type Handler struct {
	db          database.EmbedDB
	tmpDBMaker  func() database.EmbedDB
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	// aof goroutine will send msg to main goroutine through this channel
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.RWMutex
	currentDB  int
}

func NewAOFHandler(db database.EmbedDB, tmpDBMaker func() database.EmbedDB) (*Handler, error) {
	handler := &Handler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.db = db
	handler.tmpDBMaker = tmpDBMaker
	handler.LoadAof(0)
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofQueueSize)
	handler.aofFinished = make(chan struct{})
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// AddAof send command to aof goroutine through channel
func (h *Handler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && h.aofChan != nil {
		h.aofChan <- &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

// handleAof listen aof channel and write into file
func (h *Handler) handleAof() {
	// serial execution
	h.currentDB = 0
	for p := range h.aofChan {
		// prevent other goroutine from pausing aof
		h.pausingAof.RLock()
		if p.dbIndex != h.currentDB {
			// select db
			data := protocol.MakeMultiBulkReply(utils.ToCmdLine(constant.Select, strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := h.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue // skip the command
			}
			h.currentDB = p.dbIndex
		}
		data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := h.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}
		h.pausingAof.RUnlock()
	}
	h.aofFinished <- struct{}{}
}

// LoadAof read aof file
func (h *Handler) LoadAof(maxBytes int) {
	// delete aofChan prevent write again
	aofChan := h.aofChan
	h.aofChan = nil
	defer func(aofChan chan *payload) {
		h.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(h.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}

	ch := parser.ParseStream(reader)
	// only used for save dbIndex
	fakeConn := &connection.FakeConn{}
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		ret := h.db.Exec(fakeConn, r.Args)
		if protocol.IsErrorReply(ret) {
			logger.Error("exec err", err)
		}
	}
}

// Close stops aof persistence procedure
func (h *Handler) Close() {
	if h.aofFile != nil {
		close(h.aofChan)
		<-h.aofFinished // waiting for aof finish
		err := h.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
}
