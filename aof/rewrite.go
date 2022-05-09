package aof

import (
	"godis/config"
	"godis/constant"
	"godis/interface/database"
	"godis/lib/logger"
	"godis/lib/utils"
	"godis/redis/reply"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

func (h *Handler) newRewriteHandler() *Handler {
	h1 := &Handler{}
	h1.aofFilename = h.aofFilename
	h1.db = h.tmpDBMaker()
	return h1
}

// RewriteCtx holds context of an AOF rewriting procedure
type RewriteCtx struct {
	tmpFile  *os.File
	fileSize int64
	dbIdx    int // selected db index when start Rewrite
}

// Rewrite run AOF rewrite
func (h *Handler) Rewrite() {
	ctx, err := h.StartRewrite()
	if err != nil {
		logger.Warn(err)
		return
	}
	err = h.DoRewrite(ctx)
	if err != nil {
		logger.Error(err)
		return
	}

	h.FinishRewrite(ctx)
}

func (h *Handler) DoRewrite(ctx *RewriteCtx) error {
	tmpFile := ctx.tmpFile

	// load aof tmpFile
	tmpAof := h.newRewriteHandler()
	tmpAof.LoadAof(int(ctx.fileSize))

	// rewrite aof tmpFile
	for i := 0; i < config.Properties.Databases; i++ {
		// select db
		data := reply.MakeMultiBulkReply(utils.ToCmdLine(constant.Select, strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}
		// dump db
		tmpAof.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}

// StartRewrite prepare rewrite procedure
func (h *Handler) StartRewrite() (*RewriteCtx, error) {
	h.pausingAof.Lock() // pause aof
	defer h.pausingAof.Unlock()

	err := h.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// get current aof file size
	fileInfo, _ := os.Stat(h.aofFilename)
	filesize := fileInfo.Size()

	// create tmp file
	file, err := ioutil.TempFile("", "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
		dbIdx:    h.currentDB,
	}, nil
}

func (h *Handler) FinishRewrite(ctx *RewriteCtx) {
	h.pausingAof.Lock() // pausing aof
	defer h.pausingAof.Unlock()

	tmpFile := ctx.tmpFile
	// write commands executed during rewriting to tmp file
	src, err := os.Open(h.aofFilename)
	if err != nil {
		logger.Error("open aofFilename failed: " + err.Error())
		return
	}
	defer func() {
		_ = src.Close()
	}()

	_, err = src.Seek(ctx.fileSize, 0)
	if err != nil {
		logger.Error("seek failed: " + err.Error())
		return
	}

	// sync tmpFile's db index with online aofFile
	data := reply.MakeMultiBulkReply(utils.ToCmdLine(constant.Select, strconv.Itoa(ctx.dbIdx))).ToBytes()
	_, err = tmpFile.Write(data)
	if err != nil {
		logger.Error("tmp file rewrite failed: " + err.Error())
		return
	}

	// copy data
	_, err = io.Copy(tmpFile, src)
	if err != nil {
		logger.Error("copy aof file failed: " + err.Error())
		return
	}

	// replace current aof file by tmp file
	_ = h.aofFile.Close()
	_ = os.Rename(tmpFile.Name(), h.aofFilename)

	// reopen aof file for further write
	aofFile, err := os.OpenFile(h.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	h.aofFile = aofFile

	// reset selected db 重新写入一次 select 指令保证 aof 中的数据库与 h.currentDB 一致
	data = reply.MakeMultiBulkReply(utils.ToCmdLine(constant.Select, strconv.Itoa(h.currentDB))).ToBytes()
	_, err = h.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
