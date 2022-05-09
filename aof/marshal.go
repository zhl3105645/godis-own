package aof

import (
	"godis/constant"
	"godis/dataStruct/dict"
	"godis/dataStruct/list"
	"godis/dataStruct/set"
	"godis/dataStruct/sortedset"
	"godis/interface/database"
	"godis/redis/reply"
	"strconv"
	"time"
)

var (
	setCmd         = []byte(constant.Set)
	rPushAllCmd    = []byte(constant.RPush)
	sAddCmd        = []byte(constant.SAdd)
	hMSetCmd       = []byte(constant.HMSet)
	zAddCmd        = []byte(constant.ZAdd)
	pExpireAtBytes = []byte(constant.PExpireAt)
)

// EntityToCmd serialize data entity to redis command
func EntityToCmd(key string, entity *database.DataEntity) *reply.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *reply.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToCmd(key, val)
	case *list.LinkedList:
		cmd = listToCmd(key, val)
	case *set.Set:
		cmd = setToCmd(key, val)
	case dict.Dict:
		cmd = hashToCmd(key, val)
	case *sortedset.SortedSet:
		cmd = zSetToCmd(key, val)
	}
	return cmd
}

func stringToCmd(key string, bytes []byte) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return reply.MakeMultiBulkReply(args)
}

func listToCmd(key string, list *list.LinkedList) *reply.MultiBulkReply {
	args := make([][]byte, 2+list.Len())
	args[0] = rPushAllCmd
	args[1] = []byte(key)
	list.ForEach(func(i int, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i] = bytes
		return true
	})
	return reply.MakeMultiBulkReply(args)
}

func setToCmd(key string, set *set.Set) *reply.MultiBulkReply {
	args := make([][]byte, 2+set.Len())
	args[0] = sAddCmd
	args[1] = []byte(key)
	i := 0
	set.ForEach(func(val string) bool {
		args[2+i] = []byte(val)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(args)
}

func hashToCmd(key string, hash dict.Dict) *reply.MultiBulkReply {
	args := make([][]byte, 2+hash.Len()*2)
	args[0] = hMSetCmd
	args[1] = []byte(key)
	i := 0
	hash.ForEach(func(key string, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+2*i] = []byte(key)
		args[3+2*i] = bytes
		i++
		return true
	})
	return reply.MakeMultiBulkReply(args)
}

func zSetToCmd(key string, zset *sortedset.SortedSet) *reply.MultiBulkReply {
	args := make([][]byte, 2+zset.Len()*2)
	args[0] = zAddCmd
	args[1] = []byte(key)
	i := 0
	zset.ForEach(int64(0), zset.Len(), true, func(element *sortedset.Element) bool {
		value := strconv.FormatFloat(element.Score, 'f', -1, 64)
		args[2+i*2] = []byte(value)
		args[3+i*2] = []byte(element.Member)
		i++
		return true
	})
	return reply.MakeMultiBulkReply(args)
}

// MakeExpireCmd generates command line to set expiration for the given key
func MakeExpireCmd(key string, expireAt time.Time) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return reply.MakeMultiBulkReply(args)
}
