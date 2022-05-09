package constant

// command related sys
const (
	Ping = "ping"
	Auth = "auth"
)

// command related keys
const (
	Del       = "del"
	Expire    = "expire"
	ExpireAt  = "expireat"
	PExpire   = "pexpire"
	PExpireAt = "pexpireat"
	Ttl       = "ttl"
	PTtl      = "pttl"
	Persist   = "persist"
	Exists    = "exists"
	Type      = "type"
	Rename    = "rename"
	RenameNx  = "renamenx"
)

// command related server
const (
	FlushDb      = "flushdb"
	FlushAll     = "flushall"
	Keys         = "keys"
	BgRewriteAof = "bgrewriteaof"
	RewriteAof   = "rewriteaof"
	Select       = "select"
)

// command related String
const (
	Set         = "set"
	SetNx       = "setnx"
	SetEx       = "setex"
	PSetEx      = "psetex"
	MSet        = "mset"
	MGet        = "mget"
	MSetNx      = "msetnx"
	Get         = "get"
	GetSet      = "getset"
	Incr        = "incr"
	IncrBy      = "incrby"
	IncrByFloat = "incrbyfloat"
	Decr        = "decr"
	DecrBy      = "decrby"
	Append      = "append"
	SetRange    = "setrange"
	GetRange    = "getrange"
	StrLen      = "strlen"
)

// command related List
const (
	LPush     = "lpush"
	LPushX    = "lpushx"
	RPush     = "rpush"
	RPushX    = "rpushx"
	LPop      = "lpop"
	RPop      = "rpop"
	RPopLPush = "rpoplpush"
	LRem      = "lrem"
	LLen      = "llen"
	LIndex    = "lindex"
	LSet      = "lset"
	LRange    = "lrange"
)

// command related Hash
const (
	HSet         = "hset"
	HSetNx       = "hsetnx"
	HGet         = "hget"
	HExists      = "hexists"
	HDel         = "hdel"
	HLen         = "hlen"
	HMGet        = "hmget"
	HMSet        = "hmset"
	HKeys        = "hkeys"
	HVals        = "hvals"
	HGetAll      = "hgetall"
	HIncrBy      = "hincrby"
	HIncrByFloat = "hincrbyfloat"
)

// command related Set
const (
	SAdd        = "sadd"
	SIsMember   = "sismember"
	SRem        = "srem"
	SCard       = "scard"
	SMembers    = "smembers"
	SInter      = "sinter"
	SInterStore = "sinterstore"
	SUnion      = "sunion"
	SUnionStore = "sunionstore"
	SDiff       = "sdiff"
	SDiffStore  = "sdiffstore"
	SRandMember = "srandmember"
)

// command related SortedSet
const (
	ZAdd             = "zadd"
	ZScore           = "zscore"
	ZIncrBy          = "zincrby"
	ZRank            = "zrank"
	ZCount           = "zcount"
	ZRevRank         = "zrevrank"
	ZCard            = "zcard"
	ZRange           = "zrange"
	ZRevRange        = "zrevrange"
	ZRangeByScore    = "zrangebyscore"
	ZRevRangeByScore = "zrevrangebyscore"
	ZRem             = "zrem"
	ZRemRangeByScore = "zremrangebyscore"
	ZRemRangeByRank  = "zremrangebyrank"
)

// command related Pub/Sub
const (
	Publish     = "publish"
	Subscribe   = "subscribe"
	UnSubscribe = "unsubscribe"
)

// command related Geo
const (
	GeoAdd            = "GeoAdd"
	GeoPos            = "GeoPos"
	GeoDist           = "GeoDist"
	GeoHash           = "GeoHash"
	GeoRadius         = "GeoRadius"
	GeoRadiusByMember = "GeoRadiusByMember"
)
