package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type CmdType = byte

const (
	COMMAND_UNKNOWN CmdType = 0x00
	COMMAND_INLINE  CmdType = 0x01
	COMMAND_BULK    CmdType = 0x02
)

const (
	GODIS_IO_BUF     int = 1024 * 16
	GODIS_MAX_BULK   int = 1024 * 4
	GODIS_MAX_INLINE int = 1024 * 4
)

const (
	GODIS_OK  int8 = 0
	GODIS_ERR int8 = -1
)
const (
	LIST_HEAD int8 = 1
	LIST_TAIL int8 = 2
)
const (
	CMD_WRITE int = 1 << iota
	CMD_READ
	CMD_OTHER
)

// CRLF 是 redis 统一的行分隔符协议
const CRLF = "\r\n"

type GodisDB struct {
	data   *Dict
	expire *Dict
}

type GodisServer struct {
	fd             int
	port           int
	db             *GodisDB
	clients        map[int]*GodisClient
	aeLoop         *AeLoop
	dirty          int64
	bgsavechildpid int
	appendonly     int
	lastfsync      int64
	appendfd       *os.File
	appendfsync    string
	appendfilename string
	lastsave       int64
	saveparams     *saveparam
	saveparamslen  int
	dbfilename     string
	bgrewritebuf   string /* buffer taken by parent during oppend only rewrite */
	aofbuf         string /* AOF buffer, written before entering the event loop */
}

type GodisClient struct {
	fd       int
	db       *GodisDB
	args     []*Gobj
	reply    *List
	sentLen  int
	queryBuf []byte
	queryLen int
	cmdType  CmdType
	bulkNum  int
	bulkLen  int
}

type CommandProc func(c *GodisClient)

// do not support bulk command
type GodisCommand struct {
	name  string
	proc  CommandProc
	arity int
	flags int
}

// Global Varibles
var server GodisServer
var cmdTable = []GodisCommand{

	{"expireat", expireAtCommand, 3, CMD_WRITE},
	{"expire", expireCommand, 3, CMD_WRITE},

	{"del", delCommand, -2, CMD_WRITE},

	//string
	{"get", getCommand, 2, CMD_READ},
	{"set", setCommand, 3, CMD_WRITE},
	{"mget", mgetCommand, -2, CMD_READ},
	{"mset", msetCommand, -3, CMD_WRITE},
	{"msetnx", msetnxCommand, -4, CMD_WRITE},
	{"setnx", setnxCommand, 3, CMD_WRITE},
	{"setex", setexCommand, 4, CMD_WRITE},

	// list
	{"rpush", rpushCommand, -3, CMD_WRITE},
	{"lpush", lpushCommand, -3, CMD_WRITE},
	{"rpop", rpopCommand, 2, CMD_WRITE},
	{"lpop", lpopCommand, 2, CMD_WRITE},
	{"lrange", lrangeCommand, 4, CMD_READ},
	{"lindex", lindexCommand, 3, CMD_READ},
	{"llen", llenCommand, 2, CMD_READ},
	{"lrem", lremCommand, 4, CMD_WRITE},

	// set
	{"sadd", saddCommand, -3, CMD_WRITE},
	{"srem", sremCommand, -3, CMD_WRITE},
	{"sismember", sismemberCommand, 3, CMD_READ},
	{"smembers", smembersCommand, 2, CMD_READ},
	{"scard", scardCommand, 2, CMD_READ},

	// hash
	{"hset", hsetCommand, -4, CMD_WRITE},
	{"hsetnx", hsetnxCommand, 4, CMD_WRITE},
	{"hkeys", hkeysCommand, 2, CMD_READ},
	{"hvals", hvalsCommand, 2, CMD_READ},
	{"hget", hgetCommand, 3, CMD_READ},
	{"hdel", hdelCommand, -3, CMD_WRITE},

	//zset
	{"zadd", zaddCommand, -4, CMD_WRITE},
	{"zincr", zincrbyCommand, -4, CMD_WRITE},
	{"zrem", zremCommand, -3, CMD_WRITE},
	{"zscore", zscoreCommand, 3, CMD_READ},
	{"zcard", zcardCommand, 2, CMD_READ},
	{"zrank", zrankCommand, 3, CMD_READ},
	{"zrevrank", zrevrankCommand, 3, CMD_READ},
	{"zpopmin", zpopminCommand, -2, CMD_WRITE},
	{"zpopmax", zpopmaxCommand, -2, CMD_WRITE},

	// TODO LIMIT：分页参数（类似 SQL 的 LIMIT offset, count）。
	{"zrange", zrangeCommand, -4, CMD_READ},
	{"zrevrange", zrevrangeCommand, -4, CMD_READ},
	{"zrangebyscore", zrangebyscoreCommand, -4, CMD_READ},
	{"zrevrangebyscore", zrevrangebyscoreCommand, -4, CMD_READ},

	{"incr", incrCommand, 2, CMD_WRITE},
	{"decr", decrCommand, 2, CMD_WRITE},
	{"keys", keysCommand, 2, CMD_READ},

	//persist
	{"save", saveCommand, 1, CMD_OTHER},
	{"bgsave", bgsaveCommand, 1, CMD_OTHER},
	{"bgrewriteaof", bgrewriteaofCommand, 1, CMD_OTHER},

	{"info", infoCommand, 2, CMD_OTHER},

	{"hello", helloCommand, 2, CMD_OTHER},

	//兼容 redis-benchmark
	{"config", configCommand, -1, CMD_OTHER},
	{"ping", pingCommand, 1, CMD_OTHER},
	/*
		redis-benchmark -p 6767 -t set,get,lpush,rpush,del,setnx,setex,rpop,lpop,lrange,lindex,llen,lrem,sadd,srem,sismember,smembers,scard,hset,hsetnx,hkeys,hvals,hget,hdel
		redis-benchmark -t set,get,lpush,rpush,del,setnx,setex,rpop,lpop,lrange,lindex,llen,lrem,sadd,srem,sismember,smembers,scard,hset,hsetnx,hkeys,hvals,hget,hdel
	*/
}

func keysCommand(c *GodisClient) {
	// TODO 模式匹配,目前就支持输出所有的 key
	count := server.db.data.usedSize()
	iter := server.db.data.NewIterator(true) // 内层安全迭代器
	c.AddReplyArrayLen(count)
	if count == 0 {
		return
	}
	reply := strings.Builder{}
	for key, _, exists := iter.Next(); exists; key, _, exists = iter.Next() {
		val := key.StrVal()
		reply.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
	}
	c.AddReplyStr(reply.String())
}

func pingCommand(c *GodisClient) {
	c.AddReplyStr("+PONG\r\n")
}
func configCommand(c *GodisClient) {
	if c.args[1].StrVal() == "GET" {
		switch c.args[2].StrVal() {
		case "save":
			c.AddReplyStr("*2\r\n$4\r\nsave\r\n$23\r\n3600 1 300 100 60 10000\r\n")
		case "appendonly":
			c.AddReplyStr("*2\r\n$10\r\nappendonly\r\n$2\r\nno\r\n")
		default:
			c.AddReplyError("Unknown CONFIG option")
		}
	} else if c.args[1].StrVal() == "SET" {
		return
	}
}
func infoCommand(c *GodisClient) {
	if c.args[1].StrVal() != "memory" {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	info := fmt.Sprintf(
		"# Memory\r\nused_memory:%d b %.2f kb %.2f MiB %.2f GB\r\n",
		m.Alloc,
		float64(m.Alloc)/1024,
		float64(m.Alloc)/1024/1024,
		float64(m.Alloc)/1024/1024/1024,
	)
	c.AddReplyStr(info)
}

func helloCommand(c *GodisClient) {
	// 1. 构造服务器信息
	fields := []struct {
		k string
		v interface{}
	}{
		{"server", "godis"},
		{"version", "0.1"},
		{"proto", 3},
		{"id", 1},
		{"mode", "standalone"},
		{"role", "master"},
		{"modules", []string{}},
	}

	// 2. RESP3: 返回数组，每个元素是键值对
	c.AddReplyStr(fmt.Sprintf("*%d\r\n", len(fields)*2))
	for _, field := range fields {
		// 键
		c.AddReplyStr(fmt.Sprintf("$%d\r\n%s\r\n", len(field.k), field.k))
		// 值
		switch v := field.v.(type) {
		case string:
			c.AddReplyStr(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
		case int:
			c.AddReplyStr(fmt.Sprintf(":%d\r\n", v))
		case []string:
			c.AddReplyStr(fmt.Sprintf("*%d\r\n", len(v)))
			for _, item := range v {
				c.AddReplyStr(fmt.Sprintf("$%d\r\n%s\r\n", len(item), item))
			}
		default:
			c.AddReplyError("Unsupported value type")
			return
		}
	}
}

func incrDecrCommand(c *GodisClient, isIncr bool) {
	key := c.args[1]
	obj := lookupKeyWrite(key)
	if obj == nil {
		obj = CreateObject(GSTR, int64(0))
		err := server.db.data.Add(key, obj)
		if err != nil {
			return
		}
	}
	// 自增操作
	if isIncr {
		obj.Val_ = obj.Val_.(int64) + 1
	} else {
		obj.Val_ = obj.Val_.(int64) - 1
	}
	c.AddReplyStr(fmt.Sprintf(":%d\r\n", obj.Val_.(int64)))
}

func incrCommand(c *GodisClient) {
	incrDecrCommand(c, true)
}
func decrCommand(c *GodisClient) {
	incrDecrCommand(c, false)
}
func zaddCommand(c *GodisClient) {
	// zaddGenericCommand(c, c.args[1], c.args[3], 0, false)
	zaddGenericCommand_(c, ZADD_IN_NONE)
}
func zincrbyCommand(c *GodisClient) {
	zaddGenericCommand_(c, ZADD_IN_INCR)
}

func zpopmaxCommand(c *GodisClient) {
	zpopGenericCommand(c, true)
}

func zpopminCommand(c *GodisClient) {
	zpopGenericCommand(c, false)
}
func zpopGenericCommand(c *GodisClient, max bool) {
	key := c.args[1]
	zsetObj := lookupKeyWrite(key)
	if zsetObj == nil {
		c.AddReplyInt(-1)
		return
	} else if zsetObj.Type_ != GZSET {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	count := int64(-1)
	if len(c.args) > 2 {
		c.getLongFromObjectOrReply(c.args[2], &count)
	}
	if count < 0 {
		count = 1
	}
	zslist := zsetObj.Val_.(zset).zsl
	pop_len := count
	for pop_len > 0 {
		var zslnode *zskiplistNode
		if max {
			zslnode = zslist.header.level[0].forward
		} else {
			zslnode = zslist.tail
		}
		member := zslnode.obj
		score := zslnode.score
		zsetObj.Val_.(zset).ZsetDeleteElement(member)
		// 构建回复
		var reply strings.Builder
		if pop_len == count {
			reply.WriteString(fmt.Sprintf("*%d"+CRLF, count*2))
		}
		val := member.StrVal()
		reply.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
		c.AddReplyStr(reply.String())
		c.AddReplyDouble(score)
		pop_len--
	}
}
func zrankGenericCommand(c *GodisClient, reverse bool) {
	key := c.args[1]
	zsetObj := findKeyRead(key)
	if zsetObj == nil {
		c.AddReplyInt(-1)
		return
	} else if zsetObj.Type_ != GZSET {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	member := c.args[2]
	rank := zsetObj.Val_.(zset).zsetRank(member)
	if reverse {
		rank = zsetObj.Val_.(zset).zsl.length - rank
	}
	c.AddReplyLong(int64(rank))
}
func zrankCommand(c *GodisClient) {
	zrankGenericCommand(c, false)
}
func zrevrankCommand(c *GodisClient) {
	zrankGenericCommand(c, true)
}
func zremCommand(c *GodisClient) {
	key := c.args[1]
	zsetObj := lookupKeyWrite(key)
	deleted := 0
	for i := 2; i < len(c.args); i++ {
		member := c.args[i]
		zset_ := zsetObj.Val_.(zset)
		if zset_.zsl.length == 0 {
			server.db.data.Delete(key)
			c.AddReplyInt(deleted)
			return
		}
		zsetObj.Val_.(zset).ZsetDeleteElement(member)
		deleted++
	}
	c.AddReplyInt(deleted)
}

func zrevrangebyscoreCommand(c *GodisClient) {
	zrangebyscoreGenericCommand(c, true)
}

func zrangebyscoreCommand(c *GodisClient) {
	zrangebyscoreGenericCommand(c, false)
}

func zrangebyscoreGenericCommand(c *GodisClient, reverse bool) {
	key := c.args[1]
	start, _ := strconv.ParseFloat(c.args[2].StrVal(), 64)
	end, _ := strconv.ParseFloat(c.args[3].StrVal(), 64)
	withscores := false
	argc := len(c.args)
	// TODO limi
	if argc == 5 && c.args[4].StrVal() == "withscores" {
		withscores = true
	} else if argc > 5 {
		c.AddReplyError("ERR syntax error")
		return
	}
	zsetObj := findKeyRead(key)
	if zsetObj == nil {
		c.AddReplyArrayLen(0)
		return
	} else if zsetObj.Type_ != GZSET {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	range_len := int64(0)
	reply := strings.Builder{}
	zslNode := zsetObj.Val_.(zset).zsl.zslGetElementScore(start)
	for zslNode != nil && zslNode.score <= end {
		range_len++
		member := zslNode.obj
		str := member.StrVal()
		reply.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(str), str))
		if withscores {
			reply.WriteString(fmt.Sprintf("$%d\r\n%f\r\n", len(str), zslNode.score))
		}
		zslNode = zslNode.level[0].forward
	}
	c.AddReplyArrayLen(range_len)
	if range_len > 0 {
		c.AddReplyStr(reply.String())
	}
}

func zrevrangeCommand(c *GodisClient) {
	zrangeGenericCommand(c, true)
}

func zrangeCommand(c *GodisClient) {
	zrangeGenericCommand(c, false)
}

func zrangeGenericCommand(c *GodisClient, reverse bool) {
	key := c.args[1]
	var start, end int64
	c.getLongFromObjectOrReply(c.args[2], &start)
	c.getLongFromObjectOrReply(c.args[3], &end)
	withscores := false
	argc := len(c.args)
	if argc == 5 && c.args[4].StrVal() == "withscores" {
		withscores = true
	} else if argc > 5 {
		c.AddReplyError("ERR syntax error")
		return
	}

	zsetObj := findKeyRead(key)
	if zsetObj == nil {
		c.AddReplyArrayLen(0)
		return
	} else if zsetObj.Type_ != GZSET {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	zslen := zsetObj.Val_.(zset).zsl.length
	if start < 0 {
		start = int64(zslen) + start
	}
	if end < 0 {
		end = int64(zslen) + end
	}
	if start < 0 {
		start = 0
	}

	if start > int64(zslen)-1 {
		c.AddReplyArrayLen(0)
		return
	}
	if end > int64(zslen)-1 {
		end = int64(zslen) - 1
	}
	var zslNode *zskiplistNode
	if reverse {
		if start == 0 {
			zslNode = zsetObj.Val_.(zset).zsl.tail
		} else {
			zslNode = zsetObj.Val_.(zset).zsl.zslGetElementByRank(int64(zslen) - start)
		}
	} else {
		if start == 0 {
			zslNode = zsetObj.Val_.(zset).zsl.header.level[0].forward
		} else {
			zslNode = zsetObj.Val_.(zset).zsl.zslGetElementByRank(start + 1)
		}
	}
	c.AddReplyArrayLen(end - start + 1)
	for i := start; i <= end; i++ {
		member := zslNode.obj
		c.AddReplyBulk(member)
		if withscores {
			c.AddReplyDouble(zslNode.score)
		}
		zslNode = zslNode.level[0].forward
	}
}

func zscoreCommand(c *GodisClient) {
	key := c.args[1]
	zsetObj := findKeyRead(key)
	if zsetObj == nil {
		c.AddReplyInt(-1)
		return
	} else if zsetObj.Type_ != GZSET {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	resultScore := zsetObj.Val_.(zset).dict.Find(c.args[2]).Value.Val_.(float64)
	c.AddReplyDouble(resultScore)
}

func zcardCommand(c *GodisClient) {
	key := c.args[1]
	zsetObj := findKeyRead(key)
	if zsetObj == nil {
		c.AddReplyInt(0)
		return
	} else if zsetObj.Type_ != GZSET {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	length := zsetObj.Val_.(zset).zsl.length
	c.AddReplyLong(int64(length))
}

const (
	ZADD_IN_NONE = 0      // 无特殊标志
	ZADD_IN_INCR = 1 << 0 // 增加分数而不是设置它
	ZADD_IN_NX   = 1 << 1 // 不修改已存在的元素
	ZADD_IN_XX   = 1 << 2 // 只修改已存在的元素
	ZADD_IN_GT   = 1 << 3 // 仅在新分数更高时更新
	ZADD_IN_LT   = 1 << 4 // 仅在新分数更低时更新
)

func zaddGenericCommand_(c *GodisClient, flag int) {
	key := c.args[1]
	scoreIdx := 2
	ch := 0
	for scoreIdx < len(c.args) {
		opt := c.args[scoreIdx].Val_.(string)
		if opt == "NX" || opt == "nx" {
			flag |= ZADD_IN_NX
		} else if opt == "XX" || opt == "xx" {
			flag |= ZADD_IN_XX
		} else if opt == "GT" || opt == "gt" {
			flag |= ZADD_IN_GT
		} else if opt == "LT" || opt == "lt" {
			flag |= ZADD_IN_LT
		} else if opt == "CH" || opt == "ch" {
			ch = 1
		} else if opt == "INCR" || opt == "incr" {
			flag |= ZADD_IN_INCR
		} else {
			break
		}
		scoreIdx++
	}

	// 将标志位转换为易于检查的布尔变量
	incr := (flag & ZADD_IN_INCR) != 0
	nx := (flag & ZADD_IN_NX) != 0
	xx := (flag & ZADD_IN_XX) != 0
	gt := (flag & ZADD_IN_GT) != 0
	lt := (flag & ZADD_IN_LT) != 0

	// 解析选项后，参数数量应为偶数，
	// 因为之后应该是成对的分数和成员值

	elements := len(c.args) - scoreIdx
	if elements%2 != 0 || elements == 0 {
		c.AddReplyError("ZADD: wrong number of arguments")
		return
	}
	elements /= 2 // 当前变量存储的是 分数-成员 对的数量

	if nx && xx {
		c.AddReplyError("XX and NX options at the same time are not compatible")
		return
	}

	if (gt && nx) || (lt && nx) || (gt && lt) {
		c.AddReplyError("GT, LT, and/or NX options at the same time are not compatible")
		return
	}
	// 注意：XX（仅更新已存在成员）标志可以与 GT（仅更高分数时更新）或 LT（仅更低分数时更新）标志组合使用

	if incr && elements > 1 {
		c.AddReplyError("INCR option supports a single increment-element pair")
		return
	}

	// 开始解析所有分数值，必须在操作有序集合前处理所有语法错误
	// 以保证命令的原子性：要么全部执行，要么完全不执行
	scores := make([]float64, elements)
	for i := 0; i < elements; i++ {
		// 获取分数值
		score, err := Str2Double(c.args[i*2+scoreIdx].StrVal())
		if err != nil {
			c.AddReplyError("ERR value is not a valid float")
			return
		}
		// 添加分数值
		scores[i] = score
	}
	// 查询键对应的有序集合，若不存在则新建
	zsetobj := findKeyRead(key)
	if zsetobj == nil {
		if xx {
			c.AddReplyStr("0") // 键不存在且设置了XX选项：无需任何操作
			return
		}
		zsetobj = CreateZSetObject()
		server.db.data.Set(key, zsetobj)
	} else if zsetobj.Type_ != GZSET {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	added := int64(0)
	updated := int64(0)
	processed := int64(0)
	var score float64
	for i := 0; i < elements; i++ {
		score = scores[i]
		ele := c.args[scoreIdx+1+i*2]
		retflags, newscore, err := zsetobj.Val_.(zset).zsetAdd(zsetobj, score, ele, flag)
		if err != nil {
			return
		}
		if retflags&ZADD_OUT_ADDED != 0 {
			added++
		}
		if retflags&ZADD_OUT_UPDATED != 0 {
			updated++
		}
		if retflags&ZADD_OUT_NAN == 0 {
			processed++
		}
		score = newscore
	}
	server.dirty += (added + updated)

	if incr { // ZINCRBY or INCR option.
		if processed > 0 {
			c.AddReplyDouble(score)
		} else {
			c.AddReplyStr("0")
		}
	} else { // zadd
		if ch == 1 {
			c.AddReplyLong(added + updated)
		} else {
			c.AddReplyLong(added)
		}
	}

}

func Str2Double(str string) (float64, error) {
	return strconv.ParseFloat(str, 64)
}

func zaddGenericCommand(c *GodisClient, key *Gobj, obj *Gobj, score float64, isIncr bool) {
	zsetobj := findKeyRead(key)
	if zsetobj == nil {
		zsetobj = CreateZSetObject()
		server.db.data.Set(key, zsetobj)
	} else if zsetobj.Type_ != GZSET {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	zs := zsetobj.Val_.(zset)
	if isIncr {
		/* Read the old score. If the element was not present starts from 0 */
		de := zs.dict.Find(obj)
		if de != nil {
			score += de.Value.Val_.(float64)
		}
	}
	err := zs.dict.Add(obj, nil)
	if err != nil {
		c.AddReplyError("error")
		return
	}
	znode := zs.zsl.zslInsert(score, obj)
	/* Update the score in the dict entry */
	de := zs.dict.Find(obj)
	de.Value = &Gobj{
		Type_: GSTR,
		Val_:  znode.score}
	server.dirty++
	if isIncr {
		c.AddReplyDouble(score)
	} else {
		c.AddReplyInt8(1)
	}
}

func hdelCommand(c *GodisClient) {
	key := c.args[1]
	deleted := 0
	//keyremoved := 0
	hashObej := lookupKeyWrite(key)
	if hashObej == nil {
		c.AddReplyInt8(0)
		return
	} else if hashObej.Type_ != GHASH {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	// TODO
	/* Hash field expiration is optimized to avoid frequent update global HFE DS for
	 * each field deletion. Eventually active-expiration will run and update or remove
	 * the hash from global HFE DS gracefully. Nevertheless, statistic "subexpiry"
	 * might reflect wrong number of hashes with HFE to the user if it is the last
	 * field with expiration. The following logic checks if this is indeed the last
	 * field with expiration and removes it from global HFE DS. */
	deleted = hashObej.hashTypeDelete(c.args[2:])
	c.AddReplyInt(deleted)
}

// 获取哈希表的所有值
func hvalsCommand(c *GodisClient) {
	hashGenericCommand(false, true, c)
}

func hashGenericCommand(k, v bool, c *GodisClient) {
	key := c.args[1]
	hashObj := lookupKeyWrite(key)
	if hashObj == nil {
		c.AddReplyStr("*0" + CRLF)
		return
	}
	if hashObj.Type_ != GHASH {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	fields := hashObj.hashTypeFields(k, v)
	if len(fields) == 0 {
		c.AddReplyStr("*0" + CRLF)
		return
	}
	var reply strings.Builder
	reply.WriteString(fmt.Sprintf("*%d"+CRLF, len(fields)))
	for _, field := range fields {
		val := field.StrVal()
		reply.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
	}
	c.AddReplyStr(reply.String())
}

// 获取哈希表的所有字段
func hkeysCommand(c *GodisClient) {
	hashGenericCommand(true, false, c)
}

func getExpire(key *Gobj) int64 {
	if server.db.expire == nil {
		return -1
	}
	expObj := server.db.expire.Get(key)
	if expObj == nil {
		return -1
	}
	return expObj.Val_.(int64)
}
func hsetnxCommand(c *GodisClient) {
	key := c.args[1]
	field := c.args[2]
	value := c.args[3]

	var isHashDeleted bool

	hashObj := lookupKeyWrite(key)
	if hashObj == nil {
		hashObj = hashTypeCreate()
		err := server.db.data.Add(key, hashObj)
		if err != nil {
			return
		}
	} else if hashObj.Type_ != GHASH {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	if hashObj.hashTypeExists(field, &isHashDeleted) {
		c.AddReplyInt8(0)
		return
	}

	// Field expired and in turn hash deleted. Create new one!
	if isHashDeleted {
		hashObj = hashTypeCreate()
		err := server.db.data.Add(key, hashObj)
		if err != nil {
			return
		}
	}
	hashObj.hashTypeSet([]*Gobj{field, value})
	c.AddReplyInt8(1)
}

func hgetCommand(c *GodisClient) {
	key := c.args[1]
	field := c.args[2]

	// 查找哈希对象
	hashObj := lookupKeyWrite(key)
	if hashObj == nil {
		c.AddReplyInt8(-1)
		return
	}

	if hashObj.Type_ != GHASH {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	// 从哈希表中获取值
	val := hashObj.hashTypeGet(field)
	if val == nil {
		c.AddReplyInt8(-1)
		return
	}
	// 返回找到的值
	str := val.StrVal()
	c.AddReplyStr(fmt.Sprintf("$%d\r\n%s\r\n", len(str), str))
}

func hsetCommand(c *GodisClient) {
	if len(c.args)%2 != 0 {
		c.AddReplyError("wrong number of arguments for HMSET")
		return
	}

	key := c.args[1]
	hashObj := lookupKeyWrite(key)

	// 如果键不存在，创建一个新的哈希表
	if hashObj == nil {
		hashObj = hashTypeCreate()
		server.db.data.Set(key, hashObj)
		hashObj.DecrRefCount() // Set 会增加引用计数，所以这里减少一次
	} else if hashObj.Type_ != GHASH {
		// 如果键存在但不是哈希类型，返回错误
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	created := hashObj.hashTypeSet(c.args[2:])
	c.AddReplyInt(created)
}

func scardCommand(c *GodisClient) {
	key := c.args[1]
	set := findKeyRead(key)
	if set == nil {
		c.AddReplyInt8(0)
	} else if set.Type_ == GSET {
		c.AddReplyLong(set.setTypeSize())
	} else {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
}

func smembersCommand(c *GodisClient) {
	key := c.args[1]
	set := lookupKeyWrite(key)
	if set == nil {
		c.AddReplyStr("*0" + CRLF)
	} else if set.Type_ != GSET {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
	} else {
		members := set.setTypeMembers()
		if len(members) == 0 {
			c.AddReplyStr("*0" + CRLF)
			return
		}
		var reply strings.Builder
		reply.WriteString(fmt.Sprintf("*%d"+CRLF, len(members)))
		for _, member := range members {
			val := member.StrVal()
			reply.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
		}
		c.AddReplyStr(reply.String())
	}
}

func sismemberCommand(c *GodisClient) {
	key := c.args[1]
	set := lookupKeyWrite(key)
	if set == nil {
		c.AddReplyInt8(0)
	} else if set.Type_ != GSET {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
	} else {
		c.AddReplyInt8(set.setTypeIsMember(c.args[2]))
	}
}

func sremCommand(c *GodisClient) {
	key := c.args[1]
	set := lookupKeyWrite(key)
	if set == nil {
		c.AddReplyInt(0)
		return
	} else if set.Type_ != GSET {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	removed := set.setTypeRemove(c.args[2:])
	c.AddReplyLong(removed)
}

/*
lrem key count value
 1. count > 0 : 从头到尾删除 count 个值为 value 的元素
 2. count < 0 : 从尾到头删除 count 个值为 value 的元素
*/
func lremCommand(c *GodisClient) {
	key := c.args[1]
	countObj := c.args[2]
	value := c.args[3]

	lobj := lookupKeyWrite(key)
	if lobj == nil {
		c.AddReplyInt(0)
		return
	}
	if lobj.Type_ != GLIST {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	count := countObj.IntVal()
	list := lobj.Val_.(*List)
	removed := int64(0)

	if count == 0 {
		// 删除所有匹配的元素
		for node := list.First(); node != nil; {
			nextNode := node.next // 先保存下一个节点，因为当前节点可能会被删除
			if list.EqualFunc(node.Val, value) {
				list.DelNode(node)
				removed++
			}
			node = nextNode
		}
	} else if count > 0 {
		// 从头到尾删除 count 个匹配的元素
		for node := list.First(); node != nil && removed < count; {
			nextNode := node.next
			if list.EqualFunc(node.Val, value) {
				list.DelNode(node)
				removed++
			}
			node = nextNode
		}
	} else {
		// 从尾到头删除 -count 个匹配的元素
		for node := list.Last(); node != nil && removed < -count; {
			prevNode := node.prev // 先保存上一个节点，因为当前节点可能会被删除
			if list.EqualFunc(node.Val, value) {
				list.DelNode(node)
				removed++
			}
			node = prevNode
		}
	}
	c.AddReplyLong(removed)
}

func saddCommand(c *GodisClient) {
	key := c.args[1]
	set := lookupKeyWrite(key)

	// 如果键不存在，创建一个新的集合
	if set == nil {
		set = SetTypeCreate()
		err := server.db.data.Add(key, set)
		if err != nil {
			return
		}
	} else if set.Type_ != GSET {
		// 如果键存在但不是集合类型，返回错误
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	added := set.setTypeAdd(c.args[2:])
	c.AddReplyLong(added)
}

func llenCommand(c *GodisClient) {
	key := c.args[1]
	lobj := findKeyRead(key)
	c.AddReplyLong(lobj.Val_.(*List).Length())
}

func lindexCommand(c *GodisClient) {
	key := c.args[1]
	indexObj := c.args[2]

	// 获取列表对象
	lobj := findKeyRead(key)
	if lobj == nil {
		c.AddReplyStr("$-1\r\n")
		return
	}

	if lobj.Type_ != GLIST {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	// 解析索引
	var index int64
	if c.getLongFromObjectOrReply(indexObj, &index) != GODIS_OK {
		return
	}
	// index := indexObj.IntVal() 也可以这样写

	list := lobj.Val_.(*List)
	listLen := list.Length()

	// 处理负数索引
	if index < 0 {
		index = listLen + index
	}

	// 检查索引范围
	if index < 0 || index >= listLen {
		c.AddReplyStr("$-1\r\n")
		return
	}

	// 遍历列表找到指定索引的元素
	current := list.First()
	for i := int64(0); i < index && current != nil; i++ {
		current = current.next
	}

	if current == nil {
		c.AddReplyStr("$-1\r\n")
		return
	}

	// 返回找到的元素
	val := current.Val.StrVal()
	c.AddReplyStr(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
}

func pushGenericCommand(c *GodisClient, where int8) {
	key := c.args[1]
	lobj := lookupKeyWrite(key)
	// 查找或创建列表
	var list *List
	if lobj == nil {
		// 创建新的列表
		lobj = CreateListObject()
		server.db.data.Set(key, lobj)
		lobj.DecrRefCount()
	} else if lobj.Type_ != GLIST {
		c.AddReplyError("Operation against a key holding the wrong kind of value")
		return
	}
	list = lobj.Val_.(*List)
	// 添加元素到列表
	for i := 2; i < len(c.args); i++ {
		val := c.args[i]
		if where == LIST_HEAD {
			list.LPush(val)
		} else {
			list.Append(val)
		}
		// 增加值的引用计数
		val.IncrRefCount()
	}
	// 回复客户端
	c.AddReplyStr(fmt.Sprintf(":%d"+CRLF, list.Length()))
}
func lrangeCommand(c *GodisClient) {
	key := c.args[1]
	startObj := c.args[2]
	stopObj := c.args[3]

	// 获取列表对象
	lobj := findKeyRead(key)
	if lobj == nil {
		// 返回空数组
		c.AddReplyArrayLen(0)
		return
	}

	if lobj.Type_ != GLIST {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	// 解析起始和结束索引
	var start, stop int64
	if c.getLongFromObjectOrReply(startObj, &start) != GODIS_OK ||
		c.getLongFromObjectOrReply(stopObj, &stop) != GODIS_OK {
		return
	}

	list := lobj.Val_.(*List)
	listLen := list.Length()

	// 处理负数索引
	if start < 0 {
		start = listLen + start
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop = listLen + stop
		if stop < 0 {
			stop = -1 // 设置为-1以便在后续处理中返回空结果
		}
	}

	// 确保索引在有效范围内
	if start >= listLen || start < 0 {
		c.AddReplyArrayLen(0)
		return
	}
	if stop >= listLen {
		stop = listLen - 1
	}

	// 如果起始位置大于结束位置，返回空数组
	if start > stop {
		c.AddReplyArrayLen(0)
		return
	}

	// 计算返回元素的数量
	rangeLen := stop - start + 1

	// 构建回复
	var reply strings.Builder
	reply.WriteString(fmt.Sprintf("*%d"+CRLF, rangeLen))

	// 遍历列表获取指定范围的元素
	current := list.First()
	// 移动到起始位置
	for i := int64(0); i < start && current != nil; i++ {
		current = current.next
	}

	// 依次添加范围内的元素
	for i := int64(0); i < rangeLen && current != nil; i++ {
		val := current.Val.StrVal()
		reply.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
		current = current.next
	}

	c.AddReplyStr(reply.String())
}

func lpopCommand(c *GodisClient) {
	popGenericCommand(c, LIST_HEAD)
}

func popGenericCommand(c *GodisClient, where int8) {
	key := c.args[1]
	lobj := lookupKeyWrite(key)
	if lobj == nil {
		c.AddReplyStr("$-1\r\n")
		return
	}
	if lobj.Type_ != GLIST {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	list := lobj.Val_.(*List)
	// 如果列表为空，返回 nil
	if list.Length() == 0 {
		c.AddReplyStr("$-1\r\n")
		return
	}
	// 从列表头部/尾部弹出元素
	var node *Node
	if where == LIST_HEAD {
		node = list.First()
	} else {
		node = list.Last()
	}
	if node == nil {
		c.AddReplyStr("$-1\r\n")
		return
	}
	val := node.Val
	list.DelNode(node)

	// 返回被弹出的元素
	str := val.StrVal()
	c.AddReplyStr(fmt.Sprintf("$%d\r\n%s\r\n", len(str), str))
}

func rpopCommand(c *GodisClient) {
	popGenericCommand(c, LIST_TAIL)
}

func lpushCommand(c *GodisClient) {
	pushGenericCommand(c, LIST_HEAD)
}

func rpushCommand(c *GodisClient) {
	pushGenericCommand(c, LIST_TAIL)
}

func bgrewriteaofCommand(c *GodisClient) {
	// if server.bgsavechildpid != -1 {
	// 	c.AddReplyError("Background save already in progress")
	// 	return
	// }
	if rewriteAppendOnlyFileBackground() != nil {
		c.AddReplyError("Failed to start background AOF rewrite")
		return
	}
	c.AddReplyStr("+Background append-only file rewrite started" + CRLF)
}

func bgsaveCommand(c *GodisClient) {

}

func saveCommand(c *GodisClient) {
	// TODO bgsavechildpid
	// if server.bgsavechildpid != -1 {
	// 	c.AddReplyError("Background save already in progress")
	// 	return
	// }
	if rdbSave(server.dbfilename, server.db) != nil {
		c.AddReplyError("Error saving DB on disk")
		return
	}
	c.AddReplyStr("+OK" + CRLF)
}

func expireAtCommand(c *GodisClient) {

}

func lookupKey(key *Gobj) *Gobj {
	entry := server.db.data.Find(key)
	if entry == nil {
		return nil
	}
	val := entry.Value
	// TODO  /* Update the access time for the aging algorithm.
	//         * Don't do it if we have a saving child, as this will trigger
	//         * a copy on write madness. */
	return val
}

func lookupKeyWrite(key *Gobj) *Gobj {
	if expireIfNeeded(key) {
		// 如果过期了，直接删除了，不需要再去查了
		return nil
	}
	return lookupKey(key)
}

func msetGenericCommand(c *GodisClient, nx int) {
	busykeys := 0
	j := 0
	if len(c.args)%2 == 0 {
		c.AddReplyError("wrong number of arguments for MSET")
	}
	// 处理 NX 标志。MSETNX 的语义是，如果至少有一个键已存在，则返回零且不设置任何内容。
	if nx != 0 {
		for j = 1; j < len(c.args); j += 2 {
			key := c.args[j]
			if lookupKeyWrite(key) != nil {
				busykeys++
			}
		}
	}
	if busykeys > 0 {
		c.AddReplyError("one or more keys were not found")
		return
	}
	for j = 1; j < len(c.args); j += 2 {
		key := c.args[j]
		val := c.args[j+1]
		server.db.data.Set(key, val)
		server.db.expire.Delete(key)
	}
	c.AddReplyStr("+OK" + CRLF)
}

func msetCommand(c *GodisClient) {
	msetGenericCommand(c, 0)
}
func msetnxCommand(c *GodisClient) {
	msetGenericCommand(c, 1)
}

func mgetCommand(c *GodisClient) {
	// TODO addReplyMultiBulkLen(c,c->argc-1); ??
	for i := 1; i < len(c.args); i++ {
		key := c.args[i]
		val := findKeyRead(key)
		if val == nil {
			c.AddReplyStr("$-1\r\n")
		} else {
			c.AddReply(val)
		}
	}
}

func delCommand(c *GodisClient) {
	var deleted, j int
	// TODO 这里参数计算的个数 是否需要 优化？
	for j = 1; j < len(c.args); j++ {
		err := server.db.data.Delete(c.args[j])
		if err == nil {
			deleted++
		}
	}
	c.AddReplyStr(fmt.Sprintf(":%d\r\n", int64(deleted)))
}

func (c *GodisClient) getLongFromObjectOrReply(o *Gobj, target *int64) int8 {
	var value int64
	if c.getLongFromObject(o, &value) != GODIS_OK {
		return GODIS_ERR
	}
	if target != nil {
		*target = value
	}
	return GODIS_OK
}
func (c *GodisClient) getLongFromObject(o *Gobj, target *int64) int8 {
	var value int64
	if o == nil {
		value = 0
	}
	if o.Type_ != GSTR {
		c.AddReplyError("wrong type")
		return GODIS_ERR
	}
	//转换为 int 64
	value, err := strconv.ParseInt(o.StrVal(), 10, 64)
	if err != nil {
		return GODIS_ERR
	}
	// TODO 需要判断 Encoding REDIS_ENCODING_RAW/REDIS_ENCODING_INT
	if target != nil {
		*target = value
	}
	return GODIS_OK
}
func setexCommand(c *GodisClient) {
	key := c.args[1]
	value := c.args[3]
	expire := c.args[2]
	var seconds int64
	if expire != nil {
		if c.getLongFromObjectOrReply(expire, &seconds) != GODIS_OK {
			return
		}
		if seconds <= 0 {
			c.AddReplyError("invalid expire time in SETEX")
			return
		}
	}
	// Force expire of old key if needed
	expireIfNeeded(key)
	timeout := GetMsTime() + (expire.IntVal() * 1000)
	expObj := CreateFromInt(timeout)
	server.db.data.Set(key, value)
	server.db.expire.Set(key, expObj)
	expObj.DecrRefCount()
	c.AddReplyStr("+OK\r\n")
}

func setnxCommand(c *GodisClient) {
	key := c.args[1]
	if val := findKeyRead(key); val != nil {
		c.AddReplyStr(":0\r\n")
		return
	}
	setCommand(c)
	c.AddReplyStr(":1\r\n")
}

func expireIfNeeded(key *Gobj) bool {
	entry := server.db.expire.Find(key)
	if entry == nil {
		return false
	}
	when := entry.Value.Val_.(int64)
	if when > GetMsTime() {
		return false
	}
	server.db.expire.Delete(key)
	server.db.data.Delete(key)
	return true
}

func findKeyRead(key *Gobj) *Gobj {
	if expireIfNeeded(key) {
		// 如果过期了，直接删除了，不需要再去查了
		return nil
	}
	return server.db.data.Get(key)
}

func getCommand(c *GodisClient) {
	key := c.args[1]
	valObj := findKeyRead(key)
	if valObj == nil {
		//TODO: extract shared.strings
		c.AddReplyStr("$-1\r\n")
	} else if valObj.Type_ != GSTR {
		//TODO: extract shared.strings
		c.AddReplyError("wrong type")
	} else {
		var str string
		switch valObj.encoding {
		case GODIS_ENCODING_INT:
			str = fmt.Sprintf("%d", valObj.Val_.(int64))
		case GODIS_ENCODING_RAW:
			str = valObj.StrVal()
		}
		c.AddReplyStr(fmt.Sprintf("$%d\r\n%v\r\n", len(str), str))
	}
}

func setCommand(c *GodisClient) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != GSTR {
		//TODO: extract shared.strings
		c.AddReplyStr("-ERR: wrong type\r\n")
	}
	server.db.data.Set(key, val)
	server.db.expire.Delete(key)
	c.AddReplyStr("+OK\r\n")
}

func expireCommand(c *GodisClient) {
	key := c.args[1]
	if lookupKeyWrite(key) == nil {
		c.AddReplyInt8(0)
		return
	}
	val := c.args[2]
	if val.Type_ != GSTR {
		//TODO: extract shared.strings
		c.AddReplyStr("-ERR: wrong type" + CRLF)
	}
	expire := GetMsTime() + (val.IntVal() * 1000)
	expObj := CreateFromInt(expire)
	server.db.expire.Set(key, expObj)
	expObj.DecrRefCount()
	c.AddReplyInt8(1)
}

func lookupCommand(cmdStr string) *GodisCommand {
	cmdStrLower := strings.ToLower(cmdStr)
	for _, c := range cmdTable {
		if c.name == cmdStrLower {
			return &c
		}
	}
	return nil
}
func (c *GodisClient) AddReplyDouble(score float64) {
	replyStr := fmt.Sprintf("%.17g", score)
	c.AddReplyStr(fmt.Sprintf("$%v\r\n%v"+CRLF, len(replyStr), replyStr))
}
func (c *GodisClient) AddReplyError(errInfo string) {
	c.AddReplyStr("-ERR:" + errInfo + CRLF)
}
func (c *GodisClient) AddReply(o *Gobj) {
	if c.fd < 0 {
		// 如果是 mock 终端，就不回复了
		return
	}
	c.reply.Append(o)
	o.IncrRefCount()
	server.aeLoop.AddFileEvent(c.fd, AE_WRITABLE, SendReplyToClient, c)
}
func (c *GodisClient) AddReplyBulkLen(o *Gobj) {
	c.AddReplyStr(fmt.Sprintf("$%v\r\n", len(o.StrVal())))
}

func (c *GodisClient) AddReplyBulk(o *Gobj) {
	c.AddReplyBulkLen(o)
	c.AddReplyStr(o.StrVal() + CRLF)
}

func (c *GodisClient) AddReplyStr(str string) {
	o := CreateObject(GSTR, str)
	c.AddReply(o)
	o.DecrRefCount()
}
func (c *GodisClient) AddReplyLong(num int64) {
	c.AddReplyStr(fmt.Sprintf(":%d"+CRLF, num))
}
func (c *GodisClient) AddReplyInt8(num int8) {
	c.AddReplyStr(fmt.Sprintf(":%d"+CRLF, num))
}
func (c *GodisClient) AddReplyInt(num int) {
	c.AddReplyStr(fmt.Sprintf(":%d"+CRLF, num))
}
func (c *GodisClient) AddReplyArrayLen(len_ int64) {
	c.AddReplyStr(fmt.Sprintf("*%d"+CRLF, len_))
}

func ProcessCommand(c *GodisClient) {
	cmdStr := c.args[0].StrVal()
	log.Printf("process command: %v\n", cmdStr)
	if cmdStr == "quit" {
		c.AddReplyStr("+OK" + CRLF)
		SendReplyToClient(server.aeLoop, c.fd, c)
		freeClient(c)
		return
	}
	cmd := lookupCommand(cmdStr)
	if cmd == nil {
		c.AddReplyError("unknow command")
		SendReplyToClient(server.aeLoop, c.fd, c)
		resetClient(c)
		return
	} else if (cmd.arity > 0 && cmd.arity != len(c.args)) ||
		(cmd.arity < 0 && -cmd.arity > len(c.args)) {
		c.AddReplyError(fmt.Sprintf("wrong number of arguments for '%s' command", cmd.name))
		SendReplyToClient(server.aeLoop, c.fd, c)
		resetClient(c)
		return
	}
	cmd.proc(c)
	// TODO server.dirty > 0
	if cmd.flags == CMD_WRITE {
		// FeedAppendOnlyFile(cmd, c.args)
	}
	resetClient(c)
	// 处理完命令后，主动尝试发送回复
	if c.reply.Length() > 0 {
		SendReplyToClient(server.aeLoop, c.fd, c)
	}
}

func freeArgs(client *GodisClient) {
	for _, v := range client.args {
		v.DecrRefCount()
	}
}

func freeReplyList(client *GodisClient) {
	for client.reply.length != 0 {
		n := client.reply.head
		client.reply.DelNode(n)
		n.Val.DecrRefCount()
	}
}

func freeClient(client *GodisClient) {
	freeArgs(client)
	delete(server.clients, client.fd)
	server.aeLoop.RemoveFileEvent(client.fd, AE_READABLE)
	server.aeLoop.RemoveFileEvent(client.fd, AE_WRITABLE)
	freeReplyList(client)
	Close(client.fd)
}

func resetClient(client *GodisClient) {
	freeArgs(client)
	client.cmdType = COMMAND_UNKNOWN
	client.bulkLen = 0
	client.bulkNum = 0
}

func (client *GodisClient) findLineInQuery() (int, error) {
	index := strings.Index(string(client.queryBuf[:client.queryLen]), "\r\n")
	if index < 0 && client.queryLen > GODIS_MAX_INLINE {
		return index, errors.New("too big inline cmd")
	}
	return index, nil
}

func (client *GodisClient) getNumInQuery(s, e int) (int, error) {
	num, err := strconv.Atoi(string(client.queryBuf[s:e]))
	client.queryBuf = client.queryBuf[e+2:]
	client.queryLen -= e + 2
	return num, err
}

func handleInlineBuf(client *GodisClient) (bool, error) {
	index, err := client.findLineInQuery()
	if index < 0 {
		return false, err
	}

	subs := strings.Split(string(client.queryBuf[:index]), " ")
	client.queryBuf = client.queryBuf[index+2:]
	client.queryLen -= index + 2
	client.args = make([]*Gobj, len(subs))
	for i, v := range subs {
		client.args[i] = CreateObject(GSTR, v)
	}

	return true, nil
}

func handleBulkBuf(client *GodisClient) (bool, error) {
	// read bulk num
	if client.bulkNum == 0 {
		index, err := client.findLineInQuery()
		if index < 0 {
			return false, err
		}

		bnum, err := client.getNumInQuery(1, index)
		if err != nil {
			return false, err
		}
		if bnum == 0 {
			return true, nil
		}
		client.bulkNum = bnum
		client.args = make([]*Gobj, bnum)
	}
	// read every bulk string
	for client.bulkNum > 0 {
		// read bulk length
		if client.bulkLen == 0 {
			index, err := client.findLineInQuery()
			if index < 0 {
				return false, err
			}

			if client.queryBuf[0] != '$' {
				return false, errors.New("expect $ for bulk length")
			}

			blen, err := client.getNumInQuery(1, index)
			if err != nil || blen == 0 {
				return false, err
			}
			if blen > GODIS_MAX_BULK {
				return false, errors.New("too big bulk")
			}
			client.bulkLen = blen
		}
		// read bulk string
		if client.queryLen < client.bulkLen+2 {
			return false, nil
		}
		index := client.bulkLen
		if client.queryBuf[index] != '\r' || client.queryBuf[index+1] != '\n' {
			return false, errors.New("expect CRLF for bulk end")
		}
		client.args[len(client.args)-client.bulkNum] = CreateObject(GSTR, string(client.queryBuf[:index]))
		client.queryBuf = client.queryBuf[index+2:]
		client.queryLen -= index + 2
		client.bulkLen = 0
		client.bulkNum -= 1
	}
	// complete reading every bulk
	return true, nil
}

func ProcessQueryBuf(client *GodisClient) error {
	for client.queryLen > 0 {
		if client.cmdType == COMMAND_UNKNOWN {
			if client.queryBuf[0] == '*' {
				client.cmdType = COMMAND_BULK
			} else {
				client.cmdType = COMMAND_INLINE
			}
		}
		// trans query -> args
		var ok bool
		var err error
		if client.cmdType == COMMAND_INLINE {
			ok, err = handleInlineBuf(client)
		} else if client.cmdType == COMMAND_BULK {
			ok, err = handleBulkBuf(client)
		} else {
			return errors.New("unknow Godis Command Type")
		}
		if err != nil {
			return err
		}
		// after query -> args
		if ok {
			if len(client.args) == 0 {
				resetClient(client)
			} else {
				ProcessCommand(client)
			}
		} else {
			// cmd incomplete
			break
		}
	}
	return nil
}

func ReadQueryFromClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*GodisClient)
	if len(client.queryBuf)-client.queryLen < GODIS_MAX_BULK {
		client.queryBuf = append(client.queryBuf, make([]byte, GODIS_MAX_BULK)...)
	}
	n, err := Read(fd, client.queryBuf[client.queryLen:])
	if err != nil {
		log.Printf("client %v read err: %v\n", fd, err)
		freeClient(client)
		return
	}
	client.queryLen += n
	log.Printf("read %v bytes from client:%v\n", n, client.fd)
	log.Printf("ReadQueryFromClient, queryBuf : %v\n", string(client.queryBuf))
	err = ProcessQueryBuf(client)
	if err != nil {
		log.Printf("process query buf err: %v\n", err)
		freeClient(client)
		return
	}
}

func SendReplyToClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*GodisClient)
	log.Printf("SendReplyToClient, reply len:%v\n", client.reply.Length())
	for client.reply.Length() > 0 {
		rep := client.reply.First()
		buf := []byte(rep.Val.StrVal())
		bufLen := len(buf)
		if client.sentLen < bufLen {
			n, err := Write(fd, buf[client.sentLen:])
			if err != nil {
				log.Printf("send reply err: %v\n", err)
				freeClient(client)
				return
			}
			client.sentLen += n
			log.Printf("send %v bytes to client:%v\n", n, client.fd)
			// 如果当前缓冲区没有完全发送完，等待下次触发
			if client.sentLen == bufLen {
				client.reply.DelNode(rep)
				rep.Val.DecrRefCount()
				client.sentLen = 0
			} else {
				break
			}
		}
	}
	// 所有数据发送完成后，移除写事件监听
	if client.reply.Length() == 0 {
		client.sentLen = 0
		loop.RemoveFileEvent(fd, AE_WRITABLE)
	}
}

func GStrEqual(a, b *Gobj) bool {
	if a.Type_ != GSTR || b.Type_ != GSTR {
		return false
	}
	return a.StrVal() == b.StrVal()
}

func GStrHash(key *Gobj) int64 {
	if key.Type_ != GSTR {
		return 0
	}
	hash := fnv.New64()
	hash.Write([]byte(key.StrVal()))
	return int64(hash.Sum64())
}

func CreateClient(fd int) *GodisClient {
	var client GodisClient
	client.fd = fd
	client.db = server.db
	client.queryBuf = make([]byte, GODIS_IO_BUF)
	client.reply = ListCreate(ListType{EqualFunc: GStrEqual})
	return &client
}

func AcceptHandler(loop *AeLoop, fd int, extra interface{}) {
	cfd, err := Accept(fd)
	if err != nil {
		log.Printf("accept err: %v\n", err)
		return
	}
	client := CreateClient(cfd)
	//TODO: check max clients limit
	server.clients[cfd] = client
	server.aeLoop.AddFileEvent(cfd, AE_READABLE, ReadQueryFromClient, client)
	log.Printf("accept client, fd: %v\n", cfd)
}

const EXPIRE_CHECK_COUNT int = 100

// background job, runs every 100ms
// 随机过期删除策略 每 100ms执行一次 每次选 100个key
// 惰性删除策略，访问的时候检查
func ServerCron(loop *AeLoop, id int, extra interface{}) {
	for i := 0; i < EXPIRE_CHECK_COUNT; i++ {
		entry := server.db.expire.RandomGet()
		if entry == nil {
			break
		}
		if entry.Value.Val_.(int64) < time.Now().Unix() {
			server.db.data.Delete(entry.Key)
			server.db.expire.Delete(entry.Key)
		}
	}
}

func initServer(config *Config) error {
	server.port = config.Port
	server.clients = make(map[int]*GodisClient)
	server.db = &GodisDB{
		data:   DictCreate(DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
		expire: DictCreate(DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
	}
	var err error
	if server.aeLoop, err = AeLoopCreate(); err != nil {
		return err
	}
	server.fd, err = TcpServer(server.port)
	return err
}

func main() {
	//path := os.Args[1]
	log.SetOutput(io.Discard) // 关闭日志输出
	path := "./config.json"
	config, err := LoadConfig(path)
	if err != nil {
		log.Printf("config error: %v\n", err)
		return
	}
	err = initServer(config)
	if err != nil {
		log.Printf("init server error: %v\n", err)
		return
	}
	// 加载AOF 文件
	//loadAppendOnlyFile()
	// 加载 RDB 数据库
	//rdbLoad(server.dbfilename)
	server.aeLoop.AddFileEvent(server.fd, AE_READABLE, AcceptHandler, nil)
	server.aeLoop.AddTimeEvent(AE_NORMAL, 100, ServerCron, nil)
	log.Println("godis server is up.")
	server.aeLoop.AeMain()
}
