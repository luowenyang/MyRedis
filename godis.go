package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
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

// CRLF 是 redis 统一的行分隔符协议
const CRLF = "\r\n"

type GodisDB struct {
	data   *Dict
	expire *Dict
}

type GodisServer struct {
	fd      int
	port    int
	db      *GodisDB
	clients map[int]*GodisClient
	aeLoop  *AeLoop
}

type GodisClient struct {
	fd       int
	db       *GodisDB
	args     []*Gobj
	reply    *List
	sentLen  int
	queryBuf []byte
	queryLen int
	cmdTy    CmdType
	bulkNum  int
	bulkLen  int
}

type CommandProc func(c *GodisClient)

// do not support bulk command
type GodisCommand struct {
	name  string
	proc  CommandProc
	arity int
}

// Global Varibles
var server GodisServer
var cmdTable = []GodisCommand{

	{"expireat", expireAtCommand, 3},
	{"expire", expireCommand, 3},

	{"del", delCommand, -2},

	//string
	{"get", getCommand, 2},
	{"set", setCommand, 3},
	{"mget", mgetCommand, -2},
	{"mset", msetCommand, -3},
	{"msetnx", msetnxCommand, -4},
	{"setnx", setnxCommand, 3},
	{"setex", setexCommand, 4},

	// list
	{"rpush", rpushCommand, 3},
	{"lpush", lpushCommand, 3},
	{"rpop", rpopCommand, 2},
	{"lpop", lpopCommand, 2},
	{"lrange", lrangeCommand, 4},
	{"lindex", lindexCommand, 3},
	{"llen", llenCommand, 2},
	{"lrem", lremCommand, 4},

	// set
	{"sadd", saddCommand, -3},
	{"srem", sremCommand, -3},
	{"sismember", sismemberCommand, 3},
	// TODO
	{"smembers", smembersCommand, 2},
	{"scard", scardCommand, 2},

	// hash
	{"hset", hsetCommand, -4},

	//zset

	//persist
	{"save", saveCommand, 1},
	{"bgsave", bgsaveCommand, 1},
	{"bgrewriteaof", bgrewriteaofCommand, 1},
}

func hsetCommand(c *GodisClient) {
	var update int
	key := c.args[1]
	hash := lookupKeyWrite(key)
	if hash == nil {
		hash = hashTypeCreate()
		server.db.data.Set(key, hash)
	} else if hash.Type_ != GHASH {
		c.AddReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	update = hash.hashTypeSet(hash, c.args[2:])
	c.AddReplyInt(update)
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

func lremCommand(c *GodisClient) {

}

func saddCommand(c *GodisClient) {
	key := c.args[1]
	set := lookupKeyWrite(key)

	// 如果键不存在，创建一个新的集合
	if set == nil {
		set = SetTypeCreate()
		server.db.data.Set(key, set)
		set.DecrRefCount() // SetKey 会增加引用计数，所以这里减少一次
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
	val := c.args[2]
	lobj := lookupKeyWrite(key)
	// 查找或创建列表
	var list *List
	if lobj == nil {
		// 创建新的列表
		obj := CreateListObject()
		server.db.data.Set(key, obj)
		obj.DecrRefCount()
	} else {
		if lobj.Type_ != GLIST {
			c.AddReplyError("Operation against a key holding the wrong kind of value")
			return
		}
		list = lobj.Val_.(*List)
	}

	// 添加元素到列表
	if where == LIST_HEAD {
		list.LPUsh(val)
	} else {
		list.Append(val)
	}
	// 增加值的引用计数
	val.IncrRefCount()

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
		c.AddReplyStr("*0\r\n")
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
		c.AddReplyStr("*0" + CRLF)
		return
	}
	if stop >= listLen {
		stop = listLen - 1
	}

	// 如果起始位置大于结束位置，返回空数组
	if start > stop {
		c.AddReplyStr("*0" + CRLF)
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

}

func bgsaveCommand(c *GodisClient) {

}

func saveCommand(c *GodisClient) {

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
	expireIfNeeded(key)
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

func expireIfNeeded(key *Gobj) {
	entry := server.db.expire.Find(key)
	if entry == nil {
		return
	}
	when := entry.Value.IntVal()
	if when > GetMsTime() {
		return
	}
	server.db.expire.Delete(key)
	server.db.data.Delete(key)
}

func findKeyRead(key *Gobj) *Gobj {
	expireIfNeeded(key)
	return server.db.data.Get(key)
}

func getCommand(c *GodisClient) {
	key := c.args[1]
	val := findKeyRead(key)
	if val == nil {
		//TODO: extract shared.strings
		c.AddReplyStr("$-1\r\n")
	} else if val.Type_ != GSTR {
		//TODO: extract shared.strings
		c.AddReplyError("wrong type")
	} else {
		str := val.StrVal()
		c.AddReplyStr(fmt.Sprintf("$%d%v\r\n", len(str), str))
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
	val := c.args[2]
	if val.Type_ != GSTR {
		//TODO: extract shared.strings
		c.AddReplyStr("-ERR: wrong type\r\n")
	}
	expire := GetMsTime() + (val.IntVal() * 1000)
	expObj := CreateFromInt(expire)
	server.db.expire.Set(key, expObj)
	expObj.DecrRefCount()
	c.AddReplyStr("+OK" + CRLF)
}

func lookupCommand(cmdStr string) *GodisCommand {
	for _, c := range cmdTable {
		if c.name == cmdStr {
			return &c
		}
	}
	return nil
}
func (c *GodisClient) AddReplyError(errInfo string) {
	c.AddReplyStr("-ERR:" + errInfo + CRLF)
}
func (c *GodisClient) AddReply(o *Gobj) {
	c.reply.Append(o)
	o.IncrRefCount()
	server.aeLoop.AddFileEvent(c.fd, AE_WRITABLE, SendReplyToClient, c)
}
func (c *GodisClient) AddReplyBulkLen(o *Gobj) {

}

func (c *GodisClient) AddReplyBulk(o *Gobj) {
	c.AddReply(o)
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
	} else if cmd.arity > 0 && cmd.arity != len(c.args) {
		c.AddReplyError("wrong number of args")
		SendReplyToClient(server.aeLoop, c.fd, c)
		resetClient(c)
		return
	} else if cmd.arity < 0 && -cmd.arity > len(c.args) {
		c.AddReplyError("wrong number of args")
		SendReplyToClient(server.aeLoop, c.fd, c)
		resetClient(c)
		return
	}
	cmd.proc(c)
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
	client.cmdTy = COMMAND_UNKNOWN
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
		if client.cmdTy == COMMAND_UNKNOWN {
			if client.queryBuf[0] == '*' {
				client.cmdTy = COMMAND_BULK
			} else {
				client.cmdTy = COMMAND_INLINE
			}
		}
		// trans query -> args
		var ok bool
		var err error
		if client.cmdTy == COMMAND_INLINE {
			ok, err = handleInlineBuf(client)
		} else if client.cmdTy == COMMAND_BULK {
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
func ServerCron(loop *AeLoop, id int, extra interface{}) {
	for i := 0; i < EXPIRE_CHECK_COUNT; i++ {
		entry := server.db.expire.RandomGet()
		if entry == nil {
			break
		}
		if entry.Value.IntVal() < time.Now().Unix() {
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
	path := os.Args[1]
	config, err := LoadConfig(path)
	if err != nil {
		log.Printf("config error: %v\n", err)
	}
	err = initServer(config)
	if err != nil {
		log.Printf("init server error: %v\n", err)
	}
	server.aeLoop.AddFileEvent(server.fd, AE_READABLE, AcceptHandler, nil)
	server.aeLoop.AddTimeEvent(AE_NORMAL, 100, ServerCron, nil)
	log.Println("godis server is up.")
	server.aeLoop.AeMain()
}
