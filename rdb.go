package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
)

type saveparam struct {
	seconds uint
	changes int
}

const (
	GODIS_EXPIRETIME = 0xfd
	GODIS_EOF        = 0xff
)

// 写二进制数据
func rdbWriteRaw(file *os.File, data []byte) (int, error) {
	if file == nil {
		return 0, os.ErrInvalid
	}
	n, err := file.Write(data)
	if err != nil {
		return n, err
	}
	return n, nil
}

func rdbSave(filename string, db *GodisDB) error {
	// 创建临时文件
	tmpFile, err := os.CreateTemp("./", fmt.Sprintf("temp-%d.rdb", os.Getpid()))
	if err != nil {
		return err
	}
	iter := db.data.NewIterator(true)
	defer iter.Close()
	for key, value, exists := iter.Next(); exists; key, value, exists = iter.Next() {
		expiretime := getExpire(key)
		if expiretime != -1 {
			if n, err := rdbSaveType(tmpFile, []byte{GODIS_EXPIRETIME}); err != nil || n == 0 {
				return err
			}
			if n, err := rdbSaveTime(tmpFile, expiretime); err != nil || n == 0 {
				return err
			}
		}
		// [类型][len][key(string)][list,hash,set len]{[value(字节数组)],[len][value(字节数组)],[len][value(字节数组)]}
		rdbSaveType(tmpFile, []byte{byte(value.Type_)})
		rdbSaveStringObject(tmpFile, key)
		rdbSaveObject(tmpFile, value)
	}

	if n, err := rdbSaveType(tmpFile, []byte{GODIS_EOF}); err != nil || n == 0 {
		return err
	}

	tmpFile.Sync()
	tmpFile.Close()
	// 重命名临时文件为目标文件
	err = os.Rename(tmpFile.Name(), filename)
	if err != nil {
		return err
	}
	server.lastfsync = GetMsTime()
	server.dirty = 0
	return nil
}

func rdbSaveBackground() {

}
func rdbSaveObject(file *os.File, o *Gobj) (int, error) {
	switch o.Type_ {
	case GSTR:
		return rdbSaveStringObject(file, o)
	case GLIST:
		//GODIS_ENCODING_LINKEDLIST
		list := o.Val_.(*List)
		// 先保存长度
		rdbSaveLen(file, uint32(list.Length()))
		// 保存每个元素
		for e := list.First(); e != nil; e = e.next {
			elem := e.Val
			rdbSaveStringObject(file, elem)
		}
		return 0, nil
	case GZSET:
		zsetObj := o.Val_.(zset)
		// 先保存长度
		rdbSaveLen(file, uint32(zsetObj.zsl.length))
		// 保存每个元素
		zslNode := zsetObj.zsl.header.level[0].forward
		for zslNode != nil {
			rdbSaveStringObject(file, zslNode.obj)
			rdbSaveStringObject(file, &Gobj{
				Type_:    GSTR,
				Val_:     strconv.FormatFloat(zslNode.score, 'g', 17, 64),
				refCount: 1,
				encoding: GODIS_ENCODING_RAW})
			dictEntry := zsetObj.dict.Find(zslNode.obj)
			rdbSaveStringObject(file, dictEntry.Key)
			rdbSaveStringObject(file, dictEntry.Value)
			zslNode = zslNode.level[0].forward
		}
	case GSET:
		// o.encoding == GODIS_ENCODING_HT
		dict := o.Val_.(*Dict)
		// 先保存长度
		rdbSaveLen(file, uint32(dict.usedSize()))
		// 保存每个元素
		iter := dict.NewIterator(true) // 内层安全迭代器
		for key, _, exists := iter.Next(); exists; key, _, exists = iter.Next() {
			rdbSaveStringObject(file, key)
		}
		iter.Close()
	case GHASH:
		// o.encoding == GODIS_ENCODING_HT
		dict := o.Val_.(*Dict)
		rdbSaveLen(file, uint32(dict.usedSize()))
		iter := dict.NewIterator(true) // 内层安全迭代器
		for key, val, exists := iter.Next(); exists; key, val, exists = iter.Next() {
			rdbSaveStringObject(file, key)
			rdbSaveStringObject(file, val)
		}
		iter.Close()
	default:
		return 0, fmt.Errorf("unsupported type: %d", o.Type_)
	}

	return 1, nil
}

func rdbSaveRawString(file *os.File, s string) (int, error) {
	n, err := rdbWriteRaw(file, []byte(s))
	return n, err
}

func rdbSaveStringObject(file *os.File, o *Gobj) (int, error) {
	switch o.encoding {
	case GODIS_ENCODING_RAW:
		rdbSaveLen(file, uint32(len(o.Val_.([]byte))))
		return rdbSaveRawString(file, o.Val_.(string))
	case GODIS_ENCODING_INT:
		str := strconv.FormatInt(o.Val_.(int64), 10) // "123456789"
		rdbSaveLen(file, uint32(len(str)))
		return rdbSaveRawString(file, str)
	}
	rdbSaveLen(file, uint32(len(o.Val_.(string))))
	rdbSaveRawString(file, o.Val_.(string))
	return 0, nil
}

func rdbSaveType(file *os.File, save_type []byte) (int, error) {
	return rdbWriteRaw(file, save_type)
}
func rdbSaveTime(file *os.File, t int64) (int, error) {
	t_32 := uint32(t)
	data := []byte{
		byte(t_32 & 0xFF),
		byte((t_32 >> 8) & 0xFF),
		byte((t_32 >> 16) & 0xFF),
		byte((t_32 >> 24) & 0xFF),
	}
	return rdbWriteRaw(file, data)

}

func rdbSaveLen(file *os.File, length uint32) (int, error) {
	var buf []byte
	switch {
	case length < 1<<6: // 0xxxxxxx
		// 前两位 00，后6位是数值
		b := byte(length & 0x3F) // 0x3F = 0b111111
		buf = []byte{b}
	case length < 1<<14: // 01xxxxxx xxxxxxxx
		// 前两位 01，后14位是数值
		b0 := byte(((length >> 8) & 0x3F) | 0x40) // 0x40 = 01xxxxxx
		b1 := byte(length & 0xFF)
		buf = []byte{b0, b1}
	default: // 10...... [4字节]
		// 前两位 10，后4字节存长度
		b0 := byte(0x80) // 0x80 = 0b10000000
		b1 := byte((length >> 24) & 0xFF)
		b2 := byte((length >> 16) & 0xFF)
		b3 := byte((length >> 8) & 0xFF)
		b4 := byte(length & 0xFF)
		buf = []byte{b0, b1, b2, b3, b4}
	}

	n, err := file.Write(buf)
	return n, err
}

func rdbLoad(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return errors.New("open file error")
	}
	defer file.Close()
	expireTime := int64(-1)
	var type_ byte
	for {
		type_, _ = rdbLoadType(file)
		if type_ == GODIS_EXPIRETIME {
			// 处理过期时间
			expireTime, err = rdbLoadTime(file)
			if err != nil {
				return err
			}
			type_, err = rdbLoadType(file)
			if err != nil {
				return err
			}
		} else if type_ == GODIS_EOF {
			// 文件结束
			break
		}
		key, _ := rdbLoadStringObject(file)
		value, _ := rdbLoadObject(Gtype(type_), file)
		// 检查是否过期
		if expireTime != -1 && expireTime < GetMsTime() {
			// 过期
			key.DecrRefCount()
			value.DecrRefCount()
			continue
		}
		expireTime = -1 // 重置过期时间
		server.db.data.Set(key, value)
	}
	return nil
}

const DICT_HT_INITIAL_SIZE = 4

func rdbLoadObject(type_ Gtype, file *os.File) (*Gobj, error) {
	switch type_ {
	case GSTR:
		o, err := rdbLoadStringObject(file)
		if isInteger(o.StrVal()) {
			num, err := strconv.ParseInt(string(o.StrVal()), 10, 64)
			return CreateFromInt(num), err
		}
		return o, err
	case GSET:
		// 读取集合长度
		length, err := rdbLoadLen(file)
		if err != nil {
			return nil, err
		}
		// 创建新的集合对象
		set := CreateSetObject().Val_.(*Dict)
		if length > DICT_HT_INITIAL_SIZE {
			// TODO 直接拓展到指定大小
		}
		for i := uint64(0); i < length; i++ {
			elem, err := rdbLoadStringObject(file)
			if err != nil {
				return nil, err
			}
			set.Set(elem, nil)
		}
		return &Gobj{Type_: GSET, Val_: set, encoding: GODIS_ENCODING_HT}, nil
	case GLIST:
		// 读取列表长度
		length, err := rdbLoadLen(file)
		if err != nil {
			return nil, err
		}
		// 创建新的列表对象
		list := CreateListObject().Val_.(*List)
		for i := uint64(0); i < length; i++ {
			elem, err := rdbLoadStringObject(file)
			if err != nil {
				return nil, err
			}
			list.Append(elem)
		}
		return &Gobj{Type_: GLIST, Val_: list, encoding: GODIS_ENCODING_LINKEDLIST}, nil
	case GHASH:
		// 读取哈希表长度
		length, err := rdbLoadLen(file)
		if err != nil {
			return nil, err
		}
		// 创建新的哈希表对象
		hash := CreateHashObject().Val_.(*Dict)
		for i := uint64(0); i < length; i++ {
			key, err := rdbLoadStringObject(file)
			if err != nil {
				return nil, err
			}
			val, err := rdbLoadStringObject(file)
			if err != nil {
				return nil, err
			}
			hash.Set(key, val)
		}
		return &Gobj{Type_: GHASH, Val_: hash, encoding: GODIS_ENCODING_HT}, nil
	case GZSET:
		// 读取zset长度
		length, err := rdbLoadLen(file)
		if err != nil {
			return nil, err
		}
		// 创建新的zset对象
		zseObj := CreateZSetObject()
		for i := uint64(0); i < length; i++ {
			zsl_member_key, _ := rdbLoadStringObject(file)
			zsl_member_score, _ := rdbLoadStringObject(file)
			dic_member_key, _ := rdbLoadStringObject(file)
			dic_member_score, _ := rdbLoadStringObject(file)
			score := zsl_member_score.DoubleVal()
			zseObj.Val_.(zset).zsl.zslInsert(score, zsl_member_key)
			zseObj.Val_.(zset).dict.Set(dic_member_key, dic_member_score)
		}
		return zseObj, nil
	default:
		return nil, fmt.Errorf("unknown type: %d", type_)
	}
}

func rdbLoadLen(file *os.File) (uint64, error) {
	// 读取第一个字节以确定编码方式
	firstByte := make([]byte, 1)
	_, err := file.Read(firstByte)
	if err != nil {
		return 0, err
	}

	// 检查前两位以确定长度编码类型
	switch {
	case firstByte[0]>>6 == 0: // 00xxxxxx
		// 6位长度
		return uint64(firstByte[0] & 0x3F), nil
	case firstByte[0]>>6 == 1: // 01xxxxxx
		// 14位长度，需要读取下一个字节
		secondByte := make([]byte, 1)
		_, err := file.Read(secondByte)
		if err != nil {
			return 0, err
		}
		return uint64(firstByte[0]&0x3F)<<8 | uint64(secondByte[0]), nil
	default: // 10......
		// 6位后跟4字节长度
		buf := make([]byte, 4)
		_, err := io.ReadFull(file, buf)
		if err != nil {
			return 0, err
		}
		return uint64(buf[0])<<24 | uint64(buf[1])<<16 | uint64(buf[2])<<8 | uint64(buf[3]), nil
	}
}

func rdbLoadStringObject(file *os.File) (*Gobj, error) {
	length, err := rdbLoadLen(file)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return CreateObject(GSTR, nil), nil
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(file, buf)
	if err != nil {
		return nil, err
	}
	str := string(buf)
	return CreateObject(GSTR, str), nil
}

func isInteger(s string) bool {
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func rdbLoadTime(file *os.File) (int64, error) {
	var buf [4]byte // 使用数组而非切片，避免堆分配
	_, err := io.ReadFull(file, buf[:])
	if err != nil {
		return -1, err
	}
	return int64(buf[0]) | int64(buf[1])<<8 | int64(buf[2])<<16 | int64(buf[3])<<24, nil
}
func rdbLoadType(file *os.File) (byte, error) {
	buf := make([]byte, 1)
	_, err := file.Read(buf)
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}
