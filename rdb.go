package main

import (
	"fmt"
	"os"
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

func rdbLoad(filename string) {

}
func loadingProgress(pos int) {

}
func rdbSaveObject(file *os.File, o *Gobj) (int, error) {
	if o.Type_ == GSTR {
		return rdbSaveStringObject(file, o)
	} else if o.Type_ == GLIST {
		// o.encoding == GODIS_ENCODING_LINKEDLIST
		list := o.Val_.(*List)
		// 先保存长度
		rdbSaveLen(file, uint32(list.Length()))
		// 保存每个元素
		for e := list.First(); e != nil; e = e.next {
			elem := e.Val
			rdbSaveStringObject(file, elem)
		}
		return 0, nil
	} else if o.Type_ == GZSET {
		// TODO
		return 0, nil
	} else if o.Type_ == GSET {
		// o.encoding == GODIS_ENCODING_HT
		// TODO NCODING_INTSET
		dict := o.Val_.(*Dict)
		// 先保存长度
		rdbSaveLen(file, uint32(dict.usedSize()))
		// 保存每个元素
		iter := dict.NewIterator(true) // 内层安全迭代器
		for key, _, exists := iter.Next(); exists; key, _, exists = iter.Next() {
			rdbSaveStringObject(file, key)
		}
		iter.Close()
	} else if o.Type_ == GHASH {
		dict := o.Val_.(*Dict)
		rdbSaveLen(file, uint32(dict.usedSize()))
		iter := dict.NewIterator(true) // 内层安全迭代器
		for key, val, exists := iter.Next(); exists; key, val, exists = iter.Next() {
			rdbSaveStringObject(file, key)
			rdbSaveStringObject(file, val)
		}
		iter.Close()
	} else {
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
		return rdbSaveRawString(file, o.Val_.(string))
	case GODIS_ENCODING_INT:
		val := o.IntVal()
		data := []byte{
			byte(val & 0xFF),
			byte((val >> 8) & 0xFF),
			byte((val >> 16) & 0xFF),
			byte((val >> 24) & 0xFF),
		}
		return rdbWriteRaw(file, data)
	}
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
