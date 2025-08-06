package main

import (
	"strconv"
)

type Gtype byte

const (
	GSTR  Gtype = 0x00
	GLIST Gtype = 0x01
	GSET  Gtype = 0x02
	GZSET Gtype = 0x03
	GHASH Gtype = 0x04
)

type Gval interface{}

/*
type 表示 Redis 对象的逻辑类型，即这个 key 在命令层面属于哪种类型，主要有：
GSTR（字符串）、GLIST（列表）、GSET（集合）、GZSET（有序集合）和GHASH（哈希表）。
Gencoding 表示 Redis 对象的物理编码方式，即这个 key 在内存中的存储方式，
可能是原始字符串（GODIS_ENCODING_RAW）、整数（GODIS_ENCODING_INT）、
哈希表（GODIS_ENCODING_HT）、zipmap（GODIS_ENCODING_ZIPMAP）、
链表（GODIS_ENCODING_LINKEDLIST）、ziplist（GODIS_ENCODING_ZIPLIST）、
整数集合（GODIS_ENCODING_INTSET）或跳表（GODIS_ENCODING_SKIPLIST）。
这两者是不同的概念
Gtype 主要用于命令处理和逻辑判断，而 Gencoding 则用于内存优化和性能提升。
Redis 对象的类型和编码方式可以通过不同的方式进行转换和优化。
例如，字符串类型的对象可以被编码为整数以节省内存，
哈希表可以被编码为 zipmap 或 ziplist 以减少内存占用
等。
Gobj 结构体表示一个 Redis 对象，包含了类型、值、引用计数和编码方式等信息。
它是 Redis 中所有数据类型的基础结构体，所有 Redis 数据类型的对象都将基于 Gobj 进行创建和管理。


同一种 type，可能有多种 encoding。
字符串（OBJ_STRING）类型的对象可以有原始字符串（GODIS_ENCODING_RAW）或整数（GODIS_ENCODING_INT）两种编码方式。
列表（OBJ_LIST）类型的对象可以有链表（GODIS_ENCODING_LINKEDLIST）或 ziplist（GODIS_ENCODING_ZIPLIST）两种编码方式。
集合（OBJ_SET）类型的对象可以有整数集合（GODIS_ENCODING_INTSET）或哈希表（GODIS_ENCODING_HT）两种编码方式。
有序集合（OBJ_ZSET）类型的对象可以有跳表（GODIS_ENCODING_SKIPLIST）或 ziplist（GODIS_ENCODING_ZIPLIST）两种编码方式。
哈希表（OBJ_HASH）类型的对象可以有哈希表（GODIS_ENCODING_HT）或 zipmap（GODIS_ENCODING_ZIPMAP）两种编码方式。
这些编码方式的选择取决于对象的大小、访问模式和性能要求等因素
Redis 会根据对象的实际情况自动选择最合适的编码方式，以达到内存优化和性能提升的目的。
因此，同一种 type 的对象可能会有多种不同的 encoding 方式。


type：逻辑类型，决定命令语义和 API 行为
encoding：物理实现，决定内存结构和存储方式
两者解耦，便于动态优化和灵活扩展
*/

type Gobj struct {
	Type_    Gtype
	Val_     Gval
	refCount int
	encoding int
}

const (
	GODIS_ENCODING_RAW        int = 1 << iota /* Raw representation */
	GODIS_ENCODING_INT                        /* Encoded as integer */
	GODIS_ENCODING_HT                         /* Encoded as hash table */
	GODIS_ENCODING_ZIPMAP                     /* Encoded as zipmap */
	GODIS_ENCODING_LINKEDLIST                 /* Encoded as regular linked list */
	GODIS_ENCODING_ZIPLIST                    /* Encoded as ziplist */
	GODIS_ENCODING_INTSET                     /* Encoded as intset */
	GODIS_ENCODING_SKIPLIST                   /* Encoded as skiplist */
)

func (o *Gobj) IntVal() int64 {
	if o.Type_ != GSTR {
		return 0
	}
	val, _ := strconv.ParseInt(o.Val_.(string), 10, 64)
	return val
}

func (o *Gobj) IntVal_32() int {
	if o.Type_ != GSTR {
		return 0
	}
	val, _ := strconv.Atoi(o.Val_.(string))
	return val
}

func (o *Gobj) StrVal() string {
	if o.Type_ != GSTR {
		return ""
	}
	return o.Val_.(string)
}

func CreateFromInt(val int64) *Gobj {
	return &Gobj{
		Type_:    GSTR,
		Val_:     strconv.FormatInt(val, 10),
		refCount: 1,
	}
}

func CreateObject(typ Gtype, ptr interface{}) *Gobj {
	return &Gobj{
		Type_:    typ,
		Val_:     ptr,
		refCount: 1,
	}
}

func CreateSetObject() *Gobj {
	d := DictCreate(DictType{
		EqualFunc: GStrEqual,
		HashFunc:  GStrHash,
	})
	o := CreateObject(GSET, d)
	return o
}
func CreateHashObject() *Gobj {
	d := DictCreate(DictType{
		EqualFunc: GStrEqual,
		HashFunc:  GStrHash,
	})
	o := CreateObject(GHASH, d)
	return o
}
func CreateListObject() *Gobj {
	list := ListCreate(ListType{
		EqualFunc: GStrEqual,
	})
	o := CreateObject(GLIST, list)
	return o
}

func (o *Gobj) IncrRefCount() {
	o.refCount++
}

func (o *Gobj) DecrRefCount() {
	o.refCount--
	if o.refCount == 0 {
		// let GC do the work
		o.Val_ = nil
	}
}

func getDecodedObject(o *Gobj) *Gobj {
	if o.Type_ == GSTR {
		return o
	}
	// TODO For other types, we would need to decode them if necessary.
	// This is a placeholder for the actual decoding logic.
	return o
}
