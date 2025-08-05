package main

import (
	"errors"
	"math/rand"
)

const (
	INIT_SIZE    int64 = 8
	FORCE_RATIO  int64 = 2
	GROW_RATIO   int64 = 2
	DEFAULT_STEP int   = 1
)

var (
	EP_ERR = errors.New("expand error")
	EX_ERR = errors.New("key exists error")
	NK_ERR = errors.New("key doesnt exist error")
)

type Entry struct {
	Key   *Gobj
	Value *Gobj
	next  *Entry
}
type htable struct {
	table []*Entry
	size  int64
	mask  int64
	used  int64
}

// 迭代器类型
type DictIterator struct {
	d           *Dict
	table       int    // 当前遍历的表(0或1)
	index       int64  // 当前桶索引
	entry       *Entry // 当前entry
	nextEntry   *Entry // 下一个entry
	safe        bool   // 是否为安全迭代器
	fingerprint int64  // 迭代开始时字典的状态指纹
}

type DictType struct {
	HashFunc  func(key *Gobj) int64
	EqualFunc func(k1, k2 *Gobj) bool
}

type Dict struct {
	DictType
	hts           [2]*htable
	rehashidx     int64 // -1 表示未进行rehash
	safeIterators int32 // 新增：安全迭代器计数
}

func DictCreate(dictType DictType) *Dict {
	var dic Dict
	dic.DictType = dictType
	dic.rehashidx = -1
	return &dic
}

func (dict *Dict) Find(key *Gobj) *Entry {
	if dict.hts[0] == nil {
		return nil
	}
	if dict.isRehashing() {
		dict.rehashStep()
	}
	// find key in both ht
	h := dict.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				return e
			}
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	return nil
}

func (dict *Dict) expand(size int64) error {
	sz := nextPower(size)
	if dict.isRehashing() || (dict.hts[0] != nil && dict.hts[0].size >= sz) {
		return EP_ERR
	}
	ht := htable{
		table: make([]*Entry, size),
		size:  size,
		mask:  size - 1,
		used:  0,
	}
	// check for init
	if dict.hts[0] == nil {
		dict.hts[0] = &ht
		return nil
	}
	// start rehashing
	dict.hts[1] = &ht
	dict.rehashidx = 0
	return nil
}
func nextPower(size int64) int64 {
	newSize := size << 1
	if newSize < size {
		return -1
	}
	return newSize
}

func (dict *Dict) expandIfNeeded() error {
	if dict.isRehashing() {
		return nil
	}
	if dict.hts[0] == nil {
		return dict.expand(INIT_SIZE)
	}
	if (dict.hts[0].used > dict.hts[0].size) && (dict.hts[0].used/dict.hts[1].size > FORCE_RATIO) {
		return dict.expand(dict.hts[0].size * GROW_RATIO)
	}
	return nil
}

func (dict *Dict) rehashStep() {
	// TODO
	dict.rehash(DEFAULT_STEP)
}

func (dict *Dict) rehash(step int) {
	// 新增：有安全迭代器时暂停rehash
	if dict.safeIterators > 0 {
		return
	}
	for step > 0 {
		if dict.hts[0].used == 0 {
			dict.hts[0] = dict.hts[1]
			dict.hts[1] = nil
			dict.rehashidx = -1
			return
		}
		// find a slot
		for dict.hts[0].table[dict.rehashidx] == nil {
			dict.rehashidx += 1
		}
		// migrate all keys in this slot
		entry := dict.hts[0].table[dict.rehashidx]
		for entry != nil {
			ne := entry.next
			idx := dict.HashFunc(entry.Key) & dict.hts[1].mask
			entry.next = dict.hts[1].table[idx]
			dict.hts[1].table[idx] = entry
			dict.hts[0].used -= 1
			dict.hts[1].used += 1
			entry = ne
		}
		dict.hts[0].table[dict.rehashidx] = nil
		dict.rehashidx += 1
		step -= 1
	}
}

// return the index of a free slot, return -1 if the key is exists or err.
func (dict *Dict) keyIndex(key *Gobj) int64 {
	err := dict.expandIfNeeded()
	if err != nil {
		return -1
	}
	h := dict.HashFunc(key)
	var idx int64
	for i := 0; i <= 1; i++ {
		idx = h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				return -1
			}
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	return idx
}

func (dict *Dict) AddRaw(key *Gobj) *Entry {
	if dict.isRehashing() {
		dict.rehashStep()
	}
	idx := dict.keyIndex(key)
	if idx == -1 {
		return nil
	}
	// add key & return entry
	var ht *htable
	if dict.isRehashing() {
		ht = dict.hts[1]
	} else {
		ht = dict.hts[0]
	}
	var e Entry
	e.Key = key
	key.IncrRefCount()
	e.next = ht.table[idx]
	ht.table[idx] = &e
	ht.used += 1
	return &e
}

// add a new key-val pair, return err if key exists
func (dict *Dict) Add(key, val *Gobj) error {
	entry := dict.AddRaw(key)
	if entry == nil {
		return EX_ERR
	}
	entry.Value = val
	if val != nil {
		val.IncrRefCount()
	}
	return nil
}

func (dict *Dict) Set(key, val *Gobj) {
	if err := dict.Add(key, val); err == nil {
		return
	}
	entry := dict.Find(key)
	entry.Value.DecrRefCount()
	entry.Value = val
	val.IncrRefCount()
}
func (dict *Dict) Get(key *Gobj) *Gobj {
	entry := dict.Find(key)
	if entry == nil {
		return nil
	}
	return entry.Value
}
func freeEntry(entry *Entry) {
	entry.Key.DecrRefCount()
	if entry.Value != nil {
		entry.Value.DecrRefCount()
	}
}

func (dict *Dict) Delete(key *Gobj) error {
	if dict.hts[0] == nil {
		return NK_ERR
	}
	if dict.isRehashing() {
		dict.rehashStep()
	}
	// find key & delete & decr refcount
	h := dict.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		var prev *Entry
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				if prev == nil {
					dict.hts[i].table[idx] = e.next
				} else {
					prev.next = e.next
				}
				freeEntry(e)
				dict.hts[i].used--
				return nil
			}
			prev = e
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	// key doesnt exist
	return NK_ERR
}

func (dict *Dict) RandomGet() *Entry {
	if dict.hts[0] == nil {
		return nil
	}
	t := 0
	if dict.isRehashing() {
		dict.rehashStep()
		if dict.hts[1] != nil && dict.hts[1].used > dict.hts[0].used {
			// simplify the logic, random get in the bigger table
			t = 1
		}
	}
	// random slot
	idx := rand.Int63n(dict.hts[t].size)
	cnt := 0
	for dict.hts[t].table[idx] == nil && cnt < 1000 {
		idx = rand.Int63n(dict.hts[t].size)
		cnt += 1
	}
	if dict.hts[t].table[idx] == nil {
		return nil
	}

	// random entry
	var listLen int64
	p := dict.hts[t].table[idx]
	for p != nil {
		listLen += 1
		p = p.next
	}
	listIdx := rand.Int63n(listLen)
	p = dict.hts[t].table[idx]
	for i := int64(0); i < listIdx; i++ {
		p = p.next
	}
	return p
}

func (dict *Dict) usedSize() int64 {
	var retVal int64
	if dict.isRehashing() {
		retVal = dict.hts[0].used + dict.hts[1].used
	} else {
		retVal = dict.hts[0].used
	}
	return retVal
}

func (d *Dict) fingerprint() int64 {
	var hash int64 = 5381

	for i := 0; i < 2; i++ {
		ht := d.hts[i]
		if ht == nil {
			continue
		}

		// 哈希表元数据
		hash = ((hash << 5) + hash) + ht.size
		hash = ((hash << 5) + hash) + ht.used

		// 遍历所有entry
		for j := int64(0); j < ht.size; j++ {
			entry := ht.table[j]
			for entry != nil {
				hash = ((hash << 5) + hash) + d.HashFunc(entry.Key)
				// 这里要 考虑 set 结构中 value 为 空的 情况
				if entry.Value != nil {
					hash = ((hash << 5) + hash) + d.HashFunc(entry.Value)
				}
				entry = entry.next
			}
		}
	}
	return hash
}

func (d *Dict) isRehashing() bool {
	return d.rehashidx != -1
}

// NewIterator 创建迭代器
// safe=true 为安全迭代器(会阻止rehash)，safe=false 为非安全迭代器
func (d *Dict) NewIterator(safe bool) *DictIterator {
	iter := &DictIterator{
		d:     d,
		table: 0,
		index: -1,
		safe:  safe,
	}

	if safe {
		// 安全迭代器：计算初始指纹
		iter.fingerprint = d.fingerprint()
	}

	return iter
}

// Next 获取下一个键值对
func (iter *DictIterator) Next() (key, val *Gobj, exists bool) {
	for {
		if iter.entry == nil {
			// 移动到下一个桶
			ht := iter.d.hts[iter.table]

			iter.index++
			if iter.index >= ht.size {
				// 检查是否需要切换到第二个表
				if iter.d.isRehashing() && iter.table == 0 {
					iter.table++
					iter.index = 0
					ht = iter.d.hts[1]
				} else {
					return nil, nil, false
				}
			}

			iter.entry = ht.table[iter.index]
		} else {
			iter.entry = iter.nextEntry
		}

		if iter.entry != nil {
			iter.nextEntry = iter.entry.next
			return iter.entry.Key, iter.entry.Value, true
		}
	}
}

// Close 关闭迭代器
func (iter *DictIterator) Close() {
	if iter.safe {
		// 安全迭代器：检查指纹是否变化
		if iter.fingerprint != iter.d.fingerprint() {
			panic("concurrent dictionary modification detected")
		}
	}
}

// TODO 理解 这里的 迭代器 和 Rehash
