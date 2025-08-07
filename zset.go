package main

import "math/rand"

/*
<----- backward			forward	------->
*/
type zskiplistNode struct {
	obj      *Gobj
	score    float64
	backward *zskiplistNode
	level    []zskiplistLevel
}

type zskiplistLevel struct {
	forward *zskiplistNode
	span    uint32
}
type zskiplist struct {
	header, tail *zskiplistNode
	length       uint64
	level        int
}
type zset struct {
	dict *Dict
	zsl  *zskiplist
}

const ZSKIPLIST_MAXLEVEL = 32

func zslCreate() *zskiplist {
	zsl := zskiplist{
		level:  1,
		length: 0,
		header: zslCreateNode(ZSKIPLIST_MAXLEVEL, 0, nil),
		tail:   nil,
	}
	for i := 0; i < ZSKIPLIST_MAXLEVEL; i++ {
		zsl.header.level[i].forward = nil
		zsl.header.level[i].span = 0
	}
	zsl.header.backward = nil
	return &zsl
}
func zslCreateNode(level int, score float64, obj *Gobj) *zskiplistNode {
	return &zskiplistNode{
		level: make([]zskiplistLevel, level),
		score: score,
		obj:   obj,
	}
}

const ZSKIPLIST_P = 0.25

func zslRandomLevel() int {
	level := 1
	// 随机生成level 概率为 0.25
	for rand.Float64() < ZSKIPLIST_P && level < ZSKIPLIST_MAXLEVEL {
		level++
	}
	return level
}

func (zsl *zskiplist) zslInsert(score float64, obj *Gobj) *zskiplistNode {
	update := make([]*zskiplistNode, ZSKIPLIST_MAXLEVEL)
	x := zsl.header
	rank := make([]uint32, ZSKIPLIST_MAXLEVEL)
	for i := zsl.level - 1; i >= 0; i-- {
		/* store rank that is crossed to reach the insert position */
		if i == zsl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				x.level[i].forward.score == score && GStrEqual(x.level[i].forward.obj, obj)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}
	/* we assume the key is not already inside, since we allow duplicated
	 * scores, and the re-insertion of score and redis object should never
	 * happpen since the caller of zslInsert() should test in the hash table
	 * if the element is already inside or not. */
	level := zslRandomLevel()
	if level > zsl.level {
		for i := 0; i < level; i++ {
			x.level[i].forward = update[i].level[i].forward
			update[i].level[i].forward = x
			/* update span covered by update[i] as x is inserted here */
		}
		zsl.level = level
	}
	x = zslCreateNode(level, score, obj)

	for i := 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x
		/* update span covered by update[i] as x is inserted here */
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
	}
	/* increment span for untouched levels */
	for i := level; i < zsl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == zsl.header {
		x.backward = nil
	} else {
		x.backward = update[0]
	}

	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		zsl.tail = x
	}
	zsl.length++
	obj.IncrRefCount()
	return x
}
