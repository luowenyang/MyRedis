package main

import (
	"errors"
	"math/rand"
)

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

// TODO zslDelete()

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

func (zsl *zskiplist) zslUpdateScore(ele *Gobj, curscore float64, newscore float64) *zskiplistNode {
	update := make([]*zskiplistNode, ZSKIPLIST_MAXLEVEL)
	x := zsl.header
	/* We need to seek to element to update to start: this is useful anyway,
	 * we'll have to update or remove it. */
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < curscore ||
				x.level[i].forward.score == curscore && !GStrEqual(x.level[i].forward.obj, ele)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	/* Jump to our element: note that this function assumes that the
	 * element with the matching score exists. */
	x = x.level[0].forward
	/* If the node, after the score update, would be still exactly
	 * at the same position, we can just update the score without
	 * actually removing and re-inserting the element in the skiplist. */
	if (x.backward == nil || x.backward.score < newscore) &&
		(x.level[0].forward == nil || x.level[0].forward.score > newscore) {
		x.score = newscore
		return x
	}
	/* No way to reuse the old node: we need to remove and insert a new
	* one at a different place. */
	zsl.zslDeleteNode(x, update)
	newnode := zsl.zslInsert(newscore, x.obj)
	x.obj = nil
	return newnode
}

/*
跳表的删除操作需要解决一个核心问题：
删除一个节点后，必须更新所有指向该节点的指针（从最底层到该节点的最高层）。
update 数组的作用是 预先记录待删除节点在每一层的前驱节点，从而避免重复遍历跳表。

 update 数组的功能
存储每一层的前驱节点：
update[i] 表示在第 i 层中，待删除节点 x 的前一个节点（即需要修改指针的节点）。

指针更新的桥梁：
删除节点时，直接将 update[i]->level[i].forward 指向 x->level[i].forward，跳过待删除的 x。

*/

func (zsl *zskiplist) zslDeleteNode(x *zskiplistNode, update []*zskiplistNode) {
	for i := 0; i < zsl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].forward = x.level[i].forward
			update[i].level[i].span += x.level[i].span - 1
		} else {
			update[i].level[i].span -= 1
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		zsl.tail = x.backward
	}
	for zsl.level > 1 && zsl.header.level[zsl.level-1].forward == nil {
		zsl.level--
	}
	zsl.length--
}

func (zsl *zskiplist) zslGetElementByRank(rank int64) *zskiplistNode {
	x := zsl.header
	traversed := int64(0)
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (traversed+int64(x.level[i].span)) < rank {
			traversed += int64(x.level[i].span)
			x = x.level[i].forward
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}

func (zsl *zskiplist) zslGetElementScore(start_score float64) *zskiplistNode {
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.score < start_score {
			x = x.level[i].forward
		}
	}
	return x.level[0].forward
}

func (zset_ zset) zsetFindElement(ele *Gobj) (*zskiplistNode, []*zskiplistNode, uint64) {
	// 遍历跳表
	update := make([]*zskiplistNode, ZSKIPLIST_MAXLEVEL)
	x := zset_.zsl.header
	curscore := zset_.dict.Find(ele).Value.Val_.(float64)
	rank := uint64(0)
	/* We need to seek to element to update to start: this is useful anyway,
	 * we'll have to update or remove it. */
	for i := zset_.zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < curscore ||
				x.level[i].forward.score == curscore && !GStrEqual(x.level[i].forward.obj, ele)) {
			x = x.level[i].forward
			rank += uint64(x.level[i].span)
		}
		update[i] = x
	}
	x = x.level[0].forward
	return x, update, rank
}

func (zset_ zset) ZsetDeleteElement(ele *Gobj) {
	zset_.zsl.zslDelete(&zset_, ele)
	zset_.dict.Delete(ele)
	if zset_.zsl.length == 0 {
		//server.db.data.Delete(zset_.dict.)
	}
}

func (zsl *zskiplist) zslDelete(zset_ *zset, obj *Gobj) {
	removedEle, update, _ := zset_.zsetFindElement(obj)
	zsl.zslDeleteNode(removedEle, update)
}

func (zset_ zset) zsetRank(member *Gobj) uint64 {
	dictEntry := zset_.dict.Find(member)
	if dictEntry == nil {
		return uint64(0)
	}
	_, _, rank := zset_.zsetFindElement(member)
	return rank
}

const (
	// ZADD 命令输出标志位
	ZADD_OUT_NOP     = 1 << iota // 由于条件限制未执行操作
	ZADD_OUT_NAN                 // 分数值为 NaN (非数字)
	ZADD_OUT_ADDED               // 元素是新的且已添加
	ZADD_OUT_UPDATED             // 元素已存在，分数已更新
)

func (zset_ zset) zsetAdd(zsetObjct *Gobj, score float64, ele *Gobj, flag int) (int, float64, error) {
	out_flag := 0
	incr := (flag & ZADD_IN_INCR) != 0
	nx := (flag & ZADD_IN_NX) != 0
	xx := (flag & ZADD_IN_XX) != 0
	gt := (flag & ZADD_IN_GT) != 0
	lt := (flag & ZADD_IN_LT) != 0

	var curScore float64
	if zsetObjct.encoding != GODIS_ENCODING_SKIPLIST {
		return 0, 0, errors.New("Wrong encoding")
	}
	zs := zsetObjct.Val_.(zset)
	dicEntry := zs.dict.Find(ele)
	if dicEntry != nil {
		/* NX? Return, same element already exists. */
		if nx {
			out_flag |= ZADD_OUT_NOP
			return out_flag, curScore, nil
		}
		curScore = dicEntry.Value.Val_.(float64)

		if incr {
			score += curScore
		}

		// GT/LT? Only update if score is greater/less than current.
		if lt && score <= curScore || gt && score >= curScore {
			out_flag |= ZADD_OUT_NOP
			return out_flag, curScore, nil
		}

		// Remove and re-insert when score changes.
		if score != curScore {
			updateZslNode := zs.zsl.zslUpdateScore(ele, curScore, score)
			zs.dict.Set(ele, &Gobj{Type_: GSTR, Val_: updateZslNode.score, encoding: GODIS_ENCODING_RAW})
			out_flag |= ZADD_OUT_UPDATED
		}
		return out_flag, score, nil
	} else if !xx {
		zslNode := zset_.zsl.zslInsert(score, ele)
		zset_.dict.Set(ele, &Gobj{Type_: GSTR, Val_: zslNode.score, encoding: GODIS_ENCODING_RAW})
		out_flag |= ZADD_OUT_ADDED
		return out_flag, score, nil
	} else {
		out_flag |= ZADD_OUT_NOP
		return out_flag, curScore, nil
	}
}
