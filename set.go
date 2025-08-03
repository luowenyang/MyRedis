package main

// TODO intset （整数集合）
func SetTypeCreate() *Gobj {
	o := CreateSetObject()
	return o
}

func (set *Gobj) setTypeAdd(values []*Gobj) int64 {
	// 获取集合字典
	dict := set.Val_.(*Dict)
	added := int64(0)
	// 遍历所有要添加的成员
	for i := 0; i < len(values); i++ {
		member := values[i]
		// 尝试添加成员到集合中
		err := dict.Add(member, nil)
		if err == nil {
			// 添加成功
			added++
		}
		// 如果成员已存在（EX_ERR），我们不增加计数也不报错
		// 其他错误情况这里没有处理，但在当前实现中Add方法只返回EX_ERR或nil
	}
	// 返回添加成功的元素数量
	return added
}

func (set *Gobj) setTypeRemove(values []*Gobj) int64 {
	// 获取集合字典
	dict := set.Val_.(*Dict)
	removed := int64(0)
	for i := 0; i < len(values); i++ {
		member := values[i]
		// 尝试从集合中删除成员
		err := dict.Delete(member)
		if err == nil {
			// 删除成功
			removed++
		}
	}
	return removed
}
func (set *Gobj) setTypeSize() int64 {
	return set.Val_.(*Dict).usedSize()
}
func (set *Gobj) setTypeIsMember(key *Gobj) int8 {
	// 获取集合字典
	dict := set.Val_.(*Dict)
	if dict.Find(key) == nil {
		return 0
	}
	return 1
}
