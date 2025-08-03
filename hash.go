package main

func hashTypeCreate() *Gobj {
	return CreateHashObject()
}

func (hash *Gobj) hashTypeSet(values []*Gobj) int {
	updated := 0
	hashDict := hash.Val_.(*Dict)
	// 处理字段和值的配对
	for i := 0; i < len(values); i += 2 {
		field := values[i]
		value := values[i+1]

		// 尝试添加字段-值对，如果字段已存在则更新
		err := hashDict.Add(field, value)
		if err == nil {
			// 添加成功，说明是新字段
			updated++
		} else {
			// 字段已存在，更新值
			entry := hashDict.Find(field)
			if entry != nil {
				entry.Value.DecrRefCount()
				entry.Value = value
				value.IncrRefCount()
			}
		}
	}
	return updated
}

func (hash *Gobj) hashTypeGet(field *Gobj) *Gobj {
	hashDict := hash.Val_.(*Dict)
	entry := hashDict.Get(field)
	if entry != nil {
		return entry
	}
	return nil
}
