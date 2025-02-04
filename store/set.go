package store

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// 添加元素时更新 SCARD 值
func (s *BadgerStore) SADD(key, value []byte) error {
    countKey := []byte(fmt.Sprintf("set:%s:count", key))
    return s.db.Update(func(txn *badger.Txn) error {
        // 先添加元素
        setKey := []byte(fmt.Sprintf("set:%s:%s", key, value))
        if err := txn.Set(setKey, []byte{}); err != nil {
            return err
        }
        // 再更新计数
        item, err := txn.Get(countKey)
        var count uint64
        if err != nil {
            if err == badger.ErrKeyNotFound {
                count = 1
            } else {
                return err
            }
        } else {
            val, err := item.ValueCopy(nil)
            if err != nil {
                return err
            }
            count = binary.BigEndian.Uint64(val) + 1
        }
        countBytes := make([]byte, 8)
        binary.BigEndian.PutUint64(countBytes, count)
        return txn.Set(countKey, countBytes)
    })
}

// SMembers 获取Set中的所有元素
func (s *BadgerStore) SMembers(key []byte) ([][]byte, error) {
	var members [][]byte
	err := s.db.View(func(txn *badger.Txn) error {
		// 获取集合的键
		setKey := append([]byte("set:"), key...)
		// 创建迭代器，用于遍历集合中的元素
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		// 寻找集合的起始键
		for it.Seek(setKey); it.ValidForPrefix(setKey); it.Next() {
			item := it.Item()
			// 获取成员的键
			memberKey := item.Key()
			// 提取成员
			member := memberKey[len(setKey)+1:]
			members = append(members, member)
		}
		return nil
	})
	return members, err
}

// SIsMember 检查元素是否在Set中
func (s *BadgerStore) SIsMember(key, member []byte) (bool, error) {
	var exists bool
	err := s.db.View(func(txn *badger.Txn) error {
		// 获取集合的键
		setKey := append([]byte("set:"), key...)
		// 获取成员的键
		memberKey := append(setKey, []byte(":")...,)
		memberKey = append(memberKey, member...)
		// 检查成员是否存在
		_, err := txn.Get(memberKey)
		if err == nil {
			exists = true
		} else if err == badger.ErrKeyNotFound {
			exists = false
		} else {
			return err
		}
		return nil
	})
	return exists, err
}

// SRem 从Set中移除元素
func (s *BadgerStore) SRem(key, member []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// 获取集合的键
		setKey := append([]byte("set:"), key...)
		// 获取成员的键
		memberKey := append(setKey, []byte(":")...)
		memberKey = append(memberKey, member...)
		// 删除成员
		return txn.Delete(memberKey)
	})
}

// 提前存储时的 SCARD 实现
func (s *BadgerStore) SCARD(key []byte) (int, error) {
    var count int
    countKey := []byte(fmt.Sprintf("set:%s:count", key))
    err := s.db.View(func(txn *badger.Txn) error {
        item, err := txn.Get(countKey)
        if err != nil {
            if err == badger.ErrKeyNotFound {
                // 如果计数键不存在，需要重新计算
                // 这里可以调用不提前存储时的计算逻辑
                // 计算完成后更新计数键
                return nil
            }
            return err
        }
        val, err := item.ValueCopy(nil)
        if err != nil {
            return err
        }
        count = int(binary.BigEndian.Uint64(val))
        return nil
    })
    if err != nil {
        return 0, err
    }
    return count, nil
}

// SDiff 计算并返回多个集合的差集
func (s *BadgerStore) SDiff(keys ...[]byte) ([][]byte, error) {
    if len(keys) < 2 {
        return nil, fmt.Errorf("ERR wrong number of arguments for 'SDIFF' command")
    }

    // 获取第一个集合的数据作为初始差集
    firstSet, err := s.getSetMembers(keys[0])
    if err != nil {
        return nil, err
    }

    // 计算后续集合与初始差集的差集
    for _, key := range keys[1:] {
        otherSet, err := s.getSetMembers(key)
        if err != nil {
            return nil, err
        }
        firstSet = difference(firstSet, otherSet)
    }

    return firstSet, nil
}

// getSetMembers 获取集合的成员
func (s *BadgerStore) getSetMembers(key []byte) ([][]byte, error) {
    var members [][]byte
    err := s.db.View(func(txn *badger.Txn) error {
        item, err := txn.Get(key)
        if err != nil {
            if err == badger.ErrKeyNotFound {
                return nil
            }
            return err
        }
        val, err := item.ValueCopy(nil)
        if err != nil {
            return err
        }
        // 假设集合成员以字节数组切片的形式存储
        members = deserializeSetMembers(val)
        return nil
    })
    return members, err
}

// difference 计算两个集合的差集
func difference(set1, set2 [][]byte) [][]byte {
    result := make([][]byte, 0)
    for _, member := range set1 {
        if !contains(set2, member) {
            result = append(result, member)
        }
    }
    return result
}

// contains 检查集合中是否包含指定成员
func contains(set [][]byte, member []byte) bool {
    for _, m := range set {
        if bytes.Equal(m, member) {
            return true
        }
    }
    return false
}

// deserializeSetMembers 反序列化集合成员
func deserializeSetMembers(data []byte) [][]byte {
    // 简单示例，假设集合成员以字节数组切片的形式存储
    // 实际应用中可能需要更复杂的序列化和反序列化逻辑
    return bytes.Split(data, []byte(","))
}