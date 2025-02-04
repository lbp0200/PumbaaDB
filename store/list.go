package store

import (
	"PumbaaDB/helper"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

// store/badger_store.go
type ListNode struct {
    ID   string
    Value []byte
    Prev  string
    Next  string
}

func (s *BadgerStore) listKey(key []byte, parts ...string) []byte {
    return []byte(fmt.Sprintf("list:%s:%s", key, strings.Join(parts, ":")))
}

// store/badger_store.go
func (s *BadgerStore) listCreate(key []byte) error {
    return s.db.Update(func(txn *badger.Txn) error {
        // 初始化链表元数据
        lengthKey := s.listKey(key, "length")
        if err := txn.Set(lengthKey,helper.Uint64ToBytes(0)); err != nil {
            return err
        }
        startKey := s.listKey(key, "start")
        if err := txn.Set(startKey, []byte{}); err != nil {
            return err
        }
        endKey := s.listKey(key, "end")
        return txn.Set(endKey, []byte{})
    })
}

func (s *BadgerStore) listGetMeta(key []byte) (length uint64, start, end string, err error) {
    err = s.db.View(func(txn *badger.Txn) error {
        // 获取长度
        lengthItem, err := txn.Get(s.listKey(key, "length"))
        if err != nil {
            return err
        }
        lengthVal, _ := lengthItem.ValueCopy(nil)
        length =helper.BytesToUint64(lengthVal)
        
        // 获取起始节点
        startItem, err := txn.Get(s.listKey(key, "start"))
        if err == nil {
            startVal, _ := startItem.ValueCopy(nil)
            start = string(startVal)
        }
        
        // 获取结束节点
        endItem, err := txn.Get(s.listKey(key, "end"))
        if err == nil {
            endVal, _ := endItem.ValueCopy(nil)
            end = string(endVal)
        }
        return nil
    })
    return
}

func (s *BadgerStore) listUpdateMeta(txn *badger.Txn, key []byte, length uint64, start, end string) error {
    // 更新长度
    if err := txn.Set(s.listKey(key, "length"),helper.Uint64ToBytes(length)); err != nil {
        return err
    }
    // 更新起始节点
    if err := txn.Set(s.listKey(key, "start"), []byte(start)); err != nil {
        return err
    }
    // 更新结束节点
    return txn.Set(s.listKey(key, "end"), []byte(end))
}

func (s *BadgerStore) createNode(txn *badger.Txn, key []byte, value []byte) (string, error) {
    nodeID := uuid.New().String()
    nodeKey := s.listKey(key, nodeID)
    if err := txn.Set(nodeKey, value); err != nil {
        return "", err
    }
    return nodeID, nil
}

func (s *BadgerStore) linkNodes(txn *badger.Txn, key []byte, prevID, nextID string) error {
    // 更新前节点的next指针
    if prevID != "" {
        prevNextKey := s.listKey(key, prevID, "next")
        if err := txn.Set(prevNextKey, []byte(nextID)); err != nil {
            return err
        }
    }
    // 更新后节点的prev指针
    if nextID != "" {
        nextPrevKey := s.listKey(key, nextID, "prev")
        return txn.Set(nextPrevKey, []byte(prevID))
    }
    return nil
}

// LPUSH 实现
func (s *BadgerStore) LPush(key []byte, values ...[]byte) (int, error) {
    var newLength uint64
    err := s.db.Update(func(txn *badger.Txn) error {
        length, start, end, _ := s.listGetMeta(key)
        
        for _, value := range values {
            // 创建新节点
            nodeID, err := s.createNode(txn, key, value)
            if err != nil {
                return err
            }
            
            // 链接节点
            if length == 0 {  // 空链表
                start = nodeID
                end = nodeID
                if err := s.linkNodes(txn, key, nodeID, nodeID); err != nil {
                    return err
                }
            } else {
                // 链接新节点和原头节点
                if err := s.linkNodes(txn, key, nodeID, start); err != nil {
                    return err
                }
                // 更新原头节点的prev指针
                if err := txn.Set(s.listKey(key, start, "prev"), []byte(nodeID)); err != nil {
                    return err
                }
                start = nodeID
            }
            length++
        }
        
        // 更新元数据
        return s.listUpdateMeta(txn, key, length, start, end)
    })
    return int(newLength), err
}

// RPOP 实现
func (s *BadgerStore) RPop(key []byte) ([]byte, error) {
    var value []byte
    err := s.db.Update(func(txn *badger.Txn) error {
        length, start, end, err := s.listGetMeta(key)
        if length == 0 {
            return nil
        }
        
        // 获取尾节点值
        endNodeKey := s.listKey(key, end)
        item, err := txn.Get(endNodeKey)
        if err != nil {
            return err
        }
        value, _ = item.ValueCopy(nil)
        
        // 获取新的尾节点
        newEndKey := s.listKey(key, end, "prev")
        item, err = txn.Get(newEndKey)
        if err != nil {
            return err
        }
        newEndVal, _ := item.ValueCopy(nil)	
        newEnd := string(newEndVal)
        
        // 更新链表关系
        if length == 1 {
            start = ""
            newEnd = ""
        } else {
            // 断开旧尾节点连接
            if err := s.linkNodes(txn, key, newEnd, start); err != nil {
                return err
            }
        }
        
        // 删除旧节点数据
        if err := txn.Delete(endNodeKey); err != nil {
            return err
        }
        txn.Delete(s.listKey(key, end, "prev"))
        txn.Delete(s.listKey(key, end, "next"))
        
        // 更新元数据
        return s.listUpdateMeta(txn, key, length-1, start, newEnd)
    })
    return value, err
}

// LLEN 实现
func (s *BadgerStore) LLen(key []byte) (int, error) {
    length, _, _, err := s.listGetMeta(key)
    return int(length), err
}

