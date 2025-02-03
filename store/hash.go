package store

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// 哈希操作
func (s *BadgerStore) HSet(key, field, value []byte) error {
    hkey := s.hashKey(key, field)
    return s.db.Update(func(txn *badger.Txn) error {
        return txn.Set(hkey, value)
    })
}

func (s *BadgerStore) HGet(key, field []byte) ([]byte, error) {
    hkey := s.hashKey(key, field)
    var val []byte
    err := s.db.View(func(txn *badger.Txn) error {
        item, err := txn.Get(hkey)
        if err != nil {
            return err
        }
        val, err = item.ValueCopy(nil)
        return err
    })
    return val, err
}

func (s *BadgerStore) hashKey(key, field []byte) []byte {
    return []byte(fmt.Sprintf("h:%s:%s", key, field))
}