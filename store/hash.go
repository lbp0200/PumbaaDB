package store

import (
	"PumbaaDB/helper"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// 哈希操作
func (s *BadgerStore) HSet(key, field string, value interface{}) error {
	logFuncTag := "BadgerStoreHSet"
	baggerTypeKey := TypeKeyGet(key)
	bValue, err := helper.InterfaceToBytes(value)
	if err != nil {
		return fmt.Errorf("%s,%v", logFuncTag, err)
	}
	hkey := s.hashKey(key, field)
	return s.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(hkey, bValue)
		if err != nil {
			return err
		}
		return txn.Set(baggerTypeKey, []byte(KeyTypeHash))
	})
}

func (s *BadgerStore) HGet(key, field string) ([]byte, error) {
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

func (s *BadgerStore) hashKey(key, field string) []byte {
	return []byte(fmt.Sprintf("%s:%s:%s", KeyTypeHash, key, field))
}
