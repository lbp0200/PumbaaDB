package store

import (
	"time"

	"github.com/dgraph-io/badger/v4"
)

// 字符串操作
func (s *BadgerStore) Set(key, value []byte, ttl time.Duration) error {
    return s.db.Update(func(txn *badger.Txn) error {
        e := badger.NewEntry(key, value).WithTTL(ttl)
        return txn.SetEntry(e)
    })
}

func (s *BadgerStore) Get(key []byte) ([]byte, error) {
    var val []byte
    err := s.db.View(func(txn *badger.Txn) error {
        item, err := txn.Get(key)
        if err != nil {
            return err
        }
        val, err = item.ValueCopy(nil)
        return err
    })
    return val, err
}