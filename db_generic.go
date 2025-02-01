package main

import (
	"github.com/dgraph-io/badger/v3"
	"strings"
)

// Del deletes a key.
func (db *BadgerDB) Del(key string) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	// 删除带有 hash: 前缀的 key
	hashPrefix := "hash:"
	if !strings.HasPrefix(key, hashPrefix) {
		err := txn.Delete([]byte(key))
		if err != nil {
			return err
		}
	} else {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(key)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := txn.Delete(k)
			if err != nil {
				return err
			}
		}
	}

	return txn.Commit()
}
