package main

import (
	"github.com/dgraph-io/badger/v3"
)

// Set sets the value for a key.
func (b *BadgerDB) Set(key string, value []byte) error {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Set([]byte(key), value)
	if err != nil {
		return err
	}

	return txn.Commit()
}

// Get gets the value for a key.
func (b *BadgerDB) Get(key string) ([]byte, error) {
	var valCopy []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return valCopy, nil
}

// Del deletes a key.
func (b *BadgerDB) Del(key string) error {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Delete([]byte(key))
	if err != nil {
		return err
	}

	return txn.Commit()
}
