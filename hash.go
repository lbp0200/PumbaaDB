package main

import (
	"encoding/json"
	"errors"
	"github.com/dgraph-io/badger/v3"
)

// HSet sets the string value of a hash field.
func (b *BadgerDB) HSet(key, field, value []byte) error {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	var hash map[string]string
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		hash = make(map[string]string)
	} else if err != nil {
		return err
	} else {
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &hash)
		})
		if err != nil {
			return err
		}
	}

	hash[string(field)] = string(value)

	val, err := json.Marshal(hash)
	if err != nil {
		return err
	}

	err = txn.Set(key, val)
	if err != nil {
		return err
	}

	return txn.Commit()
}

// HGet gets the value of a hash field.
func (b *BadgerDB) HGet(key, field []byte) ([]byte, error) {
	var hash map[string]string
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &hash)
		})
	})
	if err != nil {
		return nil, err
	}

	value, exists := hash[string(field)]
	if !exists {
		return nil, nil
	}

	return []byte(value), nil
}

// HDel deletes one or more hash fields.
func (b *BadgerDB) HDel(key []byte, fields ...[]byte) error {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	var hash map[string]string
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	} else if err != nil {
		return err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &hash)
	})
	if err != nil {
		return err
	}

	for _, field := range fields {
		delete(hash, string(field))
	}

	val, err := json.Marshal(hash)
	if err != nil {
		return err
	}

	err = txn.Set(key, val)
	if err != nil {
		return err
	}

	return txn.Commit()
}

// HGetAll gets all the fields and values in a hash.
func (b *BadgerDB) HGetAll(key []byte) (map[string]string, error) {
	var hash map[string]string
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &hash)
		})
	})
	if err != nil {
		return nil, err
	}

	return hash, nil
}

// HKeys gets all the fields in a hash.
func (b *BadgerDB) HKeys(key []byte) ([]string, error) {
	var hash map[string]string
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &hash)
		})
	})
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(hash))
	for key := range hash {
		keys = append(keys, key)
	}

	return keys, nil
}

// HVals gets all the values in a hash.
func (b *BadgerDB) HVals(key []byte) ([]string, error) {
	var hash map[string]string
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &hash)
		})
	})
	if err != nil {
		return nil, err
	}

	values := make([]string, 0, len(hash))
	for _, value := range hash {
		values = append(values, value)
	}

	return values, nil
}

// HExists determines if a hash field exists.
func (b *BadgerDB) HExists(key, field []byte) (bool, error) {
	var hash map[string]string
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &hash)
		})
	})
	if err != nil {
		return false, err
	}

	_, exists := hash[string(field)]
	return exists, nil
}
