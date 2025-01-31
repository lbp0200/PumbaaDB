package main

import (
	"encoding/json"
	"errors"
	"github.com/dgraph-io/badger/v3"
)

func (db *BadgerDB) LPush(key string, values ...[]byte) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var list [][]byte
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		list = values
	} else if err != nil {
		return err
	} else {
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &list)
		})
		if err != nil {
			return err
		}
		list = append(values, list...)
	}

	val, err := json.Marshal(list)
	if err != nil {
		return err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (db *BadgerDB) RPush(key string, values ...[]byte) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var list [][]byte
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		list = values
	} else if err != nil {
		return err
	} else {
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &list)
		})
		if err != nil {
			return err
		}
		list = append(list, values...)
	}

	val, err := json.Marshal(list)
	if err != nil {
		return err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (db *BadgerDB) LPop(key string) ([]byte, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var list [][]byte
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &list)
	})
	if err != nil {
		return nil, err
	}

	if len(list) == 0 {
		return nil, nil
	}

	value := list[0]
	list = list[1:]

	val, err := json.Marshal(list)
	if err != nil {
		return nil, err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (db *BadgerDB) RPop(key string) ([]byte, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var list [][]byte
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &list)
	})
	if err != nil {
		return nil, err
	}

	if len(list) == 0 {
		return nil, nil
	}

	value := list[len(list)-1]
	list = list[:len(list)-1]

	val, err := json.Marshal(list)
	if err != nil {
		return nil, err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (db *BadgerDB) LRange(key string, start, stop int64) ([][]byte, error) {
	var list [][]byte
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &list)
		})
	})
	if err != nil {
		return nil, err
	}

	if start < 0 {
		start += int64(len(list))
	}
	if stop < 0 {
		stop += int64(len(list))
	}
	if start < 0 {
		start = 0
	}
	if stop >= int64(len(list)) {
		stop = int64(len(list)) - 1
	}

	if start > stop {
		return [][]byte{}, nil
	}

	return list[start : stop+1], nil
}
