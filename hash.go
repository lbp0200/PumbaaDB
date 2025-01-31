package main

import (
	"encoding/json"
	"errors"
	"github.com/dgraph-io/badger/v3"
	"strconv"
	"time"
)

// HSet sets the string value of a hash field.
func (db *BadgerDB) HSet(key, field, value []byte) error {
	txn := db.db.NewTransaction(true)
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
func (db *BadgerDB) HGet(key, field []byte) ([]byte, error) {
	var hash map[string]string
	err := db.db.View(func(txn *badger.Txn) error {
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
func (db *BadgerDB) HDel(key []byte, fields ...[]byte) error {
	txn := db.db.NewTransaction(true)
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
func (db *BadgerDB) HGetAll(key []byte) (map[string]string, error) {
	var hash map[string]string
	err := db.db.View(func(txn *badger.Txn) error {
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
func (db *BadgerDB) HKeys(key []byte) ([]string, error) {
	var hash map[string]string
	err := db.db.View(func(txn *badger.Txn) error {
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
func (db *BadgerDB) HVals(key []byte) ([]string, error) {
	var hash map[string]string
	err := db.db.View(func(txn *badger.Txn) error {
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
func (db *BadgerDB) HExists(key, field []byte) (bool, error) {
	var hash map[string]string
	err := db.db.View(func(txn *badger.Txn) error {
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
func (db *BadgerDB) HExpire(key, field []byte, expiration int64, options ...string) (bool, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var (
		//hasExpiration bool
		nx  bool
		xx  bool
		gt  bool
		lt  bool
		err error
	)

	// 解析选项
	for _, option := range options {
		switch option {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GT":
			gt = true
		case "LT":
			lt = true
		default:
			return false, errors.New("unknown option: " + option)
		}
	}

	// 检查 NX 和 XX 是否冲突
	if nx && xx {
		return false, errors.New("NX and XX options are mutually exclusive")
	}

	// 获取键的当前值
	var hash map[string]string
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		if xx {
			return false, nil // XX 且键不存在，不设置过期时间
		}
		hash = make(map[string]string)
	} else if err != nil {
		return false, err
	} else {
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &hash)
		})
		if err != nil {
			return false, err
		}
	}

	// 获取字段的当前过期时间
	expirationKey := string(key) + ":" + string(field) + ":expire"
	expirationItem, err := txn.Get([]byte(expirationKey))
	var currentExpiration int64
	if errors.Is(err, badger.ErrKeyNotFound) {
		currentExpiration = 0
	} else if err != nil {
		return false, err
	} else {
		err = expirationItem.Value(func(val []byte) error {
			currentExpiration, err = strconv.ParseInt(string(val), 10, 64)
			return err
		})
		if err != nil {
			return false, err
		}
	}

	// 检查 NX 和 XX 选项
	if nx && currentExpiration > 0 {
		return false, nil // NX 且字段已存在，不设置过期时间
	}
	if xx && currentExpiration == 0 {
		return false, nil // XX 且字段不存在，不设置过期时间
	}

	// 检查 GT 和 LT 选项
	if gt && expiration <= currentExpiration {
		return false, nil // GT 且新过期时间不大于当前过期时间，不设置过期时间
	}
	if lt && expiration >= currentExpiration {
		return false, nil // LT 且新过期时间不小于当前过期时间，不设置过期时间
	}

	// 设置新的过期时间
	expirationTime := time.Now().Unix() + expiration
	err = txn.Set([]byte(expirationKey), []byte(strconv.FormatInt(expirationTime, 10)))
	if err != nil {
		return false, err
	}

	return true, txn.Commit()
}
