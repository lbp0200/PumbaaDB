package main

import (
	"errors"
	"github.com/dgraph-io/badger/v3"
	"github.com/goccy/go-json"
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

// HEXPIREAT sets the expiration time of a hash field to a specific timestamp.
func (db *BadgerDB) HEXPIREAT(key, field []byte, expireAt int64, options ...string) (bool, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var (
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
	if gt && expireAt <= currentExpiration {
		return false, nil // GT 且新过期时间不大于当前过期时间，不设置过期时间
	}
	if lt && expireAt >= currentExpiration {
		return false, nil // LT 且新过期时间不小于当前过期时间，不设置过期时间
	}

	// 设置新的过期时间
	err = txn.Set([]byte(expirationKey), []byte(strconv.FormatInt(expireAt, 10)))
	if err != nil {
		return false, err
	}

	return true, txn.Commit()
}

// HExpireTime gets the expiration time of a hash field.
func (db *BadgerDB) HExpireTime(key, field []byte) (int64, error) {
	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	// 获取字段的当前过期时间
	expirationKey := string(key) + ":" + string(field) + ":expire"
	expirationItem, err := txn.Get([]byte(expirationKey))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil // 字段不存在过期时间
	} else if err != nil {
		return 0, err
	}

	var currentExpiration int64
	err = expirationItem.Value(func(val []byte) error {
		currentExpiration, err = strconv.ParseInt(string(val), 10, 64)
		return err
	})
	if err != nil {
		return 0, err
	}

	return currentExpiration, nil
}

// HIncrBy increments the integer value of a hash field by the given amount.
func (db *BadgerDB) HIncrBy(key, field []byte, increment int64) (int64, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var hash map[string]string
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		hash = make(map[string]string)
	} else if err != nil {
		return 0, err
	} else {
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &hash)
		})
		if err != nil {
			return 0, err
		}
	}

	// 获取当前字段值
	currentValueStr, exists := hash[string(field)]
	var currentValue int64
	if exists {
		currentValue, err = strconv.ParseInt(currentValueStr, 10, 64)
		if err != nil {
			return 0, errors.New("field value is not an integer")
		}
	}

	// 计算新值
	newValue := currentValue + increment

	// 更新哈希字段
	hash[string(field)] = strconv.FormatInt(newValue, 10)

	// 序列化并设置新的哈希值
	val, err := json.Marshal(hash)
	if err != nil {
		return 0, err
	}

	err = txn.Set(key, val)
	if err != nil {
		return 0, err
	}

	err = txn.Commit()
	if err != nil {
		return 0, err
	}

	return newValue, nil
}

// HIncrByFloat increments the float value of a hash field by the given amount.
func (db *BadgerDB) HIncrByFloat(key, field []byte, increment float64) (float64, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var hash map[string]string
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		hash = make(map[string]string)
	} else if err != nil {
		return 0, err
	} else {
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &hash)
		})
		if err != nil {
			return 0, err
		}
	}

	// 获取当前字段值
	currentValueStr, exists := hash[string(field)]
	var currentValue float64
	if exists {
		currentValue, err = strconv.ParseFloat(currentValueStr, 64)
		if err != nil {
			return 0, errors.New("field value is not a float")
		}
	}

	// 计算新值
	newValue := currentValue + increment

	// 更新哈希字段
	hash[string(field)] = strconv.FormatFloat(newValue, 'f', -1, 64)

	// 序列化并设置新的哈希值
	val, err := json.Marshal(hash)
	if err != nil {
		return 0, err
	}

	err = txn.Set(key, val)
	if err != nil {
		return 0, err
	}

	err = txn.Commit()
	if err != nil {
		return 0, err
	}

	return newValue, nil
}
