package main

import (
	"errors"
	"github.com/dgraph-io/badger/v3"
	"strconv"
	"time"
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

// SetNX sets the value for a key if it does not already exist.
func (b *BadgerDB) SetNX(key string, value []byte) (bool, error) {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	// Check if the key already exists
	_, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		// Key does not exist, proceed to set the value
		err := txn.Set([]byte(key), value)
		if err != nil {
			return false, err
		}

		err = txn.Commit()
		if err != nil {
			return false, err
		}

		return true, nil
	} else if err != nil {
		return false, err
	}

	// Key already exists
	return false, nil
}

func (b *BadgerDB) Expire(key string, expiration int64) error {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Set([]byte(key), []byte(strconv.FormatInt(expiration, 10)))
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (b *BadgerDB) SetEX(key string, value string, expiration int64) error {
	// 将键值对存储到数据库中
	err := b.Set(key, []byte(value))
	if err != nil {
		return err
	}

	// 设置键的过期时间
	expirationTime := time.Now().Unix() + expiration
	err = b.Set(key+":expire", []byte(strconv.FormatInt(expirationTime, 10)))
	if err != nil {
		return err
	}

	return nil
}
func (db *BadgerDB) Append(key, value string) (int, error) {
	// 获取当前键的值
	currentVal, err := db.Get(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return 0, err
	}

	// 如果键不存在，则将currentVal设置为空字符串
	if errors.Is(err, badger.ErrKeyNotFound) {
		currentVal = []byte("")
	}

	// 将新值追加到当前值的末尾
	newVal := string(currentVal) + value

	// 设置新的值
	err = db.Set(key, []byte(newVal))
	if err != nil {
		return 0, err
	}

	// 返回追加操作后字符串的长度
	return len(newVal), nil
}

func (db *BadgerDB) Incr(key string) (int64, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()
	var currentValue int64
	item, err := txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			currentValue = 0
		} else {
			return 0, err
		}
	} else {
		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return 0, err
		}
		currentValue, err = strconv.ParseInt(string(valCopy), 10, 64)
		if err != nil {
			return 0, err
		}
	}
	newValue := currentValue + 1
	err = txn.Set([]byte(key), []byte(strconv.FormatInt(newValue, 10)))
	if err != nil {
		return 0, err
	}
	err = txn.Commit()
	if err != nil {
		return 0, err
	}
	return newValue, nil
}

func (db *BadgerDB) Decr(key string) (int64, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var currentValue int64
	item, err := txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			currentValue = 0
		} else {
			return 0, err
		}
	} else {
		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return 0, err
		}
		currentValue, err = strconv.ParseInt(string(valCopy), 10, 64)
		if err != nil {
			return 0, err
		}
	}

	// 执行减一操作
	newValue := currentValue - 1

	// 存储新的值
	err = txn.Set([]byte(key), []byte(strconv.FormatInt(newValue, 10)))
	if err != nil {
		return 0, err
	}

	// 提交事务
	err = txn.Commit()
	if err != nil {
		return 0, err
	}

	return newValue, nil
}
func (db *BadgerDB) DecrBy(key string, decrement int64) (int64, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var currentValue int64
	item, err := txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			currentValue = 0
		} else {
			return 0, err
		}
	} else {
		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return 0, err
		}
		currentValue, err = strconv.ParseInt(string(valCopy), 10, 64)
		if err != nil {
			return 0, err
		}
	}

	// 执行减法操作
	newValue := currentValue - decrement

	// 存储新的值
	err = txn.Set([]byte(key), []byte(strconv.FormatInt(newValue, 10)))
	if err != nil {
		return 0, err
	}

	// 提交事务
	err = txn.Commit()
	if err != nil {
		return 0, err
	}

	return newValue, nil
}
func (db *BadgerDB) GetDel(key string) ([]byte, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	// 获取键的值
	item, err := txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}

	valCopy, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	// 删除键
	err = txn.Delete([]byte(key))
	if err != nil {
		return nil, err
	}

	// 提交事务
	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return valCopy, nil
}
