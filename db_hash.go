package main

import (
	"errors"
	"github.com/dgraph-io/badger/v3"
	"github.com/golang/snappy"
	"math/rand"
	"path/filepath"
	"strconv"
	"time"
)

const hashPrefix = "hash:"

// HSet sets the string value of a hash field.
func (db *BadgerDB) HSet(key, field, value []byte) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	storeKey := append([]byte(hashPrefix), append(key, field...)...)
	err := txn.Set(storeKey, snappy.Encode(nil, value))
	if err != nil {
		return err
	}

	return txn.Commit()
}

// HGet gets the value of a hash field.
func (db *BadgerDB) HGet(key, field []byte) ([]byte, error) {
	storeKey := append([]byte(hashPrefix), append(key, field...)...)
	var val []byte
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(storeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		val, err = snappy.Decode(nil, val)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return val, nil
}

// HDel deletes one or more hash fields.
func (db *BadgerDB) HDel(key []byte, fields ...[]byte) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	for _, field := range fields {
		storeKey := append([]byte(hashPrefix), append(key, field...)...)
		err := txn.Delete(storeKey)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
	}

	return txn.Commit()
}

// HGetAll gets all the fields and values in a hash.
func (db *BadgerDB) HGetAll(key []byte) (map[string]string, error) {
	hash := make(map[string]string)
	err := db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := append([]byte(hashPrefix), key...)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			v, err = snappy.Decode(nil, v)
			if err != nil {
				return err
			}
			field := k[len(prefix):]
			hash[string(field)] = string(v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return hash, nil
}

// HKeys gets all the fields in a hash.
func (db *BadgerDB) HKeys(key []byte) ([]string, error) {
	keys := []string{}
	err := db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := append([]byte(hashPrefix), key...)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			field := k[len(prefix):]
			keys = append(keys, string(field))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}

// HVals gets all the values in a hash.
func (db *BadgerDB) HVals(key []byte) ([]string, error) {
	values := []string{}
	err := db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := append([]byte(hashPrefix), key...)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			v, err = snappy.Decode(nil, v)
			if err != nil {
				return err
			}
			values = append(values, string(v))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return values, nil
}

// HExists determines if a hash field exists.
func (db *BadgerDB) HExists(key, field []byte) (bool, error) {
	storeKey := append([]byte(hashPrefix), append(key, field...)...)
	err := db.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(storeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	return true, nil
}

// HExpire sets the expiration time of a hash field to a specific duration.
func (db *BadgerDB) HExpire(key, field []byte, expiration int64, options ...string) (bool, error) {
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

	// 获取字段的当前过期时间
	expirationKey := string(hashPrefix) + string(key) + ":" + string(field) + ":expire"
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

	// 获取字段的当前过期时间
	expirationKey := string(hashPrefix) + string(key) + ":" + string(field) + ":expire"
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

// HEXPIRETIME gets the expiration time of a hash field.
func (db *BadgerDB) HEXPIRETIME(key, field []byte) (int64, error) {
	expirationKey := string(hashPrefix) + string(key) + ":" + string(field) + ":expire"
	var expirationTime int64
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(expirationKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		expirationTime, err = strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return expirationTime, nil
}

// HIncrBy increments the integer value of a hash field by the given amount.
func (db *BadgerDB) HIncrBy(key, field []byte, increment int64) (int64, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	storeKey := append([]byte(hashPrefix), append(key, field...)...)
	var currentValue int64
	item, err := txn.Get(storeKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		currentValue = 0
	} else if err != nil {
		return 0, err
	} else {
		val, err := item.ValueCopy(nil)
		if err != nil {
			return 0, err
		}
		val, err = snappy.Decode(nil, val)
		if err != nil {
			return 0, err
		}
		currentValue, err = strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return 0, errors.New("field value is not an integer")
		}
	}

	// 计算新值
	newValue := currentValue + increment

	// 更新哈希字段
	err = txn.Set(storeKey, snappy.Encode(nil, []byte(strconv.FormatInt(newValue, 10))))
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

	storeKey := append([]byte(hashPrefix), append(key, field...)...)
	var currentValue float64
	item, err := txn.Get(storeKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		currentValue = 0
	} else if err != nil {
		return 0, err
	} else {
		val, err := item.ValueCopy(nil)
		if err != nil {
			return 0, err
		}
		val, err = snappy.Decode(nil, val)
		if err != nil {
			return 0, err
		}
		currentValue, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			return 0, errors.New("field value is not a float")
		}
	}

	// 计算新值
	newValue := currentValue + increment

	// 更新哈希字段
	err = txn.Set(storeKey, snappy.Encode(nil, []byte(strconv.FormatFloat(newValue, 'f', -1, 64))))
	if err != nil {
		return 0, err
	}

	err = txn.Commit()
	if err != nil {
		return 0, err
	}

	return newValue, nil
}

// HLen gets the number of fields in a hash.
func (db *BadgerDB) HLen(key []byte) (int, error) {
	var count int
	err := db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := append([]byte(hashPrefix), key...)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return count, nil
}

// HMGet gets the values of all the given hash fields.
func (db *BadgerDB) HMGet(key []byte, fields ...[]byte) ([]interface{}, error) {
	values := make([]interface{}, len(fields))
	err := db.db.View(func(txn *badger.Txn) error {
		for i, field := range fields {
			storeKey := append([]byte(hashPrefix), append(key, field...)...)
			item, err := txn.Get(storeKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				values[i] = nil
				continue
			} else if err != nil {
				return err
			}

			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			val, err = snappy.Decode(nil, val)
			if err != nil {
				return err
			}
			values[i] = string(val)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return values, nil
}

// HMSet sets multiple hash fields to multiple values.
func (db *BadgerDB) HMSet(key []byte, fieldsValues ...[]byte) error {
	if len(fieldsValues)%2 != 0 {
		return errors.New("wrong number of arguments for HMSET")
	}

	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	for i := 0; i < len(fieldsValues); i += 2 {
		field := fieldsValues[i]
		value := fieldsValues[i+1]
		storeKey := append([]byte(hashPrefix), append(key, field...)...)
		err := txn.Set(storeKey, snappy.Encode(nil, value))
		if err != nil {
			return err
		}
	}

	return txn.Commit()
}

// HPersist removes the expiration time of a hash field.
func (db *BadgerDB) HPersist(key, field []byte) (bool, error) {
	expirationKey := string(hashPrefix) + string(key) + ":" + string(field) + ":expire"
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Delete([]byte(expirationKey))
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return false, err
	}

	return true, txn.Commit()
}

// HPEXPIRE sets the expiration time of a hash field to a specific duration.
func (db *BadgerDB) HPEXPIRE(key, field []byte, expiration int64, options ...string) (bool, error) {
	return db.HExpire(key, field, expiration, options...)
}

// HPEXPIREAT sets the expiration time of a hash field to a specific timestamp.
func (db *BadgerDB) HPEXPIREAT(key, field []byte, expireAt int64, options ...string) (bool, error) {
	return db.HEXPIREAT(key, field, expireAt, options...)
}

// HPEXPIRETIME gets the expiration time of a hash field.
func (db *BadgerDB) HPEXPIRETIME(key, field []byte) (int64, error) {
	return db.HEXPIRETIME(key, field)
}

// HPTTL gets the remaining time to live of a hash field.
func (db *BadgerDB) HPTTL(key, field []byte) (int64, error) {
	expirationTime, err := db.HEXPIRETIME(key, field)
	if err != nil {
		return -2, err
	}
	if expirationTime == 0 {
		return -1, nil
	}

	return expirationTime - time.Now().Unix(), nil
}

// HRandField returns a random field from a hash.
func (db *BadgerDB) HRandField(key []byte, count int) ([]string, error) {
	var fields []string
	err := db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := append([]byte(hashPrefix), key...)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			field := k[len(prefix):]
			fields = append(fields, string(field))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if count == 0 {
		if len(fields) == 0 {
			return nil, nil
		}
		return []string{fields[rand.Intn(len(fields))]}, nil
	}

	if count < 0 {
		count = -count
		if count > len(fields) {
			count = len(fields)
		}
		rand.Shuffle(len(fields), func(i, j int) {
			fields[i], fields[j] = fields[j], fields[i]
		})
		return fields[:count], nil
	}

	if count > len(fields) {
		count = len(fields)
	}
	rand.Shuffle(len(fields), func(i, j int) {
		fields[i], fields[j] = fields[j], fields[i]
	})
	return fields[:count], nil
}

// HScan iterates incrementally over a hash.
func (db *BadgerDB) HScan(key []byte, cursor uint64, match string, count int) ([]string, uint64, error) {
	var fields []string
	err := db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := append([]byte(hashPrefix), key...)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			field := k[len(prefix):]

			if match == "" {
				fields = append(fields, string(field))
			} else {
				matched, err := filepath.Match(match, string(field))
				if err != nil {
					return err
				}
				if matched {
					fields = append(fields, string(field))
				}
			}

			if len(fields) >= count {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	if len(fields) == 0 {
		return nil, 0, nil
	}

	return fields, 1, nil
}

// HSetNX sets the string value of a hash field, only if the field does not exist.
func (db *BadgerDB) HSetNX(key, field, value []byte) (bool, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	storeKey := append([]byte(hashPrefix), append(key, field...)...)
	_, err := txn.Get(storeKey)
	if !errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}

	err = txn.Set(storeKey, snappy.Encode(nil, value))
	if err != nil {
		return false, err
	}

	return true, txn.Commit()
}

// HStrLen gets the length of the value of a hash field.
func (db *BadgerDB) HStrLen(key, field []byte) (int, error) {
	storeKey := append([]byte(hashPrefix), append(key, field...)...)
	var val []byte
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(storeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		val, err = snappy.Decode(nil, val)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return len(val), nil
}

// HTTL gets the remaining time to live of a hash field.
func (db *BadgerDB) HTTL(key, field []byte) (int64, error) {
	return db.HPTTL(key, field)
}
