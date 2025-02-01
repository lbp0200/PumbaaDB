package main

import (
	"encoding/json"
	"errors"
	"github.com/dgraph-io/badger/v3"
	"time"
)

// LIndex gets the element at index in the list stored at key.
func (db *BadgerDB) LIndex(key string, index int64) ([]byte, error) {
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

	if index < 0 {
		index += int64(len(list))
	}
	if index < 0 || index >= int64(len(list)) {
		return nil, nil
	}

	return list[index], nil
}

// LInsert inserts element in the list stored at key either before or after the reference value pivot.
func (db *BadgerDB) LInsert(key string, before bool, pivot, value []byte) (int, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var list [][]byte
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, errors.New("no such key")
	} else if err != nil {
		return 0, err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &list)
	})
	if err != nil {
		return 0, err
	}

	for i, v := range list {
		if string(v) == string(pivot) {
			if before {
				list = append(list[:i], append([][]byte{value}, list[i:]...)...)
			} else {
				list = append(list[:i+1], append([][]byte{value}, list[i+1:]...)...)
			}
			break
		}
	}

	val, err := json.Marshal(list)
	if err != nil {
		return 0, err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return 0, err
	}

	err = txn.Commit()
	if err != nil {
		return 0, err
	}

	return len(list), nil
}

// LLen gets the length of the list stored at key.
func (db *BadgerDB) LLen(key string) (int, error) {
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
		return 0, err
	}

	return len(list), nil
}

// LMove moves the first or last element of a list from a source list to a destination list.
func (db *BadgerDB) LMove(src, dst string, srcPos, dstPos string) ([]byte, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var srcList, dstList [][]byte

	// Get source list
	srcItem, err := txn.Get([]byte(src))
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return nil, err
	}
	if !errors.Is(err, badger.ErrKeyNotFound) {
		err = srcItem.Value(func(val []byte) error {
			return json.Unmarshal(val, &srcList)
		})
		if err != nil {
			return nil, err
		}
	}

	// Get destination list
	dstItem, err := txn.Get([]byte(dst))
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return nil, err
	}
	if !errors.Is(err, badger.ErrKeyNotFound) {
		err = dstItem.Value(func(val []byte) error {
			return json.Unmarshal(val, &dstList)
		})
		if err != nil {
			return nil, err
		}
	}

	if len(srcList) == 0 {
		return nil, nil
	}

	var value []byte
	if srcPos == "LEFT" {
		value = srcList[0]
		srcList = srcList[1:]
	} else if srcPos == "RIGHT" {
		value = srcList[len(srcList)-1]
		srcList = srcList[:len(srcList)-1]
	} else {
		return nil, errors.New("invalid source position")
	}

	if dstPos == "LEFT" {
		dstList = append([][]byte{value}, dstList...)
	} else if dstPos == "RIGHT" {
		dstList = append(dstList, value)
	} else {
		return nil, errors.New("invalid destination position")
	}

	srcVal, err := json.Marshal(srcList)
	if err != nil {
		return nil, err
	}
	dstVal, err := json.Marshal(dstList)
	if err != nil {
		return nil, err
	}

	err = txn.Set([]byte(src), srcVal)
	if err != nil {
		return nil, err
	}
	err = txn.Set([]byte(dst), dstVal)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return value, nil
}

// LMPOP removes and returns the first or last elements of one or more lists.
func (db *BadgerDB) LMPOP(keys []string, count int, pos string) (map[string][][]byte, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	result := make(map[string][][]byte)

	for _, key := range keys {
		var list [][]byte
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			continue
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
			continue
		}

		var values [][]byte
		if pos == "LEFT" {
			if count >= len(list) {
				values = list
				list = [][]byte{}
			} else {
				values = list[:count]
				list = list[count:]
			}
		} else if pos == "RIGHT" {
			if count >= len(list) {
				values = list
				list = [][]byte{}
			} else {
				values = list[len(list)-count:]
				list = list[:len(list)-count]
			}
		} else {
			return nil, errors.New("invalid position")
		}

		result[key] = values

		val, err := json.Marshal(list)
		if err != nil {
			return nil, err
		}

		err = txn.Set([]byte(key), val)
		if err != nil {
			return nil, err
		}
	}

	err := txn.Commit()
	if err != nil {
		return nil, err
	}

	return result, nil
}

// LPOS returns the index of matching elements inside a Redis list.
func (db *BadgerDB) LPos(key string, value []byte, rank, count int) ([]int, error) {
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

	var positions []int
	for i, v := range list {
		if string(v) == string(value) {
			if rank == 0 || (rank > 0 && len(positions)+1 == rank) || (rank < 0 && len(positions)+1 == -rank) {
				positions = append(positions, i)
			}
			if count != 0 && len(positions) == count {
				break
			}
		}
	}

	return positions, nil
}

// LPUSHX pushes the given values to the front of the list stored at key, only if key already exists and holds a list.
func (db *BadgerDB) LPushX(key string, values ...[]byte) (int, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var list [][]byte
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &list)
	})
	if err != nil {
		return 0, err
	}

	list = append(values, list...)

	val, err := json.Marshal(list)
	if err != nil {
		return 0, err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return 0, err
	}

	err = txn.Commit()
	if err != nil {
		return 0, err
	}

	return len(list), nil
}

// LPush inserts the given values to the front of the list stored at key.
func (db *BadgerDB) LPush(key string, values ...[]byte) (int, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var list [][]byte
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		// Key does not exist, create a new list
		list = [][]byte{}
	} else if err != nil {
		return 0, err
	} else {
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &list)
		})
		if err != nil {
			return 0, err
		}
	}

	// Prepend values to the list
	list = append(values, list...)

	val, err := json.Marshal(list)
	if err != nil {
		return 0, err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return 0, err
	}

	err = txn.Commit()
	if err != nil {
		return 0, err
	}

	return len(list), nil
}

// LPop removes and returns the first element of the list stored at key.
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

// LRem removes the first count occurrences of elements equal to value from the list stored at key.
func (db *BadgerDB) LRem(key string, count int, value []byte) (int, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var list [][]byte
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &list)
	})
	if err != nil {
		return 0, err
	}

	removed := 0
	if count > 0 {
		for i := 0; i < len(list) && removed < count; i++ {
			if string(list[i]) == string(value) {
				list = append(list[:i], list[i+1:]...)
				i--
				removed++
			}
		}
	} else if count < 0 {
		for i := len(list) - 1; i >= 0 && removed > count; i-- {
			if string(list[i]) == string(value) {
				list = append(list[:i], list[i+1:]...)
				removed--
			}
		}
	} else {
		for i := 0; i < len(list); i++ {
			if string(list[i]) == string(value) {
				list = append(list[:i], list[i+1:]...)
				i--
				removed++
			}
		}
	}

	val, err := json.Marshal(list)
	if err != nil {
		return 0, err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return 0, err
	}

	err = txn.Commit()
	if err != nil {
		return 0, err
	}

	return removed, nil
}

// LRange returns the specified elements of the list stored at key.
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

	if len(list) == 0 {
		return [][]byte{}, nil
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

// LSET sets the list element at index to value.
func (db *BadgerDB) LSet(key string, index int64, value []byte) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var list [][]byte
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return errors.New("no such key")
	} else if err != nil {
		return err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &list)
	})
	if err != nil {
		return err
	}

	if index < 0 {
		index += int64(len(list))
	}
	if index < 0 || index >= int64(len(list)) {
		return errors.New("index out of range")
	}

	list[index] = value

	val, err := json.Marshal(list)
	if err != nil {
		return err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

// LTRIM trims the list stored at key to the specified range.
func (db *BadgerDB) LTrim(key string, start, stop int64) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var list [][]byte
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	} else if err != nil {
		return err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &list)
	})
	if err != nil {
		return err
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
		list = [][]byte{}
	} else {
		list = list[start : stop+1]
	}

	val, err := json.Marshal(list)
	if err != nil {
		return err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

// RPop removes and returns the last element of the list stored at key.
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

// RPOPLPUSH removes the last element in a source list and pushes it to the top of a destination list.
func (db *BadgerDB) RPopLPush(src, dst string) ([]byte, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var srcList, dstList [][]byte

	// Get source list
	srcItem, err := txn.Get([]byte(src))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	err = srcItem.Value(func(val []byte) error {
		return json.Unmarshal(val, &srcList)
	})
	if err != nil {
		return nil, err
	}

	if len(srcList) == 0 {
		return nil, nil
	}

	value := srcList[len(srcList)-1]
	srcList = srcList[:len(srcList)-1]

	// Get destination list
	dstItem, err := txn.Get([]byte(dst))
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return nil, err
	}
	if !errors.Is(err, badger.ErrKeyNotFound) {
		err = dstItem.Value(func(val []byte) error {
			return json.Unmarshal(val, &dstList)
		})
		if err != nil {
			return nil, err
		}
	}

	dstList = append([][]byte{value}, dstList...)

	srcVal, err := json.Marshal(srcList)
	if err != nil {
		return nil, err
	}
	dstVal, err := json.Marshal(dstList)
	if err != nil {
		return nil, err
	}

	err = txn.Set([]byte(src), srcVal)
	if err != nil {
		return nil, err
	}
	err = txn.Set([]byte(dst), dstVal)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return value, nil
}

// RPush inserts the given values to the tail of the list stored at key.
func (db *BadgerDB) RPush(key string, values ...[]byte) (int, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var list [][]byte
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		// Key does not exist, create a new list
		list = [][]byte{}
	} else if err != nil {
		return 0, err
	} else {
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &list)
		})
		if err != nil {
			return 0, err
		}
	}

	list = append(list, values...)

	val, err := json.Marshal(list)
	if err != nil {
		return 0, err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return 0, err
	}

	err = txn.Commit()
	if err != nil {
		return 0, err
	}

	return len(list), nil
}

// RPushX pushes the given values to the tail of the list stored at key, only if key already exists and holds a list.
func (db *BadgerDB) RPushX(key string, values ...[]byte) (int, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var list [][]byte
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &list)
	})
	if err != nil {
		return 0, err
	}

	list = append(list, values...)

	val, err := json.Marshal(list)
	if err != nil {
		return 0, err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return 0, err
	}

	err = txn.Commit()
	if err != nil {
		return 0, err
	}

	return len(list), nil
}

// BLMOVE moves the first or last element of a list from a source list to a destination list with blocking.
func (db *BadgerDB) BLMOVE(src, dst string, srcPos, dstPos string, timeout time.Duration) ([]byte, error) {
	lockKey := "lock:" + src
	startTime := time.Now()

	for {
		// Try to acquire the lock
		if err := db.acquireLock(lockKey, timeout); err != nil {
			return nil, err
		}

		// Perform the LMOVE operation
		value, err := db.LMove(src, dst, srcPos, dstPos)
		if err != nil {
			db.releaseLock(lockKey)
			return nil, err
		}

		// Release the lock
		db.releaseLock(lockKey)

		if value != nil {
			return value, nil
		}

		// Release the lock and wait for the next iteration
		db.releaseLock(lockKey)

		// Check if timeout has been reached
		if time.Since(startTime) >= timeout {
			return nil, nil
		}

		// Wait for a short period before retrying
		time.Sleep(100 * time.Millisecond)
	}
}

// BLMPOP removes and returns the first or last elements of one or more lists with blocking.
func (db *BadgerDB) BLMPOP(keys []string, count int, pos string, timeout time.Duration) (map[string][][]byte, error) {
	startTime := time.Now()

	for {
		// Try to acquire locks for all keys
		for _, key := range keys {
			lockKey := "lock:" + key
			if err := db.acquireLock(lockKey, timeout); err != nil {
				return nil, err
			}
		}

		// Perform the LMPOP operation
		result, err := db.LMPOP(keys, count, pos)
		if err != nil {
			for _, key := range keys {
				lockKey := "lock:" + key
				db.releaseLock(lockKey)
			}
			return nil, err
		}

		// Release all locks
		for _, key := range keys {
			lockKey := "lock:" + key
			db.releaseLock(lockKey)
		}

		if len(result) > 0 {
			return result, nil
		}

		// Release all locks and wait for the next iteration
		for _, key := range keys {
			lockKey := "lock:" + key
			db.releaseLock(lockKey)
		}

		// Check if timeout has been reached
		if time.Since(startTime) >= timeout {
			return nil, nil
		}

		// Wait for a short period before retrying
		time.Sleep(100 * time.Millisecond)
	}
}

// BLPOP removes and returns the first element of one or more lists with blocking.
func (db *BadgerDB) BLPOP(keys []string, timeout time.Duration) ([]byte, error) {
	startTime := time.Now()

	for {
		// Try to acquire locks for all keys
		for _, key := range keys {
			lockKey := "lock:" + key
			if err := db.acquireLock(lockKey, timeout); err != nil {
				return nil, err
			}
		}

		// Perform the LPOP operation
		for _, key := range keys {
			value, err := db.LPop(key)
			if err != nil {
				for _, key := range keys {
					lockKey := "lock:" + key
					db.releaseLock(lockKey)
				}
				return nil, err
			}

			if value != nil {
				for _, key := range keys {
					lockKey := "lock:" + key
					db.releaseLock(lockKey)
				}
				return append([]byte(key), value...), nil
			}
		}

		// Release all locks and wait for the next iteration
		for _, key := range keys {
			lockKey := "lock:" + key
			db.releaseLock(lockKey)
		}

		// Check if timeout has been reached
		if time.Since(startTime) >= timeout {
			return nil, nil
		}

		// Wait for a short period before retrying
		time.Sleep(100 * time.Millisecond)
	}
}

// BRPOP removes and returns the last element of one or more lists with blocking.
func (db *BadgerDB) BRPOP(keys []string, timeout time.Duration) ([]byte, error) {
	startTime := time.Now()

	for {
		// Try to acquire locks for all keys
		for _, key := range keys {
			lockKey := "lock:" + key
			if err := db.acquireLock(lockKey, timeout); err != nil {
				return nil, err
			}
		}

		// Perform the RPOP operation
		for _, key := range keys {
			value, err := db.RPop(key)
			if err != nil {
				for _, key := range keys {
					lockKey := "lock:" + key
					db.releaseLock(lockKey)
				}
				return nil, err
			}

			if value != nil {
				for _, key := range keys {
					lockKey := "lock:" + key
					db.releaseLock(lockKey)
				}
				return append([]byte(key), value...), nil
			}
		}

		// Release all locks and wait for the next iteration
		for _, key := range keys {
			lockKey := "lock:" + key
			db.releaseLock(lockKey)
		}

		// Check if timeout has been reached
		if time.Since(startTime) >= timeout {
			return nil, nil
		}

		// Wait for a short period before retrying
		time.Sleep(100 * time.Millisecond)
	}
}

// BRPOPLPUSH removes the last element in a source list and pushes it to the top of a destination list with blocking.
func (db *BadgerDB) BRPOPLPUSH(src, dst string, timeout time.Duration) ([]byte, error) {
	lockSrcKey := "lock:" + src
	lockDstKey := "lock:" + dst
	startTime := time.Now()

	for {
		// Try to acquire locks for source and destination keys
		if err := db.acquireLock(lockSrcKey, timeout); err != nil {
			return nil, err
		}
		if err := db.acquireLock(lockDstKey, timeout); err != nil {
			db.releaseLock(lockSrcKey)
			return nil, err
		}

		// Perform the RPopLPush operation
		value, err := db.RPopLPush(src, dst)
		if err != nil {
			db.releaseLock(lockSrcKey)
			db.releaseLock(lockDstKey)
			return nil, err
		}

		// Release all locks
		db.releaseLock(lockSrcKey)
		db.releaseLock(lockDstKey)

		if value != nil {
			return value, nil
		}

		// Release all locks and wait for the next iteration
		db.releaseLock(lockSrcKey)
		db.releaseLock(lockDstKey)

		// Check if timeout has been reached
		if time.Since(startTime) >= timeout {
			return nil, nil
		}

		// Wait for a short period before retrying
		time.Sleep(100 * time.Millisecond)
	}
}

// acquireLock tries to acquire a lock with a timeout.
func (db *BadgerDB) acquireLock(lockKey string, timeout time.Duration) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	// Check if the lock already exists
	_, err := txn.Get([]byte(lockKey))
	if err == nil {
		return errors.New("lock already acquired")
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}

	// Set the lock with a TTL
	err = txn.SetEntry(badger.NewEntry([]byte(lockKey), []byte("locked")).WithTTL(timeout))
	if err != nil {
		return err
	}

	return txn.Commit()
}

// releaseLock releases a lock.
func (db *BadgerDB) releaseLock(lockKey string) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Delete([]byte(lockKey))
	if err != nil {
		return err
	}

	return txn.Commit()
}
