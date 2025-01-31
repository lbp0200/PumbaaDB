package main

import (
	"errors"
	"github.com/dgraph-io/badger/v3"
	"strconv"
	"time"
)

// Set sets the value for a key.
func (db *BadgerDB) Set(key string, value []byte, options ...string) ([]byte, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var (
		expirationTime int64
		hasExpiration  bool
		nx             bool
		xx             bool
		keepTTL        bool
		getOldValue    bool
		oldValue       []byte
		err            error
	)

	// 解析选项
	for i := 0; i < len(options); i++ {
		switch options[i] {
		case "EX":
			if i+1 >= len(options) {
				return nil, errors.New("EX option requires a time duration in seconds")
			}
			expirationTime, err = strconv.ParseInt(options[i+1], 10, 64)
			if err != nil {
				return nil, err
			}
			expirationTime = time.Now().Unix() + expirationTime
			hasExpiration = true
			i++
		case "PX":
			if i+1 >= len(options) {
				return nil, errors.New("PX option requires a time duration in milliseconds")
			}
			expirationTime, err = strconv.ParseInt(options[i+1], 10, 64)
			if err != nil {
				return nil, err
			}
			expirationTime = time.Now().UnixNano()/int64(time.Millisecond) + expirationTime
			hasExpiration = true
			i++
		case "EXAT":
			if i+1 >= len(options) {
				return nil, errors.New("EXAT option requires a timestamp in seconds")
			}
			expirationTime, err = strconv.ParseInt(options[i+1], 10, 64)
			if err != nil {
				return nil, err
			}
			hasExpiration = true
			i++
		case "PXAT":
			if i+1 >= len(options) {
				return nil, errors.New("PXAT option requires a timestamp in milliseconds")
			}
			expirationTime, err = strconv.ParseInt(options[i+1], 10, 64)
			if err != nil {
				return nil, err
			}
			hasExpiration = true
			i++
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "KEEPTTL":
			keepTTL = true
		case "GET":
			getOldValue = true
		default:
			return nil, errors.New("unknown option: " + options[i])
		}
	}

	// 检查 NX 和 XX 是否冲突
	if nx && xx {
		return nil, errors.New("NX and XX options are mutually exclusive")
	}

	// 获取键的当前值（如果需要）
	if getOldValue || xx {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return nil, err
			}
			if xx {
				return nil, nil // XX 且键不存在，不设置值
			}
		} else {
			var err error
			oldValue, err = item.ValueCopy(nil)
			if err != nil {
				return nil, err
			}
			if nx {
				return nil, nil // NX 且键存在，不设置值
			}
		}
	}

	// 设置键的值
	err = txn.Set([]byte(key), value)
	if err != nil {
		return nil, err
	}

	// 设置过期时间（如果需要）
	if hasExpiration && !keepTTL {
		err = txn.Set([]byte(key+":expire"), []byte(strconv.FormatInt(expirationTime, 10)))
		if err != nil {
			return nil, err
		}
	}

	return oldValue, txn.Commit()
}

// Get gets the value for a key.
func (db *BadgerDB) Get(key string) ([]byte, error) {
	var valCopy []byte
	err := db.db.View(func(txn *badger.Txn) error {
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
func (db *BadgerDB) Del(key string) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Delete([]byte(key))
	if err != nil {
		return err
	}

	return txn.Commit()
}

// SetNX sets the value for a key if it does not already exist.
func (db *BadgerDB) SetNX(key string, value []byte) (bool, error) {
	txn := db.db.NewTransaction(true)
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

func (db *BadgerDB) Expire(key string, expiration int64) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Set([]byte(key), []byte(strconv.FormatInt(expiration, 10)))
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (db *BadgerDB) SetEX(key string, value string, expiration int64) error {
	// 将键值对存储到数据库中
	_, err := db.Set(key, []byte(value))
	if err != nil {
		return err
	}

	// 设置键的过期时间
	expirationTime := time.Now().Unix() + expiration
	_, err = db.Set(key+":expire", []byte(strconv.FormatInt(expirationTime, 10)))
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
	_, err = db.Set(key, []byte(newVal))
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
func (db *BadgerDB) IncrBy(key string, increment int64) (int64, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()
	var currentValue int64
	item, err := txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			currentValue = 0
		}
	}
	valCopy, err := item.ValueCopy(nil)
	if err != nil {
		return 0, err
	}
	currentValue, err = strconv.ParseInt(string(valCopy), 10, 64)
	if err != nil {
		return 0, err
	}
	newValue := currentValue + increment
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
func (db *BadgerDB) IncrByFloat(key string, increment float64) (float64, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()
	var currentValue float64
	item, err := txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			currentValue = 0
		}
	}

	valCopy, err := item.ValueCopy(nil)
	if err != nil {
		return 0, err
	}
	currentValue, err = strconv.ParseFloat(string(valCopy), 64)
	if err != nil {
		return 0, err
	}
	newValue := currentValue + increment
	err = txn.Set([]byte(key), []byte(strconv.FormatFloat(newValue, 'f', -1, 64)))
	if err != nil {
		return 0, err
	}
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
func (db *BadgerDB) GetEX(key string, expiration int64) ([]byte, error) {
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

	// 设置新的过期时间
	expirationTime := time.Now().Unix() + expiration
	err = txn.Set([]byte(key+":expire"), []byte(strconv.FormatInt(expirationTime, 10)))
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
func (db *BadgerDB) GetRange(key string, start, end int64) ([]byte, error) {
	txn := db.db.NewTransaction(false)
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

	// 将值转换为字符串
	value := string(valCopy)

	// 处理负数偏移量
	if start < 0 {
		start = int64(len(value)) + start
	}
	if end < 0 {
		end = int64(len(value)) + end
	}

	// 调整范围以确保在有效范围内
	if start < 0 {
		start = 0
	}
	if end >= int64(len(value)) {
		end = int64(len(value)) - 1
	}

	// 如果起始位置大于结束位置，返回空字符串
	if start > end {
		return []byte{}, nil
	}

	// 获取子字符串
	result := value[start : end+1]
	return []byte(result), nil
}
func (db *BadgerDB) GetSet(key string, value []byte) ([]byte, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	// 获取键的当前值
	item, err := txn.Get([]byte(key))
	var valCopy []byte
	if err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return nil, err
		}
	} else {
		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
	}

	// 设置新的值
	err = txn.Set([]byte(key), value)
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

// LCS calculates the longest common subsequence of two strings.
func (db *BadgerDB) LCS(str1, str2 string) (string, error) {
	m, n := len(str1), len(str2)
	if m == 0 || n == 0 {
		return "", nil
	}

	// Create a 2D slice to store lengths of longest common subsequence.
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	// Build the dp array from bottom up.
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if str1[i-1] == str2[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else {
				dp[i][j] = MaxInt(dp[i-1][j], dp[i][j-1])
			}
		}
	}

	// Reconstruct the longest common subsequence from the dp array.
	lcs := make([]byte, 0, dp[m][n])
	i, j := m, n
	for i > 0 && j > 0 {
		if str1[i-1] == str2[j-1] {
			lcs = append(lcs, str1[i-1])
			i--
			j--
		} else if dp[i-1][j] > dp[i][j-1] {
			i--
		} else {
			j--
		}
	}

	// Reverse the lcs slice to get the correct order.
	for left, right := 0, len(lcs)-1; left < right; left, right = left+1, right-1 {
		lcs[left], lcs[right] = lcs[right], lcs[left]
	}

	return string(lcs), nil
}

func (db *BadgerDB) MGet(keys ...string) ([][]byte, error) {
	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	values := make([][]byte, len(keys))

	for i, key := range keys {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return nil, err
			}
			// 如果键不存在，返回 nil
			values[i] = nil
		} else {
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return nil, err
			}
			values[i] = valCopy
		}
	}

	return values, nil
}

func (db *BadgerDB) MSet(pairs ...string) error {
	if len(pairs)%2 != 0 {
		return errors.New("MSET requires an even number of arguments")
	}

	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	for i := 0; i < len(pairs); i += 2 {
		key := pairs[i]
		value := []byte(pairs[i+1])

		err := txn.Set([]byte(key), value)
		if err != nil {
			return err
		}
	}

	return txn.Commit()
}

func (db *BadgerDB) MSetNX(pairs ...string) (bool, error) {
	if len(pairs)%2 != 0 {
		return false, errors.New("MSETNX requires an even number of arguments")
	}

	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	// Check if all keys exist
	for i := 0; i < len(pairs); i += 2 {
		key := pairs[i]
		_, err := txn.Get([]byte(key))
		if err == nil {
			// Key already exists
			return false, nil
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			// Other error occurred
			return false, err
		}
	}

	// Set all keys if none exist
	for i := 0; i < len(pairs); i += 2 {
		key := pairs[i]
		value := []byte(pairs[i+1])

		err := txn.Set([]byte(key), value)
		if err != nil {
			return false, err
		}
	}

	return true, txn.Commit()
}

func (db *BadgerDB) PSetEX(key string, milliseconds int64, value string) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	// 设置键的值
	err := txn.Set([]byte(key), []byte(value))
	if err != nil {
		return err
	}

	// 设置键的过期时间（毫秒）
	expirationTime := time.Now().UnixNano()/int64(time.Millisecond) + milliseconds
	err = txn.Set([]byte(key+":expire"), []byte(strconv.FormatInt(expirationTime, 10)))
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (db *BadgerDB) SetRange(key string, offset int64, value string) (int, error) {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	// 获取键的当前值
	item, err := txn.Get([]byte(key))
	var currentVal []byte
	if err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return 0, err
		}
		currentVal = []byte{}
	} else {
		currentVal, err = item.ValueCopy(nil)
		if err != nil {
			return 0, err
		}
	}

	// 将偏移量转换为 int
	offsetInt := int(offset)

	// 如果偏移量大于当前值的长度，则扩展当前值
	if offsetInt > len(currentVal) {
		currentVal = append(currentVal, make([]byte, offsetInt-len(currentVal))...)
	}

	// 将新值插入到当前值的指定偏移量处
	newVal := append(currentVal[:offsetInt], append([]byte(value), currentVal[offsetInt+len(value):]...)...)

	// 设置新的值
	err = txn.Set([]byte(key), newVal)
	if err != nil {
		return 0, err
	}

	// 提交事务
	err = txn.Commit()
	if err != nil {
		return 0, err
	}

	// 返回新的字符串长度
	return len(newVal), nil
}

func (db *BadgerDB) StrLen(key string) (int, error) {
	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	// 获取键的值
	item, err := txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}

	valCopy, err := item.ValueCopy(nil)
	if err != nil {
		return 0, err
	}

	// 返回值的长度
	return len(valCopy), nil
}
