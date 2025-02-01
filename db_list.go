package main

import (
	"encoding/gob"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"strconv"
)

// List represents a Redis-like list in BadgerDB
type List struct {
	db     *badger.DB
	key    string
	length int
	start  int
	end    int
}

// NewList initializes a new List
func NewList(db *badger.DB, key string) (*List, error) {
	l := &List{
		db:    db,
		key:   key,
		start: 0,
		end:   -1,
	}

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(l.key + ":length"))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}

		var length int
		err = item.Value(func(val []byte) error {
			length, err = strconv.Atoi(string(val))
			return err
		})
		if err != nil {
			return err
		}

		l.length = length
		l.end = length - 1
		return nil
	})

	return l, err
}

// LPush adds one or more values to the head of the list
func (l *List) LPush(values ...string) error {
	for _, value := range values {
		l.start--
		err := l.db.Update(func(txn *badger.Txn) error {
			err := txn.Set([]byte(fmt.Sprintf("%s:%d", l.key, l.start)), []byte(value))
			if err != nil {
				return err
			}

			return txn.Set([]byte(l.key+":length"), []byte(strconv.Itoa(l.length+1)))
		})
		if err != nil {
			return err
		}
		l.length++
	}

	return nil
}

// RPush adds one or more values to the tail of the list
func (l *List) RPush(values ...string) error {
	for _, value := range values {
		l.end++
		err := l.db.Update(func(txn *badger.Txn) error {
			err := txn.Set([]byte(fmt.Sprintf("%s:%d", l.key, l.end)), []byte(value))
			if err != nil {
				return err
			}

			return txn.Set([]byte(l.key+":length"), []byte(strconv.Itoa(l.length+1)))
		})
		if err != nil {
			return err
		}
		l.length++
	}

	return nil
}

// LPop removes and returns the first element of the list
func (l *List) LPop() (string, error) {
	if l.length == 0 {
		return "", nil
	}

	var value string
	err := l.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("%s:%d", l.key, l.start)))
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			value = string(val)
			return nil
		})
		if err != nil {
			return err
		}

		err = txn.Delete([]byte(fmt.Sprintf("%s:%d", l.key, l.start)))
		if err != nil {
			return err
		}

		l.start++
		l.length--
		return txn.Set([]byte(l.key+":length"), []byte(strconv.Itoa(l.length)))
	})

	return value, err
}

// RPop removes and returns the last element of the list
func (l *List) RPop() (string, error) {
	if l.length == 0 {
		return "", nil
	}

	var value string
	err := l.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("%s:%d", l.key, l.end)))
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			value = string(val)
			return nil
		})
		if err != nil {
			return err
		}

		err = txn.Delete([]byte(fmt.Sprintf("%s:%d", l.key, l.end)))
		if err != nil {
			return err
		}

		l.end--
		l.length--
		return txn.Set([]byte(l.key+":length"), []byte(strconv.Itoa(l.length)))
	})

	return value, err
}

// LRange returns a slice of elements from the list
func (l *List) LRange(start, stop int) ([]string, error) {
	if start < 0 {
		start = l.length + start
	}
	if stop < 0 {
		stop = l.length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= l.length {
		stop = l.length - 1
	}

	var values []string
	err := l.db.View(func(txn *badger.Txn) error {
		for i := start; i <= stop; i++ {
			item, err := txn.Get([]byte(fmt.Sprintf("%s:%d", l.key, l.start+i)))
			if err != nil {
				return err
			}

			var value string
			err = item.Value(func(val []byte) error {
				value = string(val)
				return nil
			})
			if err != nil {
				return err
			}

			values = append(values, value)
		}

		return nil
	})

	return values, err
}

// LLen returns the length of the list
func (l *List) LLen() int {
	return l.length
}