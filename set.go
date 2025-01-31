package main

import (
	"encoding/json"
	"errors"
	"github.com/dgraph-io/badger/v3"
)

// SAdd adds the specified members to the set stored at key.
func (db *BadgerDB) SAdd(key string, members ...[]byte) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var set map[string]struct{}
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		set = make(map[string]struct{})
	} else if err != nil {
		return err
	} else {
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &set)
		})
		if err != nil {
			return err
		}
	}

	for _, member := range members {
		set[string(member)] = struct{}{}
	}

	val, err := json.Marshal(set)
	if err != nil {
		return err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return err
	}

	return txn.Commit()
}

// SRem removes the specified members from the set stored at key.
func (db *BadgerDB) SRem(key string, members ...[]byte) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var set map[string]struct{}
	item, err := txn.Get([]byte(key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	} else if err != nil {
		return err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &set)
	})
	if err != nil {
		return err
	}

	for _, member := range members {
		delete(set, string(member))
	}

	val, err := json.Marshal(set)
	if err != nil {
		return err
	}

	err = txn.Set([]byte(key), val)
	if err != nil {
		return err
	}

	return txn.Commit()
}

// SMembers returns all the members of the set stored at key.
func (db *BadgerDB) SMembers(key string) ([][]byte, error) {
	var set map[string]struct{}
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &set)
		})
	})
	if err != nil {
		return nil, err
	}

	members := make([][]byte, 0, len(set))
	for member := range set {
		members = append(members, []byte(member))
	}

	return members, nil
}

// SIsMember returns if member is a member of the set stored at key.
func (db *BadgerDB) SIsMember(key string, member []byte) (bool, error) {
	var set map[string]struct{}
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &set)
		})
	})
	if err != nil {
		return false, err
	}

	_, exists := set[string(member)]
	return exists, nil
}
