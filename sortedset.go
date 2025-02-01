package main

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/goccy/go-json"
	"sort"
)

// ZAdd adds all the specified members with the specified scores to the sorted set stored at key.
func (db *BadgerDB) ZAdd(key []byte, members ...*Member) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var sortedSet []*Member
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		sortedSet = members
	} else if err != nil {
		return err
	} else {
		err = item.Value(func(val []byte) error {
			return json.Unmarshal(val, &sortedSet)
		})
		if err != nil {
			return err
		}

		// Merge members
		memberMap := make(map[string]*Member)
		for _, m := range sortedSet {
			memberMap[string(m.Member)] = m
		}
		for _, m := range members {
			memberMap[string(m.Member)] = m
		}

		sortedSet = make([]*Member, 0, len(memberMap))
		for _, m := range memberMap {
			sortedSet = append(sortedSet, m)
		}
	}

	// Sort by score
	sort.Slice(sortedSet, func(i, j int) bool {
		return sortedSet[i].Score < sortedSet[j].Score || (sortedSet[i].Score == sortedSet[j].Score && string(sortedSet[i].Member) < string(sortedSet[j].Member))
	})

	val, err := json.Marshal(sortedSet)
	if err != nil {
		return err
	}

	err = txn.Set(key, val)
	if err != nil {
		return err
	}

	return txn.Commit()
}

// ZRem removes the specified members from the sorted set stored at key.
func (db *BadgerDB) ZRem(key []byte, members ...[]byte) error {
	txn := db.db.NewTransaction(true)
	defer txn.Discard()

	var sortedSet []*Member
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil
	} else if err != nil {
		return err
	}

	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &sortedSet)
	})
	if err != nil {
		return err
	}

	// Remove members
	memberMap := make(map[string]*Member)
	for _, m := range sortedSet {
		memberMap[string(m.Member)] = m
	}
	for _, member := range members {
		delete(memberMap, string(member))
	}

	sortedSet = make([]*Member, 0, len(memberMap))
	for _, m := range memberMap {
		sortedSet = append(sortedSet, m)
	}

	val, err := json.Marshal(sortedSet)
	if err != nil {
		return err
	}

	err = txn.Set(key, val)
	if err != nil {
		return err
	}

	return txn.Commit()
}

// ZRange returns the specified range of elements in the sorted set stored at key.
func (db *BadgerDB) ZRange(key []byte, start, stop int64) ([]*Member, error) {
	var sortedSet []*Member
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &sortedSet)
		})
	})
	if err != nil {
		return nil, err
	}

	if start < 0 {
		start += int64(len(sortedSet))
	}
	if stop < 0 {
		stop += int64(len(sortedSet))
	}
	if start < 0 {
		start = 0
	}
	if stop >= int64(len(sortedSet)) {
		stop = int64(len(sortedSet)) - 1
	}

	if start > stop {
		return []*Member{}, nil
	}

	return sortedSet[start : stop+1], nil
}

// ZRangeByScore returns all the elements in the sorted set at key with a score between min and maxInt.
func (db *BadgerDB) ZRangeByScore(key []byte, min, max float64) ([]*Member, error) {
	var sortedSet []*Member
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &sortedSet)
		})
	})
	if err != nil {
		return nil, err
	}

	var result []*Member
	for _, m := range sortedSet {
		if m.Score >= min && m.Score <= max {
			result = append(result, m)
		}
	}

	return result, nil
}

// ZCard returns the number of elements in the sorted set stored at key.
func (db *BadgerDB) ZCard(key []byte) (int, error) {
	var sortedSet []*Member
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &sortedSet)
		})
	})
	if err != nil {
		return 0, err
	}

	return len(sortedSet), nil
}

// ZScore returns the score of member in the sorted set stored at key.
func (db *BadgerDB) ZScore(key, member []byte) (float64, error) {
	var sortedSet []*Member
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &sortedSet)
		})
	})
	if err != nil {
		return 0, err
	}

	for _, m := range sortedSet {
		if string(m.Member) == string(member) {
			return m.Score, nil
		}
	}

	return 0, nil
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
func (db *BadgerDB) ZRank(key, member []byte) (int, error) {
	var sortedSet []*Member
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &sortedSet)
		})
	})
	if err != nil {
		return 0, err
	}

	for i, m := range sortedSet {
		if string(m.Member) == string(member) {
			return i, nil
		}
	}

	return -1, nil
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
func (db *BadgerDB) ZRevRank(key, member []byte) (int, error) {
	var sortedSet []*Member
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &sortedSet)
		})
	})
	if err != nil {
		return 0, err
	}

	for i := len(sortedSet) - 1; i >= 0; i-- {
		if string(sortedSet[i].Member) == string(member) {
			return len(sortedSet) - 1 - i, nil
		}
	}

	return -1, nil
}
