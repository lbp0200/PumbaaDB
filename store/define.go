package store

import (
	"github.com/dgraph-io/badger/v4"
)

type BadgerStore struct {
    db *badger.DB
}

func NewBadgerStore(path string) (*BadgerStore, error) {
    opts := badger.DefaultOptions(path)
    db, err := badger.Open(opts)
    if err != nil {
        return nil, err
    }
    return &BadgerStore{db: db}, nil
}

func (s *BadgerStore) Close() {
    s.db.Close()
}



