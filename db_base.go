package main

import "github.com/dgraph-io/badger/v3"

type BadgerDB struct {
	db *badger.DB
}

func NewBadgerDB(path string) (*BadgerDB, error) {
	opts := badger.DefaultOptions(path)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerDB{db: db}, nil
}

func (db *BadgerDB) Close() error {
	return db.db.Close()
}

type Member struct {
	Member []byte
	Score  float64
}
