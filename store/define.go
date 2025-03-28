package store

import (
	"github.com/dgraph-io/badger/v4"
)

const (
	KeyTypeString = "STRING"
	KeyTypeList   = "LIST"
	KeyTypeHash   = "HASH"
	KeyTypeSet    = "SET"
	KeyTypeZSet   = "ZSET"
)

var (
	keyTypeBytes    = []byte("TYPE:")
	prefixKeyString = []byte("STRING:")
	prefixKeyList   = []byte("LIST:")
	prefixKeyHash   = []byte("HASH:")
	prefixKeySet    = []byte("SET:")
	prefixKeyZSet   = []byte("ZSET:")
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

func TypeKeyGet(strKey string) []byte {
	bKey := []byte(strKey)
	bKey = append(keyTypeBytes, bKey...)
	return bKey
}

func keyBadgetGet(bType, bKey []byte) []byte {
	bKey = append(bType, bKey...)
	return bKey
}
