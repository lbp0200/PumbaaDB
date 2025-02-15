package store

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

func (s *BadgerStore) DelString(key string) error {
	logFuncTag := "BadgerStoreDelString"
	bKey := []byte(key)
	baggerTypeKey := TypeKeyGet(key)
	badgerValueKey := keyBadgetGet(prefixKeyString, bKey)
	return s.db.Update(func(txn *badger.Txn) error {
		errDel := txn.Delete(baggerTypeKey)
		if errDel != nil {
			return fmt.Errorf("%s,Del Badger Type Key:%v", logFuncTag, errDel)
		}
		errDel = txn.Delete(badgerValueKey)
		if errDel != nil {
			return fmt.Errorf("%s,Del Badger Value Key:%v", logFuncTag, errDel)
		}
		return nil
	})
}

func (s *BadgerStore) Del(key string) error {
	bKey := []byte(key)
	bTypeKey := TypeKeyGet(bKey)
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(bTypeKey)
		if err != nil {
			return err
		}
		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		switch string(valCopy) {
		case KeyTypeString:
			// 处理 type1 的删除逻辑
			return s.DelString(key)
		case KeyTypeList:
			// 处理 type2 的删除逻辑
			return txn.Delete(bKey)
		case KeyTypeHash:
			// 处理 type3 的删除逻辑
			return txn.Delete(bKey)
		case KeyTypeSet:
			// 处理 type4 的删除逻辑
			return txn.Delete(bKey)
		case KeyTypeZSet:
			// 处理 type5 的删除逻辑
			return txn.Delete(bKey)
		default:
			// 默认的删除逻辑
			return txn.Delete(bKey)
		}
	})
}
