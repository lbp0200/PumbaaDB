package helper

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
)

// store/badger_store.go
func Uint64ToBytes(n uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}

func BytesToUint64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func ProtectGoroutine(goFunc func()) {
	if err := recover(); err != nil {
		fmt.Println("func:ProtectGoroutine Error:", err)
	}
	go goFunc()
}

func InterfaceToBytes(data interface{}) ([]byte, error) {
	logFuncTag := "InterfaceToBytes"
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, fmt.Errorf("%s,Encode Error:%v", logFuncTag, err)
	}
	return buf.Bytes(), nil
}
