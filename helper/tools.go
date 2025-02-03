package helper

import "encoding/binary"

// store/badger_store.go
func uint64ToBytes(n uint64) []byte {
    buf := make([]byte, 8)
    binary.BigEndian.PutUint64(buf, n)
    return buf
}

func bytesToUint64(b []byte) uint64 {
    if len(b) != 8 {
        return 0
    }
    return binary.BigEndian.Uint64(b)
}