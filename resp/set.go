package resp

import (
	"PumbaaDB/store"
	"encoding/binary"
	"fmt"
	"net"
)

func HandleSCARD(conn net.Conn, args [][]byte, store *store.BadgerStore) {
    if len(args) != 1 {
        conn.Write(Encode(fmt.Errorf("ERR wrong number of arguments")))
        return
    }
    key := args[0]
    count, err := store.SCARD(key)
    if err != nil {
        conn.Write(Encode(err))
    } else {
        // 将整数转换为字节切片并编码发送
        var buf [8]byte
        binary.BigEndian.PutUint64(buf[:], uint64(count))
        conn.Write(Encode(buf[:]))
    }
}  