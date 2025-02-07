package resp

import (
	"PumbaaDB/store"
	"fmt"
	"net"
)


func handleLPush(conn net.Conn, args [][]byte, store *store.BadgerStore) {
	if len(args) < 2 {
		conn.Write(Encode(fmt.Errorf("ERR wrong number of arguments")))
		return
	}
	key := args[0]
	values := args[1:]
	length, err := store.LPush(key, values...)
	if err != nil {
		conn.Write(Encode(err))
	} else {
		conn.Write(Encode(length))
	}
}

func handleRPop(conn net.Conn, args [][]byte, store *store.BadgerStore) {
	if len(args) != 1 {
		conn.Write(Encode(fmt.Errorf("ERR wrong number of arguments")))
		return
	}
	value, err := store.RPop(args[0])
	if err != nil {
		conn.Write(Encode(err))
	} else if value == nil {
		conn.Write(Encode(nil))
	} else {
		conn.Write(Encode(value))
	}
}

func handleLLen(conn net.Conn, args [][]byte, store *store.BadgerStore) {
	if len(args) != 1 {
		conn.Write(Encode(fmt.Errorf("ERR wrong number of arguments")))
		return
	}
	length, err := store.LLen(args[0])
	if err != nil {
		conn.Write(Encode(err))
	} else {
		conn.Write(Encode(length))
	}
}