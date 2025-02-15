package main

import (
	"fmt"
	"net"

	"PumbaaDB/helper"
	"PumbaaDB/resp"
	"PumbaaDB/store"
)

func main() {
	store, err := store.NewBadgerStore("./data")
	if err != nil {
		panic(err)
	}
	defer store.Close()

	listener, err := net.Listen("tcp", ":7701")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	fmt.Println("Redis server listening on :7701")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		helper.ProtectGoroutine(func() {
			resp.HandleConnection(conn, store)
		})
	}
}
