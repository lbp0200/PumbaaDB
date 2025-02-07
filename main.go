package main

import (
	"fmt"
	"net"

	"PumbaaDB/resp"
	"PumbaaDB/store"
)

func main() {
    store, err := store.NewBadgerStore("./data")
    if err != nil {
        panic(err)
    }
    defer store.Close()

    listener, err := net.Listen("tcp", ":6379")
    if err != nil {
        panic(err)
    }
    defer listener.Close()

    fmt.Println("Redis server listening on :6379")
    for {
        conn, err := listener.Accept()
        if err != nil {
            fmt.Println("Error accepting connection:", err)
            continue
        }
        go resp.HandleConnection(conn, store)
    }
}
