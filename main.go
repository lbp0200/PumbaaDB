package main

import (
	"fmt"
	"net"
	"strconv"
	"time"

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
        go handleConnection(conn, store)
    }
}

func handleConnection(conn net.Conn, store *store.BadgerStore) {
    defer conn.Close()
    for {
        args, err := resp.Parse(conn)
        if err != nil {
            return
        }
        if len(args) == 0 {
            continue
        }
        cmd := string(args[0])
        switch cmd {
        case "SET":
            handleSet(conn, args[1:], store)
        case "GET":
            // handleGet(conn, args[1:], store)
        case "HSET":
            // handleHSet(conn, args[1:], store)
        case "HGET":
            // handleHGet(conn, args[1:], store)
		case "SCARD":
        resp.    HandleSCARD(conn, args[1:], store)
        default:
            conn.Write(resp.Encode(fmt.Errorf("ERR unknown command '%s'", cmd)))
        }
    }
}

func handleSet(conn net.Conn, args [][]byte, store *store.BadgerStore) {
    if len(args) < 2 {
        conn.Write(resp.Encode(fmt.Errorf("ERR wrong number of arguments")))
        return
    }
    var ttl time.Duration
    if len(args) > 2 {
        for i := 2; i < len(args); i++ {
            switch string(args[i]) {
            case "EX":
                if i+1 >= len(args) {
                    conn.Write(resp.Encode(fmt.Errorf("ERR syntax error")))
                    return
                }
                sec, _ := strconv.Atoi(string(args[i+1]))
                ttl = time.Duration(sec) * time.Second
                i++
            }
        }
    }
    err := store.Set(args[0], args[1], ttl)
    if err != nil {
        conn.Write(resp.Encode(err))
    } else {
        conn.Write(resp.Encode("OK"))
    }
}
