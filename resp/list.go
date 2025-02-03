// main.go
func handleConnection(conn net.Conn, store *store.BadgerStore) {
    // ...
    switch cmd {
    case "LPUSH":
        handleLPush(conn, args[1:], store)
    case "RPOP":
        handleRPop(conn, args[1:], store)
    case "LLEN":
        handleLLen(conn, args[1:], store)
    // ... 其他命令
    }
}

func handleLPush(conn net.Conn, args [][]byte, store *store.BadgerStore) {
    if len(args) < 2 {
        conn.Write(resp.Encode(fmt.Errorf("ERR wrong number of arguments")))
        return
    }
    key := args[0]
    values := args[1:]
    length, err := store.LPush(key, values...)
    if err != nil {
        conn.Write(resp.Encode(err))
    } else {
        conn.Write(resp.Encode(length))
    }
}

func handleRPop(conn net.Conn, args [][]byte, store *store.BadgerStore) {
    if len(args) != 1 {
        conn.Write(resp.Encode(fmt.Errorf("ERR wrong number of arguments")))
        return
    }
    value, err := store.RPop(args[0])
    if err != nil {
        conn.Write(resp.Encode(err))
    } else if value == nil {
        conn.Write(resp.Encode(nil))
    } else {
        conn.Write(resp.Encode(value))
    }
}

func handleLLen(conn net.Conn, args [][]byte, store *store.BadgerStore) {
    if len(args) != 1 {
        conn.Write(resp.Encode(fmt.Errorf("ERR wrong number of arguments")))
        return
    }
    length, err := store.LLen(args[0])
    if err != nil {
        conn.Write(resp.Encode(err))
    } else {
        conn.Write(resp.Encode(length))
    }
}