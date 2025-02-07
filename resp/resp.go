package resp

import (
	"PumbaaDB/store"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
)

func Parse(reader io.Reader) ([][]byte, error) {
    buf := make([]byte, 1024)
    n, err := reader.Read(buf)
    if err != nil {
        return nil, err
    }
    buf = buf[:n]

    if buf[0] != '*' {
        return nil, fmt.Errorf("invalid RESP format")
    }

    end := bytes.IndexByte(buf, '\r')
    if end == -1 {
        return nil, fmt.Errorf("invalid RESP format")
    }

    count, _ := strconv.Atoi(string(buf[1:end]))
    args := make([][]byte, 0, count)
    pos := end + 2

    for i := 0; i < count; i++ {
        if buf[pos] != '$' {
            return nil, fmt.Errorf("invalid RESP format")
        }
        pos++
        end = bytes.IndexByte(buf[pos:], '\r')
        length, _ := strconv.Atoi(string(buf[pos : pos+end]))
        pos += end + 2
        args = append(args, buf[pos:pos+length])
        pos += length + 2
    }
    return args, nil
}

func Encode(value interface{}) []byte {
    switch v := value.(type) {
    case string:
        return []byte(fmt.Sprintf("+%s\r\n", v))
    case []byte:
        if v == nil {
            return []byte("$-1\r\n")
        }
        return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
    case int:
        return []byte(fmt.Sprintf(":%d\r\n", v))
    case error:
        return []byte(fmt.Sprintf("-%s\r\n", v.Error()))
    case []interface{}:
        buf := &bytes.Buffer{}
        buf.WriteString(fmt.Sprintf("*%d\r\n", len(v)))
        for _, item := range v {
            buf.Write(Encode(item))
        }
        return buf.Bytes()
    default:
        return []byte("-ERR unknown type\r\n")
    }
}

func HandleConnection(conn net.Conn, store *store.BadgerStore) {
    defer conn.Close()
    for {
        args, err := Parse(conn)
        if err != nil {
            return
        }
        if len(args) == 0 {
            continue
        }
        cmd := string(args[0])
        switch cmd {
        case "SET":
            HandleSet(conn, args[1:], store)
        case "GET":
            // handleGet(conn, args[1:], store)
        case "HSET":
            // handleHSet(conn, args[1:], store)
        case "HGET":
            // handleHGet(conn, args[1:], store)
        default:
            conn.Write(Encode(fmt.Errorf("ERR unknown command '%s'", cmd)))
        }
    }
}
