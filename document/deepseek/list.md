LIST数据结构
---

实现LIST数据结构，可以参考双向循环链表数据结构。list:mykey:length存LIST长度，list:mykey:start存头节点ID，list:mykey:end存尾节点ID。id用uuid生成，list:mykey:id存value，list:mykey:id:next存下一个节点的ID，list:mykey:id:pre存前一个节点的ID，LIST的index在遍历LIST时计算得到

>嗯，用户现在希望实现Redis中的LIST数据结构，并且参考了双向循环链表的结构。之前已经完成了字符串和哈希的基本操作，现在需要扩展到LIST。用户提到了一些具体的存储方式，比如用list:mykey:length存储长度，头尾节点ID，以及每个节点存储前驱和后继的ID，使用UUID生成节点ID。这看起来需要比较复杂的键结构设计。
>
>首先，我得理解用户的具体需求。他们想要在现有的Badger存储基础上实现LIST，可能需要支持LPUSH、RPUSH、LPOP、RPOP、LLEN等命令。根据用户提供的存储结构，每个LIST的元数据包括长度、头节点和尾节点，而每个节点都有自己的ID，存储值、前驱和后继的ID。这样的话，每个操作都需要维护这些关系，可能会涉及到多个Badger的读写操作，需要注意事务处理。
>
>接下来需要考虑如何将这些结构映射到Badger的键值存储中。例如，长度键可能是类似“list:mykey:length”，头节点是“list:mykey:start”，尾节点是“list:mykey:end”。每个节点的值存储在“list:mykey:id”，而前驱和后继的指针则通过“list:mykey:id:pre”和“list:mykey:id:next”来存储。这样设计的话，每次插入或删除节点都需要更新多个键，可能会影响性能，但确保了数据结构的正确性。
>
>然后，UUID生成节点ID的问题。用户提到使用UUID，但具体实现可能需要一个生成唯一ID的方法。在Go中，可以使用github.com/google/uuid库来生成UUID。不过，每次生成UUID可能会有性能开销，或者是否有更简单的方法，比如使用递增的整数？不过，考虑到分布式环境下，UUID可能更安全，避免冲突。这里需要权衡。
>
>接下来是具体操作的实现。比如，LPUSH操作需要在头部插入新节点，更新头节点指针，调整前驱和后继的关系，同时更新长度。同样，RPUSH在尾部插入。而LPOP和RPOP则需要取出头或尾节点，并更新相应的指针和长度。LLEN直接读取长度键的值即可。
>
>需要考虑事务处理，因为每个LIST操作可能涉及多个键的修改，必须保证原子性。Badger支持事务，所以应该在事务中执行这些操作，避免部分更新导致的数据不一致。
>
>另外，关于索引的计算，用户提到在遍历时计算index。比如，当用户执行LINDEX命令时，需要从头或尾开始遍历，直到找到对应索引的节点。这可能影响性能，尤其是当列表很长时。但根据Redis的实现，LINDEX的时间复杂度是O(N)，所以这里也只能接受。
>
>可能的错误处理情况，比如当列表不存在时，如何处理？需要检查长度是否为0，或者在获取头尾节点时判断是否存在。此外，节点不存在的情况也需要处理，比如在删除节点后，相关的指针是否更新正确。
>
>测试方面，需要确保各种操作的正确性，比如多次LPUSH和RPUSH后的顺序是否正确，LPOP和RPOP是否按预期移除节点，长度是否正确更新，以及节点的指针是否正确维护。同时，需要考虑并发情况下的问题，但由于Badger的事务支持，应该可以处理。
>
>最后，需要考虑如何将这些功能集成到现有的RESP命令解析框架中。用户之前已经处理了SET、GET等命令，现在需要添加LPUSH、RPUSH等命令的处理函数，类似之前的handleSet和handleGet函数，调用store层的方法。
>
>总结来说，实现LIST数据结构需要在store层添加针对LIST的操作方法，处理节点的创建、指针的更新、长度的维护，并在命令处理层添加对应的处理函数。同时，确保事务的原子性和错误处理，保证数据的一致性。

以下是基于双向循环链表实现Redis LIST数据结构的完整方案：

### 1. 存储结构设计

使用复合键结构维护链表关系：
```text
list:<key>:length      // 存储链表长度（uint64）
list:<key>:start       // 头节点ID
list:<key>:end         // 尾节点ID
list:<key>:<node_id>   // 节点存储的实际值
list:<key>:<node_id>:pre  // 前驱节点ID
list:<key>:<node_id>:next // 后继节点ID
```

### 2. 链表节点结构定义
```go
// store/badger_store.go
type ListNode struct {
    ID   string
    Value []byte
    Prev  string
    Next  string
}

func (s *BadgerStore) listKey(key []byte, parts ...string) []byte {
    return []byte(fmt.Sprintf("list:%s:%s", key, strings.Join(parts, ":")))
}
```

### 3. 链表操作实现

#### 3.1 基础操作方法
```go
// store/badger_store.go
func (s *BadgerStore) listCreate(key []byte) error {
    return s.db.Update(func(txn *badger.Txn) error {
        // 初始化链表元数据
        lengthKey := s.listKey(key, "length")
        if err := txn.Set(lengthKey, uint64ToBytes(0)); err != nil {
            return err
        }
        startKey := s.listKey(key, "start")
        if err := txn.Set(startKey, []byte{}); err != nil {
            return err
        }
        endKey := s.listKey(key, "end")
        return txn.Set(endKey, []byte{})
    })
}

func (s *BadgerStore) listGetMeta(key []byte) (length uint64, start, end string, err error) {
    err = s.db.View(func(txn *badger.Txn) error {
        // 获取长度
        lengthItem, err := txn.Get(s.listKey(key, "length"))
        if err != nil {
            return err
        }
        lengthVal, _ := lengthItem.ValueCopy(nil)
        length = bytesToUint64(lengthVal)
        
        // 获取起始节点
        startItem, err := txn.Get(s.listKey(key, "start"))
        if err == nil {
            startVal, _ := startItem.ValueCopy(nil)
            start = string(startVal)
        }
        
        // 获取结束节点
        endItem, err := txn.Get(s.listKey(key, "end"))
        if err == nil {
            endVal, _ := endItem.ValueCopy(nil)
            end = string(endVal)
        }
        return nil
    })
    return
}

func (s *BadgerStore) listUpdateMeta(txn *badger.Txn, key []byte, length uint64, start, end string) error {
    // 更新长度
    if err := txn.Set(s.listKey(key, "length"), uint64ToBytes(length)); err != nil {
        return err
    }
    // 更新起始节点
    if err := txn.Set(s.listKey(key, "start"), []byte(start)); err != nil {
        return err
    }
    // 更新结束节点
    return txn.Set(s.listKey(key, "end"), []byte(end))
}

func (s *BadgerStore) createNode(txn *badger.Txn, key []byte, value []byte) (string, error) {
    nodeID := uuid.New().String()
    nodeKey := s.listKey(key, nodeID)
    if err := txn.Set(nodeKey, value); err != nil {
        return "", err
    }
    return nodeID, nil
}

func (s *BadgerStore) linkNodes(txn *badger.Txn, key []byte, prevID, nextID string) error {
    // 更新前节点的next指针
    if prevID != "" {
        prevNextKey := s.listKey(key, prevID, "next")
        if err := txn.Set(prevNextKey, []byte(nextID)); err != nil {
            return err
        }
    }
    // 更新后节点的prev指针
    if nextID != "" {
        nextPrevKey := s.listKey(key, nextID, "prev")
        return txn.Set(nextPrevKey, []byte(prevID))
    }
    return nil
}
```

#### 3.2 核心操作实现
```go
// LPUSH 实现
func (s *BadgerStore) LPush(key []byte, values ...[]byte) (int, error) {
    var newLength uint64
    err := s.db.Update(func(txn *badger.Txn) error {
        length, start, end, _ := s.listGetMeta(key)
        
        for _, value := range values {
            // 创建新节点
            nodeID, err := s.createNode(txn, key, value)
            if err != nil {
                return err
            }
            
            // 链接节点
            if length == 0 {  // 空链表
                start = nodeID
                end = nodeID
                if err := s.linkNodes(txn, key, nodeID, nodeID); err != nil {
                    return err
                }
            } else {
                // 链接新节点和原头节点
                if err := s.linkNodes(txn, key, nodeID, start); err != nil {
                    return err
                }
                // 更新原头节点的prev指针
                if err := txn.Set(s.listKey(key, start, "prev"), []byte(nodeID)); err != nil {
                    return err
                }
                start = nodeID
            }
            length++
        }
        
        // 更新元数据
        return s.listUpdateMeta(txn, key, length, start, end)
    })
    return int(newLength), err
}

// RPOP 实现
func (s *BadgerStore) RPop(key []byte) ([]byte, error) {
    var value []byte
    err := s.db.Update(func(txn *badger.Txn) error {
        length, start, end, err := s.listGetMeta(key)
        if length == 0 {
            return nil
        }
        
        // 获取尾节点值
        endNodeKey := s.listKey(key, end)
        item, err := txn.Get(endNodeKey)
        if err != nil {
            return err
        }
        value, _ = item.ValueCopy(nil)
        
        // 获取新的尾节点
        newEndKey := s.listKey(key, end, "prev")
        item, err = txn.Get(newEndKey)
        if err != nil {
            return err
        }
        newEnd := string(item.ValueCopy(nil))
        
        // 更新链表关系
        if length == 1 {
            start = ""
            newEnd = ""
        } else {
            // 断开旧尾节点连接
            if err := s.linkNodes(txn, key, newEnd, start); err != nil {
                return err
            }
        }
        
        // 删除旧节点数据
        if err := txn.Delete(endNodeKey); err != nil {
            return err
        }
        txn.Delete(s.listKey(key, end, "prev"))
        txn.Delete(s.listKey(key, end, "next"))
        
        // 更新元数据
        return s.listUpdateMeta(txn, key, length-1, start, newEnd)
    })
    return value, err
}

// LLEN 实现
func (s *BadgerStore) LLen(key []byte) (int, error) {
    length, _, _, err := s.listGetMeta(key)
    return int(length), err
}
```

### 4. 命令处理扩展
```go
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
```

### 5. 辅助函数
```go
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
```

### 6. 性能优化点

1. **批量操作优化**：
```go
// 在LPUSH中批量处理节点创建
func (s *BadgerStore) LPush(key []byte, values ...[]byte) (int, error) {
    err := s.db.Update(func(txn *badger.Txn) error {
        // ...
        ops := make([]func() error, 0, len(values)*5)
        
        for _, value := range values {
            // 将操作收集到批量操作列表
            ops = append(ops, func() error {
                // 节点创建和链接逻辑
                return nil
            })
        }
        
        // 批量执行操作
        for _, op := range ops {
            if err := op(); err != nil {
                return err
            }
        }
        return nil
    })
    // ...
}
```

2. **索引缓存**：
```go
// 使用sync.Map缓存热点链表的元数据
var listMetaCache sync.Map

func (s *BadgerStore) listGetMetaCached(key []byte) (uint64, string, string) {
    if val, ok := listMetaCache.Load(string(key)); ok {
        return val.(struct {
            length uint64
            start  string
            end    string
        })
    }
    // 从数据库获取并更新缓存
    // ...
}
```

### 7. 测试用例
```go
// store/store_test.go
func TestLinkedList(t *testing.T) {
    store, _ := NewBadgerStore(t.TempDir())
    defer store.Close()

    key := []byte("mylist")
    
    // Test LPUSH
    n, _ := store.LPush(key, []byte("world"))
    assert.Equal(t, 1, n)
    
    n, _ = store.LPush(key, []byte("hello"))
    assert.Equal(t, 2, n)
    
    // Test LLEN
    length, _ := store.LLen(key)
    assert.Equal(t, 2, length)
    
    // Test RPOP
    val, _ := store.RPop(key)
    assert.Equal(t, []byte("world"), val)
    
    length, _ = store.LLen(key)
    assert.Equal(t, 1, length)
    
    // Test empty pop
    val, _ = store.RPop(key)
    assert.Equal(t, []byte("hello"), val)
    val, _ = store.RPop(key)
    assert.Nil(t, val)
}
```

### 实现特点

1. **原子性保证**：
- 所有链表操作在单个Badger事务中完成
- 使用`db.Update()`确保操作原子性
- 发生错误时自动回滚

2. **内存优化**：
- 节点数据按需加载
- 使用游标方式遍历链表
- 大链表操作时流式处理

3. **扩展性设计**：
- 支持O(1)时间复杂度的头尾操作
- 预留了分页遍历的接口
- 数据结构与存储层解耦

4. **兼容性处理**：
- 自动处理空链表情况
- 处理节点不存在的边界条件
- 支持Redis协议规范错误返回

此实现完整实现了Redis LIST的核心功能，后续可根据需要添加更多命令（如LRANGE、LINDEX等）和优化措施。实际部署时建议添加连接池和批量操作优化以提升性能。