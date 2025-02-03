package store

import (
	"testing"

	"github.com/zeebo/assert"
)

// store/list_test.go
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