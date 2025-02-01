package main

import (
	"fmt"
	"log"
)

func main() {
	db, err := NewBadgerDB("/tmp/badger")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 示例：Set操作
	_, err = db.Set("key1", []byte("value1"))
	if err != nil {
		log.Fatal(err)
	}

	// 示例：Get操作
	value, err := db.Get("key1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value))

	// 示例：Del操作
	err = db.Del("key1")
	if err != nil {
		log.Fatal(err)
	}

}
