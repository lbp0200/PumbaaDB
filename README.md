# PumbaaDB

开发中

一种数据直接存硬盘的Redis集群模式实现，灵感来自pika(https://github.com/OpenAtomFoundation/pika)和kvrocks(https://kvrocks.apache.org/)。

和kvrocks思路一致，只是用golang重复造了一个轮子。不同的地方在于纯golang实现，底层数据存储使用badger(https://github.com/dgraph-io/badger)，没有使用rocksDB。

Redis数据结构到KV数据结构的映射，由DeepSeek协助实现。