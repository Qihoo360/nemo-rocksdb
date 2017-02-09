# Introduction

nemo-rocksdb is compatiable with rocksdb, and I added TTL feature on it, supporting to put a record with any specified TTL. The performance is close to rocksdb, so you can use it without worring about performance penalty. It uses rocksdb as a submodule, so it's easy to upgrade rocksdb to the latest version if needed, now it's using rocksdb(v5.0.1). Besides, it is going to be used in [Nemo](https://github.com/Qihoo360/nemo) as a submodule.

# Features
## TTL
  DBNemo is derived from rocksdb::StackableDB, so it is compatiable with rocksdb, you can use the following methods as usual
  

|   Method    |
| --- |
|DBNemo::Put()         |
|DBNemo::Write()       |
|DBNemo::Get()         |
|DBNemo::Delete()      |
|DBNemo::NewIterator() |
|DBNemo::CompactRange()|


Besides, DBNemo adds two new methods:
 
* Status **DBNemo::PutWithKeyTTL**(const WriteOptions& options, const Slice& key, const Slice& val, **int32_t ttl** = 0)

* Status **DBNemo::WriteWithKeyTTL**(const WriteOptions& opts, WriteBatch\* updates, **int32_t ttl** = 0)

you can use these two methods to insert a record into rocksdb with any specified TTL (ttl <=0 means never to be expired), you can also create NemoIterator to iterate db, it will ignore the expired records automaticly.

DBNemo uses NemoCompactionFilterFactory and NemoCompactionFilter to drop the expired records in compaction process by default, you can also add your own CompactionFilterFactory or CompactionFilter in rocksdb::Options, that's ok!

# Usage
```cpp
#include "db_nemo_impl.h"
#include <iostream>

int main() {
  rocksdb::DBNemo *db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status s = rocksdb::DBNemo::Open(options, "./db", &db);
  
  /*
   * 1. Insert a persistent record
   */
  s = db->Put(rocksdb::WriteOptions(), "persistent_key", "value");
  if (!s.ok()) {
    std::cout << "Put Error: " << s.ToString() << std::endl;
  }
  
  /*
   * 2. Insert a record with 6s TTL
   */
  s = db->PutWithKeyTTL(rocksdb::WriteOptions(), "expire_key", "value", 6);
  if (!s.ok()) {
    std::cout << "Put Error: " << s.ToString() << std::endl;
  }
  
  /*
   * 3. Insert three records in one batch with 8s TTL
   */
  rocksdb::WriteBatch batch;
  batch.Put("expire_key_1", "value1");
  batch.Put("expire_key_2", "value2");
  batch.Put("expire_key_3", "value3");
  s = db->WriteWithKeyTTL(rocksdb::WriteOptions(), &batch, 8);
  if (!s.ok()) {
    std::cout << "Put Error: " << s.ToString() << std::endl;
  }
  
  /*
   * 4. Iterate db, the expired keys would be ignored automaticly
   */
  rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());
  while (true) {
    iter->SeekToFirst();
    std::cout << "---------------------------" << std::endl;
    while (iter->Valid()) {
      std::cout << iter->key().ToString() << " "
        << iter->value().ToString() << " "
        << static_cast<rocksdb::NemoIterator*>(iter)->timestamp()
        << std::endl;
      
      iter->Next();
    }
    std::cout << "---------------------------" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  
  delete db;
  return 0;
}

```
see [More](https://github.com/KernelMaker/nemo-rocksdb/blob/master/example/example.cc)

# Performance

It almost **costs NOTHING** to add TTL feature in NemoDB, so its performance is close to rocksdb.

In my environment, it used 390s to finish the benchmark(put 100 millions records), the benchmark code is [here](https://github.com/KernelMaker/nemo-rocksdb/blob/master/example/benchmark.cc)
