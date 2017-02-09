#include "db_nemo_impl.h"
#include <iostream>
#include <thread>
#include <chrono>

int main() {
  rocksdb::DBNemo *db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status s = rocksdb::DBNemo::Open(options, "./db", &db);

/*
 * 1. Test PutWithKeyTTL
 *
  {
  s = db->PutWithKeyTTL(rocksdb::WriteOptions(), "key", "value", 3);
  if (!s.ok()) {
    std::cout << "Put Error: " << s.ToString() << std::endl;
  }

  std::string value;
  while (true) {
    s = db->Get(rocksdb::ReadOptions(), "key", &value);
    if (s.ok()) {
      std::cout << "Get value: " << value << std::endl;
    } else if (s.IsNotFound()) {
      std::cout << "Get Nothing" << std::endl;
    } else {
      std::cout << "Get Error: " << s.ToString() << std::endl;
    }
//    std::this_thread::sleep_for(std::chrono::duration<int, std::ratio<1, 1> >(1));
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  }
 */

/*
 * 2. Test Iterator
 *
  {
  rocksdb::WriteBatch batch;
  batch.Put("key1", "value1");
  batch.Put("key2", "value2");
  batch.Put("key3", "value3");
  s = db->WriteWithKeyTTL(rocksdb::WriteOptions(), &batch, 3);
  if (!s.ok()) {
    std::cout << "Write Error: " << s.ToString() << std::endl;
  }
  s = db->PutWithKeyTTL(rocksdb::WriteOptions(), "key4", "value4", 4);
  if (!s.ok()) {
    std::cout << "Put Error: " << s.ToString() << std::endl;
  }
  s = db->PutWithKeyTTL(rocksdb::WriteOptions(), "key5", "value5", 5);
  if (!s.ok()) {
    std::cout << "Put Error: " << s.ToString() << std::endl;
  }
  s = db->Put(rocksdb::WriteOptions(), "key6", "value6");
  if (!s.ok()) {
    std::cout << "Put Error: " << s.ToString() << std::endl;
  }

  rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());
  while (true) {
    iter->SeekToFirst();
    std::cout << "---------------------------" << std::endl;
    while (iter->Valid()) {
      std::cout << iter->key().ToString() << " " << iter->value().ToString() << " " << static_cast<rocksdb::NemoIterator*>(iter)->timestamp() << std::endl;
      iter->Next();
    }
    std::cout << "---------------------------" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  }
 *
 */

/*
 * 3. Test CompactFilter
 */
  {
  s = db->Put(rocksdb::WriteOptions(), "persistent_key", "KernelMaker");
  if (!s.ok()) {
    std::cout << "Put Error: " << s.ToString() << std::endl;
  }

  for (int i = 0; i < 10; i++) {
    s = db->PutWithKeyTTL(rocksdb::WriteOptions(), std::to_string(i)+"_key", "value", 3);
    if (!s.ok()) {
      std::cout << "Put Error: " << s.ToString() << std::endl;
    }
  }
  for (int i = 0; i < 10; i++) {
    s = db->PutWithKeyTTL(rocksdb::WriteOptions(), std::to_string(i+10)+"_key", "value", 10);
    if (!s.ok()) {
      std::cout << "Put Error: " << s.ToString() << std::endl;
    }
  }
  std::string value;
  for (int i = 0; i < 20; i++) {
    s = db->Get(rocksdb::ReadOptions(), std::to_string(i)+"_key", &value);
    if (s.ok()) {
      std::cout << "Get key: " << std::to_string(i)+"_key" << " value: "<< value << std::endl;
    } else if (s.IsNotFound()) {
      std::cout << "Get Nothing" << std::endl;
    } else {
      std::cout << "Get Error: " << s.ToString() << std::endl;
    }
  }
  std::this_thread::sleep_for(std::chrono::seconds(5));
  s = db->CompactRange(rocksdb::CompactRangeOptions(), NULL, NULL);
  std::cout << "CompactRange return: " << s.ToString() << std::endl;
  for (int i = 0; i < 20; i++) {
    s = db->Get(rocksdb::ReadOptions(), std::to_string(i)+"_key", &value);
    if (s.ok()) {
      std::cout << "Get key: " << std::to_string(i)+"_key" << " value: "<< value << std::endl;
    } else if (s.IsNotFound()) {
      std::cout << "Get Nothing, key: " << std::to_string(i)+"_key" << std::endl;
    } else {
      std::cout << "Get Error: " << s.ToString() << std::endl;
    }
  }
  std::this_thread::sleep_for(std::chrono::seconds(6));
  s = db->CompactRange(rocksdb::CompactRangeOptions(), NULL, NULL);
  std::cout << "CompactRange return: " << s.ToString() << std::endl;
  for (int i = 0; i < 20; i++) {
    s = db->Get(rocksdb::ReadOptions(), std::to_string(i)+"_key", &value);
    if (s.ok()) {
      std::cout << "Get key: " << std::to_string(i)+"_key" << " value: "<< value << std::endl;
    } else if (s.IsNotFound()) {
      std::cout << "Get Nothing, key: " << std::to_string(i)+"_key" << std::endl;
    } else {
      std::cout << "Get Error: " << s.ToString() << std::endl;
    }
  }
  }

  delete db;
}
