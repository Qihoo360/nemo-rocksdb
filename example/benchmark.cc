#include "db_nemo_impl.h"
#include <iostream>
#include <thread>
#include <chrono>

const int num = 100000000;

void Persistent(rocksdb::DBNemo* db, int start, int num) {
  rocksdb::Status s;
  for (int i = start; i < start + num; i++) {
    s = db->Put(rocksdb::WriteOptions(), std::to_string(i)+"_persistent_key", "HiThereIAmAValue");
    if (!s.ok()) {
      std::cout << "Thread " << std::this_thread::get_id() << " Put Error: " << s.ToString() << std::endl;
    }
  }
}

void Expire(rocksdb::DBNemo* db, int start, int num) {
  rocksdb::Status s;
  for (int i = start; i < start + num; i++) {
    s = db->Put(rocksdb::WriteOptions(), std::to_string(i)+"_expire_key", "HiThereIAmAValue", i%1000);
    if (!s.ok()) {
      std::cout << "Thread " << std::this_thread::get_id() << " Put Error: " << s.ToString() << std::endl;
    }
  }
}

int main() {
  rocksdb::DBNemo* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  options.base_background_compactions = 4;
  options.max_background_compactions = 6;
  rocksdb::Status s = rocksdb::DBNemo::Open(options, "./db", &db);

  auto start = std::chrono::steady_clock::now();
  std::thread* threads[10];
  for (int i = 0; i < 5; i++) {
    threads[i] = new std::thread(Persistent, db, i*(num/10), num/10);
    std::cout << "start thread Persistent from " << i*(num/10) << " to " << num/10 << std::endl;
  }
  for (int i = 5; i < 10; i++) {
    threads[i] = new std::thread(Expire, db, i*(num/10), num/10);
    std::cout << "start thread Expire from " << i*(num/10) << " to " << num/10 << std::endl;
  }

  for (int i = 0; i < 10; i++) {
    threads[i]->join();
    delete threads[i];
  }
  auto end = std::chrono::steady_clock::now();
  std::cout << "used " << (end-start).count() << std::endl;

  delete db;
  std::cout << "Bye!!!" << std::endl;
}

