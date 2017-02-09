#include "db_nemo_impl.h"
#include <iostream>
#include <thread>
#include <chrono>

int main() {
  rocksdb::DBNemo *db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status s = rocksdb::DBNemo::Open(options, "./db", &db);
  s = db->PutWithKeyTTL(rocksdb::WriteOptions(), "key", "value", 3);
//  s = db->Put(rocksdb::WriteOptions(), "key", "value");
  if (!s.ok()) {
    std::cout << "Put Error: " << s.ToString() << std::endl;
  }

  std::string value;
  int times = 0;
  while (times < 5) {
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

  delete db;
}
