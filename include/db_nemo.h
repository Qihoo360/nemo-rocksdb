//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <vector>

#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/db.h"

namespace rocksdb {

class DBNemo: public StackableDB {
 public:

  static Status Open(const Options& options, const std::string& dbname,
                     DBNemo** dbptr,
                     bool read_only = false);

  static Status Open(const DBOptions& db_options, const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     DBNemo** dbptr,
                     bool read_only = false);

  virtual Status PutWithKeyTTL(const WriteOptions& options, const Slice& key, const Slice& val, int32_t ttl = 0) = 0;

  virtual Status WriteWithKeyTTL(const WriteOptions& opts, WriteBatch* updates, int32_t ttl = 0) = 0;

 protected:
  explicit DBNemo(DB* db) : StackableDB(db) {}
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
