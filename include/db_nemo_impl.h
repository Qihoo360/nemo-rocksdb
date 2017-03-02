// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <string>
#include <vector>

//#include <iostream>

#include "db_nemo.h"

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/utility_db.h"
#include "db/db_impl.h"

#ifdef _WIN32
// Windows API macro interference
#undef GetCurrentTime
#endif


namespace rocksdb {

const char kMetaPrefixKv = '\0';
const char kMetaPrefixHash = 'H';
const char kMetaPrefixZset = 'Z';
const char kMetaPrefixSet = 'S';
const char kMetaPrefixList = 'L';

class NemoCompactionFilter;
class NemoCompactionFilterFactory;

class DBNemoImpl : public DBNemo {
 public:
  static void SanitizeOptions(ColumnFamilyOptions* options,
                              Env* env, char meta_prefix);

  explicit DBNemoImpl(DB* db, char meta_prefix);

  virtual ~DBNemoImpl();

  using StackableDB::CreateColumnFamily;
  Status CreateColumnFamily(const ColumnFamilyOptions& options,
                            const std::string& column_family_name,
                            ColumnFamilyHandle** handle) override;

//  using StackableDB::Put;
  using DBNemo::Put;
  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& val) override;

  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family,
                     const Slice& key, const Slice& val,
                     int32_t ttl) override;

  using StackableDB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     std::string* value) override;

  using StackableDB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  using StackableDB::KeyMayExist;
  virtual bool KeyMayExist(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value,
                           bool* value_found = nullptr) override;

  using StackableDB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) override;

  using DBNemo::Write;
  virtual Status Write(const WriteOptions& opts, WriteBatch* updates) override;
  virtual Status Write(const WriteOptions& opts, WriteBatch* updates,
                       int32_t ttl) override;

  using DBNemo::PutWithExpiredTime;
  virtual Status PutWithExpiredTime(const WriteOptions& options, const Slice& key, const Slice& val, int32_t expired_time) override;

  using DBNemo::WriteWithExpiredTime;
  virtual Status WriteWithExpiredTime(const WriteOptions& opts, WriteBatch* updates, int32_t expired_time) override;

  using DBNemo::PutWithKeyVersion;
  virtual Status PutWithKeyVersion(const WriteOptions& options, const Slice& key, const Slice& val) override;

  using DBNemo::WriteWithKeyVersion;
  virtual Status WriteWithKeyVersion(const WriteOptions& opts, WriteBatch* updates) override;

  using DBNemo::WriteWithOldKeyTTL;
  virtual Status WriteWithOldKeyTTL(const WriteOptions& opts, WriteBatch* updates) override;

  using DBNemo::GetKeyTTL;
  virtual Status GetKeyTTL(const ReadOptions& options, const Slice& key, int32_t *ttl) override;

  using StackableDB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& opts,
                                ColumnFamilyHandle* column_family) override;

  virtual DB* GetBaseDB() override { return db_; }

  static bool GetVersionAndTS(DB* db, char meta_prefix,
         const Slice& key, uint32_t* version, int32_t* timestamp);

  static Status SanityCheckTimestamp(const Slice& str, Env* env);

  static Status AppendVersionAndTS(const Slice& val, std::string* val_with_ver_ts,
                                   Env* env, uint32_t version, int32_t ttl);

  static Status AppendVersionAndExpiredTime(const Slice& val, std::string* val_with_ver_ts,
                                   Env* env, uint32_t version, int32_t expire_time);

  Status SanityCheckVersionAndTS(const Slice& key, const Slice& val);

  static bool IsStale(int32_t timestamp, Env* env);
  
  static Status ExtractVersionAndTS(const Slice& value, uint32_t* version, int32_t *timestamp);

  static void ExtractUserKey(char meta_prefix, const Slice& key, std::string* user_key);

  static Status StripTS(std::string* str);

  static Status StripVersionAndTS(std::string* str);


  static const uint32_t kTSLength = sizeof(int32_t);  // size of timestamp
  static const uint32_t kVersionLength = sizeof(uint32_t);  // size of version
 private:
  char meta_prefix_;
};

static NemoCompactionFilter* g_compaction_filter = nullptr;
static NemoCompactionFilterFactory* g_compaction_filter_factory = nullptr;

class NemoIterator : public Iterator {

 public:
  explicit NemoIterator(Iterator* iter, Env* env, DB* db,
                        char meta_prefix)
    : iter_(iter), env_(env),
      db_(db), meta_prefix_(meta_prefix),
      version_(0),
      timestamp_(0) { assert(iter_); }

  ~NemoIterator() { delete iter_; }

  bool Valid() const override { return iter_->Valid(); }

  void SeekToFirst() override {
    iter_->SeekToFirst();
    while (iter_->Valid()) {
      if (IsAlive()) {
        break;
      }
      iter_->Next();
    }
  }

  void SeekToLast() override {
    iter_->SeekToLast();
    while (iter_->Valid()) {
      if (IsAlive()) {
        break;
      }
      iter_->Prev();
    }
  }

  void Seek(const Slice& target) override {
    iter_->Seek(target);
    while (iter_->Valid()) {
      if (IsAlive()) {
        break;
      }
      iter_->Next();
    }
  }

  void SeekForPrev(const Slice& target) override {
    iter_->SeekForPrev(target);
    while (iter_->Valid()) {
      if (IsAlive()) {
        break;
      }
      iter_->Prev();
    }
  }

  void Next() override {
    while (iter_->Valid()) {
      iter_->Next();
      if (iter_->Valid() && IsAlive()) {
        break;
      }
    }
  }

  void Prev() override {
    while (iter_->Valid()) {
      iter_->Prev();
      if (iter_->Valid() && IsAlive()) {
        break;
      }
    }
  }

  Slice key() const override { return iter_->key(); }

  int32_t timestamp() const {
    return DecodeFixed32(iter_->value().data() + iter_->value().size() -
                         DBNemoImpl::kTSLength);
  }

  Slice value() const override {
    // TODO: handle timestamp corruption like in general iterator semantics
    Slice trimmed_value = iter_->value();
    trimmed_value.size_ -= (DBNemoImpl::kVersionLength + DBNemoImpl::kTSLength);
    return trimmed_value;
  }

  Status status() const override { return iter_->status(); }

 private:
  Iterator* iter_;
  Env* env_;
  DB* db_;
  char meta_prefix_;
  std::string user_key_;
  uint32_t version_;
  int32_t timestamp_;
  
  bool IsAlive() {

    uint32_t ver;
    int32_t ts;
//    std::cout << "---Iterator------------------------------------------" << std::endl;
    Status s = DBNemoImpl::ExtractVersionAndTS(iter_->value(), &ver, &ts);
    if (!s.ok()) {
      return false;
    }
//    std::cout << "old_version: " << ver << " old_TS: " << ts << std::endl;

    if (meta_prefix_ == kMetaPrefixKv) {
      if (DBNemoImpl::IsStale(ts, env_)) {
//        std::cout << "Is Died " << iter_->key().ToString() << std::endl;
        return false;
      } else {
//        std::cout << "Is Alive" << std::endl;
        return true;
      }
    }

    if ((iter_->key())[0] == meta_prefix_) {
      if (*((int64_t*)iter_->value().data()) <= 0) {
        return false;
      }
    }

    if ((iter_->key().size() <= 1)) {
      return false;
    }


    std::string user_key;
    DBNemoImpl::ExtractUserKey(meta_prefix_, iter_->key(), &user_key);
//    std::cout << "meta_prefix: " << meta_prefix_ << " key: " << iter_->key().ToString() << " user_key: " << user_key << " user_key_: " << user_key_ << " meta_version: " << version_ << " meta_TS: " << timestamp_ << std::endl;

    if (user_key != user_key_) {
      user_key_ = user_key;
      DBNemoImpl::GetVersionAndTS(db_, meta_prefix_, iter_->key(), &version_, &timestamp_);
//      std::cout << "Update Meta, meta_version: " << version_ << " meta_TS: " << timestamp_ << std::endl;
    }

    if (DBNemoImpl::IsStale(timestamp_, env_) || ver < version_) {
//      std::cout << "Is Died " << iter_->key().ToString() << " ver: " << ver << " version: " << version_ << std::endl;
      return false;
    } else {
//      std::cout << "Is Alive" << std::endl;
      return true;
    }
  }
};

class NemoCompactionFilter : public CompactionFilter {
 public:
  NemoCompactionFilter(
      Env* env, const CompactionFilter* user_comp_filter,
      DB* db, char meta_prefix,
      std::unique_ptr<const CompactionFilter> user_comp_filter_from_factory =
          nullptr)
      : env_(env),
        user_comp_filter_(user_comp_filter),
        db_(db),
        meta_prefix_(meta_prefix),
        version_(0), timestamp_(0),
        find_meta_(true),
        user_comp_filter_from_factory_(
            std::move(user_comp_filter_from_factory)) {
    // Unlike the merge operator, compaction filter is necessary for TTL, hence
    // this would be called even if user doesn't specify any compaction-filter
    if (!user_comp_filter_) {
      user_comp_filter_ = user_comp_filter_from_factory_.get();
    }
  }

  virtual bool Filter(int level, const Slice& key, const Slice& old_val,
                      std::string* new_val, bool* value_changed) const
      override {
    assert(old_val.size() >= DBNemoImpl::kTSLength);
    Slice old_val_without_ts(old_val.data(),
                             old_val.size() - DBNemoImpl::kVersionLength -
                             DBNemoImpl::kTSLength);
    if (user_comp_filter_ != nullptr && user_comp_filter_->Filter(level,
          key, old_val_without_ts, new_val, value_changed)) {
      return true;
    }

    if (ShouldDrop(key, old_val)) {
      return true;
    }

    if (*value_changed) {
      new_val->append(
          old_val.data() + old_val.size() - DBNemoImpl::kTSLength,
          DBNemoImpl::kTSLength);
    }
    return false;
  }

  virtual const char* Name() const override { return "Delete By TTL"; }

 private:
  Env* env_;
  const CompactionFilter* user_comp_filter_;
  DB* db_;
  char meta_prefix_;
  mutable std::string user_key_;
  mutable uint32_t version_;
  mutable int32_t timestamp_;
  mutable bool find_meta_;
  std::unique_ptr<const CompactionFilter> user_comp_filter_from_factory_;
  bool ShouldDrop(const Slice& key, const Slice& old_val) const {

    uint32_t ver;
    int32_t ts;
//    std::cout << "---Filter------------------------------------------" << std::endl;
    Status s = DBNemoImpl::ExtractVersionAndTS(old_val, &ver, &ts);
    if (!s.ok()) {
      return true;
    }
//    std::cout << "old_version: " << ver << " old_TS: " << ts << std::endl;

    if (meta_prefix_ == kMetaPrefixKv) {
      if (DBNemoImpl::IsStale(ts, env_)) {
//        std::cout << "Should Drop " << key.ToString() << std::endl;
        return true;
      } else {
//        std::cout << "Should Reserve" << std::endl;
        return false;
      }
    }

    if (key[0] == meta_prefix_) {
      if (old_val.size() < sizeof(int64_t) + DBNemoImpl::kVersionLength +
                            DBNemoImpl::kTSLength) {
//        std::cout << "Invalid old_val length, reserve" << std::endl;
        return false;
      }

      int64_t curtime;
      if (!(env_->GetCurrentTime(&curtime)).ok()) {
        return false;
      }

      uint32_t meta_version = DecodeFixed32(old_val.data() +
          old_val.size() - DBNemoImpl::kVersionLength -
          DBNemoImpl::kTSLength);
      int32_t meta_timestamp = DecodeFixed32(old_val.data() + old_val.size() -
          DBNemoImpl::kTSLength);

      if (meta_timestamp != 0 && meta_timestamp < curtime) {
//        std::cout << "meta_key: " <<key.ToString() << " timestamp: " << meta_timestamp << ", curtime: " << curtime << " Drop" << std::endl;
        return true;
      }

      int64_t meta_size = *((int64_t*)old_val.data());
      if (meta_size > 0) {
//        std::cout << "meta_key: " <<key.ToString() << " size: " << meta_size << ", reserve" << std::endl;
        return false;
      }
      if (meta_version < curtime) {
//        std::cout << "meta_key: " <<key.ToString() << " version: " << meta_version << " timestamp: " << meta_timestamp << " curtime: " << curtime << " Drop" << std::endl;
        return true;
      }
      return false;
    }

    if ((key.size() <= 1)) {
      return false;
    }

    std::string user_key;
    DBNemoImpl::ExtractUserKey(meta_prefix_, key, &user_key);
//    std::cout << "meta_prefix: " << meta_prefix_ << " key: " << key.ToString() << " user_key: " << user_key << " user_key_: " << user_key_ << " meta_version: " << version_ << " meta_TS: " << timestamp_ << std::endl;

    if (user_key != user_key_) {
      user_key_ = user_key;
      find_meta_ = DBNemoImpl::GetVersionAndTS(db_, meta_prefix_, key, &version_, &timestamp_);
//      std::cout << "Update meta, meta_version: " << version_ << " meta_TS: " << timestamp_ << std::endl;
    }

    if (!find_meta_ || ver < version_ || DBNemoImpl::IsStale(timestamp_, env_)) {
//      std::cout << "should drop" << std::endl;
//      std::cout << "data_key: " <<key.ToString() << " find_meta_: " << find_meta_ << " version: " << version_ << " ver: " << ver << " Drop" << std::endl;
      return true;
    } else {
//      std::cout << "shouldn't drop" << std::endl;
      return false;
    }
  }
};

class NemoCompactionFilterFactory : public CompactionFilterFactory {
 public:
  NemoCompactionFilterFactory(
      Env* env,
      std::shared_ptr<CompactionFilterFactory> comp_filter_factory,
      DB* db, char meta_prefix)
      : env_(env), user_comp_filter_factory_(comp_filter_factory),
        db_(db), meta_prefix_(meta_prefix) {}

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    std::unique_ptr<const CompactionFilter> user_comp_filter_from_factory =
        nullptr;
    if (user_comp_filter_factory_) {
      user_comp_filter_from_factory =
          user_comp_filter_factory_->CreateCompactionFilter(context);
    }

    return std::unique_ptr<NemoCompactionFilter>(new NemoCompactionFilter(
        env_, nullptr, db_, meta_prefix_, std::move(user_comp_filter_from_factory)));
  }

  virtual const char* Name() const override {
    return "NemoCompactionFilterFactory";
  }
  
  void SetDBAndMP(DB* db, char meta_prefix) const {
    db_ = db;
    meta_prefix_ = meta_prefix;
  }

 private:
  Env* env_;
  std::shared_ptr<CompactionFilterFactory> user_comp_filter_factory_;
  mutable DB* db_;
  mutable char meta_prefix_;
};

class NemoMergeOperator : public MergeOperator {

 public:
  explicit NemoMergeOperator(const std::shared_ptr<MergeOperator>& merge_op,
                             Env* env)
      : user_merge_op_(merge_op), env_(env) {
    assert(merge_op);
    assert(env);
  }

  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override {
    const uint32_t ts_len = DBNemoImpl::kTSLength;
    if (merge_in.existing_value && merge_in.existing_value->size() < ts_len) {
      Log(InfoLogLevel::ERROR_LEVEL, merge_in.logger,
          "Error: Could not remove timestamp from existing value.");
      return false;
    }

    // Extract time-stamp from each operand to be passed to user_merge_op_
    std::vector<Slice> operands_without_ts;
    for (const auto& operand : merge_in.operand_list) {
      if (operand.size() < ts_len) {
        Log(InfoLogLevel::ERROR_LEVEL, merge_in.logger,
            "Error: Could not remove timestamp from operand value.");
        return false;
      }
      operands_without_ts.push_back(operand);
      operands_without_ts.back().remove_suffix(ts_len);
    }

    // Apply the user merge operator (store result in *new_value)
    bool good = true;
    int32_t ttl = 0;
    bool have_existing_value = false;
    MergeOperationOutput user_merge_out(merge_out->new_value,
                                        merge_out->existing_operand);
    if (merge_in.existing_value) {
      if (DBNemoImpl::SanityCheckTimestamp(*(merge_in.existing_value), env_).ok()) {
        have_existing_value = true;
        ttl = DecodeFixed32(merge_in.existing_value->data() +
                            merge_in.existing_value->size() -
                            DBNemoImpl::kTSLength);
        Slice existing_value_without_ts(merge_in.existing_value->data(),
                                        merge_in.existing_value->size() - ts_len);
        good = user_merge_op_->FullMergeV2(
            MergeOperationInput(merge_in.key, &existing_value_without_ts,
                                operands_without_ts, merge_in.logger),
            &user_merge_out);
      } else {
        merge_out->new_value = std::string(nullptr, 0);
      }
    }
    if (!have_existing_value) {
      good = user_merge_op_->FullMergeV2(
          MergeOperationInput(merge_in.key, nullptr, operands_without_ts,
                              merge_in.logger),
          &user_merge_out);
    }

    // Return false if the user merge operator returned false
    if (!good) {
      return false;
    }

    if (merge_out->existing_operand.data()) {
      merge_out->new_value.assign(merge_out->existing_operand.data(),
                                  merge_out->existing_operand.size());
      merge_out->existing_operand = Slice(nullptr, 0);
    }

    // Augment the *new_value with the ttl time-stamp 0
    char ts_string[ts_len];
    EncodeFixed32(ts_string, ttl);
    merge_out->new_value.append(ts_string, ts_len);
    return true;
  }

  virtual bool PartialMergeMulti(const Slice& key,
                                 const std::deque<Slice>& operand_list,
                                 std::string* new_value, Logger* logger) const
      override {
    const uint32_t ts_len = DBNemoImpl::kTSLength;
    std::deque<Slice> operands_without_ts;

    for (const auto& operand : operand_list) {
      if (operand.size() < ts_len) {
        Log(InfoLogLevel::ERROR_LEVEL, logger,
            "Error: Could not remove timestamp from value.");
        return false;
      }

      operands_without_ts.push_back(
          Slice(operand.data(), operand.size() - ts_len));
    }

    // Apply the user partial-merge operator (store result in *new_value)
    assert(new_value);
    if (!user_merge_op_->PartialMergeMulti(key, operands_without_ts, new_value,
                                           logger)) {
      return false;
    }

    // Augment the *new_value with the ttl time-stamp 0
    char ts_string[ts_len];
    EncodeFixed32(ts_string, (int32_t)0);
    new_value->append(ts_string, ts_len);
    return true;
  }

  virtual const char* Name() const override { return "Merge By TTL"; }

 private:
  std::shared_ptr<MergeOperator> user_merge_op_;
  Env* env_;
};
}
#endif  // ROCKSDB_LITE
