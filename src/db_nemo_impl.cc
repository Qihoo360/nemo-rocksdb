// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef ROCKSDB_LITE

#include "db_nemo_impl.h"

#include "rocksdb/convenience.h"

namespace rocksdb {

static NemoCompactionFilter* g_compaction_filter = nullptr;
static NemoCompactionFilterFactory* g_compaction_filter_factory = nullptr;

void DBNemoImpl::SanitizeOptions(ColumnFamilyOptions* options,
                                    Env* env, char meta_prefix) {
  if (options->compaction_filter) {
    options->compaction_filter = g_compaction_filter = 
        new NemoCompactionFilter(env, options->compaction_filter, nullptr, meta_prefix);
  } else {
    g_compaction_filter_factory = new NemoCompactionFilterFactory(
     env, options->compaction_filter_factory, nullptr, meta_prefix);
    options->compaction_filter_factory =
        std::shared_ptr<CompactionFilterFactory>(g_compaction_filter_factory);
  }

  if (options->merge_operator) {
    options->merge_operator.reset(
        new NemoMergeOperator(options->merge_operator, env));
  }
}

// Open the db inside DBNemoImpl because options needs pointer to its ttl
DBNemoImpl::DBNemoImpl(DB* db, char meta_prefix) :
  DBNemo(db), meta_prefix_(meta_prefix) {}

DBNemoImpl::~DBNemoImpl() {
  // Need to stop background compaction before getting rid of the filter
  CancelAllBackgroundWork(db_, /* wait = */ true);
  delete GetOptions().compaction_filter;
}

Status DBNemo::Open(Options& options, const std::string& dbname,
                       DBNemo** dbptr, char meta_prefix, bool read_only) {

  bool manual_disable_auto_compaction = false;
  if (options.disable_auto_compactions) {
    manual_disable_auto_compaction = true;
  }
  options.disable_auto_compactions = true;

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DBNemo::Open(db_options, dbname, column_families, &handles,
                             dbptr, meta_prefix, read_only,
                             manual_disable_auto_compaction);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }
  return s;
}

Status DBNemo::Open(
    DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DBNemo** dbptr,
    char meta_prefix, bool read_only,
    bool manual_disable_auto_compaction) {

  std::vector<ColumnFamilyDescriptor> column_families_sanitized =
      column_families;
  for (size_t i = 0; i < column_families_sanitized.size(); ++i) {
    DBNemoImpl::SanitizeOptions(
        &column_families_sanitized[i].options,
        db_options.env == nullptr ? Env::Default() : db_options.env,
        meta_prefix);
  }
  DB* db;

  Status st;
  if (read_only) {
    st = DB::OpenForReadOnly(db_options, dbname, column_families_sanitized,
                             handles, &db);
  } else {
    st = DB::Open(db_options, dbname, column_families_sanitized, handles, &db);
  }
  if (st.ok()) {
    *dbptr = new DBNemoImpl(db, meta_prefix);
  } else {
    *dbptr = nullptr;
  }
  g_compaction_filter_factory->SetDBAndMP(db, meta_prefix);
  if (st.ok() && !manual_disable_auto_compaction) {
    db->EnableAutoCompaction(*handles);
  }
  return st;
}

Status DBNemoImpl::CreateColumnFamily(const ColumnFamilyOptions& options,
                                         const std::string& column_family_name,
                                         ColumnFamilyHandle** handle) {
  ColumnFamilyOptions sanitized_options = options;
  DBNemoImpl::SanitizeOptions(&sanitized_options, GetEnv(), meta_prefix_);

  return DBNemo::CreateColumnFamily(sanitized_options, column_family_name,
                                       handle);
}

// Returns corruption if the length of the string is lesser than timestamp
// Returns NotFound if the encoded timestamp is lesser than current time
Status DBNemoImpl::SanityCheckTimestamp(const Slice& str, Env* env) {
  if (str.size() < kVersionLength + kTSLength) {
    return Status::Corruption("Error: value's length less than timestamp's\n");
  }

  int32_t timestamp_value = DecodeFixed32(str.data() + str.size() - kTSLength);
  int64_t curtime;
  if (!(env->GetCurrentTime(&curtime)).ok()) {
    return Status::OK();  // Treat the data as fresh if could not get current time
  }
  if (timestamp_value != 0 && timestamp_value < curtime) {
    return Status::NotFound("Is stale\n");
  }
  return Status::OK();
}

// Checks if the string is stale or not according to timestamp provided
bool DBNemoImpl::IsStale(int32_t timestamp, Env* env) {
  if (timestamp <= 0) {  // Data is fresh if TTL is non-positive
    return false;
  }
  int64_t curtime;
  if (!env->GetCurrentTime(&curtime).ok()) {
    return false;  // Treat the data as fresh if could not get current time
  }
  return timestamp < curtime;
}

Status DBNemoImpl::ExtractVersionAndTS(const Slice& value, uint32_t* version, int32_t *timestamp) {
  Status st;
  if (value.size() < kVersionLength + kTSLength) {
    return Status::Corruption("Bad version-timestamp in key-value");
  }
  *version = DecodeFixed32(value.data() + value.size() - kVersionLength - kTSLength);
  *timestamp = DecodeFixed32(value.data() + value.size() - kTSLength);
  return st;
}

void DBNemoImpl::ExtractUserKey(char meta_prefix, const Slice& key, std::string* user_key) {
  if (meta_prefix == kMetaPrefixKv) {
    user_key->assign(key.data(), key.size());
      return;
  }
  if (meta_prefix == key[0]) {
    user_key->assign(key.data()+1, key.size()-1);
  } else {
    int32_t len = *((uint8_t *)(key.data() + 1));
    user_key->assign(key.data() + 2, len);
  }
  return;
}

// Strips the TS from the end of the string
Status DBNemoImpl::StripTS(std::string* str) {
  Status st;
  if (str->length() < kTSLength) {
    return Status::Corruption("Bad timestamp in key-value");
  }
  // Erasing characters which hold the TS
  str->erase(str->length() - kTSLength, kTSLength);
  return st;
}

// Strips the Version and TS from the end of the string
Status DBNemoImpl::StripVersionAndTS(std::string* str) {
  Status st;
  if (str->length() < kVersionLength + kTSLength) {
    return Status::Corruption("Bad version-timestamp in key-value");
  }
  // Erasing characters which hold the TS
  str->erase(str->length() - kVersionLength - kTSLength, kVersionLength + kTSLength);
  return st;
}

Status DBNemoImpl::Put(const WriteOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key,
    const Slice& val) {
  return Put(options, column_family, key, val, 0);
}

Status DBNemoImpl::Put(const WriteOptions& options, ColumnFamilyHandle* column_family, const Slice& key, const Slice& val, int32_t ttl) {
  WriteBatch batch;
  batch.Put(column_family, key, val);
  return Write(options, &batch, ttl);
}

Status DBNemoImpl::Get(const ReadOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key,
    std::string* value) {
  Status st = db_->Get(options, column_family, key, value);
  if (!st.ok()) {
    return st;
  }
  st = SanityCheckVersionAndTS(key, *value);
  if (!st.ok()) {
    return st;
  }
  return StripVersionAndTS(value);
}

std::vector<Status> DBNemoImpl::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  auto statuses = db_->MultiGet(options, column_family, keys, values);
  for (size_t i = 0; i < keys.size(); ++i) {
    if (!statuses[i].ok()) {
      continue;
    }
    statuses[i] = SanityCheckTimestamp((*values)[i], db_->GetEnv());
    if (!statuses[i].ok()) {
      continue;
    }
    statuses[i] = StripTS(&(*values)[i]);
  }
  return statuses;
}

bool DBNemoImpl::KeyMayExist(const ReadOptions& options,
    ColumnFamilyHandle* column_family,
    const Slice& key, std::string* value,
    bool* value_found) {
  bool ret = db_->KeyMayExist(options, column_family, key, value, value_found);
  if (ret && value != nullptr && value_found != nullptr && *value_found) {
    if (!SanityCheckTimestamp(*value, db_->GetEnv()).ok() || !StripTS(value).ok()) {
      return false;
    }
  }
  return ret;
}

Status DBNemoImpl::Merge(const WriteOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key,
    const Slice& value) {
  WriteBatch batch;
  batch.Merge(column_family, key, value);
  return Write(options, &batch);
}

Status DBNemoImpl::PutWithExpiredTime(const WriteOptions& options, const Slice& key, const Slice& val, int32_t expired_time) {
  WriteBatch batch;
  batch.Put(DefaultColumnFamily(), key, val);
  return WriteWithExpiredTime(options, &batch, expired_time);
}

Status DBNemoImpl::PutWithKeyVersion(const WriteOptions& options, const Slice& key, const Slice& val) {
  WriteBatch batch;
  batch.Put(DefaultColumnFamily(), key, val);
  return WriteWithKeyVersion(options, &batch);
}

Status DBNemoImpl::Write(const WriteOptions& opts, WriteBatch* updates) {
  return Write(opts, updates, 0);
}

Status DBNemoImpl::Write(const WriteOptions& opts, WriteBatch* updates, int32_t ttl) {
  class Handler : public WriteBatch::Handler {
   public:
    DBImpl* db_;
    WriteBatch updates_ttl;
    Status batch_rewrite_status;

    explicit Handler(Env* env, int32_t ttl, DB* db, char meta_prefix)
        : db_(reinterpret_cast<DBImpl*>(db)), env_(env), ttl_(ttl),
          meta_prefix_(meta_prefix) {}

    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
      std::string value_with_ver_ts;
      uint32_t version;
      int32_t timestamp;
      GetVersionAndTS(db_, meta_prefix_, key, &version, &timestamp);

//      std::cout << "Write, prefix: " << meta_prefix_ << " key: " << key.ToString() << " value: " << value.ToString() <<  " version: " << version << " timestamp: " << timestamp << std::endl;

      Status st = AppendVersionAndTS(value, &value_with_ver_ts,
                      env_, version, ttl_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      std::string value_with_ver_ts;
      Status st = AppendVersionAndTS(value, &value_with_ver_ts,
                      env_, 0, 0);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                  value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {
      WriteBatchInternal::Delete(&updates_ttl, column_family_id, key);
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) override {
      updates_ttl.PutLogData(blob);
    }

   private:
    Env* env_;
    int32_t ttl_;
    char meta_prefix_;
  };
  //@ADD assign the db pointer
  Handler handler(GetEnv(), ttl, db_, meta_prefix_);

  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Status DBNemoImpl::WriteWithExpiredTime(const WriteOptions& opts, WriteBatch* updates, int32_t expired_time) {
  class Handler : public WriteBatch::Handler {
   public:
    DBImpl* db_;
    WriteBatch updates_ttl;
    Status batch_rewrite_status;

    explicit Handler(Env* env, DB* db, char meta_prefix, int32_t expired_time)
        : db_(reinterpret_cast<DBImpl*>(db)), env_(env),
          expired_time_(expired_time), meta_prefix_(meta_prefix) {}

    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
      std::string value_with_ver_ts;
      uint32_t version;
      int32_t timestamp;
      GetVersionAndTS(db_, meta_prefix_, key, &version, &timestamp);

//      std::cout << "WriteWithExpiredTime, prefix: " << meta_prefix_ << " key: " << key.ToString() << " value: " << value.ToString() <<  " version: " << version << " timestamp: " << timestamp << std::endl;

      Status st = AppendVersionAndExpiredTime(value, &value_with_ver_ts,
                      env_, version, expired_time_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      std::string value_with_ver_ts;
      Status st = AppendVersionAndTS(value, &value_with_ver_ts,
                      env_, 0, 0);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                  value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {
      WriteBatchInternal::Delete(&updates_ttl, column_family_id, key);
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) override {
      updates_ttl.PutLogData(blob);
    }

   private:
    Env* env_;
    int32_t expired_time_;
    char meta_prefix_;
  };
  //@ADD assign the db pointer
  Handler handler(GetEnv(), db_, meta_prefix_, expired_time);

  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Status DBNemoImpl::WriteWithKeyVersion(const WriteOptions& opts, WriteBatch* updates) {
  class Handler : public WriteBatch::Handler {
   public:
    DBImpl* db_;
    WriteBatch updates_ttl;
    Status batch_rewrite_status;

    explicit Handler(Env* env, DB* db, char meta_prefix)
        : db_(reinterpret_cast<DBImpl*>(db)), env_(env),
          meta_prefix_(meta_prefix) {}

    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
      std::string value_with_ver_ts;
      uint32_t version;
      int32_t timestamp;
      GetVersionAndTS(db_, meta_prefix_, key, &version, &timestamp);

//      std::cout << "WriteWithKeyVersionTTL, prefix: " << meta_prefix_ << " key: " << key.ToString() << " value: " << value.ToString() <<  " version: " << version << " timestamp: " << timestamp << std::endl;

      int64_t curtime;
      uint32_t new_version;
      if (!env_->GetCurrentTime(&curtime).ok()) {
        curtime = version;
      }
      new_version = curtime;
      if (curtime <= version) {
        new_version = version + 1;
      }
//      std::cout << "WriteWithKeyVersion, version: " << version << " curtime: " << curtime << " new_version: " << new_version << std::endl;

      Status st = AppendVersionAndTS(value, &value_with_ver_ts,
                      env_, new_version, 0);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      std::string value_with_ver_ts;
      Status st = AppendVersionAndTS(value, &value_with_ver_ts,
                      env_, 0, 0);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                  value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {
      WriteBatchInternal::Delete(&updates_ttl, column_family_id, key);
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) override {
      updates_ttl.PutLogData(blob);
    }

   private:
    Env* env_;
    char meta_prefix_;
  };
  //@ADD assign the db pointer
  Handler handler(GetEnv(), db_, meta_prefix_);

  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Status DBNemoImpl::WriteWithOldKeyTTL(const WriteOptions& opts, WriteBatch* updates) {
  class Handler : public WriteBatch::Handler {
   public:
    DBImpl* db_;
    WriteBatch updates_ttl;
    Status batch_rewrite_status;

    explicit Handler(Env* env, DB* db, char meta_prefix)
        : db_(reinterpret_cast<DBImpl*>(db)), env_(env),
          meta_prefix_(meta_prefix), version_(0),
          timestamp_(0), is_first_(true) {
            env_->GetCurrentTime(&now_);
          }

    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
      std::string value_with_ver_ts;

      if (is_first_) {
//        std::cout << "is first, now: " << now_ << std::endl;
        bool find_meta = GetVersionAndTS(db_, meta_prefix_, key, &version_, &timestamp_);
        if (!find_meta) {
//          std::cout <<  "Update version " << key.ToString() << ", meta not found, use now: " << now_ << std::endl;
          version_ = now_;
        }

        int64_t curtime;
        if (env_->GetCurrentTime(&curtime).ok()) {
            if (timestamp_ != 0 && timestamp_ < curtime) {
//                std::cout << "Meta expired, version++: ";
                version_++;
//                std::cout << version_ << std::endl;
                timestamp_ = 0;
            }
        } else {
            timestamp_ = 0;
        }

        is_first_ = false;
      }
//      std::cout << "Put key: " << key.ToString() << ", version_: " << version_ <<
//        " timestamp_: " << timestamp_ << std::endl;
//      if (key[0] == kMetaPrefixHash) {
//        std::cout << "WriteWithOldKeyTTL --- 1-1, prefix: " << meta_prefix_ << " key: " << key.ToString() << " value: " << *((int64_t*)value.data()) <<  " version: " << version << " timestamp: " << timestamp << std::endl;
//      } else {
//        std::cout << "WriteWithOldKeyTTL --- 1-2, prefix: " << meta_prefix_ << " key: " << key.ToString() << " value: " << value.ToString() <<  " version: " << version << " timestamp: " << timestamp << std::endl;
//      }

//      if (key[0] == kMetaPrefixHash) {
//        std::cout << "WriteWithOldKeyTTL --- 2, prefix: " << meta_prefix_ << " key: " << key.ToString() << " value: " << *((int64_t*)value.data()) <<  " version: " << version << " timestamp: " << timestamp << std::endl;
//      } else {
//        std::cout << "WriteWithOldKeyTTL --- 2, prefix: " << meta_prefix_ << " key: " << key.ToString() << " value: " << value.ToString() <<  " version: " << version << " timestamp: " << timestamp << std::endl;
//      }


      Status st = AppendVersionAndExpiredTime(value, &value_with_ver_ts,
                      env_, version_, timestamp_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      std::string value_with_ver_ts;
      Status st = AppendVersionAndTS(value, &value_with_ver_ts,
                      env_, 0, 0);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                  value_with_ver_ts);
      }
      return Status::OK();
    }
    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {
      WriteBatchInternal::Delete(&updates_ttl, column_family_id, key);
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) override {
      updates_ttl.PutLogData(blob);
    }

   private:
    Env* env_;
    char meta_prefix_;
    int64_t now_;
    uint32_t version_;
    int32_t timestamp_;
    bool is_first_;
  };
  //@ADD assign the db pointer
  Handler handler(GetEnv(), db_, meta_prefix_);

  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Status DBNemoImpl::GetKeyTTL(const ReadOptions& options, const Slice& key, int32_t *ttl) {

    std::string value;
    Status st = db_->Get(options, DefaultColumnFamily(), key, &value);
    if (!st.ok()) {
        return st;
    }

    uint32_t version;
    int32_t timestamp;
    int64_t curtime;

    st = ExtractVersionAndTS(value, &version, &timestamp);
    if (!st.ok()) {
      *ttl = -1;
      return st;
    }

    if (meta_prefix_ == kMetaPrefixKv) {
      if (timestamp == 0) {
        *ttl = -1;
        return Status::OK();
      }
      if (!GetEnv()->GetCurrentTime(&curtime).ok()) {
        *ttl = -1;
        return Status::OK();
      }
      if (curtime -1 > timestamp) {
        *ttl = -2;
        return Status::NotFound("Is Stale");
      } else {
        *ttl = timestamp - curtime + 1 > 0 ? timestamp - curtime + 1 : 0;
      }
      return Status::OK();
    }

    st = SanityCheckVersionAndTS(key, value);
    if (st.ok()) {
      if (timestamp == 0) {
        *ttl = -1;
        return Status::OK();
      }
      if (!GetEnv()->GetCurrentTime(&curtime).ok()) {
        *ttl = -1;
        return Status::OK();
      }
      if (curtime -1 > timestamp) {
        *ttl = -2;
        return Status::NotFound("Is Stale");
      } else {
        *ttl = timestamp - curtime + 1 > 0 ? timestamp - curtime + 1 : 0;
      }
      return Status::OK();
    } else {
      *ttl = -2;
    }
    return st;
}

Iterator* DBNemoImpl::NewIterator(const ReadOptions& opts,
                                     ColumnFamilyHandle* column_family) {
  return new NemoIterator(db_->NewIterator(opts, column_family), db_->GetEnv(), db_, meta_prefix_);
}

void DBNemoImpl::StopAllBackgroundWork(bool wait) {
  CancelAllBackgroundWork(db_, wait);
}

Status DBNemoImpl::AppendVersionAndTS(const Slice& val, 
    std::string* val_with_ver_ts, Env* env, uint32_t version, int32_t ttl) {

  val_with_ver_ts->reserve(kVersionLength + kTSLength + val.size());
  val_with_ver_ts->append(val.data(), val.size());

  char ver_string[kVersionLength];
  EncodeFixed32(ver_string, version);
  val_with_ver_ts->append(ver_string, kVersionLength);

  char ts_string[kTSLength];
  if (ttl <= 0) {
    EncodeFixed32(ts_string, 0);
  } else {
    int64_t curtime;
    Status st = env->GetCurrentTime(&curtime);
    if (!st.ok()) {
      return st;
    }
    EncodeFixed32(ts_string, (int32_t)(curtime+ttl-1));
  }
  val_with_ver_ts->append(ts_string, kTSLength);

  return Status::OK(); 
}

Status DBNemoImpl::AppendVersionAndExpiredTime(const Slice& val, 
    std::string* val_with_ver_ts, Env* env, uint32_t version, int32_t expire_time) {

  val_with_ver_ts->reserve(kVersionLength + kTSLength + val.size());
  val_with_ver_ts->append(val.data(), val.size());

  char ver_string[kVersionLength];
  EncodeFixed32(ver_string, version);
  val_with_ver_ts->append(ver_string, kVersionLength);

  char ts_string[kTSLength];
  EncodeFixed32(ts_string, (int32_t)(expire_time));
  val_with_ver_ts->append(ts_string, kTSLength);

  return Status::OK(); 
}

bool DBNemoImpl::GetVersionAndTS(DB* db, char meta_prefix,
      const Slice& key, uint32_t* version, int32_t* timestamp) {
  *version = *timestamp = 0;

  if (meta_prefix == kMetaPrefixKv) {
    return true;
  }

  std::string value;
  Status s;

  if (meta_prefix == key[0]) {
    s = db->Get(ReadOptions(), key, &value);
//    std::cout << "GetVersionAndTS, meta, " << s.ToString() << " key: " << key.ToString() << " value: " << *((int64_t*)value.data()) << std::endl;
  } else {
    if (key.size() == 1) {
      // this is Seperator between meta and data, just ignore
      *version = *timestamp = 0;
      return true;
    }

    std::string meta_key(1, meta_prefix);
    int32_t len = *((uint8_t*)(key.data()+1));
    meta_key.append(key.data()+2, len);
    s = db->Get(ReadOptions(), meta_key, &value);
//    std::cout << "GetVersionAndTS, data, " << s.ToString() << " key: " << meta_key << " value: " << (*(int64_t*)value.data()) << std::endl;
  }

  if (s.ok()) {
    *version = DecodeFixed32(value.data() + value.size() - kVersionLength - kTSLength);
    *timestamp = DecodeFixed32(value.data() + value.size() - kTSLength);
    return true;
  }

  return false; 

}

Status DBNemoImpl::SanityCheckVersionAndTS(const Slice& key,
                    const Slice& val) {

  std::string meta_value;

  int32_t timestamp_value = DecodeFixed32(val.data() + val.size() - kTSLength);
  // data key
  if (meta_prefix_ != kMetaPrefixKv && meta_prefix_ != key[0]) {
    std::string meta_key(1, meta_prefix_);
    int32_t len = *((uint8_t *)(key.data() + 1));
    meta_key.append(key.data() + 2, len);

    Status st = db_->Get(ReadOptions(), meta_key, &meta_value);
    if (st.ok()) {
      // Checks that Version is not older than key version
      uint32_t meta_version = DecodeFixed32(meta_value.data() + meta_value.size() - kTSLength - kVersionLength);
      uint32_t data_version = DecodeFixed32(val.data() + val.size() - kTSLength - kVersionLength);
      if (data_version < meta_version) {
        return Status::NotFound("old version\n");
      }
    }
    timestamp_value = DecodeFixed32(meta_value.data() + meta_value.size() - kTSLength);
  }

  int64_t curtime;
  // Treat the data as fresh if could not get current time
  if (GetEnv()->GetCurrentTime(&curtime).ok()) {
    if (timestamp_value != 0 && timestamp_value < curtime) { // 0 means fresh
      return Status::NotFound("Is stale\n");
    }
  }

  return Status::OK();
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
