// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef ROCKSDB_LITE

#include "db_nemo_impl.h"

#include "rocksdb/convenience.h"

namespace rocksdb {

void DBNemoImpl::SanitizeOptions(ColumnFamilyOptions* options,
                                    Env* env) {
  if (options->compaction_filter) {
    options->compaction_filter =
        new NemoCompactionFilter(env, options->compaction_filter);
  } else {
    options->compaction_filter_factory =
        std::shared_ptr<CompactionFilterFactory>(new NemoCompactionFilterFactory(
         env, options->compaction_filter_factory));
  }

  if (options->merge_operator) {
    options->merge_operator.reset(
        new NemoMergeOperator(options->merge_operator, env));
  }
}

// Open the db inside DBNemoImpl because options needs pointer to its ttl
DBNemoImpl::DBNemoImpl(DB* db) : DBNemo(db) {}

DBNemoImpl::~DBNemoImpl() {
  // Need to stop background compaction before getting rid of the filter
  CancelAllBackgroundWork(db_, /* wait = */ true);
  delete GetOptions().compaction_filter;
}

Status DBNemo::Open(const Options& options, const std::string& dbname,
                       DBNemo** dbptr, bool read_only) {

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DBNemo::Open(db_options, dbname, column_families, &handles,
                             dbptr, read_only);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }
  return s;
}

Status DBNemo::Open(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DBNemo** dbptr,
    bool read_only) {

  std::vector<ColumnFamilyDescriptor> column_families_sanitized =
      column_families;
  for (size_t i = 0; i < column_families_sanitized.size(); ++i) {
    DBNemoImpl::SanitizeOptions(
        &column_families_sanitized[i].options,
        db_options.env == nullptr ? Env::Default() : db_options.env);
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
    *dbptr = new DBNemoImpl(db);
  } else {
    *dbptr = nullptr;
  }
  return st;
}

Status DBNemoImpl::CreateColumnFamily(const ColumnFamilyOptions& options,
                                         const std::string& column_family_name,
                                         ColumnFamilyHandle** handle) {
  ColumnFamilyOptions sanitized_options = options;
  DBNemoImpl::SanitizeOptions(&sanitized_options, GetEnv());

  return DBNemo::CreateColumnFamily(sanitized_options, column_family_name,
                                       handle);
}

// Returns corruption if the length of the string is lesser than timestamp
// Returns NotFound if the encoded timestamp is lesser than current time
Status DBNemoImpl::SanityCheckTimestamp(const Slice& str, Env* env) {
  if (str.size() < kTSLength) {
    return Status::Corruption("Error: value's length less than timestamp's\n");
  }

  int32_t timestamp_value = DecodeFixed32(str.data() + str.size() - kTSLength);
  int64_t curtime;
//  if (!(db_->GetEnv()->GetCurrentTime(&curtime)).ok()) {
  if (!(env->GetCurrentTime(&curtime)).ok()) {
    return Status::OK();  // Treat the data as fresh if could not get current time
  }
  if (timestamp_value != 0 && timestamp_value < curtime) {
    return Status::NotFound("Is stale\n");
  }
  return Status::OK();
}

// Checks if the string is stale or not according to TTl provided
bool DBNemoImpl::IsStale(const Slice& value, int32_t ttl, Env* env) {
  if (ttl <= 0) {  // Data is fresh if TTL is non-positive
    return false;
  }
  int64_t curtime;
  if (!env->GetCurrentTime(&curtime).ok()) {
    return false;  // Treat the data as fresh if could not get current time
  }
  int32_t timestamp_value =
      DecodeFixed32(value.data() + value.size() - kTSLength);
  return (timestamp_value + ttl) < curtime;
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
  st = SanityCheckTimestamp(*value, db_->GetEnv());
  if (!st.ok()) {
    return st;
  }
  return StripTS(value);
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

Status DBNemoImpl::Write(const WriteOptions& opts, WriteBatch* updates) {
  return Write(opts, updates, 0);
}

Status DBNemoImpl::Write(const WriteOptions& opts, WriteBatch* updates, int32_t ttl) {
  class Handler : public WriteBatch::Handler {
   public:
    DBImpl* db_;
    WriteBatch updates_ttl;
    Status batch_rewrite_status;

    explicit Handler(Env* env, int32_t ttl, DB* db)
        : db_(reinterpret_cast<DBImpl*>(db)), env_(env), ttl_(ttl) {}

    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
      std::string value_with_ts;

      Status st = AppendTS(value, &value_with_ts, env_, ttl_);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Put(&updates_ttl, column_family_id, key,
                                value_with_ts);
      }
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      std::string value_with_ts;
      Status st = AppendTS(value, &value_with_ts, env_, 0);
      if (!st.ok()) {
        batch_rewrite_status = st;
      } else {
        WriteBatchInternal::Merge(&updates_ttl, column_family_id, key,
                                  value_with_ts);
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
  };
  //@ADD assign the db pointer
  Handler handler(GetEnv(), ttl, db_);

  updates->Iterate(&handler);
  if (!handler.batch_rewrite_status.ok()) {
    return handler.batch_rewrite_status;
  } else {
    return db_->Write(opts, &(handler.updates_ttl));
  }
}

Iterator* DBNemoImpl::NewIterator(const ReadOptions& opts,
                                     ColumnFamilyHandle* column_family) {
  return new NemoIterator(db_->NewIterator(opts, column_family), db_->GetEnv());
}

Status DBNemoImpl::AppendTS(const Slice& val, std::string* val_with_ts,
                               Env* env, int32_t ttl) {
  val_with_ts->reserve(kTSLength + val.size());
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
  val_with_ts->append(val.data(), val.size());
  val_with_ts->append(ts_string, kTSLength);
  return Status::OK();
}
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
