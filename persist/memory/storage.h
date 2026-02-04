/**
 * @file storage.h
 * @brief volatile memory-backed storage implementation for the Bigtable emulator.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MEMORY_STORAGE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MEMORY_STORAGE_H

#include "persist/storage.h"
#include "persist/metadata_view.h"
#include "persist/storage_row_tx.h"
#include "persist/memory/storage_row_tx.h"
#include "table.h"
#include "persist/rocksdb/storage_row_tx.h"
#include "persist/rocksdb/column_family_stream.h"
#include "persist/rocksdb/filtered_table_stream.h"
#include "filter.h"
#include "table.h"
#include "persist/proto/storage.pb.h"
#include "absl/strings/str_cat.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/internal/make_status.h"
#include "google/protobuf/util/field_mask_util.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/iterator.h"
#include "absl/types/optional.h"
#include <algorithm>
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

using google::protobuf::util::FieldMaskUtil;

/**
 * RocksDB-backed storage implementation for the Bigtable emulator.
 */
class MemoryStorage : public Storage {
 private:
  std::map<std::string, std::shared_ptr<Table>> table_by_name_;
  mutable std::mutex mu_;

  StatusOr<std::shared_ptr<Table>> FindTable(std::string const& table_name) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = table_by_name_.find(table_name);
      if (it == table_by_name_.end()) {
        return NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                                                  "table_name", table_name));
      }
      return it->second;
    }
  };

 public:

  /** Constructs storage from a RocksDB config. */
  explicit MemoryStorage() {}

  virtual CachedTablesMetadataView Tables(const std::string& prefix) const override {
    std::vector<std::tuple<std::string, storage::TableMeta>> tables_metas;
    std::transform(table_by_name_.begin(), table_by_name_.end(), std::back_inserter(tables_metas), [](auto el) -> auto {
      const auto schema = el.second->GetSchema();
      storage::TableMeta meta;
      *meta.mutable_table() = schema;
      return std::make_tuple(el.first, meta);
    });

    DBG("$$$$  PREFIXERR");
    if (prefix.size() > 0) {
      std::vector<std::tuple<std::string, storage::TableMeta>> tables_metas_filtered;
      std::copy_if (tables_metas.begin(), tables_metas.end(), std::back_inserter(tables_metas_filtered), [prefix](auto el) {
        DBG(absl::StrCat("$$$$$ prefix ", std::get<0>(el), "|", prefix));
        return std::get<0>(el).rfind(prefix, 0) == 0;
      });
      std::swap(tables_metas_filtered, tables_metas);
    }

    return CachedTablesMetadataView(tables_metas);
  }

  /** Closes the storage and releases resources. */
  virtual Status Close() override {
    return Status();
  };

  /** Starts a row-scoped transaction. */
  virtual std::unique_ptr<StorageRowTX> RowTransaction(std::string const& table_name, std::string const& row_key) override {
    auto maybe_table = FindTable(table_name);
    // FIXME: Maybe this should return status somewhere instead of doing assert and failing
    assert(maybe_table.ok());
    return std::unique_ptr<MemoryStorageRowTX>(new MemoryStorageRowTX(table_name, row_key, this, maybe_table.value()));
  };

  /** Opens the storage; optionally creates additional column families. */
  virtual Status Open(std::vector<std::string> additional_cf_names = {}) override {
    return Status();
  };

  /** Creates a table with the given schema. */
  virtual Status CreateTable(google::bigtable::admin::v2::Table& schema) override {
    auto maybe_table = Table::Create(schema);
    if (!maybe_table) {
      return maybe_table.status();
    }
    {
      std::lock_guard<std::mutex> lock(mu_);
      const auto& table_name = schema.name();
      if (!table_by_name_.emplace(table_name, *maybe_table).second) {
        return google::cloud::internal::AlreadyExistsError(
            "Table already exists.",
            GCP_ERROR_INFO().WithMetadata("table_name", table_name));
      }
    }
    return Status();
  };

  /** Deletes a table after running the precondition. */
  virtual Status DeleteTable(std::string table_name, std::function<Status(std::string, storage::TableMeta)>&& precondition_fn) override {
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = table_by_name_.find(table_name);
      if (it == table_by_name_.end()) {
        return NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                                                   "table_name", table_name));
      }
      storage::TableMeta table_meta;
      *table_meta.mutable_table() = it->second->GetSchema();
      const auto precondition_status = precondition_fn(table_name, table_meta);
      if (!precondition_status.ok()) {
        return precondition_status;
      }
      table_by_name_.erase(it);
    }
    return Status();
  };
  /** Returns table metadata. */
  virtual StatusOr<storage::TableMeta> GetTable(std::string table_name) const override {
    std::shared_ptr<Table> found_table;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = table_by_name_.find(table_name);
      if (it == table_by_name_.end()) {
        return NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                                                  "table_name", table_name));
      }
      found_table = it->second;
    }
    storage::TableMeta table_meta;
    *table_meta.mutable_table() = found_table->GetSchema();
    return table_meta;
  };
  /** Updates table metadata (e.g. after CreateTable()). */
  virtual Status UpdateTableMetadata(std::string table_name, storage::TableMeta const& meta) override {
    auto maybe_table = FindTable(table_name);
    if (!maybe_table.ok()) {
      return maybe_table.status();
    }
    google::protobuf::FieldMask allowed_mask;
    FieldMaskUtil::FromString(
        "change_stream_config,"
        "change_stream_config.retention_period,"
        "deletion_protection",
        &allowed_mask);
    return (*maybe_table)->Update(meta.table(), allowed_mask);
  };
  /** Ensures the given column families exist. */
  virtual Status EnsureColumnFamiliesExist(std::vector<std::string> const& cf_names) override {
    return Status();
  };
  /** Returns true if the table exists. */
  virtual bool HasTable(std::string table_name) const override {
    std::lock_guard<std::mutex> lock(mu_);
    return table_by_name_.find(table_name) != table_by_name_.end();
  };
  /** Deletes a column family (may be expensive depending on implementation). */
  virtual Status DeleteColumnFamily(std::string const& cf_name) override {
    return Status();
  };

  virtual StatusOr<CellStream> StreamTable(
    std::string const& table_name,
    std::shared_ptr<StringRangeSet> range_set,
    bool prefetch_all_columns
  ) override {
    auto maybe_table = FindTable(table_name);
    // FIXME: Propagate error instead of failing on assert
    if(!maybe_table.ok()) {
      return maybe_table.status();
    }
    return maybe_table.value()->CreateCellStream(range_set, absl::nullopt);
  }
  
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MEMORY_STORAGE_H
