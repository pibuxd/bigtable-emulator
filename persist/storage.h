/**
 * @file storage.h
 * @brief Abstract storage interface and helpers for the Bigtable emulator.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H

#include "persist/logging.h"
#include "persist/storage_row_tx.h"
#include "persist/metadata_view.h"
#include "persist/proto/storage.pb.h"
#include "absl/strings/str_cat.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/flags/declare.h"
#include "filter.h"
#include "absl/flags/flag.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

ABSL_DECLARE_FLAG(std::string, storage_path);

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

/**
 * Ensures a cell value is set in the row data.
 * @param row The row data to update.
 * @param column_qualifier The column qualifier for the cell.
 * @param timestamp The cell timestamp.
 * @param value The cell value.
 * @todo Maybe rename and move to a better place.
 */
static inline void RowDataEnsure(
    storage::RowData& row,
    std::string const& column_qualifier,
    std::chrono::milliseconds const& timestamp,
    std::string const& value
) {
  DBG(absl::StrCat("RowDataEnsure: setting cell timestamp ", timestamp.count()));
  (*(*row.mutable_column()).mutable_cells())[timestamp.count()] = value;
}

/**
 * Creates a dummy table schema proto with the given name.
 * @param table_name The name for the table in the schema.
 * @return A Table proto with only the name set.
 */
static inline google::bigtable::admin::v2::Table FakeSchema(std::string const& table_name) {
  google::bigtable::admin::v2::Table schema;
  schema.set_name(table_name);
  return schema;
}

/**
 * Abstract storage interface for the Bigtable emulator.
 */
class Storage {
 public:
  /** Default constructor. */
  Storage() {}
  /** Destroys the storage. */
  virtual ~Storage() = default;
  /** Closes the storage and releases resources. */
  virtual Status Close() = 0;

  /** Starts a row-scoped transaction. */
  virtual std::unique_ptr<StorageRowTX> RowTransaction(std::string const& table_name, std::string const& row_key) = 0;

  /** Opens the storage; optionally creates additional column families. */
  virtual Status Open(std::vector<std::string> additional_cf_names = {}) = 0;

  /** Creates a table with the given schema. */
  virtual Status CreateTable(google::bigtable::admin::v2::Table& schema) = 0;

  /** Deletes a table after running the precondition. */
  virtual Status DeleteTable(std::string table_name, std::function<Status(std::string, storage::TableMeta)>&& precondition_fn) = 0;
  /** Returns table metadata. */
  virtual StatusOr<storage::TableMeta> GetTable(std::string table_name) const = 0;
  /** Updates table metadata (e.g. after CreateTable()). */
  virtual Status UpdateTableMetadata(std::string table_name, storage::TableMeta const& meta) = 0;
  /** Ensures the given column families exist. */
  virtual Status EnsureColumnFamiliesExist(std::vector<std::string> const& cf_names) = 0;
  /** Returns true if the table exists. */
  virtual bool HasTable(std::string table_name) const = 0;
  /** Deletes a column family (may be expensive depending on implementation). */
  virtual Status DeleteColumnFamily(std::string const& cf_name) = 0;

  /** Returns a view of all table metadata. */
  virtual CachedTablesMetadataView Tables() const {
    return Tables("");
  }

  /** Returns a view of table metadata with keys starting with prefix. */
  virtual CachedTablesMetadataView Tables(const std::string& prefix) const = 0;

  virtual StatusOr<CellStream> StreamTableFull(
    std::string const& table_name
  ) {
    return this->StreamTable(table_name, false);
  }

  virtual StatusOr<CellStream> StreamTable(
      std::string const& table_name,
      bool prefetch_all_columns
  ) {
    auto all_rows_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
    return this->StreamTable(table_name, all_rows_set, prefetch_all_columns);
  }

  /**
  * Returns a cell stream over the table for the given row set.
  * @param table_name Table name.
  * @param range_set Row keys to include.
  * @param prefetch_all_columns If true, prefetch all columns per row.
  */
  virtual StatusOr<CellStream> StreamTable(
      std::string const& table_name,
      std::shared_ptr<StringRangeSet> range_set,
      bool prefetch_all_columns
  ) = 0;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H
