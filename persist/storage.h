/**
 * @file storage.h
 * @brief Abstract storage interface and helpers for the Bigtable emulator.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H

#include "persist/storage_row_tx.h"
#include "persist/proto/storage.pb.h"
#include "absl/strings/str_cat.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#define DBG(TEXT) if(true){ std::cout << (TEXT) << "\n"; std::cout.flush(); }

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
  virtual Status DeleteTable(std::string table_name, std::function<Status(std::string, storage::TableMeta)>&& precondition_fn) const = 0;
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
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H
