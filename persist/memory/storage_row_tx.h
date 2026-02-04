/**
 * @file storage_row_tx.h
 * @brief RocksDB implementation of row-scoped storage transaction.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MEMORY_STORAGE_ROW_TX_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MEMORY_STORAGE_ROW_TX_H

#include "table.h"
#include "persist/storage_row_tx.h"
#include "persist/proto/storage.pb.h"
#include "rocksdb/utilities/transaction.h"
#include <map>
#include <string>
#include <tuple>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class MemoryStorage;

/**
 * RocksDB implementation of a row-scoped storage transaction.
 */
class MemoryStorageRowTX : public StorageRowTX {
  friend class MemoryStorage;
 public:
  /** Destroys the transaction (rolls back if not committed). */
  virtual ~MemoryStorageRowTX();

  /** Commits the transaction. */
  virtual Status Commit() override;

  /** Rolls back the transaction. */
  virtual Status Rollback(Status status) override;

  /** @copydoc StorageRowTX::SetCell */
  virtual Status SetCell(
      std::string const& column_family,
      std::string const& column_qualifier,
      std::chrono::milliseconds timestamp,
      std::string const& value
  ) override;

  /** @copydoc StorageRowTX::UpdateCell */
  virtual StatusOr<absl::optional<std::string>> UpdateCell(
      std::string const& column_family,
      std::string const& column_qualifier,
      std::chrono::milliseconds timestamp,
      std::string& value,
      std::function<StatusOr<std::string>(std::string const&, std::string&&)> const& update_fn
  ) override;

  /** @copydoc StorageRowTX::DeleteRowColumn */
  virtual Status DeleteRowColumn(
      std::string const& column_family,
      std::string const& column_qualifier,
      ::google::bigtable::v2::TimestampRange const& time_range
  ) override;

  /** @copydoc StorageRowTX::DeleteRowFromAllColumnFamilies */
  virtual Status DeleteRowFromAllColumnFamilies() override;

  /** @copydoc StorageRowTX::DeleteRowFromColumnFamily */
  virtual Status DeleteRowFromColumnFamily(
      std::string const& column_family
  ) override;

 private:
  const std::string row_key_;
  const std::string table_name_;
  MemoryStorage* db_;
  std::shared_ptr<Table> table_;

  bool committed_;
  std::stack<absl::variant<DeleteValue, RestoreValue>> undo_;

  void Undo();

  explicit MemoryStorageRowTX(
      const std::string table_name,
      const std::string row_key,
      MemoryStorage* db,
      std::shared_ptr<Table> table
  );
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MEMORY_STORAGE_ROW_TX_H
