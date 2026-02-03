/**
 * @file storage_row_tx.h
 * @brief RocksDB implementation of row-scoped storage transaction.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_STORAGE_ROW_TX_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_STORAGE_ROW_TX_H

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

class RocksDBStorage;

/**
 * RocksDB implementation of a row-scoped storage transaction.
 */
class RocksDBStorageRowTX : public StorageRowTX {
  friend class RocksDBStorage;
 public:
  /** Destroys the transaction (rolls back if not committed). */
  virtual ~RocksDBStorageRowTX();

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

  /** Loads row data for the given column into local transaction state. */
  Status LoadRow(std::string const& column_family, std::string const& column_qualifier);

 private:
  rocksdb::Transaction* txn_;
  rocksdb::ReadOptions roptions_;
  const std::string row_key_;
  const std::string table_name_;
  RocksDBStorage* db_;

  using row_data_key_t = std::tuple<std::string, std::string>;
  std::map<row_data_key_t, storage::RowData> lazy_row_data_;

  void lazyRowDataRemoveColumnFamily(const std::string& column_family);
  bool hasLazyRowData(const std::string& column_family, const std::string& column_qualifier);
  storage::RowData& lazyRowDataRef(const std::string& column_family, const std::string& column_qualifier);
  Status lazyRowDataDelete(const std::string& column_family, const std::string& column_qualifier);

  explicit RocksDBStorageRowTX(
      const std::string table_name,
      const std::string row_key,
      rocksdb::Transaction* txn,
      RocksDBStorage* db
  );
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_STORAGE_ROW_TX_H
