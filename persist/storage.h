/**
 * @file storage.h
 * @brief RocksDB-backed storage and row transaction types for the Bigtable emulator.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H

#include <vector>
#include <chrono>
#include <thread>
#include "cell_view.h"
#include "filter.h"
#include "filtered_map.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "persist/proto/storage.pb.h"
#include "absl/strings/str_cat.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/internal/make_status.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <rocksdb/iterator.h>
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"

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
 * Represents a storage row transaction.
 * Transactions in Bigtable are row-scoped and atomic; most operations here refer to row operations.
 */
class StorageRowTX {
 public:
        /** Destroys the transaction. */
        virtual ~StorageRowTX() = default;
        /** Commits the transaction. */
        virtual Status Commit() = 0;
        /** Rolls back the transaction with the given status. */
        virtual Status Rollback(Status s) = 0;

        /**
         * Sets a specific cell value.
         * @param column_family The column family name.
         * @param column_qualifier The column qualifier.
         * @param timestamp The cell timestamp.
         * @param value The cell value.
         */
        virtual Status SetCell(
            std::string const& column_family,
            std::string const& column_qualifier,
            std::chrono::milliseconds timestamp,
            std::string const& value
        ) {
            return Status();
        }

        /**
         * Updates a specific cell with the given functor and returns the old value.
         * @param column_family The column family name.
         * @param column_qualifier The column qualifier.
         * @param timestamp The cell timestamp.
         * @param value The value passed to the update functor.
         * @param update_fn Functor that takes (column_qualifier, value) and returns new value or error.
         */
        virtual StatusOr<absl::optional<std::string>> UpdateCell(
            std::string const& column_family,
            std::string const& column_qualifier,
            std::chrono::milliseconds timestamp,
            std::string& value,
            std::function<StatusOr<std::string>(std::string const&, std::string&&)> const& update_fn
        ) {
            return Status();
        }

        /**
         * Deletes cells in the given time range (inclusive start, exclusive end: start <= timestamp < end).
         * @param column_family The column family name.
         * @param column_qualifier The column qualifier.
         * @param time_range The timestamp range to delete.
         */
        virtual Status DeleteRowColumn(
            std::string const& column_family,
            std::string const& column_qualifier,
            ::google::bigtable::v2::TimestampRange const& time_range
        ) {
            return Status();
        }

        /**
         * Deletes cells in the given time range (inclusive start, exclusive end).
         * @param column_family The column family name.
         * @param column_qualifier The column qualifier.
         * @param start Start of range (inclusive).
         * @param end End of range (exclusive).
         */
        Status DeleteRowColumn(
            std::string const& column_family,
            std::string const& column_qualifier,
            std::chrono::milliseconds& start,
            std::chrono::milliseconds& end
        ) {
            ::google::bigtable::v2::TimestampRange t_range;
            t_range.set_start_timestamp_micros(std::chrono::duration_cast<std::chrono::microseconds>(start).count());
            t_range.set_end_timestamp_micros(std::chrono::duration_cast<std::chrono::microseconds>(end).count());
            return this->DeleteRowColumn(column_family, column_qualifier, t_range);
        }

        /**
         * Deletes the cell at the exact timestamp (removes values where start <= timestamp < start+1ms).
         * @param column_family The column family name.
         * @param column_qualifier The column qualifier.
         * @param value The timestamp of the cell to delete.
         */
        Status DeleteRowColumn(
            std::string const& column_family,
            std::string const& column_qualifier,
            std::chrono::milliseconds& value
        ) {
            std::chrono::milliseconds value_end = value;
            ++value_end;
            return this->DeleteRowColumn(
                column_family,
                column_qualifier,
                value,
                value_end
            );
        }

        /**
         * Deletes the row from the given column family.
         * @param column_family The column family name.
         */
        virtual Status DeleteRowFromColumnFamily(
            std::string const& column_family
        ) {
            return Status();
        }

        /**
         * Deletes the row from all column families (potentially expensive).
         */
        virtual Status DeleteRowFromAllColumnFamilies() {
            return Status();
        }
};

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
        virtual Status Commit();

        /** Rolls back the transaction. */
        virtual Status Rollback(
            Status status
        ) {
            const auto txn_status = txn_->Rollback();
            assert(txn_status.ok());
            delete txn_;
            txn_ = nullptr;
            if (!status.ok()) {
                return status;
            }
            return Status();
        }

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
        inline virtual Status DeleteRowFromColumnFamily(
            std::string const& column_family
        ) override;

        /** Loads row data for the given column into local transaction state. */
        Status LoadRow(std::string const& column_family, std::string const& column_qualifier);

 private:
        /** Active RocksDB transaction. */
        rocksdb::Transaction* txn_;
        /** Read options for the transaction. */
        rocksdb::ReadOptions roptions_;
        /** Row key for this transaction. */
        const std::string row_key_;
        /** Table name for this transaction. */
        const std::string table_name_;
        /** Pointer to the owning storage. */
        RocksDBStorage* db_;

        /**
         * Lazy-loaded row data: [column_family, column_qualifier] => RowData.
         * Use lazyRowData* methods; do not access directly.
         */
        using row_data_key_t = std::tuple<std::string, std::string>;
        std::map<row_data_key_t, storage::RowData> lazy_row_data_;

        /** Removes the column family from local lazy row data. */
        inline void lazyRowDataRemoveColumnFamily(
            const std::string& column_family
        );

        /** Returns true if we have the given column in local transaction state. */
        inline bool hasLazyRowData(
            const std::string& column_family,
            const std::string& column_qualifier
        );

        /** Returns reference to row data for the given column (loads if needed). */
        inline storage::RowData& lazyRowDataRef(
            const std::string& column_family,
            const std::string& column_qualifier
        );

        /** Deletes the column from lazy state and underlying storage. */
        inline Status lazyRowDataDelete(
            const std::string& column_family,
            const std::string& column_qualifier
        );

        /** Private constructor; use RocksDBStorage::RowTransaction(). */
        explicit RocksDBStorageRowTX(
            const std::string table_name,
            const std::string row_key,
            rocksdb::Transaction* txn,
            RocksDBStorage* db
        );

};

/**
 * RocksDB-backed column family stream; iterates over cells in a column family for a row set.
 */
class RocksDBColumnFamilyStream : public AbstractFamilyColumnStreamImpl {
    friend class RocksDBStorage;
 public:
  /**
   * Constructs a column family stream.
   * @param column_family_name Name of the column family; used in returned CellViews.
   * @param handle RocksDB column family handle (must remain valid for this object's lifetime).
   * @param row_set Row keys to include in the stream.
   * @param db Pointer to the RocksDB storage.
   * @param table_name Table name.
   * @param prefetch_all_columns If true, prefetch all columns for each row.
   */
  RocksDBColumnFamilyStream(
    std::string const& column_family_name,
    rocksdb::ColumnFamilyHandle* handle,
    std::shared_ptr<StringRangeSet const> row_set,
    RocksDBStorage* db,
    const std::string table_name,
    const bool prefetch_all_columns
  );

  /** Destroys the stream and releases the row iterator. */
  virtual ~RocksDBColumnFamilyStream();

  /** @copydoc AbstractFamilyColumnStreamImpl::ApplyFilter */
  bool ApplyFilter(InternalFilter const& internal_filter) override {
    return true;
  }
  /** @copydoc AbstractFamilyColumnStreamImpl::HasValue */
  bool HasValue() const override;
  /** @copydoc AbstractFamilyColumnStreamImpl::Value */
  CellView const& Value() const override;
  /** @copydoc AbstractFamilyColumnStreamImpl::Next */
  bool Next(NextMode mode) override;
  /** @copydoc AbstractFamilyColumnStreamImpl::column_family_name */
  std::string const& column_family_name() const override { return column_family_name_; }

 private:
  using TColumnRow = std::map<std::chrono::milliseconds, std::string>;
  using TColumnFamilyRow = std::map<std::string, TColumnRow>;

  /** Column family name. */
  std::string column_family_name_;
  /** Table name. */
  std::string table_name_;
  /** Whether to prefetch all columns per row. */
  const bool prefetch_all_columns_;
  /** RocksDB column family handle. */
  rocksdb::ColumnFamilyHandle* handle_;
  /** Row key set to iterate. */
  std::shared_ptr<StringRangeSet const> row_ranges_;
  /** Row key regex filters. */
  std::vector<std::shared_ptr<re2::RE2 const>> row_regexes_;
  /** Column qualifier range filter. */
  mutable StringRangeSet column_ranges_;
  /** Column qualifier regex filters. */
  std::vector<std::shared_ptr<re2::RE2 const>> column_regexes_;
  /** Timestamp range filter. */
  mutable TimestampRangeSet timestamp_ranges_;
  /** Pointer to RocksDB storage. */
  RocksDBStorage* db_;
  /** Iterator over rows in the column family. */
  mutable rocksdb::Iterator* row_iter_;
  /** Current row key. */
  mutable std::string row_key_;
  /** Current row's column family data (lazy-loaded). */
  mutable absl::optional<TColumnFamilyRow> row_data_;

  /** Advances to the next row in range. */
  void NextRow() const;

  /** Lazily initializes the row iterator and seeks to first row. */
  void InitializeIfNeeded() const;
  /**
   * Adjust the internal iterators after `column_it_` advanced.
   *
   * We need to make sure that either we reach the end of the column family or:
   * * `column_it_` doesn't point to `end()`
   * * `cell_it` points to a cell in the column family pointed to by
   *     `column_it_`
   *
   * @return whether we've managed to find another cell in currently pointed
   *     row.
   */
  bool PointToFirstCellAfterColumnChange() const;
  /**
   * Adjust the internal iterators after `row_it_` advanced.
   *
   * Similarly to `PointToFirstCellAfterColumnChange()` it ensures that all
   * internal iterators are valid (or we've reached `end()`).
   *
   * @return whether we've managed to find another cell
   */
  /** Repositions iterators after row change; returns true if a cell was found. */
  bool PointToFirstCellAfterRowChange() const;

  /** Filtered view of columns in current row. */
  mutable absl::optional<
      RegexFiteredMapView<StringRangeFilteredMapView<TColumnFamilyRow>>>
      columns_;
  /** Filtered view of cells in current column. */
  mutable absl::optional<TimestampRangeFilteredMapView<TColumnRow>> cells_;

  /**
   * Invariant: if row_data_ has value, then column_it_ and cell_it_ are valid
   * (not end).
   */
  mutable absl::optional<RegexFiteredMapView<
      StringRangeFilteredMapView<TColumnFamilyRow>>::const_iterator>
      column_it_;
  mutable absl::optional<
      TimestampRangeFilteredMapView<TColumnRow>::const_iterator>
      cell_it_;
  /** Current cell view (cached). */
  mutable absl::optional<CellView> cur_value_;
  /** Whether the stream has been initialized. */
  mutable bool initialized_{false};
};

/**
 * Merge cell stream over multiple column families using AbstractFamilyColumnStreamImpl.
 * Supports FamilyNameRegex and ColumnRange filters by pruning streams.
 */
class StorageFitleredTableStream : public MergeCellStreams {
 public:
  /** Constructs from a vector of column family stream implementations. */
  explicit StorageFitleredTableStream(
      std::vector<std::unique_ptr<AbstractFamilyColumnStreamImpl>> cf_streams)
      : MergeCellStreams(CreateCellStreams(std::move(cf_streams))) {}

  /** Applies filter; prunes streams that don't match FamilyNameRegex or ColumnRange. */
  bool ApplyFilter(InternalFilter const& internal_filter) override {
    if (!absl::holds_alternative<FamilyNameRegex>(internal_filter) &&
        !absl::holds_alternative<ColumnRange>(internal_filter)) {
        return MergeCellStreams::ApplyFilter(internal_filter);
    }
    // internal_filter is either FamilyNameRegex or ColumnRange
    for (auto stream_it = unfinished_streams_.begin();
        stream_it != unfinished_streams_.end();) {
        auto* cf_stream =
            dynamic_cast<AbstractFamilyColumnStreamImpl*>(&(*stream_it)->impl());
        assert(cf_stream);

        if ((absl::holds_alternative<FamilyNameRegex>(internal_filter) &&
            !re2::RE2::PartialMatch(
                cf_stream->column_family_name(),
                *absl::get<FamilyNameRegex>(internal_filter).regex)) ||
            (absl::holds_alternative<ColumnRange>(internal_filter) &&
            absl::get<ColumnRange>(internal_filter).column_family !=
                cf_stream->column_family_name())) {
        stream_it = unfinished_streams_.erase(stream_it);
        continue;
        }

        if (absl::holds_alternative<ColumnRange>(internal_filter) &&
            absl::get<ColumnRange>(internal_filter).column_family ==
                cf_stream->column_family_name()) {
        cf_stream->ApplyFilter(internal_filter);
        }

        stream_it++;
    }

    return true;
  }

 private:
  /** Builds CellStream vector from column family stream implementations. */
  static std::vector<CellStream> CreateCellStreams(
      std::vector<std::unique_ptr<AbstractFamilyColumnStreamImpl>> cf_streams
  ) {
    std::vector<CellStream> res;
    res.reserve(cf_streams.size());
    for (auto& stream : cf_streams) {
        res.emplace_back(std::move(stream));
    }
    return res;
  }
};

/**
 * RocksDB-backed storage implementation for the Bigtable emulator.
 */
class RocksDBStorage : public Storage {
    friend class RocksDBStorageRowTX;
    friend class RocksDBColumnFamilyStream;
 private:
        /** Creates an iterator over the metadata column family (table keys). */
        inline rocksdb::Iterator* createTablesIterator() const {
            return db->NewIterator(roptions, metaHandle);
        }
 public:
        /** Column family handle type. */
        using CFHandle = rocksdb::ColumnFamilyHandle*;

        /** Constructs storage from a RocksDB config. */
        explicit RocksDBStorage(const storage::StorageRocksDBConfig& arg_storage_config) {
            storage_config = arg_storage_config;
        }

        /** Constructs storage using FLAGS_storage_path and default meta CF name. */
        explicit RocksDBStorage() {
            storage_config = storage::StorageRocksDBConfig();
            storage_config.set_db_path(absl::GetFlag(FLAGS_storage_path));
            storage_config.set_meta_column_family("bte_metadata");
        }

        /** Destroys the storage and closes the database. */
        virtual ~RocksDBStorage() {
            Close();
        }

        /**
         * Builds the RocksDB key for a (table, row, column_qualifier).
         */
        static inline std::string storageKey(
            const std::string& table_name,
            const std::string& row_key,
            const std::string& column_qualifier
        ) {
            return absl::StrCat(table_name, "|", row_key, "|", column_qualifier);
        }

        /**
         * Builds the RocksDB key prefix for a (table, row) to iterate all columns.
         */
        static inline std::string storageKeyPartial(
            const std::string& table_name,
            const std::string& row_key
        ) {
            return absl::StrCat(table_name, "|", row_key);
        }

        /**
         * Parses a RocksDB key into (table_name, row_key, column_qualifier).
         */
        inline std::tuple<std::string, std::string, std::string> RawDataParseRowKey(
            rocksdb::Iterator* iter
        ) {
            const auto key = iter->key().ToString();
            std::vector<std::string_view> result; 
            auto left = key.begin(); 
            for (auto it = left; it != key.end(); ++it) 
            { 
                if (*it == '|') 
                { 
                    result.emplace_back(&*left, it - left); 
                    left = it + 1; 
                } 
            } 
            if (left != key.end()) {
                result.emplace_back(&*left, key.end() - left); 
            }
            return std::make_tuple(std::string(result[0]), std::string(result[1]), std::string(result[2]));
        }

        /**
         * Seeks the iterator to the first key for (table_name, row_prefix).
         * RawData* methods implement lowest-level row storage; this seeks so we iterate a superset of the prefix.
         */
        inline void RawDataSeekPrefixed(
            rocksdb::Iterator* iter,
            const std::string& table_name,
            const std::string& row_prefix
        ) {
            iter->Seek(rocksdb::Slice(storageKeyPartial(table_name, row_prefix)));
        }

        /**
         * Puts serialized row data for (table, row, column) into RocksDB.
         */
        inline Status RawDataPut(
            rocksdb::Transaction* txn,
            const std::string& table_name,
            const std::string& column_family,
            const std::string& row_key,
            const std::string& column_qualifier,
            std::string&& data
        ) {
            const auto& cf = column_families_handles_map[column_family];
            return GetStatus(txn->Put(cf, rocksdb::Slice(storageKey(table_name, row_key, column_qualifier)), rocksdb::Slice(std::move(data))), "Update commit row");
        }

        /**
         * Gets serialized row data for (table, row, column) from RocksDB.
         */
        inline Status RawDataGet(
            rocksdb::Transaction* txn,
            const std::string& table_name,
            const std::string& column_family,
            const std::string& row_key,
            const std::string& column_qualifier,
            std::string* out
        ) {
            rocksdb::ReadOptions roptions;
            const auto& cf = column_families_handles_map[column_family];
            return GetStatus(txn->Get(roptions, cf, rocksdb::Slice(storageKey(table_name, row_key, column_qualifier)), out), "Load commit row", Status());
        }

        /**
         * Deletes the underlying data for the given row's column.
         */
        inline Status RawDataDeleteColumn(
            rocksdb::Transaction* txn,
            const std::string& table_name,
            const std::string& column_family,
            const std::string& row_key,
            const std::string& column_qualifier
        ) {
            const auto& cf = column_families_handles_map[column_family];
            return GetStatus(txn->Delete(cf, rocksdb::Slice(storageKey(table_name, row_key, column_qualifier))), "Delete row column");
        }

        /**
         * Deletes all columns for the given row in the column family (may be more expensive than RawDataDeleteColumn).
         */
        inline Status RawDataDelete(
            rocksdb::Transaction* txn,
            const std::string& table_name,
            const std::string& column_family,
            const std::string& row_key
        ) {
            const auto& cf = column_families_handles_map[column_family];
            // Need to iterate through the columns
            auto it = txn->GetIterator(roptions, cf);
            for(it->Seek(rocksdb::Slice(storageKeyPartial(table_name, row_key))); it->Valid(); it->Next()) {
                const auto& [k_table_name, k_row_key, _] = RawDataParseRowKey(it);
                if (k_table_name != table_name || k_row_key != row_key) {
                    break;
                }
                auto status = GetStatus(txn->Delete(cf, it->key()), "Delete row");
                if (!status.ok()) {
                    delete it;
                    return status;
                }
            }
            delete it;
            return Status();
        }

        /** @copydoc Storage::RowTransaction */
        virtual std::unique_ptr<StorageRowTX> RowTransaction(std::string const& table_name, std::string const& row_key) {
            return std::unique_ptr<RocksDBStorageRowTX>(new RocksDBStorageRowTX(table_name, row_key, StartRocksTransaction(), this));
        }

        /** @copydoc Storage::Close */
        virtual Status Close() {
            if (db == nullptr) {
                return Status();
            }

            DBG("RocksDBStorage:Close killing meta handle");
            for (auto& pair : column_families_handles_map) {
                delete pair.second;
            }
            column_families_handles_map.clear();

            rocksdb::WaitForCompactOptions opts;
            opts.close_db = true;
            DBG("RocksDBStorage:Close waiting for compact");
            auto status = GetStatus(db->WaitForCompact(opts), "Wait for compact");
            if (!status.ok()) {
                DBG(absl::StrCat("RocksDBStorage:Close error: ", status.message()));
                return status;
            }
            DBG("RocksDBStorage:Close deleting DB");
            db = nullptr;
            DBG("RocksDBStorage:Close deleted DB");
            metaHandle = nullptr;
            DBG("RocksDBStorage:Close exit");
            return Status();
        }

        /** @copydoc Storage::Open */
        virtual Status Open(std::vector<std::string> additional_cf_names = {}) {
            options = rocksdb::Options();
            txn_options = rocksdb::TransactionDBOptions();
            woptions = rocksdb::WriteOptions();
            roptions = rocksdb::ReadOptions();

            options.create_if_missing = true;

            std::vector<std::string> existing_cf_names;
            auto status = rocksdb::TransactionDB::ListColumnFamilies(
                options, storage_config.db_path(), &existing_cf_names);
            bool is_new_database = !status.ok();
            if (is_new_database) {
                DBG("RocksDBStorage:Open no existing DB, will create new one");
                existing_cf_names.clear();
                // New database - start with just default CF
                existing_cf_names.push_back(rocksdb::kDefaultColumnFamilyName);
            }

            // Build set of all column families we need
            std::set<std::string> all_cf_set;
            for (auto const& cf_name : existing_cf_names) {
                all_cf_set.insert(cf_name);
            }
            
            // Add additional CFs from parameter
            for (auto const& cf_name : additional_cf_names) {
                all_cf_set.insert(cf_name);
            }
            
            // Always need meta CF
            bool need_meta_cf = all_cf_set.find(storage_config.meta_column_family()) == all_cf_set.end();
            if (need_meta_cf) {
                all_cf_set.insert(storage_config.meta_column_family());
            }

            std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
            for (auto const& cf_name : all_cf_set) {
                column_families.push_back(rocksdb::ColumnFamilyDescriptor(
                    cf_name, rocksdb::ColumnFamilyOptions()));
            }

            std::vector<CFHandle> handles;
            auto open_status = OpenDBWithRetry(options, column_families, &handles);
            
            // If we're creating a new DB and need meta CF, we need to do a two-step process
            if (is_new_database && need_meta_cf && !open_status.ok()) {
                std::vector<rocksdb::ColumnFamilyDescriptor> minimal_cfs;
                minimal_cfs.push_back(rocksdb::ColumnFamilyDescriptor(
                    rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
                
                open_status = OpenDBWithRetry(options, minimal_cfs, &handles);
                if (!open_status.ok()) {
                    DBG(absl::StrCat("RocksDBStorage:Open failed to create new DB: ", open_status.message()));
                    return open_status;
                }
                
                // Step 2: Add meta column family
                CFHandle meta_cf_handle;
                auto create_cf_status = db->CreateColumnFamily(
                    rocksdb::ColumnFamilyOptions(), storage_config.meta_column_family(), &meta_cf_handle);
                
                if (!create_cf_status.ok()) {
                    DBG(absl::StrCat("RocksDBStorage:Open failed to create meta CF: ", create_cf_status.ToString()));
                    // Clean up
                    for (auto h : handles) delete h;
                    delete db;
                    db = nullptr;
                    return StorageError("Failed to create meta column family");
                }
                
                handles.push_back(meta_cf_handle);
            } else if (!open_status.ok()) {
                DBG(absl::StrCat("RocksDBStorage:Open failed: ", open_status.message()));
                return open_status;
            }
            
            metaHandle = nullptr;
            for (auto const& handle : handles) {
                column_families_handles_map[handle->GetName()] = handle;
                if (handle->GetName() == storage_config.meta_column_family()) {
                    metaHandle = handle;
                }
            }

            if (metaHandle == nullptr) {
                return StorageError("Meta column family not found after opening");
            }

            return Status();
        }

        /** @copydoc Storage::CreateTable */
        virtual Status CreateTable(google::bigtable::admin::v2::Table& schema) {
            auto txn = StartRocksTransaction();

            auto key = rocksdb::Slice(schema.name());
            if(KeyExists(txn, metaHandle, key)) {
                return AlreadyExistsError("Table already exists.", GCP_ERROR_INFO().WithMetadata(
                    "table_name", schema.name()));
            }

            storage::TableMeta meta;
            *meta.mutable_table() = schema;
            
            std::vector<std::string> missing_cfs;
            for(auto& cf : meta.table().column_families()) {
                auto iter = column_families_handles_map.find(cf.first);
                if (iter == column_families_handles_map.end()) {
                    missing_cfs.push_back(cf.first);
                }
            }
            
            if (!missing_cfs.empty()) {
                txn->Rollback();
                delete txn;
                
                // Close current database
                Close();
                
                // Open regular RocksDB (not TransactionDB) to add column families
                rocksdb::DB* regular_db;
                std::vector<rocksdb::ColumnFamilyDescriptor> existing_cfs;
                std::vector<std::string> cf_names;
                auto list_status = rocksdb::DB::ListColumnFamilies(options, storage_config.db_path(), &cf_names);
                if (!list_status.ok()) {
                    return StorageError("Failed to list CFs during CF addition");
                }
                
                std::vector<CFHandle> cf_handles;
                for (auto const& name : cf_names) {
                    existing_cfs.push_back(rocksdb::ColumnFamilyDescriptor(name, rocksdb::ColumnFamilyOptions()));
                }
                
                auto db_status = rocksdb::DB::Open(options, storage_config.db_path(), existing_cfs, &cf_handles, &regular_db);
                if (!db_status.ok()) {
                    return StorageError("Failed to open regular DB for CF addition: " + db_status.ToString());
                }
                
                for (auto const& cf_name : missing_cfs) {
                    CFHandle new_handle;
                    auto create_status = regular_db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), cf_name, &new_handle);
                    if (!create_status.ok()) {
                        for (auto h : cf_handles) delete h;
                        delete regular_db;
                        return StorageError("Failed to create column family " + cf_name + ": " + create_status.ToString());
                    }
                    cf_handles.push_back(new_handle);
                }
                
                for (auto h : cf_handles) delete h;
                delete regular_db;
                
                auto reopen_status = Open();
                if (!reopen_status.ok()) {
                    return reopen_status;
                }
                
                txn = StartRocksTransaction();

                missing_cfs.clear();
                for(auto& cf : meta.table().column_families()) {
                    auto iter = column_families_handles_map.find(cf.first);
                    if (iter == column_families_handles_map.end()) {
                        missing_cfs.push_back(cf.first);
                    }
                }
            }
            
            auto status = GetStatus(txn->Put(metaHandle, key, rocksdb::Slice(SerializeTableMeta(meta))), "Put table key to metadata cf");
            
            status = Commit(txn);
            if (!status.ok()) {
                return status;
            }

            // User needs to restart emulator with pre-created column families
            if (!missing_cfs.empty()) {
                std::string missing_list;
                for (auto const& cf : missing_cfs) {
                    if (!missing_list.empty()) missing_list += ", ";
                    missing_list += cf;
                }
                return InvalidArgumentError(
                    "Column families do not exist. Please restart the emulator to create them.",
                    GCP_ERROR_INFO()
                        .WithMetadata("missing_column_families", missing_list)
                        .WithMetadata("table_name", schema.name()));
            }
            
            // TODO: Implement garbage collection for old cell versions based on gc_rule
            return Status();
        }

        /** @copydoc Storage::DeleteTable */
        virtual Status DeleteTable(std::string table_name, std::function<Status(std::string, storage::TableMeta)>&& precondition_fn) const {
            auto txn = StartRocksTransaction();
            
            std::string out;
            auto status = GetStatus(
                txn->Get(roptions, metaHandle, rocksdb::Slice(table_name), &out),
                "Get table",
                NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                    "table_name", table_name)));
            if (!status.ok()) {
                return Rollback(txn, status);
            }
            const auto meta = DeserializeTableMeta(std::move(out));
            if (!meta.ok()) {
                return Rollback(txn, meta.status());
            }
            const auto precondition_status = precondition_fn(table_name, meta.value());
            if (!precondition_status.ok()) {
                return Rollback(txn, precondition_status);
            }

            const auto delete_status = GetStatus(
                txn->Delete(metaHandle, rocksdb::Slice(table_name)),
                "Delete table",
                NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                    "table_name", table_name)));

            if (!delete_status.ok()) {
                return Rollback(txn, delete_status);
            }
            return Commit(txn);
        }

        /** @copydoc Storage::HasTable */
        virtual bool HasTable(std::string table_name) const {
            std::string _;
            return db->KeyMayExist(roptions, metaHandle, rocksdb::Slice(table_name), &_) && db->Get(roptions, metaHandle, rocksdb::Slice(table_name), &_).ok();
        }

        /** @copydoc Storage::GetTable */
        virtual StatusOr<storage::TableMeta> GetTable(std::string table_name) const {
            std::string out;
            auto status = GetStatus(
                db->Get(roptions, metaHandle, rocksdb::Slice(table_name), &out),
                "Get table", 
                NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                    "table_name", table_name)));
            if (!status.ok()) {
                return status;
            }
            return DeserializeTableMeta(std::move(out));
        }

        /** @copydoc Storage::UpdateTableMetadata */
        virtual Status UpdateTableMetadata(std::string table_name, storage::TableMeta const& meta) {
            auto txn = StartRocksTransaction();
            auto key = rocksdb::Slice(table_name);
            auto serialized = SerializeTableMeta(meta);
            
            auto status = GetStatus(
                txn->Put(metaHandle, key, rocksdb::Slice(serialized)),
                "Update table metadata");
            
            if (!status.ok()) {
                txn->Rollback();
                delete txn;
                return status;
            }
            
            return Commit(txn);
        }

        /** @copydoc Storage::EnsureColumnFamiliesExist */
        virtual Status EnsureColumnFamiliesExist(std::vector<std::string> const& cf_names) {
            std::vector<std::string> missing_cfs;
            for (auto const& cf_name : cf_names) {
                auto iter = column_families_handles_map.find(cf_name);
                if (iter == column_families_handles_map.end()) {
                    missing_cfs.push_back(cf_name);
                }
            }
            
            if (missing_cfs.empty()) {
                return Status();
            }
            
            for (auto& pair : column_families_handles_map) {
                delete pair.second;
            }
            column_families_handles_map.clear();
            delete db;
            db = nullptr;
            
            rocksdb::DB* regular_db;
            std::vector<rocksdb::ColumnFamilyDescriptor> existing_cfs;
            std::vector<std::string> existing_cf_names;
            auto list_status = rocksdb::DB::ListColumnFamilies(options, storage_config.db_path(), &existing_cf_names);
            if (!list_status.ok()) {
                return StorageError("Failed to list CFs during CF addition");
            }
            
            std::vector<CFHandle> cf_handles;
            for (auto const& name : existing_cf_names) {
                existing_cfs.push_back(rocksdb::ColumnFamilyDescriptor(name, rocksdb::ColumnFamilyOptions()));
            }
            
            auto db_status = rocksdb::DB::Open(options, storage_config.db_path(), existing_cfs, &cf_handles, &regular_db);
            if (!db_status.ok()) {
                return StorageError("Failed to open regular DB for CF addition: " + db_status.ToString());
            }
            
            for (auto const& cf_name : missing_cfs) {
                CFHandle new_handle;
                auto create_status = regular_db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), cf_name, &new_handle);
                if (!create_status.ok()) {
                    for (auto h : cf_handles) delete h;
                    delete regular_db;
                    return StorageError("Failed to create column family " + cf_name + ": " + create_status.ToString());
                }
                cf_handles.push_back(new_handle);
            }
            
            for (auto h : cf_handles) delete h;
            delete regular_db;
            
            return Open();
        }

        template<bool with_prefix, typename T>
        class TablesMetadataView;

        /**
         * Forward iterator over table metadata (table name, TableMeta) in RocksDB metadata CF.
         * @tparam with_prefix If true, only keys with the given prefix are iterated.
         */
        template<bool with_prefix>
        class TablesMetadataIterator {
         public:
            using iterator_category = std::forward_iterator_tag;
            using value_type = std::tuple<std::string, storage::TableMeta>;
            using difference_type = std::ptrdiff_t;
            using pointer = value_type;
            using reference = value_type;
         private:
            template<bool p_with_prefix, typename T> friend class TablesMetadataView;

            /** Underlying RocksDB iterator. */
            rocksdb::Iterator* iter;
            /** Cached (key, TableMeta) for current position. */
            absl::optional<value_type> loaded_table_data;
            /** Error from last DeserializeTableMeta. */
            Status error;

            /** When with_prefix is true, the key prefix to filter by. */
            std::conditional_t<with_prefix == true, std::string, std::monostate> prefix;

            /** Returns true if the iterator is valid (and optionally matches prefix). */
            inline bool isValid() const {
                if constexpr (with_prefix) {
                    return iter != nullptr && error.ok() && iter->Valid() && iter->key().starts_with(prefix);
                } else {
                    return iter != nullptr && error.ok() && iter->Valid();
                }
            }

            /** Loads (key, TableMeta) from current iterator position. */
            inline void loadTableData() {
                if (!isValid()) {
                    return;
                }
                auto meta = DeserializeTableMeta(iter->value().ToString());
                if (!meta.ok()) {
                    error = meta.status();
                    return;
                }
                loaded_table_data = std::make_pair(iter->key().ToString(), meta.value());
            }

            template<
                bool B = with_prefix,
                typename std::enable_if<B, int>::type = 0
            >
            explicit TablesMetadataIterator(
                rocksdb::Iterator*&& rocks_iterator,
                const std::string& search_prefix
            ): iter(std::move(rocks_iterator)), loaded_table_data(absl::nullopt), prefix(search_prefix) {
                iter->Seek(rocksdb::Slice(prefix));
                loadTableData();
            };

            template<
                bool B = !with_prefix,
                typename std::enable_if<B, int>::type = 0
            >
            explicit TablesMetadataIterator(
                rocksdb::Iterator*&& rocks_iterator
            ): iter(std::move(rocks_iterator)), loaded_table_data(absl::nullopt) {
                iter->SeekToFirst();
                loadTableData();
            };

            explicit TablesMetadataIterator(std::monostate _): iter(nullptr) {}

         public:
            /** Destroys the iterator and deletes the RocksDB iterator. */
            ~TablesMetadataIterator() {
                if (iter != nullptr) {
                    delete iter;
                }
            }

            reference operator*() const { return loaded_table_data.value(); }
            pointer operator->() { return loaded_table_data.value(); }

            /** Prefix increment. */
            TablesMetadataIterator& operator++() { 
                if (!isValid()) {

                    return *this;
                }
                iter->Next();
                loadTableData();
                return *this;
            }

            /** Postfix increment. */
            TablesMetadataIterator operator++(int) { TablesMetadataIterator tmp = *this; ++(*this); return tmp; }

            friend bool operator== (const TablesMetadataIterator& a, const TablesMetadataIterator& b) { return a.iter == b.iter || (!a.isValid() && !b.isValid()); };
            friend bool operator!= (const TablesMetadataIterator& a, const TablesMetadataIterator& b) { return a.iter != b.iter && (a.isValid() || b.isValid()); };

            /** Returns error from last load. */
            Status status() const {
                return error;
            }
        };

        /**
         * Range/view over table metadata (optionally filtered by key prefix).
         * @tparam with_prefix If true, only tables with the given key prefix are included.
         * @tparam T Storage type (e.g. RocksDBStorage) that provides createTablesIterator().
         */
        template<bool with_prefix, typename T>
        class TablesMetadataView {
         private:
            /** When with_prefix, the key prefix. */
            const std::conditional_t<with_prefix, std::string, std::monostate> prefix;
            /** Pointer to the storage instance. */
            const T* storage;

            template<
                bool B = with_prefix,
                typename std::enable_if<B, int>::type = 0
            >
            explicit TablesMetadataView(const T* storage_ptr, const std::string prefix): storage(storage_ptr), prefix(std::move(prefix)) {};

            template<
                bool B = !with_prefix,
                typename std::enable_if<B, int>::type = 0
            >
            explicit TablesMetadataView(const T* storage_ptr): storage(storage_ptr) {};
            
            
            friend T;
         public:
            /** @cond */
            template<
                bool B = with_prefix,
                typename std::enable_if<B, int>::type = 0
            >
            inline TablesMetadataIterator<true> begin() const {
                return TablesMetadataIterator<true>(std::move(storage->createTablesIterator()), prefix);
            }

            template<
                bool B = !with_prefix,
                typename std::enable_if<B, int>::type = 0
            >
            inline TablesMetadataIterator<false> begin() const {
                return TablesMetadataIterator<false>(std::move(storage->createTablesIterator()));
            }
            /** @endcond */

            /** Returns past-the-end iterator. */
            inline TablesMetadataIterator<with_prefix> end() const {
                return TablesMetadataIterator<with_prefix>(std::monostate{});
            }

            /** Returns vector of table names in this view. */
            inline std::vector<std::string> names() const {
                std::vector<std::string> tmp;
                DBG("TablesMetadataView:names listing tables");
                std::transform(begin(), end(), std::back_inserter(tmp), [](auto el) -> auto {
                    DBG(absl::StrCat("TablesMetadataView:names => ", std::get<0>(el)));
                    return std::get<0>(el);
                });
                DBG("TablesMetadataView:names end");
                return tmp;
            }
        };

        /** Returns a view of all table metadata. */
        inline TablesMetadataView<false, RocksDBStorage> Tables() const {
            return TablesMetadataView<false, RocksDBStorage>(this);
        }

        /** Returns a view of table metadata with keys starting with prefix. */
        inline TablesMetadataView<true, RocksDBStorage> Tables(const std::string& prefix) const {
            return TablesMetadataView<true, RocksDBStorage>(this, prefix);
        }

        /** @copydoc Storage::DeleteColumnFamily */
        virtual Status DeleteColumnFamily(std::string const& cf_name) override {
            auto it = column_families_handles_map.find(cf_name);
            if (it == column_families_handles_map.end()) {
                return Status(); // Already deleted or doesn't exist
            }
            auto* handle = it->second;

            // Drop from RocksDB (records drop in MANIFEST)
            auto status = db->DropColumnFamily(handle);
            if (!status.ok()) {
                return GetStatus(status, "DropColumnFamily");
            }

            // Destroy the handle to free memory
            status = db->DestroyColumnFamilyHandle(handle);
            if (!status.ok()) {
                return GetStatus(status, "DestroyColumnFamilyHandle");
            }

            // Remove from internal map so Destructor doesn't double-free
            column_families_handles_map.erase(it);
            
            return Status();
        }

        /** Returns a cell stream over all rows in the table. */
        CellStream StreamTable(
            std::string const& table_name
        ) {
            auto all_rows_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
            return StreamTable(table_name, all_rows_set);
        }

        /**
         * Returns a cell stream over the table for the given row set.
         * @param table_name Table name.
         * @param range_set Row keys to include.
         * @param prefetch_all_columns If true, prefetch all columns per row.
         */
        CellStream StreamTable(
            std::string const& table_name,
            std::shared_ptr<StringRangeSet> range_set,
            bool prefetch_all_columns = false
        ) {
            std::vector<std::unique_ptr<AbstractFamilyColumnStreamImpl>> iters;
            auto table = GetTable(table_name);
            auto const& m = table.value().table().column_families();
            for (auto const& it : m) {
                const auto x = column_families_handles_map[it.first];
                if (x == nullptr) {
                    continue;
                }
                std::unique_ptr<AbstractFamilyColumnStreamImpl> c = std::make_unique<RocksDBColumnFamilyStream>(it.first, x, range_set, this, table_name, prefetch_all_columns);
                iters.push_back(std::move(c));
            }
            return CellStream(std::make_unique<StorageFitleredTableStream>(std::move(iters)));
        }

    private:
        /** RocksDB configuration. */
        storage::StorageRocksDBConfig storage_config;

        /** RocksDB options. */
        rocksdb::Options options;
        /** Transaction DB options. */
        rocksdb::TransactionDBOptions txn_options;
        /** Write options. */
        rocksdb::WriteOptions woptions;
        /** Read options. */
        rocksdb::ReadOptions roptions;
        /** Transaction DB instance. */
        rocksdb::TransactionDB* db;
        /** Handle for the metadata column family. */
        CFHandle metaHandle = nullptr;
        /** Map from column family name to handle. */
        std::map<std::string, CFHandle> column_families_handles_map;

        /** Starts a new RocksDB transaction. */
        inline rocksdb::Transaction* StartRocksTransaction() const {
            return db->BeginTransaction(woptions);
        }

        /** Deserializes RowData from string. */
        inline StatusOr<storage::RowData> DeserializeRow(std::string&& data) const {
            storage::RowData row;
            if (!row.ParseFromString(data)) {
                return StorageError("DeserializeRow()");
            }
            return row;
        }

        /** Serializes RowData to string. */
        inline std::string SerializeRow(storage::RowData row) const {
            std::string out;
            row.SerializeToString(&out);
            return out;
        }

        /** Deserializes TableMeta from string. */
        static inline StatusOr<storage::TableMeta> DeserializeTableMeta(std::string&& data) {
            storage::TableMeta meta;
            if (!meta.ParseFromString(data)) {
                return StorageError("DeserializeTableMeta()");
            }
            assert(meta.has_table());
            return meta;
        }

        /** Serializes TableMeta to string. */
        inline std::string SerializeTableMeta(storage::TableMeta meta) const {
            std::string out;
            meta.SerializeToString(&out);
            return out;
        }

        /** Returns true if the key exists in the column family. */
        inline bool KeyExists(
            rocksdb::Transaction* txn,
            rocksdb::ColumnFamilyHandle* column_family,
            const rocksdb::Slice& key
        ) const {
            std::string _;
            return txn->Get(roptions, column_family, key, &_).ok();
        }

        /** Rolls back the transaction and returns the given status. */
        inline Status Rollback(
            rocksdb::Transaction* txn,
            Status status
        ) const {
            const auto txn_status = txn->Rollback();
            assert(txn_status.ok());
            delete txn;
            if (!status.ok()) {
                return status;
            }
            return Status();
        }

        /** Commits the transaction. */
        inline Status Commit(
            rocksdb::Transaction* txn
        ) const {
            const auto status = txn->Commit();
            assert(status.ok());
            delete txn;
            return Status();
        }

        /** Opens the DB with retries on IO error. */
        inline Status OpenDBWithRetry(
            rocksdb::Options options,
            std::vector<rocksdb::ColumnFamilyDescriptor> column_families,
            std::vector<rocksdb::ColumnFamilyHandle*>* handles
        ) {
            auto status = rocksdb::Status();
            for(auto i = 0; i<5; ++i) {
                status = rocksdb::TransactionDB::Open(options, txn_options, storage_config.db_path(), column_families, handles, &db);
                if (status.IsIOError()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(250));
                    continue;
                } else {
                    break;
                }
            }
            return GetStatus(status, "Open database");
        }

        /** Builds a storage error status. */
        static inline Status StorageError(std::string&& operation) {
            return InternalError("Storage error",
                        GCP_ERROR_INFO().WithMetadata(
                            "operation", operation));
        }

        /** Converts RocksDB status; uses not_found_status when status is NotFound. */
        inline Status GetStatus(rocksdb::Status status, std::string&& operation, Status&& not_found_status) const {
            if (status.IsNotFound()) {
                return not_found_status;
            }
            if (!status.ok()) {
                return StorageError(std::move(operation));
            }
            return Status();
        }

        /** Converts RocksDB status to Storage Status. */
        inline Status GetStatus(rocksdb::Status status, std::string&& operation) const {
            if (!status.ok()) {
                return StorageError(std::move(operation));
            }
            return Status();
        }

};

inline RocksDBStorageRowTX::~RocksDBStorageRowTX() {
    DBG("RocksDBStorageRowTX:~RocksDBStorageRowTX destructor entered");
    if (txn_ != nullptr) {
        DBG("RocksDBStorageRowTX:~RocksDBStorageRowTX performing rollback");
        Rollback(Status());
    }
    DBG("RocksDBStorageRowTX:~RocksDBStorageRowTX exit");
}

inline RocksDBStorageRowTX::RocksDBStorageRowTX(
    const std::string table_name,
    const std::string row_key,
    rocksdb::Transaction* txn,
    RocksDBStorage* db
): table_name_(std::move(table_name)), row_key_(std::move(row_key)), txn_(txn), roptions_(), db_(db) {}

inline void RocksDBStorageRowTX::lazyRowDataRemoveColumnFamily(
    const std::string& column_family
) {
    for (auto it = lazy_row_data_.begin(); it != lazy_row_data_.end();) {   
        (std::get<0>(it->first) == column_family) ? lazy_row_data_.erase(it++) : (++it);
    }
}

inline bool RocksDBStorageRowTX::hasLazyRowData(
    const std::string& column_family,
    const std::string& column_qualifier
) {
    return lazy_row_data_.find(std::move(std::make_tuple(column_family, column_qualifier))) != lazy_row_data_.end();
}

inline storage::RowData& RocksDBStorageRowTX::lazyRowDataRef(
    const std::string& column_family,
    const std::string& column_qualifier
) {
    return lazy_row_data_[std::make_tuple(column_family, column_qualifier)];
}

inline Status RocksDBStorageRowTX::lazyRowDataDelete(
    const std::string& column_family,
    const std::string& column_qualifier
) {
    lazy_row_data_.erase(lazy_row_data_.find(std::move(std::make_tuple(column_family, column_qualifier))));
    auto del_status = db_->RawDataDeleteColumn(
        txn_, table_name_, column_family, row_key_, column_qualifier
    );
    return del_status;
}

inline Status RocksDBStorageRowTX::DeleteRowFromColumnFamily(
    std::string const& column_family
) {
    lazyRowDataRemoveColumnFamily(column_family);
    return db_->RawDataDelete(txn_, table_name_, column_family, row_key_);
}

inline Status RocksDBStorageRowTX::Commit() {
    for (auto const& row_entry : lazy_row_data_) {
        auto out = db_->SerializeRow(row_entry.second);
        auto status = db_->RawDataPut(
            txn_, table_name_, std::get<0>(row_entry.first), row_key_, std::get<1>(row_entry.first), std::move(out)
        );
        if (!status.ok()) {
            return Rollback(status);
        }
    }
    const auto status = txn_->Commit();
    assert(status.ok());
    delete txn_;
    txn_ = nullptr;
    return Status();
}

inline Status RocksDBStorageRowTX::LoadRow(std::string const& column_family, std::string const& column_qualifier) {
    if(!hasLazyRowData(column_family, column_qualifier)) {
        std::string out;
        // Get won't fail if there's no row
        auto get_status = db_->RawDataGet(
            txn_, table_name_, column_family, row_key_, column_qualifier, &out
        );
        if (!get_status.ok()) {
            return get_status;
        }
        // There is no such row
        if (out.size() == 0) {
            lazyRowDataRef(column_family, column_qualifier) = storage::RowData();
        } else {
            auto row_data = db_->DeserializeRow(std::move(out));
            if (!row_data.ok()) {
                return row_data.status();
            }
            lazyRowDataRef(column_family, column_qualifier) = row_data.value();
        }
    }
    return Status();
}

inline Status RocksDBStorageRowTX::SetCell(
    std::string const& column_family,
    std::string const& column_qualifier,
    std::chrono::milliseconds timestamp,
    std::string const& value
) {
    DBG("RocksDBStorageRowTX:SetCell executing");
    auto status = LoadRow(column_family, column_qualifier);
    if (!status.ok()) {
        return status;
    }
    RowDataEnsure(lazyRowDataRef(column_family, column_qualifier), column_qualifier, timestamp, value);
    DBG("RocksDBStorageRowTX:SetCell exit");
    return Status();
}

inline StatusOr<absl::optional<std::string>> RocksDBStorageRowTX::UpdateCell(
    std::string const& column_family,
    std::string const& column_qualifier,
    std::chrono::milliseconds timestamp,
    std::string& value,
    std::function<StatusOr<std::string>(std::string const&, std::string&&)> const& update_fn
) {
    DBG("RocksDBStorageRowTX:UpdateCell executing");
    auto status = LoadRow(column_family, column_qualifier);
    if (!status.ok()) {
        return status;
    }

    auto& row_data = lazyRowDataRef(column_family, column_qualifier);
    absl::optional<std::string> old_value = absl::nullopt;
    const auto& cells = (row_data.mutable_column()->mutable_cells());
    auto cell_it = cells->find(timestamp.count());
    if (cell_it != cells->end()) {
        old_value = cell_it->second;
    }

    auto maybe_new_value = update_fn(column_qualifier, std::move(value));
    if (!maybe_new_value.ok()) {
        return maybe_new_value.status();
    }

    RowDataEnsure(row_data, column_qualifier, timestamp, maybe_new_value.value());
    DBG("RocksDBStorageRowTX:UpdateCell exit");
    return old_value;
}

inline Status RocksDBStorageRowTX::DeleteRowColumn(
    std::string const& column_family,
    std::string const& column_qualifier,
    ::google::bigtable::v2::TimestampRange const& time_range
) {
    DBG("RocksDBStorageRowTX:DeleteRowColumn executing");
    auto status = LoadRow(column_family, column_qualifier);
    if (!status.ok()) {
        return status;
    }

    auto& row_data = lazyRowDataRef(column_family, column_qualifier);
    auto& cells = (*row_data.mutable_column()->mutable_cells());
    
    // Determine time range bounds
    int64_t start_micros = time_range.start_timestamp_micros();
    absl::optional<int64_t> maybe_end_micros = 
        time_range.end_timestamp_micros() == 0 ? absl::nullopt : absl::optional<int64_t>(time_range.end_timestamp_micros());
    
    auto start_millis = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::microseconds(start_micros)).count();
    
    // Delete cells in the time range
    for (auto cell_it = cells.begin(); cell_it != cells.end(); ) {
        int64_t cell_timestamp = cell_it->first;
        
        // Check if in range: [start, end) - timestamps are decreasing order in BT
        bool in_range = cell_timestamp >= start_millis;
        if (maybe_end_micros.has_value()) {
            auto end_millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::microseconds(*maybe_end_micros)).count();
            in_range = in_range && cell_timestamp < end_millis;
        }
        
        if (in_range) {
            DBG(absl::StrCat("RocksDBStorageRowTX:DeleteRowColumn erased cell timestamp ", cell_timestamp));
            cell_it = cells.erase(cell_it);
        } else {
            DBG(absl::StrCat("RocksDBStorageRowTX:DeleteRowColumn preserved cell ", cell_timestamp, " vs start ", start_millis));
            ++cell_it;
        }
    }
    DBG("RocksDBStorageRowTX:DeleteRowColumn end iteration");

    if (cells.empty()) {
        DBG("RocksDBStorageRowTX:DeleteRowColumn empty column deleting");
        auto del_status = lazyRowDataDelete(column_family, column_qualifier);
        if (!del_status.ok()) {
            return del_status;
        }
    }

    DBG("RocksDBStorageRowTX:DeleteRowColumn exit");
    return Status();
}

inline Status RocksDBStorageRowTX::DeleteRowFromAllColumnFamilies() {
    DBG("RocksDBStorageRowTX:DeleteRowFromAllColumnFamilies executing");
    for (auto const& cf_entry : db_->column_families_handles_map) {
        if (cf_entry.first == db_->storage_config.meta_column_family()) {
            // Skip metadata column family
            continue;
        }
        
        auto status = DeleteRowFromColumnFamily(cf_entry.first);
        if (!status.ok()) {
            return status;
        }
    }
    DBG("RocksDBStorageRowTX:DeleteRowFromAllColumnFamilies exit");
    return Status();
}

inline RocksDBColumnFamilyStream::~RocksDBColumnFamilyStream() {
    delete row_iter_;
}

inline RocksDBColumnFamilyStream::RocksDBColumnFamilyStream(
    std::string const& column_family_name,
    rocksdb::ColumnFamilyHandle* handle,
    std::shared_ptr<StringRangeSet const> row_set,
    RocksDBStorage* db,
    const std::string table_name,
    const bool prefetch_all_columns
): 
    column_family_name_(column_family_name),
    handle_(handle),
    row_ranges_(std::move(row_set)),
    column_ranges_(StringRangeSet::All()),
    timestamp_ranges_(TimestampRangeSet::All()),
    db_(db), initialized_(false), row_iter_(nullptr), table_name_(std::move(table_name)), prefetch_all_columns_(prefetch_all_columns) {}


inline bool RocksDBColumnFamilyStream::HasValue() const {
  DBG("RocksDBColumnFamilyStream:HasValue executing");
  InitializeIfNeeded();
  DBG("RocksDBColumnFamilyStream:HasValue exit");
  return row_data_.has_value();
}
inline CellView const& RocksDBColumnFamilyStream::Value() const {
  InitializeIfNeeded();
  if (!cur_value_) {
    cur_value_ = CellView(row_key_, column_family_name_,
                          column_it_.value()->first, cell_it_.value()->first,
                          cell_it_.value()->second);
  }
  return cur_value_.value();
}

inline void RocksDBColumnFamilyStream::NextRow() const {
    DBG("RocksDBColumnFamilyStream:NextRow executing");
    if (!initialized_) {
        auto const& ranges = row_ranges_->disjoint_ranges();
        if (!ranges.empty()) {
            auto const& first_range = *ranges.begin();
            if (absl::holds_alternative<std::string>(first_range.start())) {
                db_->RawDataSeekPrefixed(row_iter_, table_name_, absl::get<std::string>(first_range.start()));
            } else {
                row_iter_->SeekToFirst();
            }
        } else {
            row_iter_->SeekToFirst();
        }
    }

    DBG("RocksDBColumnFamilyStream:NextRow while loop");
    while (row_iter_->Valid()) {
        const auto [table_name, row_key, column_qualifier] = db_->RawDataParseRowKey(row_iter_);
        DBG(absl::StrCat("RocksDBColumnFamilyStream:NextRow table_name=", table_name, " key=", row_key));
        if (table_name != table_name_) {
            break;
        }
        DBG(absl::StrCat("RocksDBColumnFamilyStream:NextRow key=", row_key));
        
        // Check if key is in any of our ranges
        bool in_range = false;
        for (auto const& range : row_ranges_->disjoint_ranges()) {
            if (range.IsWithin(row_key)) {
                in_range = true;
                break;
            }
        }
        
        if (in_range) {
            auto data = TColumnFamilyRow();
            DBG("RocksDBColumnFamilyStream:NextRow collecting columns");
            while(row_iter_->Valid()) {
                const auto [c_table_name, c_row_key, c_column_qualifier] = db_->RawDataParseRowKey(row_iter_);
                DBG(absl::StrCat("RocksDBColumnFamilyStream:NextRow column=", c_column_qualifier));
                if (c_table_name != table_name_ || c_row_key != row_key_) {
                    break;
                }
                auto maybe_row = db_->DeserializeRow(row_iter_->value().ToString());
                if (!maybe_row.ok()) {
                    row_data_ = absl::nullopt;
                    return;
                }
                for (auto const& i : maybe_row.value().column().cells()) {
                    data[c_column_qualifier][std::chrono::milliseconds(i.first)] = i.second;
                }
                row_iter_->Next();
                if (!prefetch_all_columns_) {
                    break;
                }
            }
            DBG("RocksDBColumnFamilyStream:NextRow done collection");
            for (auto i : data) {
                DBG(absl::StrCat("RocksDBColumnFamilyStream:NextRow collected ", i.first));
            }
            row_data_ = std::move(data);
            row_key_ = row_key;
            return;
        }

        bool past_all_ranges = true;
        for (auto const& range : row_ranges_->disjoint_ranges()) {
            if (!range.IsAboveEnd(row_key)) {
                past_all_ranges = false;
                break;
            }
        }

        if (past_all_ranges) {
            break;
        }

        row_iter_->Next();
    }
    row_data_ = absl::nullopt;
}

inline bool RocksDBColumnFamilyStream::Next(NextMode mode) {
  InitializeIfNeeded();
  cur_value_.reset();
  assert(row_data_.has_value());
  assert(column_it_.value() != columns_.value().end());
  assert(cell_it_.value() != cells_.value().end());

  if (mode == NextMode::kCell) {
    ++(cell_it_.value());
    if (cell_it_.value() != cells_.value().end()) {
      return true;
    }
  }
  if (mode == NextMode::kCell || mode == NextMode::kColumn) {
    ++(column_it_.value());
    if (PointToFirstCellAfterColumnChange()) {
      return true;
    }
  }
  DBG("RocksDBColumnFamilyStream:Next advancing to next row");
  NextRow();
  PointToFirstCellAfterRowChange();
  return true;
}

inline void RocksDBColumnFamilyStream::InitializeIfNeeded() const {
  DBG("RocksDBColumnFamilyStream:InitializeIfNeeded executing");
  if (!initialized_) {
    rocksdb::ColumnFamilyHandle* cf = nullptr;
    for (auto const& i : db_->column_families_handles_map) {
        if(i.first == column_family_name_) {
            cf = i.second;
        }
    }
    DBG("RocksDBColumnFamilyStream:InitializeIfNeeded creating row iterator");
    row_iter_ = db_->db->NewIterator(db_->roptions, cf);
    row_data_ = absl::nullopt;
    DBG("RocksDBColumnFamilyStream:InitializeIfNeeded calling NextRow");
    NextRow();
    initialized_ = true;
    PointToFirstCellAfterRowChange();
  }
}

inline bool RocksDBColumnFamilyStream::PointToFirstCellAfterColumnChange() const {
  for (; column_it_.value() != columns_.value().end(); ++(column_it_.value())) {
    cells_ = TimestampRangeFilteredMapView<TColumnRow>(
        column_it_.value()->second, timestamp_ranges_);
    cell_it_ = cells_.value().begin();
    if (cell_it_.value() != cells_.value().end()) {
      return true;
    }
  }
  return false;
}

inline bool RocksDBColumnFamilyStream::PointToFirstCellAfterRowChange() const {
  DBG("RocksDBColumnFamilyStream:PointToFirstCellAfterRowChange executing");
  for (; row_data_.has_value(); NextRow()) {
    // StringRangeFilteredMapView<google::protobuf::Map<std::string, storage::RowColumnData>
    columns_ = RegexFiteredMapView<StringRangeFilteredMapView<TColumnFamilyRow>>(
        StringRangeFilteredMapView<TColumnFamilyRow>(row_data_.value(),
                                                    column_ranges_),
        column_regexes_);
    column_it_ = columns_.value().begin();
    if (PointToFirstCellAfterColumnChange()) {
      return true;
    }
  }
  return false;
}


}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H