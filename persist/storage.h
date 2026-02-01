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
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"

#define DBG(TEXT) if(true){ std::cout << (TEXT) << "\n"; std::cout.flush(); }

ABSL_DECLARE_FLAG(std::string, storage_path);


namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

// Helper method to set the cell
// TODO: Maybe rename it and move it to some better place?
static inline void RowDataEnsure(
    storage::RowData& row,
    std::string const& column_qualifier,
    std::chrono::milliseconds const& timestamp,
    std::string const& value
) {
    DBG("SETCELL timestamp is ");
    DBG(timestamp.count());
    (*(*row.mutable_columns())[column_qualifier].mutable_cells())[timestamp.count()] = value;
}

// Helper method to create dummy table schema proto
static inline google::bigtable::admin::v2::Table FakeSchema(std::string const& table_name) {
  google::bigtable::admin::v2::Table schema;
  schema.set_name(table_name);
  return schema;
}

/**
 * This class represents storage row transaction.
 * Transactions in BT are row-scoped and should be atomic, so most of the things here
 * refer to row operations.
 */
class StorageRowTX {
    public:

        virtual ~StorageRowTX() = default;
        virtual Status Commit() = 0;
        virtual Status Rollback(Status s) = 0;

        virtual Status SetCell(
            std::string const& column_family,
            std::string const& column_qualifier,
            std::chrono::milliseconds timestamp,
            std::string const& value
        ) {
            return Status();
        }

        virtual StatusOr<absl::optional<std::string>> UpdateCell(
            std::string const& column_family,
            std::string const& column_qualifier,
            std::chrono::milliseconds timestamp,
            std::string& value,
            std::function<StatusOr<std::string>(std::string const&, std::string&&)> const& update_fn
        ) {
            return Status();
        }
        
        virtual Status DeleteRowColumn(
            std::string const& column_family,
            std::string const& column_qualifier,
            ::google::bigtable::v2::TimestampRange const& time_range
        ) {
            return Status();
        }
        
        virtual Status DeleteRowFromColumnFamily(
            std::string const& column_family
        ) {
            return Status();
        }

        virtual Status DeleteRowFromAllColumnFamilies() {
            return Status();
        }
        // }
};

class Storage {
    public:
        Storage() {};
        virtual ~Storage() = default;
        virtual Status Close() = 0;

        virtual std::unique_ptr<StorageRowTX> RowTransaction(std::string const& row_key) = 0;

        virtual Status Open(std::vector<std::string> additional_cf_names = {}) = 0;

        virtual Status CreateTable(google::bigtable::admin::v2::Table& schema) = 0;

        virtual Status DeleteTable(std::string table_name, std::function<Status(std::string, storage::TableMeta)>&& precondition_fn) const = 0;
        virtual StatusOr<storage::TableMeta> GetTable(std::string table_name) const = 0;
        virtual Status UpdateTableMetadata(std::string table_name, storage::TableMeta const& meta) = 0;
        virtual Status EnsureColumnFamiliesExist(std::vector<std::string> const& cf_names) = 0;
        virtual bool HasTable(std::string table_name) const = 0;

        virtual Status ForEachTable(std::function<Status(std::string, storage::TableMeta)>&& fn) const = 0;

        virtual Status ForEachTable(std::function<Status(std::string, storage::TableMeta)>&& fn, const std::string& prefix) const = 0;

        virtual Status DeleteColumnFamily(std::string const& cf_name) = 0;
};

class RocksDBStorage;

// Transaction implementation
class RocksDBStorageRowTX : public StorageRowTX {
    friend class RocksDBStorage;
    public:
        virtual ~RocksDBStorageRowTX();

        virtual Status Commit();

        virtual Status Rollback(
            Status status
        ) {
            std::cout << "TX ROLLBACK\n";
            const auto txn_status = txn_->Rollback();
            assert(txn_status.ok());
            delete txn_;
            txn_ = nullptr;
            if (!status.ok()) {
                return status;
            }
            return Status();
        }

        virtual Status SetCell(
            std::string const& column_family,
            std::string const& column_qualifier,
            std::chrono::milliseconds timestamp,
            std::string const& value
        ) override;

        virtual StatusOr<absl::optional<std::string>> UpdateCell(
            std::string const& column_family,
            std::string const& column_qualifier,
            std::chrono::milliseconds timestamp,
            std::string& value,
            std::function<StatusOr<std::string>(std::string const&, std::string&&)> const& update_fn
        ) override;

        virtual Status DeleteRowColumn(
            std::string const& column_family,
            std::string const& column_qualifier,
            ::google::bigtable::v2::TimestampRange const& time_range
        ) override;

        virtual Status DeleteRowFromAllColumnFamilies() override;

        inline virtual Status DeleteRowFromColumnFamily(
            std::string const& column_family
        ) override;

        Status LoadRow(std::string const& column_family);
    private:
        rocksdb::Transaction* txn_;
        rocksdb::ReadOptions roptions_;
        std::string row_key_;
        RocksDBStorage* db_;
        
        // This is loaded row data. We load it lazily
        std::map<std::string, storage::RowData> data_;

        explicit RocksDBStorageRowTX(
            std::string const& row_key,
            rocksdb::Transaction* txn,
            RocksDBStorage* db
        );

};

// This is stupid implementation of the column stream
// You have implementation later in this file
class RocksDBColumnFamilyStream : public AbstractFamilyColumnStreamImpl {
 friend class RocksDBStorage;
 public:
  /**
   * Construct a new object.
   *
   * @column_family the family to iterate over. It should not change over this
   *     objects lifetime.
   * @column_family_name the name of this column family. It will be used to
   *     populate the returned `CellView`s.
   * @row_set the row set indicating which row keys include in the returned
   *     values.
   */
  RocksDBColumnFamilyStream(
    std::string const& column_family_name,
    rocksdb::ColumnFamilyHandle* handle,
    std::shared_ptr<StringRangeSet const> row_set,
    RocksDBStorage* db
  );

  bool ApplyFilter(InternalFilter const& internal_filter) override {
    return true;
  }
  bool HasValue() const override;
  CellView const& Value() const override;
  bool Next(NextMode mode) override;
  std::string const& column_family_name() const override { return column_family_name_; }

 private:

  using TColumnRow = std::map<std::chrono::milliseconds, std::string>;
  using TColumnFamilyRow = std::map<std::string, TColumnRow>;

  std::string column_family_name_;
  rocksdb::ColumnFamilyHandle* handle_;
  std::shared_ptr<StringRangeSet const> row_ranges_;
  std::vector<std::shared_ptr<re2::RE2 const>> row_regexes_;
  mutable StringRangeSet column_ranges_;
  std::vector<std::shared_ptr<re2::RE2 const>> column_regexes_;
  mutable TimestampRangeSet timestamp_ranges_;
  RocksDBStorage* db_;
  mutable rocksdb::Iterator* row_iter_;
  mutable std::string row_key_;
  mutable absl::optional<TColumnFamilyRow> row_data_;

  void NextRow() const;

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
  bool PointToFirstCellAfterRowChange() const;

  mutable absl::optional<
      RegexFiteredMapView<StringRangeFilteredMapView<TColumnFamilyRow>>>
      columns_;
  mutable absl::optional<TimestampRangeFilteredMapView<TColumnRow>> cells_;

  // If row_it_ == rows_.end() we've reached the end.
  // We maintain the following invariant:
  //   if (row_it_ != rows_.end()) then
  //   cell_it_ != cells.end() && column_it_ != columns_.end().
  mutable absl::optional<RegexFiteredMapView<
      StringRangeFilteredMapView<TColumnFamilyRow>>::const_iterator>
      column_it_;
  mutable absl::optional<
      TimestampRangeFilteredMapView<TColumnRow>::const_iterator>
      cell_it_;
  mutable absl::optional<CellView> cur_value_;
  mutable bool initialized_{false};
};

// This is special version of merge cell stream (for multiple column families)
// this was copied from FilteredTableStream
// The only difference is that we use "AbstractFamilyColumnStreamImpl" instead of "FilteredColumnFamilyStream" type
// (So this exists only for type compatibility)
//
// Later we can just get rid of original FilteredTableStream (this class is more generic thus better ;))
class StorageFitleredTableStream : public MergeCellStreams {
 public:
  StorageFitleredTableStream(
      std::vector<std::unique_ptr<AbstractFamilyColumnStreamImpl>> cf_streams)
      : MergeCellStreams(CreateCellStreams(std::move(cf_streams))) {}

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

// Implementation of the RocksDB storage >:3c
class RocksDBStorage : public Storage {
    friend class RocksDBStorageRowTX;
    friend class RocksDBColumnFamilyStream;
    public:
        // This isn't that useful. Maybe you can find better name for it?
        using CFHandle = rocksdb::ColumnFamilyHandle*;

        RocksDBStorage() {
            storage_config = storage::StorageRocksDBConfig();
            storage_config.set_db_path(absl::GetFlag(FLAGS_storage_path));
            storage_config.set_meta_column_family("bte_metadata");
        }
        
        virtual ~RocksDBStorage() {
            if (db != nullptr) {
                for (auto& pair : column_families_handles_map) {
                    delete pair.second;
                }
                column_families_handles_map.clear();
                delete db;
                db = nullptr;
            }
        }

        void ExampleFun() {
            auto r = rocksdb::ReadOptions();
            auto cf = column_families_handles_map["test_column_family"];
            rocksdb::Iterator* iter = db->NewIterator(r, cf);
        }

        // Start transaction
        virtual std::unique_ptr<StorageRowTX> RowTransaction(std::string const& row_key) {
            return std::unique_ptr<RocksDBStorageRowTX>(new RocksDBStorageRowTX(row_key, StartRocksTransaction(), this));
        }

        virtual Status Close() {
            auto status = GetStatus(db->WaitForCompact(rocksdb::WaitForCompactOptions()), "Close DB");
            if (!status.ok()) {
                return status;
            }
            delete db;
            metaHandle = nullptr;
            db = nullptr;
            return Status();
        }

        // Initialize storage
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
                DBG("Storage::Open() No existing DB, will create new one");
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
                    DBG("Failed to create new DB: " + open_status.message());
                    return open_status;
                }
                
                // Step 2: Add meta column family
                CFHandle meta_cf_handle;
                auto create_cf_status = db->CreateColumnFamily(
                    rocksdb::ColumnFamilyOptions(), storage_config.meta_column_family(), &meta_cf_handle);
                
                if (!create_cf_status.ok()) {
                    DBG("Failed to create meta CF: " + create_cf_status.ToString());
                    // Clean up
                    for (auto h : handles) delete h;
                    delete db;
                    db = nullptr;
                    return StorageError("Failed to create meta column family");
                }
                
                handles.push_back(meta_cf_handle);
            } else if (!open_status.ok()) {
                DBG("Open failed: " + open_status.message());
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

            std::cout << "STORAGE OK! (" << column_families_handles_map.size() << " column families)\n";
            return Status();
        }

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
                for (auto& pair : column_families_handles_map) {
                    delete pair.second;
                }
                column_families_handles_map.clear();
                delete db;
                db = nullptr;
                
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

        // Delete table
        virtual Status DeleteTable(std::string table_name, std::function<Status(std::string, storage::TableMeta)>&& precondition_fn) const {

            // schema_.deletion_protection();
            //rocksdb::Transaction* txn = db->BeginTransaction(woptions);
            auto txn = StartRocksTransaction();
            
            std::string out;
            auto status = GetStatus(
                txn->Get(roptions, metaHandle, rocksdb::Slice(table_name), &out),
                "Get table",
                NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                    "table_name", table_name)));
            if (!status.ok()) {
                // ?
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

        // Has table
        virtual bool HasTable(std::string table_name) const {
            std::string _;
            return db->KeyMayExist(roptions, metaHandle, rocksdb::Slice(table_name), &_) && db->Get(roptions, metaHandle, rocksdb::Slice(table_name), &_).ok();
        }

        // Get table
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

        // Iterate tables
        virtual Status ForEachTable(std::function<Status(std::string, storage::TableMeta)>&& fn) const {
            rocksdb::Iterator* iter = db->NewIterator(roptions, metaHandle);
            for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
                auto meta = DeserializeTableMeta(iter->value().ToString());
                if (!meta.ok()) {
                    return meta.status();
                }
                const auto fn_status = fn(iter->key().ToString(), meta.value());
                if(!fn_status.ok()) {
                    delete iter;
                    return fn_status;
                }
            }
            assert(iter->status().ok());
            delete iter;
            return Status();
        }

        // Iterate tables with prefix
        virtual Status ForEachTable(std::function<Status(std::string, storage::TableMeta)>&& fn, const std::string& prefix) const {
            rocksdb::Iterator* iter = db->NewIterator(roptions, metaHandle);
            std::cout << "SEEK SLICE=" << prefix << "\n";
            for (iter->Seek(rocksdb::Slice(prefix)); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
                auto meta = DeserializeTableMeta(iter->value().ToString());
                if (!meta.ok()) {
                    return meta.status();
                }
                const auto fn_status = fn(iter->key().ToString(), meta.value());
                if(!fn_status.ok()) {
                    delete iter;
                    return fn_status;
                }
            }
            assert(iter->status().ok());
            delete iter;
            return Status();
        }

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

        CellStream StreamTable(
            std::string const& table_name
        ) {
            auto all_rows_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
            return StreamTable(table_name, all_rows_set);
        }

        CellStream StreamTable(
            std::string const& table_name,
            std::shared_ptr<StringRangeSet> range_set
        ) {
            std::vector<std::unique_ptr<AbstractFamilyColumnStreamImpl>> iters;
            auto table = GetTable(table_name);
            auto const& m = table.value().table().column_families();
            for (auto const& it : m) {
                const auto x = column_families_handles_map[it.first];
                if (x == nullptr) {
                    continue;
                }
                std::unique_ptr<AbstractFamilyColumnStreamImpl> c = std::make_unique<RocksDBColumnFamilyStream>(it.first, x, range_set, this);
                iters.push_back(std::move(c));
            }
            return CellStream(std::make_unique<StorageFitleredTableStream>(std::move(iters)));
        }


    private:
        storage::StorageRocksDBConfig storage_config;

        rocksdb::Options options;
        rocksdb::TransactionDBOptions txn_options;
        rocksdb::WriteOptions woptions;
        rocksdb::ReadOptions roptions;
        rocksdb::TransactionDB* db;
        CFHandle metaHandle = nullptr;
        std::map<std::string, CFHandle> column_families_handles_map;

        inline rocksdb::Transaction* StartRocksTransaction() const {
            return db->BeginTransaction(woptions);
        }

        inline StatusOr<storage::RowData> DeserializeRow(std::string&& data) const {
            storage::RowData row;
            if (!row.ParseFromString(data)) {
                return StorageError("DeserializeRow()");
            }
            return row;
        }

        inline std::string SerializeRow(storage::RowData row) const {
            std::string out;
            if (!row.SerializeToString(&out)) {
                std::cout << "SERIALIZE FAILED!~~!!!!\n";
            }
            return out;
        }

        inline StatusOr<storage::TableMeta> DeserializeTableMeta(std::string&& data) const {
            storage::TableMeta meta;
            if (!meta.ParseFromString(data)) {
                return StorageError("DeserializeTableMeta()");
            }
            assert(meta.has_table());
            return meta;
        }

        inline std::string SerializeTableMeta(storage::TableMeta meta) const {
            std::string out;
            if (!meta.SerializeToString(&out)) {
                std::cout << "SERIALIZE FAILED!~~!!!!\n";
            }
            std::cout << "SAVED => " << meta.mutable_table()->name() << "\n";
            return out;
        }

        inline bool KeyExists(
            rocksdb::Transaction* txn,
            rocksdb::ColumnFamilyHandle* column_family,
            const rocksdb::Slice& key
        ) const {
            std::string _;
            return //txn->KeyMayExist(roptions, column_family, key, &_) &&
                txn->Get(roptions, column_family, key, &_).ok();
        }

        inline Status Rollback(
            rocksdb::Transaction* txn,
            Status status
        ) const {
            std::cout << "ROLLBACK\n";
            const auto txn_status = txn->Rollback();
            assert(txn_status.ok());
            delete txn;
            if (!status.ok()) {
                return status;
            }
            return Status();
        }

        inline Status Commit(
            rocksdb::Transaction* txn
        ) const {
            std::cout << "COMMIT!\n";
            const auto status = txn->Commit();
            assert(status.ok());
            delete txn;
            return Status();
        }

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

        inline Status StorageError(std::string&& operation) const {
            return InternalError("Storage error",
                        GCP_ERROR_INFO().WithMetadata(
                            "operation", operation));
        }

        inline Status GetStatus(rocksdb::Status status, std::string&& operation, Status&& not_found_status) const {
            if (status.IsNotFound()) {
                return not_found_status;
            }
            if (!status.ok()) {
                return StorageError(std::move(operation));
            }
            return Status();
        }

        inline Status GetStatus(rocksdb::Status status, std::string&& operation) const {
            if (!status.ok()) {
                std::cout << "ERR: " << status.ToString() << "\n";
                return StorageError(std::move(operation));
            }
            return Status();
        }

};

inline RocksDBStorageRowTX::~RocksDBStorageRowTX() {
    DBG("RocksDBStorageRowTX::~RocksDBStorageRowTX()");
    if (txn_ != nullptr) {
        DBG("RocksDBStorageRowTX::~RocksDBStorageRowTX() Need to do rollback");
        Rollback(Status());
    }
    DBG("RocksDBStorageRowTX::~RocksDBStorageRowTX() EXIT");
}

inline RocksDBStorageRowTX::RocksDBStorageRowTX(
    std::string const& row_key,
    rocksdb::Transaction* txn,
    RocksDBStorage* db
): row_key_(row_key), txn_(txn), roptions_(), db_(db), data_() {}

inline Status RocksDBStorageRowTX::DeleteRowFromColumnFamily(
    std::string const& column_family
) {
    auto cf = db_->column_families_handles_map[column_family];
    data_.erase(column_family);
    return db_->GetStatus(txn_->Delete(cf, rocksdb::Slice(row_key_)), "Delete row");
}

inline Status RocksDBStorageRowTX::Commit() {
    for (auto const& row_entry : data_) {
        auto cf = db_->column_families_handles_map[row_entry.first];
        auto out = db_->SerializeRow(row_entry.second);
        auto status = db_->GetStatus(txn_->Put(cf, rocksdb::Slice(row_key_), rocksdb::Slice(std::move(out))), "Update commit row");
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

inline Status RocksDBStorageRowTX::LoadRow(std::string const& column_family) {
    if(data_.find(column_family) == data_.end()) {
        auto cf = db_->column_families_handles_map[column_family];
        std::string out;
        // Get won't fail if there's no row
        auto get_status = db_->GetStatus(txn_->Get(roptions_, cf, rocksdb::Slice(row_key_), &out), "Load commit row", Status());
        if (!get_status.ok()) {
            return get_status;
        }
        // There is no such row
        if (out.size() == 0) {
            data_[column_family] = storage::RowData();
        } else {
            auto row_data = db_->DeserializeRow(std::move(out));
            if (!row_data.ok()) {
                return row_data.status();
            }
            data_[column_family] = row_data.value();
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
    DBG("SETCELL()");
    // fetch column family
    DBG("SETCELL: RowDataEnsure");
    auto status = LoadRow(column_family);
    if (!status.ok()) {
        return status;
    }
    RowDataEnsure(data_[column_family], column_qualifier, timestamp, value);
    DBG("SETCELL: Exit");
    return Status();
}

inline StatusOr<absl::optional<std::string>> RocksDBStorageRowTX::UpdateCell(
    std::string const& column_family,
    std::string const& column_qualifier,
    std::chrono::milliseconds timestamp,
    std::string& value,
    std::function<StatusOr<std::string>(std::string const&, std::string&&)> const& update_fn
) {
    DBG("UpdateCell()");
    auto status = LoadRow(column_family);
    if (!status.ok()) {
        return status;
    }

    auto& row_data = data_[column_family];
    auto columns_map = row_data.mutable_columns();
    auto column_it = columns_map->find(column_qualifier);
    
    absl::optional<std::string> old_value = absl::nullopt;
    
    if (column_it != columns_map->end()) {
        auto& cells = (*column_it->second.mutable_cells());
        auto cell_it = cells.find(timestamp.count());
        if (cell_it != cells.end()) {
            old_value = cell_it->second;
        }
    }

    auto maybe_new_value = update_fn(column_qualifier, std::move(value));
    if (!maybe_new_value.ok()) {
        return maybe_new_value.status();
    }

    RowDataEnsure(row_data, column_qualifier, timestamp, maybe_new_value.value());
    DBG("UpdateCell: Exit");
    return old_value;
}

inline Status RocksDBStorageRowTX::DeleteRowColumn(
    std::string const& column_family,
    std::string const& column_qualifier,
    ::google::bigtable::v2::TimestampRange const& time_range
) {
    DBG("DeleteRowColumn()");
    auto status = LoadRow(column_family);
    if (!status.ok()) {
        return status;
    }

    auto& row_data = data_[column_family];
    auto columns_map = row_data.mutable_columns();
    auto column_it = columns_map->find(column_qualifier);
    
    if (column_it == columns_map->end()) {
        // Column doesn't exist, nothing to delete
        return Status();
    }

    auto& cells = (*column_it->second.mutable_cells());
    
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
            cell_it = cells.erase(cell_it);
        } else {
            ++cell_it;
        }
    }
    
    // If column is now empty, remove it
    if (cells.empty()) {
        columns_map->erase(column_it);
    }
    
    DBG("DeleteRowColumn: Exit");
    return Status();
}

inline Status RocksDBStorageRowTX::DeleteRowFromAllColumnFamilies() {
    DBG("DeleteRowFromAllColumnFamilies()");
    
    // Get all column families from the table
    // We need to iterate through all column families and delete this row from each
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
    
    DBG("DeleteRowFromAllColumnFamilies: Exit");
    return Status();
}

inline RocksDBColumnFamilyStream::RocksDBColumnFamilyStream(
    std::string const& column_family_name,
    rocksdb::ColumnFamilyHandle* handle,
    std::shared_ptr<StringRangeSet const> row_set,
    RocksDBStorage* db
): 
    column_family_name_(column_family_name),
    handle_(handle),
    row_ranges_(std::move(row_set)),
    column_ranges_(StringRangeSet::All()),
    timestamp_ranges_(TimestampRangeSet::All()),
    db_(db), initialized_(false), row_iter_(nullptr) {}


inline bool RocksDBColumnFamilyStream::HasValue() const {
  InitializeIfNeeded();
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
    if (!initialized_) {
        // Seek to the start of the first row range
        auto const& ranges = row_ranges_->disjoint_ranges();
        if (!ranges.empty()) {
            auto const& first_range = *ranges.begin();
            if (absl::holds_alternative<std::string>(first_range.start())) {
                row_iter_->Seek(absl::get<std::string>(first_range.start()));
            } else {
                row_iter_->SeekToFirst();
            }
        } else {
            row_iter_->SeekToFirst();
        }
    } else {
        row_iter_->Next();
    }
    
    // Skip rows until we find one in our range
    while (row_iter_->Valid()) {
        std::string key = row_iter_->key().ToString();
        
        // Check if key is in any of our ranges
        bool in_range = false;
        for (auto const& range : row_ranges_->disjoint_ranges()) {
            if (range.IsWithin(key)) {
                in_range = true;
                break;
            }
        }
        
        if (in_range) {
            // Found a valid row in range
            auto maybe_row = db_->DeserializeRow(row_iter_->value().ToString());
            if (!maybe_row.ok()) {
                row_data_ = absl::nullopt;
                return;
            }
            auto data = TColumnFamilyRow();
            for (auto const& i : maybe_row.value().columns()) {
                for (auto const& j : i.second.cells()) {
                    data[i.first][std::chrono::milliseconds(j.first)] = j.second;
                }
            }
            row_data_ = std::move(data);
            row_key_ = key;
            return;
        }
        
        // Key not in range, check if we've passed all ranges
        bool past_all_ranges = true;
        for (auto const& range : row_ranges_->disjoint_ranges()) {
            if (!range.IsAboveEnd(key)) {
                past_all_ranges = false;
                break;
            }
        }
        
        if (past_all_ranges) {
            // We've passed all ranges, stop searching
            break;
        }
        
        row_iter_->Next();
    }
    
    // No valid row found
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
  NextRow();
  PointToFirstCellAfterRowChange();
  return true;
}

inline void RocksDBColumnFamilyStream::InitializeIfNeeded() const {
  if (!initialized_) {
    rocksdb::ColumnFamilyHandle* cf = nullptr;
    for (auto const& i : db_->column_families_handles_map) {
        if(i.first == column_family_name_) {
            cf = i.second;
        }
    }
    row_iter_ = db_->db->NewIterator(db_->roptions, cf);
    row_data_ = absl::nullopt;
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