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

#define DBG(TEXT) if(true){ std::cout << (TEXT) << "\n"; std::cout.flush(); }


namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

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

static inline google::bigtable::admin::v2::Table FakeSchema(std::string const& table_name) {
  google::bigtable::admin::v2::Table schema;
  schema.set_name(table_name);
  return schema;
}

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

        // virtual StatusOr<std::vector<std::tuple<std::string, std::string, std::chrono::milliseconds, std::string>>> ReadModifyWriteRow(
        //     std::string const& column_family,
        //     std::string const& column_qualifier,
        //     std::string const& append_value,
        // ) {
        //     return std::vector<std::tuple<std::string, std::string, std::chrono::milliseconds, std::string>>();
        // }
};

class Storage {
    public:
        // Type of column family pointer
        //using CFHandle = TCFPointer;
        // Column family type along with the pointer
        //using CFMeta = std::pair<google::bigtable::admin::v2::Type, CFHandle>;

        Storage() {};
        virtual Status Close() = 0;

        // Start transaction
        virtual std::unique_ptr<StorageRowTX> RowTransaction(std::string const& row_key) = 0;

        // Initialize storage
        virtual Status Open() = 0;

        // Create table
        virtual Status CreateTable(google::bigtable::admin::v2::Table& schema) = 0;

        // Delete table
        virtual Status DeleteTable(std::string table_name, std::function<Status(std::string, storage::TableMeta)>&& precondition_fn) const = 0;

        // Get table
        virtual StatusOr<storage::TableMeta> GetTable(std::string table_name) const = 0;

        // Has table
        virtual bool HasTable(std::string table_name) const = 0;

        // Iterate tables
        virtual Status ForEachTable(std::function<Status(std::string, storage::TableMeta)>&& fn) const = 0;

        // Iterate tables with prefix
        virtual Status ForEachTable(std::function<Status(std::string, storage::TableMeta)>&& fn, const std::string& prefix) const = 0;

        // Get column family type and handle
        //virtual StatusOr<CFMeta> GetColumnFamily(std::string table_name, std::string column_family) = 0;
};

// class RocksDBCellIterator {
//     typedef typename std::iterator_traits<InIt>::value_type      value_type;
//     typedef typename std::iterator_traits<InIt>::difference_type difference_type;
//     typedef typename std::iterator_traits<InIt>::reference       reference;
//     typedef typename std::iterator_traits<InIt>::pointer         pointer;
//     my_iterator(InIt it, InIt end, Pred pred): it_(it), end_(end), pred_(pred) {}
//     bool operator== (my_iterator const& other) const { reutrn this->it_ == other.it_; }
//     bool operator!= (my_iterator const& other) const { return !(*this == other); }
//     reference operator*() { return *this->it_; }
//     pointer   operator->() { return this->it_; }
//     my_iterator& operator++() {
//         this->it_ = std::find_if(this->it_, this->end_, this->pred_);
//         return *this;
//     }
//     my_iterator operator++(int)
//     { my_iterator rc(*this); this->operator++(); return rc; }
// private:
//     InIt it_, end_;
//     Pred pred_;
// }

class RocksDBStorage;
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
            //delete txn;
            //return GetStatus(status, "Transaction commit");
        }
        //RocksDBStorageRowTX(rocksdb::Transaction* txn): txn_(txn) {}

        virtual Status SetCell(
            std::string const& column_family,
            std::string const& column_qualifier,
            std::chrono::milliseconds timestamp,
            std::string const& value
        ) override;

        inline virtual Status DeleteRowFromColumnFamily(
            std::string const& column_family
        ) override;

        Status LoadRow(std::string const& column_family);

        // Status DeleteRow(CFHandle column_family, std::string const& row_key, std::string const& column_qualifier) {
        //     return GetStatus(db->Delete(woptions, column_family, rocksdb::Slice(RowKey(row_key, column_qualifier))), "Delete row");
        // }

        // Status PutRow(CFHandle column_family, std::string const& row_key, std::string const& column_qualifier, const storage::Row& row) {
        //     return GetStatus(db->Put(woptions, column_family, rocksdb::Slice(RowKey(row_key, column_qualifier)), rocksdb::Slice(SerializeRow(row))), "Put row");
        // }

        // StatusOr<storage::Row> GetRow(CFHandle column_family, std::string const& row_key, std::string const& column_qualifier) {
        //     std::string out;
        //     auto status = GetStatus(
        //         db->Get(roptions, column_family, rocksdb::Slice(RowKey(row_key, column_qualifier)), &out),
        //         "Get row", 
        //         NotFoundError("No such row.", GCP_ERROR_INFO().WithMetadata(
        //             "column_family", column_family->GetName()).WithMetadata("column_qualifier", column_qualifier).WithMetadata("row_key", row_key)));
        //     if (!status.ok()) {
        //         return status;
        //     }
        //     return DeserializeRow(std::move(out));
        // }

        // virtual ~RocksDBStorageRowTX() {
        //     if (txn_ != nullptr) {
        //         delete txn_;
        //         txn_ = nullptr;
        //     }
        // }
    private:
        rocksdb::Transaction* txn_;
        rocksdb::ReadOptions roptions_;
        std::string row_key_;
        RocksDBStorage* db_;
        std::map<std::string, storage::RowData> data_;
        explicit RocksDBStorageRowTX(
            std::string const& row_key,
            rocksdb::Transaction* txn,
            RocksDBStorage* db
        );

};


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

  //google::protobuf::Map<std::string, storage::RowColumnData>;
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

class RocksDBStorage : public Storage {
    friend class RocksDBStorageRowTX;
    friend class RocksDBColumnFamilyStream;
    public:
        using CFHandle = rocksdb::ColumnFamilyHandle*;
        //using CFMeta = typename Storage<rocksdb::ColumnFamilyHandle*>::CFMeta;

        RocksDBStorage() {
            storage_config = storage::StorageRocksDBConfig();
            storage_config.set_db_path("/tmp/rocksdb-for-bigtable-test4");
            storage_config.set_meta_column_family("bte_metadata");
        }

        void ExampleFun() {
            DBG("ExampleFun()");
            auto r = rocksdb::ReadOptions();
            auto cf = column_families_handles_map["test_column_family"];
            DBG(cf->GetName());
            rocksdb::Iterator* iter = db->NewIterator(r, cf);
            DBG("ExampleFun() exit");
        }

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
        virtual Status Open() {
            options = rocksdb::Options();
            txn_options = rocksdb::TransactionDBOptions();
            woptions = rocksdb::WriteOptions();
            roptions = rocksdb::ReadOptions();

            options.create_if_missing = true;

            DBG("Storage::Open() List column families");

            std::vector<std::string> column_families_names;
            std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
            std::vector<CFHandle> handles;
            auto status = GetStatus(rocksdb::TransactionDB::ListColumnFamilies(options, storage_config.db_path(), &column_families_names), "List column families");
            if (!status.ok()) {
                // We can ignore error and warn here
                //return status;
                DBG("Storage::Open() Listing error ignored");
            }
            DBG("Storage::Open() After listing column families");

            column_families.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
            for (auto const& family_name : column_families_names) {
                column_families.push_back(rocksdb::ColumnFamilyDescriptor(family_name, rocksdb::ColumnFamilyOptions()));
            }

            DBG("Storage::Open() Open DB with retry");
            status = OpenDBWithRetry(options, column_families, &handles);
            if (!status.ok()) {
                DBG(status.message());
                return status;
            }
            DBG("Storage::Open() Open was successfull!");

            // Create missing collumn family
            if (std::find(column_families_names.begin(), column_families_names.end(), storage_config.meta_column_family()) == column_families_names.end()) {
                CFHandle cf;
                DBG("Storage::Open() Create missing meta cf");
                status = GetStatus(db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), storage_config.meta_column_family(), &cf), "Create meta column family");
                if (!status.ok()) {
                    return status;
                }
                handles.push_back(cf);
            }
            DBG("Storage::Open() Meta cf is there!");

            metaHandle = nullptr;
            for (auto const& handle : handles) {
                column_families_handles_map[handle->GetName()] = handle;
            }
            auto meta_handle_iter = column_families_handles_map.find(storage_config.meta_column_family());
            if (meta_handle_iter != column_families_handles_map.end()) {
                metaHandle = meta_handle_iter->second;
            } else {
                return StorageError("Cannot find meta column family");
            }

            std::cout << "STORAGE OK!\n";
            return Status();
        }

        // Create table
        virtual Status CreateTable(google::bigtable::admin::v2::Table& schema) {
            //rocksdb::Transaction* txn = db->BeginTransaction(woptions);
            auto txn = StartRocksTransaction();
            google::bigtable::admin::v2::Table* xschema = new google::bigtable::admin::v2::Table();
            *xschema = schema;

            auto key = rocksdb::Slice(schema.name());
            if(KeyExists(txn, metaHandle, key)) {
                // Table exists
                DBG("Storage::CreateTable() TABLE EXISTS");
                return AlreadyExistsError("Table already exists.", GCP_ERROR_INFO().WithMetadata(
                    "table_name", schema.name()));
            }

            storage::TableMeta meta;
            //meta.set_allocated_table(xschema);
            meta.set_allocated_table(&schema);
            auto status = GetStatus(txn->Put(metaHandle, key, rocksdb::Slice(SerializeTableMeta(meta))), "Put table key to metadata cf");
            
            // Commit now
            status = Commit(txn);
            if (!status.ok()) {
                return status;
            }

            // Make sure column families exist
            DBG("Storage::CreateTable() Create all column families");
            for(auto& cf : meta.table().column_families()) {
                auto iter = column_families_handles_map.find(cf.first);
                if (iter == column_families_handles_map.end()) {
                    // Column family from table does not exist
                    CFHandle handle;
                    DBG("Create table cf = ");
                    DBG(cf.first);
                    status = GetStatus(db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), cf.first, &handle), "Create table column family");
                    if (!status.ok()) {
                        return status;
                    }
                    // Dummy row
                    status = GetStatus(db->Put(woptions, handle, rocksdb::Slice("_"), rocksdb::Slice(SerializeRow(storage::RowData()))), "Put dummy row");
                    if (!status.ok()) {
                        return status;
                    }
                    column_families_handles_map[cf.first] = handle;
                } else {
                    DBG("Storage::CreateTable() Table cf already exists");
                    DBG(cf.first);
                }
            }
            meta.release_table();
            return Status();
            //return Commit(txn);
            //return Commit(txn);
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

        CellStream StreamTable(
            std::string const& table_name
        ) {
            auto all_rows_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
            return StreamTable(table_name, all_rows_set);
        }

        // //std::map<std::chrono::milliseconds, std::string,
        //                           std::greater<>>::const_iterator
        CellStream StreamTable(
            std::string const& table_name,
            std::shared_ptr<StringRangeSet> range_set
        ) {
            std::vector<std::unique_ptr<AbstractFamilyColumnStreamImpl>> iters;
            auto table = GetTable(table_name);
            auto const& m = table.value().table().column_families();
            for (auto const& it : m) {
                const auto x = column_families_handles_map[it.first];
                std::unique_ptr<AbstractFamilyColumnStreamImpl> c = std::make_unique<RocksDBColumnFamilyStream>(it.first, x, range_set, this);
                iters.push_back(std::move(c));
            }
            //auto x = StorageFitleredTableStream(std::move(iters));
            //auto x = new StorageFitleredTableStream(std::move(iters));
            //return CellStream(std::unique_ptr<AbstractCellStreamImpl>(x));
            return CellStream(std::make_unique<StorageFitleredTableStream>(std::move(iters)));
        }

        // Get column family type and handle
        // StatusOr<CFMeta> __GetColumnFamily(std::string table_name, std::string column_family) {
        //     auto meta = GetTable(table_name);
        //     if (!meta.ok()) {
        //         return meta.status();
        //     }
        //     auto cfs = meta.value().mutable_table()->mutable_column_families();
        //     auto cf_iter = cfs->find(column_family);
        //     if (cf_iter == cfs->end()) {
        //         return NotFoundError("No such column family.", GCP_ERROR_INFO().WithMetadata(
        //             "table_name", table_name).WithMetadata("column_family", column_family));
        //     }
        //     return std::make_pair(cf_iter->second.value_type(), column_families_handles_map[column_family]);
        // }

        // Status DeleteRow(CFHandle column_family, std::string const& row_key, std::string const& column_qualifier) {
        //     return GetStatus(db->Delete(woptions, column_family, rocksdb::Slice(RowKey(row_key, column_qualifier))), "Delete row");
        // }

        // Status PutRow(CFHandle column_family, std::string const& row_key, std::string const& column_qualifier, const storage::Row& row) {
        //     return GetStatus(db->Put(woptions, column_family, rocksdb::Slice(RowKey(row_key, column_qualifier)), rocksdb::Slice(SerializeRow(row))), "Put row");
        // }

        // StatusOr<storage::Row> GetRow(CFHandle column_family, std::string const& row_key, std::string const& column_qualifier) {
        //     std::string out;
        //     auto status = GetStatus(
        //         db->Get(roptions, column_family, rocksdb::Slice(RowKey(row_key, column_qualifier)), &out),
        //         "Get row", 
        //         NotFoundError("No such row.", GCP_ERROR_INFO().WithMetadata(
        //             "column_family", column_family->GetName()).WithMetadata("column_qualifier", column_qualifier).WithMetadata("row_key", row_key)));
        //     if (!status.ok()) {
        //         return status;
        //     }
        //     return DeserializeRow(std::move(out));
        // }



    //     std::string const& row_key, std::string const& column_qualifier,
    // std::chrono::milliseconds timestamp, std::string const& value) {

    private:
        storage::StorageRocksDBConfig storage_config;

        rocksdb::Options options;
        rocksdb::TransactionDBOptions txn_options;
        rocksdb::WriteOptions woptions;
        rocksdb::ReadOptions roptions;
        rocksdb::TransactionDB* db;
        CFHandle metaHandle = nullptr;
        std::map<std::string, CFHandle> column_families_handles_map;

        static inline std::string RowKey(std::string const& row_key, std::string const& column_qualifier) {
            return absl::StrCat(row_key, "/", column_qualifier);
        }

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
            //delete txn;
            //return GetStatus(status, "Transaction commit");
        }

        inline Status Commit(
            rocksdb::Transaction* txn
        ) const {
            std::cout << "COMMIT!\n";
            const auto status = txn->Commit();
            assert(status.ok());
            delete txn;
            return Status();
            //delete txn;
            //return GetStatus(status, "Transaction commit");
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

        // inline bool KeyExists(rocksdb::Transaction* txn, rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Slice& key) const {
        //     std::string _;
        //     return //txn->KeyMayExist(roptions, column_family, key, &_) &&
        //         txn->Get(roptions, column_family, key, &_).ok();
        // }
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
): row_key_(row_key), txn_(txn), roptions_(), db_(db), data_() {
    // Get row here
    //data_ = nullptr;
}

inline Status RocksDBStorageRowTX::DeleteRowFromColumnFamily(
    std::string const& column_family
) {
    auto cf = db_->column_families_handles_map[column_family];
    data_.erase(column_family);
    return db_->GetStatus(txn_->Delete(cf, rocksdb::Slice(row_key_)), "Delete row");
}

inline Status RocksDBStorageRowTX::Commit() {
    // Commit all column families rows
    DBG("RocksDBStorageRowTX::Commit()");
    for (auto const& row_entry : data_) {
        auto cf = db_->column_families_handles_map[row_entry.first];
        DBG("RocksDBStorageRowTX::Commit() Serialize row");
        auto out = db_->SerializeRow(row_entry.second);
        DBG("RocksDBStorageRowTX::Commit() Put");
        auto status = db_->GetStatus(txn_->Put(cf, rocksdb::Slice(row_key_), rocksdb::Slice(std::move(out))), "Update commit row");
        if (!status.ok()) {
            DBG("RocksDBStorageRowTX::Commit() Put caused rollback");
            return Rollback(status);
        }
    }
    DBG("RocksDBStorageRowTX::Commit() Commit");
    const auto status = txn_->Commit();
    assert(status.ok());
    DBG("RocksDBStorageRowTX::Commit() Delete txn");
    delete txn_;
    txn_ = nullptr;
    DBG("RocksDBStorageRowTX::Commit() Exit");
    return Status();
}

inline Status RocksDBStorageRowTX::LoadRow(std::string const& column_family) {
    if(data_.find(column_family) == data_.end()) {
        auto cf = db_->column_families_handles_map[column_family];
        DBG("SETCELL: Found column family");
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
    //auto cells = data_[column_family].mutable_columns()->find(column_qualifier)->second.mutable_cells();
    //data_[column_family].mutable_columns()->find(column_qualifier)->second.mutable_cells()->find(timestamp)->
    //(*cells)[timestamp.count()] = value;
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
    db_(db), initialized_(false), row_iter_(nullptr) {
        
    }



// bool RocksDBColumnFamilyStream::ApplyFilter(
//     InternalFilter const& internal_filter) {
//   assert(!initialized_);
//   return absl::visit(FilterApply(*this), internal_filter);
// }

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
    DBG("RocksDBColumnFamilyStream::NextRow()");
    if (!initialized_) {
        // Setup is required
        DBG("RocksDBColumnFamilyStream::NextRow() SeekToFirst()");
        row_iter_->SeekToFirst();
    }
     if (!row_iter_->Valid()) {
        DBG("RocksDBColumnFamilyStream::NextRow() NOT VALID ITERATOR");
        row_data_ = absl::nullopt;
     } else {
        DBG("RocksDBColumnFamilyStream::NextRow() Deserialize");
        DBG(row_iter_->key().ToString());
        auto maybe_row = db_->DeserializeRow(row_iter_->value().ToString());
        if (!maybe_row.ok()) {
            return;
        }
        DBG("RocksDBColumnFamilyStream::NextRow() Remap");
        auto data = TColumnFamilyRow();
        for (auto const& i : maybe_row.value().columns()) {
            for (auto const& j : i.second.cells()) {
                data[i.first][std::chrono::milliseconds(j.first)] = j.second;
            }
        }
        DBG("RocksDBColumnFamilyStream::NextRow() Set row data");
        row_data_ = std::move(data);

        //row_data_ = std::move(maybe_row.value());
        DBG("RocksDBColumnFamilyStream::NextRow() Row iter next!");
        row_key_ = row_iter_->key().ToString();
        row_iter_->Next();
    }
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
    DBG("InitializeIfNeeded Create NewIterator()");
    DBG(column_family_name_);
    rocksdb::ColumnFamilyHandle* cf = nullptr;
    for (auto const& i : db_->column_families_handles_map) {
        if(i.first == column_family_name_) {
            cf = i.second;
        }
    }
    // db_->column_families_handles_map[column_family_name_]
    row_iter_ = db_->db->NewIterator(db_->roptions, cf);
    DBG("NewIterator() created");
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

// inline bool RocksDBColumnFamilyStream::HasValue() const {
//     return true;
// }
// inline CellView const& RocksDBColumnFamilyStream::Value() const {
//     return CellView("", "", "", std::chrono::milliseconds::zero(), "");
// }
// inline bool RocksDBColumnFamilyStream::Next(NextMode mode) {
//     if (!row_data_.has_value()) {
//         // Setup is required
//         row_iter_->SeekToFirst();
//         if (!row_iter_->Valid()) {
//             // Finished iteration
//             return false;
//         }
//         auto maybe_row = db_->DeserializeRow(row_iter_->value().ToString());
//         if (!maybe_row.ok()) {
//             return false;
//         }
//         row_data_ = std::move(maybe_row.value());
//     }
//     //google::protobuf::Map<std::string, storage::RowColumnData>
//     // auto xxx_columns = RegexFiteredMapView<StringRangeFilteredMapView<google::protobuf::Map<std::string, storage::RowColumnData>>>(
//     //     StringRangeFilteredMapView<google::protobuf::Map<std::string, storage::RowColumnData>>(row_data_.value().columns(),
//     //                                                 column_ranges_),
//     //     column_regexes_);

//     return true;

//     // for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
//     //     auto meta = DeserializeTableMeta(iter->value().ToString());
//     //     if (!meta.ok()) {
//     //         return meta.status();
//     //     }
//     //     const auto fn_status = fn(iter->key().ToString(), meta.value());
//     //     if(!fn_status.ok()) {
//     //         delete iter;
//     //         return fn_status;
//     //     }
//     // }
//     // return true;
// }


}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H