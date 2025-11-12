#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H

#include <vector>
#include <chrono>
#include <thread>
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

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

static inline google::bigtable::admin::v2::Table FakeSchema(std::string const& table_name) {
  google::bigtable::admin::v2::Table schema;
  schema.set_name(table_name);
  return schema;
}

template<typename TCFPointer>
class Storage {
    public:
        // Type of column family pointer
        using CFHandle = TCFPointer;
        // Column family type along with the pointer
        using CFMeta = std::pair<google::bigtable::admin::v2::Type, CFHandle>;

        Storage() {};
        virtual Status Close() = 0;

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
        virtual StatusOr<CFMeta> GetColumnFamily(std::string table_name, std::string column_family) = 0;
};

class RocksDBStorage : Storage<rocksdb::ColumnFamilyHandle*> {
    public:
        using CFHandle = typename Storage<rocksdb::ColumnFamilyHandle*>::CFHandle;
        using CFMeta = typename Storage<rocksdb::ColumnFamilyHandle*>::CFMeta;

        RocksDBStorage() {
            storage_config = storage::StorageRocksDBConfig();
            storage_config.set_db_path("/tmp/rocksdb-for-bigtable-test4");
            storage_config.set_meta_column_family("bte_metadata");
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

            std::cout << "STORAGE INITIALIZED!\n";

            std::vector<std::string> column_families_names;
            std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
            std::vector<CFHandle> handles;
            auto status = GetStatus(rocksdb::TransactionDB::ListColumnFamilies(options, storage_config.db_path(), &column_families_names), "List column families");
            if (!status.ok()) {
                // We can ignore error and warn here
                //return status;
            }

            column_families.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
            for (auto const& family_name : column_families_names) {
                column_families.push_back(rocksdb::ColumnFamilyDescriptor(family_name, rocksdb::ColumnFamilyOptions()));
            }

            status = OpenDBWithRetry(options, column_families, &handles);
            if (!status.ok()) {
                return status;
            }

            // Create missing collumn family
            if (std::find(column_families_names.begin(), column_families_names.end(), storage_config.meta_column_family()) == column_families_names.end()) {
                CFHandle cf;
                std::cout.flush();
                status = GetStatus(db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), storage_config.meta_column_family(), &cf), "Create meta column family");
                if (!status.ok()) {
                    return status;
                }
                handles.push_back(cf);
            }

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
            rocksdb::Transaction* txn = db->BeginTransaction(woptions);
            google::bigtable::admin::v2::Table* xschema = new google::bigtable::admin::v2::Table();
            *xschema = schema;

            auto key = rocksdb::Slice(schema.name());
            if(KeyExists(txn, metaHandle, key)) {
                // Table exists
                return AlreadyExistsError("Table already exists.", GCP_ERROR_INFO().WithMetadata(
                    "table_name", schema.name()));
            }

            storage::TableMeta meta;
            //meta.set_allocated_table(xschema);
            meta.set_allocated_table(&schema);
            auto status = GetStatus(db->Put(woptions, metaHandle, key, rocksdb::Slice(SerializeTableMeta(meta))), "Put table key to metadata cf");
            meta.release_table();
            // Make sure column families exist
            for(auto& cf : *meta.mutable_table()->mutable_column_families()) {
                auto iter = column_families_handles_map.find(cf.first);
                if (iter == column_families_handles_map.end()) {
                    // Column family from table does not exist
                    CFHandle handle;
                    status = GetStatus(db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), cf.first, &handle), "Create table column family");
                    if (!status.ok()) {
                        return status;
                    }
                    column_families_handles_map[cf.first] = handle;
                }
            }

            return Commit(txn);
        }

        // Delete table
        virtual Status DeleteTable(std::string table_name, std::function<Status(std::string, storage::TableMeta)>&& precondition_fn) const {

            // schema_.deletion_protection();
            rocksdb::Transaction* txn = db->BeginTransaction(woptions);
            
            std::string out;
            auto status = GetStatus(
                txn->Get(roptions, metaHandle, rocksdb::Slice(table_name), &out),
                "Get table",
                NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                    "table_name", table_name)));
            if (!status.ok()) {
                return Rollback(status, txn);
            }
            const auto meta = DeserializeTableMeta(std::move(out));
            if (!meta.ok()) {
                return Rollback(meta.status(), txn);
            }
            const auto precondition_status = precondition_fn(table_name, meta.value());
            if (!precondition_status.ok()) {
                return Rollback(precondition_status, txn);
            }

            const auto delete_status = GetStatus(
                txn->Delete(metaHandle, rocksdb::Slice(table_name)),
                "Delete table",
                NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                    "table_name", table_name)));

            if (!delete_status.ok()) {
                return Rollback(delete_status, txn);
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

        // Get column family type and handle
        virtual StatusOr<CFMeta> GetColumnFamily(std::string table_name, std::string column_family) {
            auto meta = GetTable(table_name);
            if (!meta.ok()) {
                return meta.status();
            }
            auto cfs = meta.value().mutable_table()->mutable_column_families();
            auto cf_iter = cfs->find(column_family);
            if (cf_iter == cfs->end()) {
                return NotFoundError("No such column family.", GCP_ERROR_INFO().WithMetadata(
                    "table_name", table_name).WithMetadata("column_family", column_family));
            }
            return std::make_pair(cf_iter->second.value_type(), column_families_handles_map[column_family]);
        }

        Status DeleteRow(CFHandle column_family, std::string const& row_key, std::string const& column_qualifier) {
            return GetStatus(db->Delete(woptions, column_family, rocksdb::Slice(RowKey(row_key, column_qualifier))), "Delete row");
        }

        Status PutRow(CFHandle column_family, std::string const& row_key, std::string const& column_qualifier, const storage::Row& row) {
            return GetStatus(db->Put(woptions, column_family, rocksdb::Slice(RowKey(row_key, column_qualifier)), rocksdb::Slice(SerializeRow(row))), "Put row");
        }

        StatusOr<storage::Row> GetRow(CFHandle column_family, std::string const& row_key, std::string const& column_qualifier) {
            std::string out;
            auto status = GetStatus(
                db->Get(roptions, column_family, rocksdb::Slice(RowKey(row_key, column_qualifier)), &out),
                "Get row", 
                NotFoundError("No such row.", GCP_ERROR_INFO().WithMetadata(
                    "column_family", column_family->GetName()).WithMetadata("column_qualifier", column_qualifier).WithMetadata("row_key", row_key)));
            if (!status.ok()) {
                return status;
            }
            return DeserializeRow(std::move(out));
        }



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

        inline StatusOr<storage::Row> DeserializeRow(std::string&& data) const {
            storage::Row row;
            if (!row.ParseFromString(data)) {
                return StorageError("DeserializeRow()");
            }
            return row;
        }

        inline std::string SerializeRow(storage::Row row) const {
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

        inline Status Rollback(
            Status status,
            rocksdb::Transaction* txn
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

        inline bool KeyExists(rocksdb::Transaction* txn, rocksdb::ColumnFamilyHandle* column_family, const rocksdb::Slice& key) const {
            std::string _;
            return //txn->KeyMayExist(roptions, column_family, key, &_) &&
                txn->Get(roptions, column_family, key, &_).ok();
        }
};


}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H