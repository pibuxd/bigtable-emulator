
#include <vector>
#include <chrono>
#include <thread>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

#include "persist/proto/storage.pb.h"

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include <google/bigtable/admin/v2/table.pb.h>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

google::bigtable::admin::v2::Table FakeSchema(std::string const& table_name) {
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
        virtual Status CreateNewTableEntry(std::string table_name) = 0;

        // Get table
        virtual StatusOr<storage::TableMeta> GetTable(std::string table_name) = 0;

        // Iterate tables
        virtual Status ForEachTable(std::function<void(std::string, storage::TableMeta)>&& fn) = 0;

        // Iterate tables with prefix
        virtual Status ForEachTable(std::function<void(std::string, storage::TableMeta)>&& fn, std::string&& prefix) = 0;

        // Get column family type and handle
        virtual StatusOr<CFMeta> GetColumnFamily(std::string table_name, std::string column_family) = 0;
};

class RocksDBStorage : Storage<rocksdb::ColumnFamilyHandle*> {
    public:
        using CFHandle = typename Storage<rocksdb::ColumnFamilyHandle*>::CFHandle;
        using CFMeta = typename Storage<rocksdb::ColumnFamilyHandle*>::CFMeta;

        RocksDBStorage() {
            storage_config = storage::StorageRocksDBConfig();
            storage_config.set_db_path("/tmp/rocksdb-for-bigtable-test2");
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
            woptions = rocksdb::WriteOptions();
            roptions = rocksdb::ReadOptions();

            options.create_if_missing = true;

            std::cout << "STORAGE INITIALIZED!\n";

            std::vector<std::string> column_families_names;
            std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
            std::vector<CFHandle> handles;
            auto status = GetStatus(rocksdb::DB::ListColumnFamilies(options, storage_config.db_path(), &column_families_names), "List column families");
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
        virtual Status CreateNewTableEntry(std::string table_name) {
            google::bigtable::admin::v2::Table* schema = new google::bigtable::admin::v2::Table();
            *schema = FakeSchema(table_name);
            storage::TableMeta meta;
            meta.set_allocated_table(schema);
            auto status = GetStatus(db->Put(woptions, metaHandle, rocksdb::Slice(table_name), rocksdb::Slice(SerializeTableMeta(meta))), "Put table key to metadata cf");
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
            return Status();
        }

        // Get table
        virtual StatusOr<storage::TableMeta> GetTable(std::string table_name) {
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
        virtual Status ForEachTable(std::function<void(std::string, storage::TableMeta)>&& fn) {
            rocksdb::Iterator* iter = db->NewIterator(roptions, metaHandle);
            for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
                auto meta = DeserializeTableMeta(iter->value().ToString());
                if (!meta.ok()) {
                    return meta.status();
                }
                fn(iter->key().ToString(), meta.value());
            }
            assert(iter->status().ok());
            delete iter;
            return Status();
        }

        // Iterate tables with prefix
        virtual Status ForEachTable(std::function<void(std::string, storage::TableMeta)>&& fn, std::string&& prefix) {
            rocksdb::Iterator* iter = db->NewIterator(roptions, metaHandle);
            std::cout << "SEEK SLICE=" << prefix << "\n";
            for (iter->Seek(rocksdb::Slice(prefix)); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
                auto meta = DeserializeTableMeta(iter->value().ToString());
                if (!meta.ok()) {
                    return meta.status();
                }
                fn(iter->key().ToString(), meta.value());
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
        rocksdb::WriteOptions woptions;
        rocksdb::ReadOptions roptions;
        rocksdb::DB* db;
        CFHandle metaHandle = nullptr;
        std::map<std::string, CFHandle> column_families_handles_map;

        static inline std::string RowKey(std::string const& row_key, std::string const& column_qualifier) {
            return row_key + "/" + column_qualifier;
        }

        inline StatusOr<storage::Row> DeserializeRow(std::string&& data) {
            storage::Row row;
            if (!row.ParseFromString(data)) {
                return StorageError("DeserializeRow()");
            }
            return row;
        }

        inline std::string SerializeRow(storage::Row row) {
            std::string out;
            if (!row.SerializeToString(&out)) {
                std::cout << "SERIALIZE FAILED!~~!!!!\n";
            }
            return out;
        }

        inline StatusOr<storage::TableMeta> DeserializeTableMeta(std::string&& data) {
            storage::TableMeta meta;
            if (!meta.ParseFromString(data)) {
                return StorageError("DeserializeTableMeta()");
            }
            assert(meta.has_table());
            return meta;
        }

        inline std::string SerializeTableMeta(storage::TableMeta meta) {
            std::string out;
            if (!meta.SerializeToString(&out)) {
                std::cout << "SERIALIZE FAILED!~~!!!!\n";
            }
            return out;
        }

        inline Status OpenDBWithRetry(
            rocksdb::Options options,
            std::vector<rocksdb::ColumnFamilyDescriptor> column_families,
            std::vector<rocksdb::ColumnFamilyHandle*>* handles
        ) {
            auto status = rocksdb::Status();
            for(auto i = 0; i<5; ++i) {
                status = rocksdb::DB::Open(options, storage_config.db_path(), column_families, handles, &db);
                if (status.IsIOError()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(250));
                    continue;
                } else {
                    break;
                }
            }
            return GetStatus(status, "Open database");
        }

        inline Status StorageError(std::string&& operation) {
            return InternalError("Storage error",
                        GCP_ERROR_INFO().WithMetadata(
                            "operation", operation));
        }

        inline Status GetStatus(rocksdb::Status status, std::string&& operation, Status&& not_found_status) {
            if (status.IsNotFound()) {
                return not_found_status;
            }
            if (!status.ok()) {
                return StorageError(std::move(operation));
            }
            return Status();
        }

        inline Status GetStatus(rocksdb::Status status, std::string&& operation) {
            if (!status.ok()) {
                std::cout << "ERR: " << status.ToString() << "\n";
                return StorageError(std::move(operation));
            }
            return Status();
        }
};


}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google