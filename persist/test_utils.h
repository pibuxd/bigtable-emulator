#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_PERSIST_TEST_UTILS
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_PERSIST_TEST_UTILS

#include "google/cloud/testing_util/chrono_literals.h"
#include "column_family.h"
#include "filter.h"
#include "persist/memory/storage.h"
#include "persist/rocksdb/storage.h"
#include "range_set.h"
#include "re2/re2.h"
#include "table.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#define EXPECT_TABLE_NAMES_PREFIX(PREFIX, ...) \
  EXPECT_EQ((storage->Tables(PREFIX).names()), \
            (std::vector<std::string>{__VA_ARGS__}));

#define EXPECT_OK(VALUE)          \
  if (true) {                     \
    auto v = (VALUE);             \
    if (!v.ok()) {                \
      EXPECT_EQ(v.message(), ""); \
    }                             \
  };

#define EXPECT_ROWS(MANAGER, TABLE_NAME, ...) \
  EXPECT_EQ(((MANAGER).getTableRowsDump(TABLE_NAME)), (rows_dump{__VA_ARGS__}));

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

using rows_dump = std::vector<
    std::tuple<std::string, std::chrono::milliseconds, std::string>>;

template <typename StorageT>
class StorageTestManager {
 private:
  std::shared_ptr<StorageT> storage;
  std::string const test_table_prefix = "projects/test/instances/test";
  size_t test_table_uid = 1;

 protected:
  inline void setStorage(std::shared_ptr<StorageT>&& new_storage) {
    storage = new_storage;
  }

 public:
  inline std::string const testTablePrefix() const { return test_table_prefix; }

  inline std::shared_ptr<StorageT> getStorage() const { return storage; }

  inline std::string testTableName(std::string const& name) const {
    return absl::StrCat(testTablePrefix(), "/tables/", name);
  }

  inline rows_dump getTableRowsDump(std::string const& table_name) {
    rows_dump vals;
    auto stream = storage->StreamTableFull(table_name).value();
    DBG("STREAM HAS VALUE??? ");
    DBG(stream.HasValue());
    for (; stream.HasValue(); stream.Next(NextMode::kCell)) {
      auto& v = stream.Value();
      auto row_msg = absl::StrCat(v.column_family(), ".", v.row_key(), ".",
                                  v.column_qualifier());
      vals.push_back(std::make_tuple(row_msg, v.timestamp(), v.value()));
    }
    return vals;
  }

  inline std::string createTestTable(
      std::vector<std::string> const column_family_names = {}) {
    auto const table_name =
        testTableName(absl::StrCat("table_", test_table_uid));
    ++test_table_uid;
    ::google::bigtable::admin::v2::Table schema;
    schema.set_name(table_name);
    for (auto& column_family_name : column_family_names) {
      (*schema.mutable_column_families())[column_family_name] =
          ::google::bigtable::admin::v2::ColumnFamily();
    }
    auto create_table_status = storage->CreateTable(schema);
    if (!create_table_status.ok()) {
      DBG(create_table_status.message());
    }
    assert(create_table_status.ok());
    return table_name;
  }

  inline static uint64_t toMicros(std::chrono::milliseconds const& millis) {
    return std::chrono::duration_cast<std::chrono::microseconds>(millis)
        .count();
  }

  inline std::chrono::milliseconds now() {
    std::chrono::time_point<std::chrono::system_clock> now =
        std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration);
  }
};

class MemoryStorageTestManager : public StorageTestManager<MemoryStorage> {
 public:
  explicit MemoryStorageTestManager() {
    setStorage(std::make_shared<MemoryStorage>());
  }
};

class RocksDBStorageTestManager : public StorageTestManager<RocksDBStorage> {
 private:
  storage::StorageRocksDBConfig storage_config;
  std::filesystem::path test_storage_path;

 public:
  explicit RocksDBStorageTestManager() {
    test_storage_path = {(std::filesystem::temp_directory_path() /=
                          "bte_test") /= std::tmpnam(nullptr)};
    // Attempt to create the directory.
    std::filesystem::create_directories(test_storage_path);

    storage_config.set_db_path(test_storage_path);
    storage_config.set_meta_column_family("bte_metadata");
    setStorage(std::make_shared<RocksDBStorage>(storage_config));
    assert(getStorage()->Open().ok());
  }

  inline Status reconnect() {
    auto close_status = getStorage()->Close();
    if (!close_status.ok()) {
      return close_status;
    }
    getStorage().reset();
    setStorage(std::make_unique<RocksDBStorage>(storage_config));
    auto open_status = getStorage()->Open();
    if (!open_status.ok()) {
      return open_status;
    }
    return Status();
  }
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_PERSIST_TEST_UTILS