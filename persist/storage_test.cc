// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "table.h"
#include "column_family.h"
#include "filter.h"
#include "range_set.h"
#include "google/cloud/testing_util/chrono_literals.h"
#include <gtest/gtest.h>
#include "re2/re2.h"
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <filesystem>

#define EXPECT_TABLE_NAMES_PREFIX(PREFIX, ...) \
  EXPECT_EQ((storage->Tables(PREFIX).names()), (std::vector<std::string>{__VA_ARGS__}));

#define EXPECT_OK(VALUE) \
  if (true) { \
    auto v = (VALUE); \
    if (!v.ok()) { \
      EXPECT_EQ(v.message(), ""); \
    } \
  };

#define EXPECT_ROWS(MANAGER, TABLE_NAME, ...) \
  EXPECT_EQ(((MANAGER).getTableRowsDump(TABLE_NAME)), (rows_dump{__VA_ARGS__}));

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

using rows_dump = std::vector<std::tuple<std::string, std::chrono::milliseconds, std::string>>;

class RocksDBStorageTestManager {
private:
  std::unique_ptr<RocksDBStorage> storage;
  storage::StorageRocksDBConfig storage_config;
  std::filesystem::path test_storage_path;

  const std::string test_table_prefix = "projects/test/instances/test";
  size_t test_table_uid = 1;
public:
    explicit RocksDBStorageTestManager() {
      test_storage_path = {(std::filesystem::temp_directory_path() /= "bte_test") /= std::tmpnam(nullptr)};
      // Attempt to create the directory.
      std::filesystem::create_directories(test_storage_path);

      storage_config.set_db_path(test_storage_path);
      storage_config.set_meta_column_family("bte_metadata");
      storage = std::make_unique<RocksDBStorage>(storage_config);
      assert(storage->Open().ok());
    }

    inline const std::string testTablePrefix() const {
      return test_table_prefix;
    }

    inline RocksDBStorage* getStorage() const {
      return &*storage;
    }

    inline std::string testTableName(const std::string& name) const {
      return absl::StrCat(testTablePrefix(), "/tables/", name);
    }

    inline rows_dump getTableRowsDump(const std::string& table_name) {
      rows_dump vals;
      auto stream = storage->StreamTable(table_name);
      DBG("STREAM HAS VALUE??? "); DBG(stream.HasValue());
      for (; stream.HasValue(); stream.Next(NextMode::kCell)) {
        auto& v = stream.Value();
        auto row_msg = absl::StrCat(v.column_family(), ".", v.row_key(), ".", v.column_qualifier());
        vals.push_back(std::make_tuple(row_msg, v.timestamp(), v.value()));
      }
      return vals;
    }

    inline std::string createTestTable(const std::vector<std::string> column_family_names = {}) {
      const auto table_name = testTableName(absl::StrCat("table_", test_table_uid));
      ++test_table_uid;
      ::google::bigtable::admin::v2::Table schema;
      schema.set_name(table_name);
      for (auto& column_family_name : column_family_names ) {
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

    inline std::chrono::milliseconds now() {
      std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
      auto duration = now.time_since_epoch();
      return std::chrono::duration_cast<std::chrono::milliseconds>(duration);
    }

    inline Status reconnect() {
      auto close_status = storage->Close();
      if (!close_status.ok()) {
        return close_status;
      }
      storage.reset();
      storage = std::make_unique<RocksDBStorage>(storage_config);
      auto open_status = storage->Open();
      if (!open_status.ok()) {
        return open_status;
      }
      return Status();
    }

    ~RocksDBStorageTestManager() {
      // Do nothing
    }
};
  
TEST(RocksDBStorage, CreateTableBasicRestart) {
  RocksDBStorageTestManager m;
  auto storage = m.getStorage();

  const auto table_name = m.createTestTable();
  // Verify all table was created
  EXPECT_TABLE_NAMES_PREFIX(m.testTablePrefix(), table_name);

  EXPECT_OK(m.reconnect());

  // Verify that the table survived restart
  storage = m.getStorage();
  EXPECT_TABLE_NAMES_PREFIX(m.testTablePrefix(), table_name);
}

TEST(RocksDBStorage, TableRowsRead) {
  RocksDBStorageTestManager m;
  auto storage = m.getStorage();

  const auto table_name1 = m.createTestTable({ "cf_1" });
  const auto table_name2 = m.createTestTable({ "cf_1" });
  EXPECT_TABLE_NAMES_PREFIX(m.testTablePrefix(), table_name1, table_name2);

  const auto write_tx = storage->RowTransaction(table_name1, "row_1");
  auto t = m.now();
  auto t1 = t++;
  EXPECT_OK(write_tx->SetCell("cf_1", "col_1", t1, "value_1"));
  EXPECT_OK(write_tx->Commit());

  // Test if we do not mirror row inserts across tables
  EXPECT_ROWS(m, table_name1, {"cf_1.row_1.col_1", t1, "value_1"});
  EXPECT_ROWS(m, table_name2);

  const auto write_tx2 = storage->RowTransaction(table_name2, "row_1");
  auto t2 = t++;
  EXPECT_OK(write_tx2->SetCell("cf_1", "col_1", t1, "value_2a"));
  EXPECT_OK(write_tx2->SetCell("cf_1", "col_1", t2, "value_2b"));
  EXPECT_OK(write_tx2->SetCell("cf_1", "col_2", t2, "value_3"));
  EXPECT_OK(write_tx2->Commit());

  // Verify that we have new rows
  EXPECT_ROWS(m, table_name1, {"cf_1.row_1.col_1", t1, "value_1"});
  EXPECT_ROWS(m, table_name2, {"cf_1.row_1.col_1", t1, "value_2a"}, {"cf_1.row_1.col_1", t2, "value_2b"},{"cf_1.row_1.col_2", t2, "value_3"});

  auto t3 = t++;
  const auto del_tx3 = storage->RowTransaction(table_name2, "row_1");
  EXPECT_OK(del_tx3->DeleteRowColumn("cf_1", "col_1", t1, t3));
  EXPECT_OK(del_tx3->Commit());

  EXPECT_ROWS(m, table_name2, {"cf_1.row_1.col_2", t2, "value_3"});

  // We delete entire row
  const auto del_tx4 = storage->RowTransaction(table_name2, "row_1");
  del_tx4->DeleteRowFromColumnFamily("cf_1");
  del_tx4->Commit();

  // Table should be empty, second table should be unchanged
  EXPECT_ROWS(m, table_name2);
  EXPECT_ROWS(m, table_name1, {"cf_1.row_1.col_1", t1, "value_1"});

  // Empty second table as well
  const auto del_tx5 = storage->RowTransaction(table_name1, "row_1");
  del_tx5->DeleteRowFromAllColumnFamilies();
  del_tx5->Commit();
  EXPECT_ROWS(m, table_name1);

}

TEST(RocksDBStorage, TableManyRowsDeleteFromAllColumnFamilies) {
  RocksDBStorageTestManager m;
  auto storage = m.getStorage();

  const auto table_name1 = m.createTestTable({ "cf_1", "cf_2", "cf_3" });
  const auto write_tx = storage->RowTransaction(table_name1, "row_1");
  auto t = m.now();
  auto t1 = t++;
  EXPECT_OK(write_tx->SetCell("cf_1", "col_1", t1, "value_1"));
  EXPECT_OK(write_tx->SetCell("cf_2", "col_2", t1, "value_2"));
  EXPECT_OK(write_tx->SetCell("cf_3", "col_3", t1, "value_3"));
  EXPECT_OK(write_tx->SetCell("cf_1", "col_4", t1, "value_4"));
  EXPECT_OK(write_tx->SetCell("cf_3", "col_2", t1, "value_5"));
  EXPECT_OK(write_tx->Commit());

  const auto write_tx2 = storage->RowTransaction(table_name1, "row_2");
  EXPECT_OK(write_tx2->SetCell("cf_3", "col_1", t1, "value_6"));
  EXPECT_OK(write_tx2->Commit());

  EXPECT_ROWS(m, table_name1, {"cf_1.row_1.col_1", t1, "value_1"}, {"cf_1.row_1.col_4", t1, "value_4"},{"cf_2.row_1.col_2", t1, "value_2"}, {"cf_3.row_1.col_2", t1, "value_5"}, {"cf_3.row_1.col_3", t1, "value_3"}, {"cf_3.row_2.col_1", t1, "value_6"});

  const auto del_tx = storage->RowTransaction(table_name1, "row_1");
  del_tx->DeleteRowFromAllColumnFamilies();
  del_tx->Commit();
  EXPECT_ROWS(m, table_name1, {"cf_3.row_2.col_1", t1, "value_6"});
}


}  // anonymous namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
