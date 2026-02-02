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
  EXPECT_EQ(storage->tables(PREFIX).names(), std::vector<std::string>{__VA_ARGS__});


namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

class RocksDBStorageTestManager {
private:
  std::unique_ptr<RocksDBStorage> storage;
  storage::StorageRocksDBConfig storage_config;
  std::filesystem::path test_storage_path;
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

    inline RocksDBStorage* getStorage() const {
      return &*storage;
    }

    inline Status reconnect() {
      DBG("RECONNECT CLOSE");
      auto close_status = storage->Close();
      if (!close_status.ok()) {
        return close_status;
      }
      DBG("RECONNECT RELEASE");
      storage.release();
      storage = std::make_unique<RocksDBStorage>(storage_config);
      DBG("RECONNECT OPEN");
      auto open_status = storage->Open();
      if (!open_status.ok()) {
        return open_status;
      }
      DBG("RECONNECT DONE");
      return Status();
    }

    ~RocksDBStorageTestManager() {
      // Do nothing
    }
};
  
TEST(RocksDBStorage, CreateTableBasicRestart) {
  RocksDBStorageTestManager m;
  auto storage = m.getStorage();

  const auto table_prefix = "projects/test/instances/test";
  const auto table_name = absl::StrCat(table_prefix, "/tables/test");
  ::google::bigtable::admin::v2::Table schema;
  schema.set_name(table_name);
  EXPECT_TRUE(storage->CreateTable(schema).ok());

  // Verify all table was created
  EXPECT_TABLE_NAMES_PREFIX(table_prefix, table_name);

  EXPECT_TRUE(m.reconnect().ok());

  // Verify that the table survived restart
  storage = m.getStorage();
  EXPECT_TABLE_NAMES_PREFIX(table_prefix, table_name);
}


}  // anonymous namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
