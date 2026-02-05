/**
 * @file persist/utils/test_utils.cc
 * @brief Definitions for test utilities shared by persistence tests.
 */

#include "persist/utils/test_utils.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

IntegrationServer::IntegrationServer(size_t test_uid,
                                     std::shared_ptr<Storage> storage)
    : storage_(std::move(storage)),
      port_(static_cast<uint16_t>(8080 + test_uid)),
      host_("0.0.0.0") {
  auto maybe_emulator =
      google::cloud::bigtable::emulator::CreateDefaultEmulatorServer(
          host_, port_, storage_);
  assert(maybe_emulator.ok());
  emulator_ = std::move(maybe_emulator.value());
  worker_ = std::thread([this]() { this->emulator_->Wait(); });
}

Options IntegrationServer::getClientOptions() {
  return Options{}
      .set<EndpointOption>(fmt::format("{}:{}", host_, port_))
      .set<LoggingComponentsOption>(
          std::set<std::string>{"rpc", "rpc-streams", "auth"})
      .set<UnifiedCredentialsOption>(MakeInsecureCredentials());
}

void IntegrationServer::Kill() {
  if (!active_) {
    return;
  }
  emulator_->Shutdown();
  if (worker_.joinable()) {
    worker_.join();
  }
  active_ = false;
}

IntegrationServer::~IntegrationServer() { Kill(); }

cbt::Table IntegrationServer::Table(std::string project_id,
                                    std::string instance_id,
                                    std::string table_id) {
  return cbt::Table(cbt::MakeDataConnection(getClientOptions()),
                    cbt::TableResource(std::move(project_id),
                                       std::move(instance_id),
                                       std::move(table_id)));
}

cbta::BigtableTableAdminClient IntegrationServer::Client() {
  return cbta::BigtableTableAdminClient(
      cbta::MakeBigtableTableAdminConnection(getClientOptions()));
}

MemoryStorageTestManager::MemoryStorageTestManager() {
  setStorage(std::make_shared<MemoryStorage>());
}

RocksDBStorageTestManager::RocksDBStorageTestManager() {
  test_storage_path = (std::filesystem::temp_directory_path() /= "bte_test") /=
                      std::tmpnam(nullptr);
  // Attempt to create the directory.
  std::filesystem::create_directories(test_storage_path);

  storage_config.set_db_path(test_storage_path);
  storage_config.set_meta_column_family("bte_metadata");
  setStorage(std::make_shared<RocksDBStorage>(storage_config));
  assert(getStorage()->Open().ok());
}

Status RocksDBStorageTestManager::reconnect() {
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

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

