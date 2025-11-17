#include "server.h"
#include "cluster.h"
#include "row_streamer.h"
#include "to_grpc_status.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status_or.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.grpc.pb.h>
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/longrunning/operations.pb.h>
#include <google/protobuf/empty.pb.h>
#include <google/protobuf/util/time_util.h>
#include "absl/strings/str_cat.h"
#include <grpcpp/impl/call_op_set.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "persist/storage.h"

#define DBG(TEXT) if(true){ std::cout << (TEXT) << "\n"; std::cout.flush(); }

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

StatusOr<std::shared_ptr<Table>> CreateTable(
    std::string const& table_name, std::vector<std::string>& column_families) {
  ::google::bigtable::admin::v2::Table schema;
  schema.set_name(table_name);
  for (auto& column_family_name : column_families) {
    (*schema.mutable_column_families())[column_family_name] =
        ::google::bigtable::admin::v2::ColumnFamily();
  }

  return Table::Create(schema);
}

static inline void ExampleClusterCode(std::shared_ptr<Cluster> cluster) {
    DBG("Running ExampleClusterCode() from example.cc");

    auto const* const table_name = "projects/test/instances/test/tables/test";
    auto const* const column_family_name = "test_column_family";
    auto const* const row_key = "0";
    auto const* const column_qualifier = "column_1";
    auto timestamp_micros = 1000;
    auto const* const true_mutation_value = "set by a true mutation";
    auto const* const false_mutation_value = "set by a false mutation";

    std::vector<std::string> column_families = {column_family_name};
    ::google::bigtable::admin::v2::Table schema;
    schema.set_name(table_name);
    for (auto& column_family_name : column_families) {
        (*schema.mutable_column_families())[column_family_name] =
            ::google::bigtable::admin::v2::ColumnFamily();
    }
    auto maybe_table = cluster->CreateTable(table_name, schema);
    if (!maybe_table.ok()) {
      DBG(maybe_table.status().message());
      return;
    }

    ::google::bigtable::admin::v2::Table_View view = ::google::bigtable::admin::v2::Table_View_FULL;
    auto maybe_tables = cluster->ListTables("projects/test/instances/test", view);
    if (!maybe_tables.ok()) {
      DBG(maybe_tables.status().message());
      return;
    }
    DBG("Iterate over tables:");
    for(auto& x: maybe_tables.value()) {
      DBG(x.name());
    }

    auto t = cluster->GetTable(table_name, view);
    if (!t.ok()) {
      DBG(t.status().message());
      return;
    }
    DBG(t.value().name());

    // Delete and list everything again
    auto s = cluster->DeleteTable(table_name);
    if (!s.ok()) {
      DBG(s.message());
      return;
    }
    maybe_tables = cluster->ListTables("projects/test/instances/test", view);
    if (!maybe_tables.ok()) {
      DBG(maybe_tables.status().message());
      return;
    }
    DBG("Iterate over tables:");
    for(auto& x: maybe_tables.value()) {
      DBG(x.name());
    }
    // Check existence
    assert(!cluster->HasTable(table_name));
    maybe_table = cluster->CreateTable(table_name, schema);
    if (!maybe_table.ok()) {
      DBG(maybe_table.status().message());
      return;
    }

    // Check existence
    assert(cluster->HasTable(table_name));
    
    auto maybe_get_table = cluster->FindTable(table_name);
    if (!maybe_get_table.ok()) {
      DBG(maybe_get_table.status().message());
      return;
    }
    auto primary_table = maybe_get_table.value();

    auto mut_request = google::bigtable::v2::CheckAndMutateRowRequest();
    auto mut1 = mut_request.add_true_mutations();
    auto mut1_sc = mut1->mutable_set_cell();
    mut1_sc->set_family_name(column_family_name);
    mut1_sc->set_column_qualifier(column_qualifier);
    //mut1_sc->set_column_qualifier(column_qualifier);
    mut1_sc->set_timestamp_micros(123);
    mut1_sc->set_value("TEST_ROW_VALUE");
    mut_request.set_row_key("TEST_ROW");

    auto x = primary_table->CheckAndMutateRow(mut_request);
    if (!x.ok()) {
      DBG(x.status().message());
      return;
    }

    DBG("Done running ExampleClusterCode() from example.cc");
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
