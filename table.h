// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H

#include "persist/storage.h"
#include "column_family.h"
#include "limits.h"
#include "filter.h"
#include "range_set.h"
#include "row_streamer.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/types/variant.h"
#include "google/protobuf/repeated_ptr_field.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include "absl/types/optional.h"
#include <grpcpp/support/sync_stream.h>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stack>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {


// Stupid temporary wrapper that does nothing, just groups methods 
class Table2 {
  private:
    RocksDBStorage* storage_;
    std::string name_;


    absl::optional<google::bigtable::admin::v2::Type> GetColumnFamilyType(std::string const& family_name) {
      auto& cfs = storage_->GetTable(name_).value().mutable_table()->column_families();
      auto const& it = cfs.find(family_name);
      if (it == cfs.end()) {
        return absl::nullopt;
      }
      return it->second.value_type();
    }
  public:
    Table2(std::string const& name, RocksDBStorage* storage): name_(name), storage_(storage) {}

    Status DoMutationsWithPossibleRollback(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const& mutations
    ) {
      if (row_key.size() > kMaxRowLen) {
        return InvalidArgumentError(
            "The row_key is longer than 4KiB",
            GCP_ERROR_INFO().WithMetadata("row_key size",
                                          absl::StrFormat("%zu", row_key.size())));
      }

      //RowTransaction row_transaction(this->get(), row_key);
      auto txn = storage_->RowTransaction(row_key);

      for (auto const& mutation : mutations) {
        if (mutation.has_set_cell()) {
          auto const& set_cell = mutation.set_cell();

          absl::optional<std::chrono::milliseconds> timestamp_override =
              absl::nullopt;

          if (set_cell.timestamp_micros() < -1) {
            return InvalidArgumentError(
                "Timestamp micros cannot be < -1.",
                GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
          }

          if (set_cell.timestamp_micros() == -1) {
            timestamp_override.emplace(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()));
          }

          auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::microseconds(set_cell.timestamp_micros()));

          if (timestamp_override.has_value()) {
            timestamp = timestamp_override.value();
          }

          auto status = txn->SetCell(set_cell.family_name(), set_cell.column_qualifier(), timestamp, set_cell.value());
          if (!status.ok()) {
            return status;
          }
        } else if (mutation.has_add_to_cell()) {
          auto const& add_to_cell = mutation.add_to_cell();

          absl::optional<std::chrono::milliseconds> timestamp_override =
              absl::nullopt;

          std::chrono::milliseconds timestamp = std::chrono::milliseconds::zero();

          if (add_to_cell.has_timestamp() &&
              add_to_cell.timestamp().has_raw_timestamp_micros()) {
            timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::microseconds(
                    add_to_cell.timestamp().raw_timestamp_micros()));
          }

          // If no valid timestamp is provided, override with the system time.
          if (timestamp <= std::chrono::milliseconds::zero()) {
            timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch());
            timestamp_override.emplace(std::move(timestamp));
          }

          auto cf_value_type = GetColumnFamilyType(add_to_cell.family_name());
          if (!cf_value_type.has_value() ||
              !cf_value_type.value().has_aggregate_type()) {
            return InvalidArgumentError(
                "column family is not configured to contain aggregation cells or "
                "aggregation type not properly configured",
                GCP_ERROR_INFO().WithMetadata("column family",
                                              add_to_cell.family_name()));
          }

          // Ensure that we support the aggregation that is configured in the
          // column family.
          switch (cf_value_type.value().aggregate_type().aggregator_case()) {
            case google::bigtable::admin::v2::Type::Aggregate::kSum:
            case google::bigtable::admin::v2::Type::Aggregate::kMin:
            case google::bigtable::admin::v2::Type::Aggregate::kMax:
              break;
            default:
              return UnimplementedError(
                  "column family configured with unimplemented aggregation",
                  GCP_ERROR_INFO()
                      .WithMetadata("column family", add_to_cell.family_name())
                      .WithMetadata("configured aggregation",
                                    absl::StrFormat("%d", cf_value_type.value()
                                                              .aggregate_type()
                                                              .aggregator_case())));
          }

          if (!add_to_cell.has_input()) {
            return InvalidArgumentError(
                "input not set",
                GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
          }

          switch (add_to_cell.input().kind_case()) {
            case google::bigtable::v2::Value::kIntValue:
              if (!add_to_cell.input().has_int_value()) {
                return InvalidArgumentError("input value not set",
                                            GCP_ERROR_INFO().WithMetadata(
                                                "mutation", add_to_cell.DebugString()));
              }
              break;
            default:
              return InvalidArgumentError(
                  "only int64 values are supported",
                  GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
          }
          auto int64_input = add_to_cell.input().int_value();

          auto value = google::cloud::internal::EncodeBigEndian(int64_input);
          //auto row_key = row_key_;

          std::chrono::milliseconds ts_ms;
          if (timestamp_override.has_value()) {
            ts_ms = timestamp_override.value();
          } else {
            ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::microseconds(
                    add_to_cell.timestamp().raw_timestamp_micros()));
          }

          if (!add_to_cell.has_column_qualifier() ||
              !add_to_cell.column_qualifier().has_raw_value()) {
            return InvalidArgumentError(
                "column qualifier not set",
                GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
          }
          auto column_qualifier = add_to_cell.column_qualifier().raw_value();

          auto maybe_old_value = txn->UpdateCell(add_to_cell.family_name(), column_qualifier, ts_ms, value, [](std::string const& /*existing_value*/, std::string&& new_value) -> StatusOr<std::string> {
            return new_value;
          });

          //auto maybe_old_value = cf.UpdateCell(row_key, column_qualifier, ts_ms, value);
          if (!maybe_old_value) {
            return maybe_old_value.status();
          }

          return Status();
        } else if (mutation.has_merge_to_cell()) {
          return UnimplementedError(
              "Unsupported mutation type.",
              GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
        // } else if (mutation.has_delete_from_column()) {
        //   auto const& delete_from_column = mutation.delete_from_column();
        //   auto status = row_transaction.DeleteFromColumn(delete_from_column);
        //   if (!status.ok()) {
        //     return status;
        //   }
        // } else if (mutation.has_delete_from_family()) {
        //   auto const& delete_from_family = mutation.delete_from_family();
        //   auto status = row_transaction.DeleteFromFamily(delete_from_family);
        //   if (!status.ok()) {
        //     return status;
        //   }
        // } else if (mutation.has_delete_from_row()) {
        //   auto status = row_transaction.DeleteFromRow();
        //   if (!status.ok()) {
        //     return status;
        //   }
        } else {
          return UnimplementedError(
              "Unsupported mutation type.",
              GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
        }
      }

      // If we get here, all mutations on the row have succeeded. We can
      // commit and return which will prevent the destructor from undoing
      // the transaction.
      //row_transaction.commit();
      return txn->Commit();
    }

    StatusOr<google::bigtable::v2::CheckAndMutateRowResponse> CheckAndMutateRow(
      google::bigtable::v2::CheckAndMutateRowRequest const& request
    ) {
      auto const& row_key = request.row_key();

      if (row_key.size() > kMaxRowLen) {
        return InvalidArgumentError(
            "The row_key is longer than 4KiB",
            GCP_ERROR_INFO()
                .WithMetadata("row_key size",
                              absl::StrFormat("%zu", row_key.size()))
                .WithMetadata("CheckAndMutateRequest", request.DebugString()));
      }

      if (row_key.empty()) {
        return InvalidArgumentError(
            "row key required",
            GCP_ERROR_INFO().WithMetadata("CheckAndMutateRowRequest",
                                          request.DebugString()));
      }

      if (request.true_mutations_size() == 0 &&
          request.false_mutations_size() == 0) {
        return InvalidArgumentError(
            "both true mutations and false mutations are empty",
            GCP_ERROR_INFO().WithMetadata("CheckAndMutateRowRequest",
                                          request.DebugString()));
      }

      auto range_set = std::make_shared<StringRangeSet>();

      // FIXME: MAKE THIS WORK
      // range_set->Sum(StringRangeSet::Range(row_key, false, row_key, false));

      // StatusOr<CellStream> maybe_stream;
      // if (request.has_predicate_filter()) {
      //   maybe_stream =
      //       CreateCellStream(range_set, std::move(request.predicate_filter()));
      // } else {
      //   maybe_stream = CreateCellStream(range_set, absl::nullopt);
      // }

      // if (!maybe_stream) {
      //   return maybe_stream.status();
      // }

      // bool a_cell_is_found = false;

      // CellStream& stream = *maybe_stream;
      // if (stream) {  // At least one cell/value found when filter is applied
      //   a_cell_is_found = true;
      // }

      bool a_cell_is_found = true;

      Status status;
      if (a_cell_is_found) {
        status = DoMutationsWithPossibleRollback(request.row_key(),
                                                request.true_mutations());
      } else {
        status = DoMutationsWithPossibleRollback(request.row_key(),
                                                request.false_mutations());
      }

      if (!status.ok()) {
        return status;
      }

      google::bigtable::v2::CheckAndMutateRowResponse success_response;
      success_response.set_predicate_matched(a_cell_is_found);

      return success_response;
    }
};


/// Objects of this class represent Bigtable tables.
class Table : public std::enable_shared_from_this<Table> {
 public:
  static StatusOr<std::shared_ptr<Table>> Create(
      google::bigtable::admin::v2::Table schema);

  google::bigtable::admin::v2::Table GetSchema() const;

  Status Update(google::bigtable::admin::v2::Table const& new_schema,
                google::protobuf::FieldMask const& to_update);

  StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request);

  bool IsDeleteProtected() const;

  StatusOr<google::bigtable::v2::CheckAndMutateRowResponse> CheckAndMutateRow(
      google::bigtable::v2::CheckAndMutateRowRequest const& request);
  Status MutateRow(google::bigtable::v2::MutateRowRequest const& request);
  Status DoMutationsWithPossibleRollbackLocked(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations) {
    std::lock_guard<std::mutex> lock(mu_);

    return DoMutationsWithPossibleRollback(row_key, mutations);
  }

  StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const;

  Status ReadRows(google::bigtable::v2::ReadRowsRequest const& request,
                  RowStreamer& row_streamer) const;

  StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request);

  std::map<std::string, std::shared_ptr<ColumnFamily>>::iterator begin() {
    return column_families_.begin();
  }
  std::map<std::string, std::shared_ptr<ColumnFamily>>::iterator end() {
    return column_families_.end();
  }
  std::map<std::string, std::shared_ptr<ColumnFamily>>::iterator find(
      std::string const& column_family) {
    return column_families_.find(column_family);
  }

  Status SampleRowKeys(
      double pass_probability,
      grpc::ServerWriter<google::bigtable::v2::SampleRowKeysResponse>* writer);

  std::shared_ptr<Table> get() { return shared_from_this(); }

  Status DropRowRange(
      ::google::bigtable::admin::v2::DropRowRangeRequest const& request);

 private:
  Table() = default;
  friend class RowSetIterator;
  friend class RowTransaction;

  template <typename MESSAGE>
  StatusOr<std::reference_wrapper<ColumnFamily>> FindColumnFamily(
      MESSAGE const& message) const;
  bool IsDeleteProtectedNoLock() const;
  Status Construct(google::bigtable::admin::v2::Table schema);
  Status DoMutationsWithPossibleRollback(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations);

  mutable std::mutex mu_;
  google::bigtable::admin::v2::Table schema_;
  std::map<std::string, std::shared_ptr<ColumnFamily>> column_families_;
};

struct RestoreValue {
  ColumnFamily& column_family;
  std::string column_qualifier;
  std::chrono::milliseconds timestamp;
  std::string value;
};

struct DeleteValue {
  ColumnFamily& column_family;
  std::string column_qualifier;
  std::chrono::milliseconds timestamp;
};

class RowTransaction {
 public:
  explicit RowTransaction(std::shared_ptr<Table> table,
                          std::string const& row_key)
      : row_key_(row_key) {
    table_ = std::move(table);
    committed_ = false;
  };

  ~RowTransaction() {
    if (!committed_) {
      Undo();
    }
  };

  void commit() { committed_ = true; }

  // timestamp_override, if provided, will be used instead of
  // set_cell.timestamp. The override is used to set the timestamp to
  // the server time in case a timestamp <= 0 is provided.
  Status SetCell(::google::bigtable::v2::Mutation_SetCell const& set_cell,
                 absl::optional<std::chrono::milliseconds> timestamp_override =
                     absl::nullopt);
  Status AddToCell(
      ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
      absl::optional<std::chrono::milliseconds> timestamp_override);
  Status MergeToCell(
      ::google::bigtable::v2::Mutation_MergeToCell const& merge_to_cell);
  Status DeleteFromColumn(
      ::google::bigtable::v2::Mutation_DeleteFromColumn const&
          delete_from_column);
  Status DeleteFromFamily(
      ::google::bigtable::v2::Mutation_DeleteFromFamily const&
          delete_from_family);
  Status DeleteFromRow();

  StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request);

 private:
  void Undo();

  bool committed_;
  std::shared_ptr<Table> table_;
  std::stack<absl::variant<DeleteValue, RestoreValue>> undo_;
  // row_key_ is initialized from the request proto and therefore it
  // is safe to access it while the mutation request is ongoing. We
  // store a reference to it to avoid copying a potentially very large
  // (up to 4KB) value.
  std::string const& row_key_;
};

google::bigtable::v2::ReadModifyWriteRowResponse
FamiliesToReadModifyWriteResponse(
    std::string const& row_key,
    std::map<std::string, ColumnFamily> const& families);

/**
 * A `AbstractCellStreamImpl` which streams filtered contents of the table.
 *
 * Underneath is essentially a collection of `FilteredColumnFamilyStream`s.
 * All filters applied to `FilteredColumnFamilyStream` are propagated to the
 * underlying `FilteredColumnFamilyStream`, except for `FamilyNameRegex`, which
 * is handled by this subclass.
 *
 * This class is public only to enable testing.
 */
class FilteredTableStream : public MergeCellStreams {
 public:
  explicit FilteredTableStream(
      std::vector<std::unique_ptr<FilteredColumnFamilyStream>> cf_streams)
      : MergeCellStreams(CreateCellStreams(std::move(cf_streams))) {}

  bool ApplyFilter(InternalFilter const& internal_filter) override;

 private:
  static std::vector<CellStream> CreateCellStreams(
      std::vector<std::unique_ptr<FilteredColumnFamilyStream>> cf_streams);
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H
