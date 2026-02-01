/**
 * Rewrite of the table interface.
 * Most of the OOP classes are too interdependent, so the best way to migrate was to copy the class.
 * This class have no CC compilation unit and FOR NOW is header only (static inline methods)
 * - for easier development.
 **/
#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE2_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE2_H

#include "persist/storage.h"
#include "column_family.h"
#include "limits.h"
#include "filter.h"
#include "range_set.h"
#include "row_streamer.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/internal/big_endian.h"
#include "absl/types/variant.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "google/protobuf/util/field_mask_util.h"
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

static inline StatusOr<StringRangeSet> CreateStringRangeSet(
    google::bigtable::v2::RowSet const& row_set
) {
  StringRangeSet res;
  for (auto const& row_key : row_set.row_keys()) {
    if (row_key.size() > kMaxRowLen) {
      return InvalidArgumentError(
          "The row_key in row_set is longer than 4KiB",
          GCP_ERROR_INFO()
              .WithMetadata("row_key size",
                            absl::StrFormat("%zu", row_key.size()))
              .WithMetadata("row_set", row_set.DebugString()));
    }

    if (row_key.empty()) {
      return InvalidArgumentError(
          "`row_key` empty",
          GCP_ERROR_INFO().WithMetadata("row_set", row_set.DebugString()));
    }
    res.Sum(StringRangeSet::Range(row_key, false, row_key, false));
  }
  for (auto const& row_range : row_set.row_ranges()) {
    auto maybe_range = StringRangeSet::Range::FromRowRange(row_range);
    if (!maybe_range) {
      return maybe_range.status();
    }
    if (maybe_range->IsEmpty()) {
      continue;
    }
    res.Sum(*std::move(maybe_range));
  }
  return res;
}

/**
 * Stupid wrapper that just temporarily group common methods together.
 */
class Table2 {
  private:
    std::shared_ptr<RocksDBStorage> storage_;
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
    Table2(std::string const& name, std::shared_ptr<RocksDBStorage> storage): name_(name), storage_(storage) {}

    Status MutateRow(google::bigtable::v2::MutateRowRequest const& request) {
      return DoMutationsWithPossibleRollback(request.row_key(), request.mutations());
    }

    Status ReadRows(
      google::bigtable::v2::ReadRowsRequest const& request,
      RowStreamer& row_streamer
    ) const {
      std::shared_ptr<StringRangeSet> row_set;
      if (request.has_rows() && (request.rows().row_ranges_size() > 0 ||
                                 request.rows().row_keys_size() > 0)) {
        auto maybe_row_set = CreateStringRangeSet(request.rows());
        if (!maybe_row_set) {
          return maybe_row_set.status();
        }
        row_set = std::make_shared<StringRangeSet>(*std::move(maybe_row_set));
      } else {
        row_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
      }

      StatusOr<CellStream> maybe_stream;
      if (request.has_filter()) {
        maybe_stream = CreateCellStream(row_set, std::move(request.filter()));
      } else {
        maybe_stream = CreateCellStream(row_set, absl::nullopt);
      }

      if (!maybe_stream) {
        return maybe_stream.status();
      }

      std::int64_t rows_count = 0;
      absl::optional<std::string> current_row_key;

      CellStream& stream = *maybe_stream;
      for (; stream; ++stream) {
        if (request.rows_limit() > 0) {
          if (!current_row_key.has_value() ||
              stream->row_key() != current_row_key.value()) {
            rows_count++;
            current_row_key = stream->row_key();
          }

          if (rows_count > request.rows_limit()) {
            break;
          }
        }

        if (!row_streamer.Stream(*stream)) {
          return AbortedError("Stream closed by the client.", GCP_ERROR_INFO());
        }
      }

      if (!row_streamer.Flush(true)) {
        return AbortedError("Stream closed by the client.", GCP_ERROR_INFO());
      }
      return Status();
    }

    Status SampleRowKeys(
      double pass_probability,
      grpc::ServerWriter<google::bigtable::v2::SampleRowKeysResponse>* writer
    ) {
      if (pass_probability <= 0.0) {
        return InvalidArgumentError(
            "The sampling probabality must be positive",
            GCP_ERROR_INFO().WithMetadata("provided sampling probability",
                                          absl::StrFormat("%f", pass_probability)));
      }

      auto sample_every = static_cast<std::uint64_t>(std::ceil(1.0 / pass_probability));

      auto all_rows_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
      auto maybe_all_rows_stream = CreateCellStream(all_rows_set, absl::nullopt);
      if (!maybe_all_rows_stream) {
        return maybe_all_rows_stream.status();
      }

      auto& stream = *maybe_all_rows_stream;

      absl::optional<std::string> first_row_key;
      std::size_t row_size_estimate = 0;

      for (; stream; ++stream) {
        if (first_row_key.has_value() &&
            stream->row_key() != first_row_key.value()) {
          break;
        }
        first_row_key = stream->row_key();
        row_size_estimate += stream->row_key().size();
        row_size_estimate += stream->column_qualifier().size();
        row_size_estimate += stream->value().size();
        row_size_estimate += sizeof(stream->timestamp());
      }

      if (!first_row_key.has_value()) {
        google::bigtable::v2::SampleRowKeysResponse resp;
        resp.set_row_key("");
        resp.set_offset_bytes(0);
        auto opts = grpc::WriteOptions();
        opts.set_last_message();
        writer->WriteLast(std::move(resp), opts);
        return Status();
      }

      std::int64_t offset_delta = sample_every * row_size_estimate;

      google::bigtable::v2::RowFilter sample_filter;
      sample_filter.set_row_sample_filter(pass_probability);

      auto maybe_stream = CreateCellStream(all_rows_set, sample_filter);
      if (!maybe_stream) {
        return maybe_stream.status();
      }

      auto& sampled_stream = *maybe_stream;
      std::int64_t offset = 0;
      bool wrote_a_sample = false;

      for (; sampled_stream; sampled_stream.Next(NextMode::kRow)) {
        google::bigtable::v2::SampleRowKeysResponse resp;
        offset += offset_delta;
        resp.set_row_key(sampled_stream->row_key());
        resp.set_offset_bytes(offset);
        writer->Write(std::move(resp));
        wrote_a_sample = true;
      }

      if (!wrote_a_sample) {
        google::bigtable::v2::SampleRowKeysResponse resp;
        resp.set_row_key(first_row_key.value() + "\\x00");
        resp.set_offset_bytes(row_size_estimate);
        auto opts = grpc::WriteOptions();
        opts.set_last_message();
        writer->WriteLast(std::move(resp), opts);
      }

      return Status();
    }

    Status DoMutationsWithPossibleRollback(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const& mutations
    ) {
      // This handling logic was copied from table.cc - Table::DoMutationsWithPossibleRollback()
      if (row_key.size() > kMaxRowLen) {
        return InvalidArgumentError(
            "The row_key is longer than 4KiB",
            GCP_ERROR_INFO().WithMetadata("row_key size",
                                          absl::StrFormat("%zu", row_key.size())));
      }

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
        } else if (mutation.has_delete_from_column()) {
          auto const& delete_from_column = mutation.delete_from_column();

          // We need to check if the given timerange is empty or reversed, but
          // only up to the server's time accuracy (in our case, milliseconds)
          // - For example a time range of [1000, 1200] would be empty.
          if (delete_from_column.has_time_range()) {
            auto start = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::microseconds(
                    delete_from_column.time_range().start_timestamp_micros()));
            auto end = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::microseconds(
                    delete_from_column.time_range().end_timestamp_micros()));

            // An end timestamp micros of 0 is to be interpreted as infinity,
            // so we allow that.
            if (end <= start &&
                delete_from_column.time_range().end_timestamp_micros() != 0) {
              return InvalidArgumentError(
                  "empty or reversed time range: the end timestamp must be more than "
                  "the start timestamp when they are truncated to the server's time "
                  "precision (milliseconds)",
                  GCP_ERROR_INFO().WithMetadata("delete_from_column proto",
                                                delete_from_column.DebugString()));
            }
          }

          auto status = txn->DeleteRowColumn(
            delete_from_column.family_name(),
            delete_from_column.column_qualifier(),
            delete_from_column.time_range()
          );
          if (!status.ok()) {
            return status;
          }
        } else if (mutation.has_delete_from_family()) {
          auto const& delete_from_family = mutation.delete_from_family();
          auto status = txn->DeleteRowFromColumnFamily(
            delete_from_family.family_name()
          );
          if (!status.ok()) {
            return status;
          }
        } else if (mutation.has_delete_from_row()) {
          auto status = txn->DeleteRowFromAllColumnFamilies();
          if (!status.ok()) {
            return status;
          }
        } else {
          return UnimplementedError(
              "Unsupported mutation type.",
              GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
        }
      }

      // If we get here, all mutations on the row have succeeded. We can
      // commit and return which will prevent the destructor from undoing
      // the transaction.
      return txn->Commit();
    }

    StatusOr<google::bigtable::v2::ReadModifyWriteRowResponse> ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request
    ) {
      if (request.row_key().size() > kMaxRowLen) {
        return InvalidArgumentError(
            "The row_key is longer than 4KiB",
            GCP_ERROR_INFO().WithMetadata("row_key size",
                                          absl::StrFormat("%zu", request.row_key().size())));
      }

      if (request.row_key().empty()) {
        return InvalidArgumentError(
            "row key not set",
            GCP_ERROR_INFO().WithMetadata("request", request.DebugString()));
      }

      auto txn = storage_->RowTransaction(request.row_key());

      // Build response row with modified cells
      google::bigtable::v2::ReadModifyWriteRowResponse resp;
      auto* response_row = resp.mutable_row();
      response_row->set_key(request.row_key());

      for (auto const& rule : request.rules()) {
        // Verify column family exists
        auto cf_type = GetColumnFamilyType(rule.family_name());
        if (!cf_type.has_value()) {
          return InvalidArgumentError(
              "Column family not found",
              GCP_ERROR_INFO().WithMetadata("family_name", rule.family_name()));
        }

        std::chrono::milliseconds timestamp;
        std::string new_value;

        if (rule.has_append_value()) {
          // Append operation: concatenate append_value to existing cell value
          timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch());

          std::string append_str = rule.append_value();
          auto maybe_old_value = txn->UpdateCell(
              rule.family_name(),
              rule.column_qualifier(),
              timestamp,
              append_str,
              [](std::string const& old_value, std::string&& append_value) -> StatusOr<std::string> {
                // Concatenate old value with append value
                return old_value + append_value;
              });

          if (!maybe_old_value.ok()) {
            return maybe_old_value.status();
          }

          // If there was an old value, new_value is old + append; otherwise just append
          if (maybe_old_value.value().has_value()) {
            new_value = maybe_old_value.value().value() + append_str;
          } else {
            new_value = append_str;
          }

        } else if (rule.has_increment_amount()) {
          // Increment operation: decode existing int64, add increment_amount, encode back
          timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch());

          std::int64_t increment = rule.increment_amount();
          std::string dummy_value;
          
          auto maybe_old_value = txn->UpdateCell(
              rule.family_name(),
              rule.column_qualifier(),
              timestamp,
              dummy_value,
              [increment](std::string const& old_value, std::string&&) -> StatusOr<std::string> {
                if (old_value.empty()) {
                  // No existing value, just use increment amount
                  return google::cloud::internal::EncodeBigEndian(increment);
                }
                
                // Decode existing value and add increment
                auto maybe_old_int = google::cloud::internal::DecodeBigEndian<std::int64_t>(old_value);
                if (!maybe_old_int.ok()) {
                  return maybe_old_int.status();
                }
                
                std::int64_t result = maybe_old_int.value() + increment;
                return google::cloud::internal::EncodeBigEndian(result);
              });

          if (!maybe_old_value.ok()) {
            return maybe_old_value.status();
          }

          // Get the new value that was written
          std::int64_t result_int = increment;
          if (maybe_old_value.value().has_value()) {
            auto maybe_old_int = google::cloud::internal::DecodeBigEndian<std::int64_t>(
                maybe_old_value.value().value());
            if (maybe_old_int.ok()) {
              result_int = maybe_old_int.value() + increment;
            }
          }
          
          new_value = google::cloud::internal::EncodeBigEndian(result_int);

        } else {
          return InvalidArgumentError(
              "either append value or increment amount must be set",
              GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
        }

        // Add modified cell to response
        google::bigtable::v2::Family* family = nullptr;
        for (auto& fam : *response_row->mutable_families()) {
          if (fam.name() == rule.family_name()) {
            family = &fam;
            break;
          }
        }
        if (family == nullptr) {
          family = response_row->add_families();
          family->set_name(rule.family_name());
        }

        auto* column = family->add_columns();
        column->set_qualifier(rule.column_qualifier());
        
        auto* cell = column->add_cells();
        cell->set_timestamp_micros(
            std::chrono::duration_cast<std::chrono::microseconds>(timestamp).count());
        cell->set_value(std::move(new_value));
      }

      // Commit transaction
      auto commit_status = txn->Commit();
      if (!commit_status.ok()) {
        return commit_status;
      }

      return resp;
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

      range_set->Sum(StringRangeSet::Range(row_key, false, row_key, false));

      StatusOr<CellStream> maybe_stream;
      if (request.has_predicate_filter()) {
        maybe_stream =
            CreateCellStream(range_set, std::move(request.predicate_filter()));
      } else {
        maybe_stream = CreateCellStream(range_set, absl::nullopt);
      }

      if (!maybe_stream) {
        return maybe_stream.status();
      }

      bool a_cell_is_found = false;

      CellStream& stream = *maybe_stream;
      if (stream) {  // At least one cell/value found when filter is applied
        a_cell_is_found = true;
        DBG("a_cell_is_found = true");
      } else {
        DBG("a_cell_is_found = false");
      }

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

      DBG("EXIT CheckAndMutateRow()");
      return success_response;
    }

    StatusOr<CellStream> CreateCellStream(
        std::shared_ptr<StringRangeSet> range_set,
        absl::optional<google::bigtable::v2::RowFilter> maybe_row_filter
    ) const {
        auto table_stream_ctor = [range_set = std::move(range_set), this] {
            return storage_->StreamTable(name_, range_set);
        };

        if (maybe_row_filter.has_value()) {
            return CreateFilter(maybe_row_filter.value(), table_stream_ctor);
        }

        return table_stream_ctor();
    }

    // Admin operations

    Status Update(
        google::bigtable::admin::v2::Table const& new_schema,
        google::protobuf::FieldMask const& to_update
    ) {
      using google::protobuf::util::FieldMaskUtil;
      google::protobuf::FieldMask allowed_mask;
      FieldMaskUtil::FromString(
          "change_stream_config,"
          "change_stream_config.retention_period,"
          "deletion_protection",
          &allowed_mask);
      
      if (!FieldMaskUtil::IsValidFieldMask<google::bigtable::admin::v2::Table>(to_update)) {
        return InvalidArgumentError(
            "Update mask is invalid.",
            GCP_ERROR_INFO().WithMetadata("mask", to_update.DebugString()));
      }
      
      google::protobuf::FieldMask disallowed_mask;
      FieldMaskUtil::Subtract<google::bigtable::admin::v2::Table>(
          to_update, allowed_mask, &disallowed_mask);
      
      if (disallowed_mask.paths_size() > 0) {
        return UnimplementedError(
            "Update mask contains disallowed fields.",
            GCP_ERROR_INFO().WithMetadata("mask", disallowed_mask.DebugString()));
      }

      // Get current table metadata
      auto maybe_meta = storage_->GetTable(name_);
      if (!maybe_meta.ok()) {
        return maybe_meta.status();
      }

      // Update schema fields based on mask
      auto updated_schema = maybe_meta.value().table();
      FieldMaskUtil::MergeMessageTo(new_schema, to_update,
                                    FieldMaskUtil::MergeOptions(), &updated_schema);

      // Save updated schema back to storage
      // Note: This is simplified - in production you'd need a transaction
      storage::TableMeta new_meta;
      *new_meta.mutable_table() = updated_schema;
      
      // We need to update the metadata, but RocksDB storage doesn't expose
      // a direct update method. For now, we'll rely on the fact that
      // the schema is loaded on demand.
      // TODO: Implement atomic schema updates with storage sync
      
      return Status();
    }

    StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
        google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request
    ) {
      // Get current table schema
      auto maybe_meta = storage_->GetTable(name_);
      if (!maybe_meta.ok()) {
        return maybe_meta.status();
      }

      auto new_schema = maybe_meta.value().table();

      // Track new column families that need to be created
      std::vector<std::string> new_cf_names;

      for (auto const& modification : request.modifications()) {
        if (modification.drop()) {
          // Check deletion protection
          if (new_schema.deletion_protection()) {
            return FailedPreconditionError(
                "The table has deletion protection.",
                GCP_ERROR_INFO().WithMetadata("modification", modification.DebugString()));
          }
          
          // Remove column family from schema
          if (new_schema.mutable_column_families()->erase(modification.id()) == 0) {
            return NotFoundError(
                "No such column family.",
                GCP_ERROR_INFO().WithMetadata("modification", modification.DebugString()));
          }

          // Delete the column family from RocksDB
          auto drop_status = storage_->DeleteColumnFamily(modification.id());
          if (!drop_status.ok()) {
              return drop_status;
          }
          
        } else if (modification.has_update()) {
          auto& cfs = *new_schema.mutable_column_families();
          auto cf_it = cfs.find(modification.id());
          if (cf_it == cfs.end()) {
            return NotFoundError(
                "No such column family.",
                GCP_ERROR_INFO().WithMetadata("modification", modification.DebugString()));
          }

          using google::protobuf::util::FieldMaskUtil;
          google::protobuf::FieldMask effective_mask;
          
          if (modification.has_update_mask()) {
            effective_mask = modification.update_mask();
            if (!FieldMaskUtil::IsValidFieldMask<google::bigtable::admin::v2::ColumnFamily>(effective_mask)) {
              return InvalidArgumentError(
                  "Update mask is invalid.",
                  GCP_ERROR_INFO().WithMetadata("modification", modification.DebugString()));
            }
          } else {
            FieldMaskUtil::FromString("gc_rule", &effective_mask);
            if (!FieldMaskUtil::IsValidFieldMask<google::bigtable::admin::v2::ColumnFamily>(effective_mask)) {
              return InternalError(
                  "Default update mask is invalid.",
                  GCP_ERROR_INFO().WithMetadata("mask", effective_mask.DebugString()));
            }
          }

          // Disallow modification of value_type
          if (FieldMaskUtil::IsPathInFieldMask("value_type", effective_mask)) {
            return InvalidArgumentError(
                "The value_type cannot be changed after column family creation",
                GCP_ERROR_INFO().WithMetadata("mask", effective_mask.DebugString()));
          }

          FieldMaskUtil::MergeMessageTo(modification.update(), effective_mask,
                                        FieldMaskUtil::MergeOptions(), &(cf_it->second));
                                        
        } else if (modification.has_create()) {
          // Add new column family to schema
          if (!new_schema.mutable_column_families()
                   ->emplace(modification.id(), modification.create())
                   .second) {
            return AlreadyExistsError(
                "Column family already exists.",
                GCP_ERROR_INFO().WithMetadata("modification", modification.DebugString()));
          }

          new_cf_names.push_back(modification.id());
          
        } else {
          return UnimplementedError(
              "Unsupported modification.",
              GCP_ERROR_INFO().WithMetadata("modification", modification.DebugString()));
        }
      }

      if (!new_cf_names.empty()) {
        auto ensure_status = storage_->EnsureColumnFamiliesExist(new_cf_names);
        if (!ensure_status.ok()) {
          return ensure_status;
        }
      }

      storage::TableMeta updated_meta;
      *updated_meta.mutable_table() = new_schema;
      
      auto status = storage_->UpdateTableMetadata(name_, updated_meta);
      if (!status.ok()) {
        return status;
      }
      
      return new_schema;
    }

    Status DropRowRange(
        google::bigtable::admin::v2::DropRowRangeRequest const& request
    ) {
      if (!request.has_row_key_prefix() && !request.has_delete_all_data_from_table()) {
        return InvalidArgumentError(
            "Neither row prefix nor delete all data from table is set",
            GCP_ERROR_INFO().WithMetadata("DropRowRange request", request.DebugString()));
      }

      if (request.has_delete_all_data_from_table()) {
        // TODO: Implement efficient range delete using RocksDB DeleteRange
        return UnimplementedError(
            "delete_all_data_from_table not yet implemented for RocksDB storage",
            GCP_ERROR_INFO());
      }

      auto const& row_key_prefix = request.row_key_prefix();
      if (row_key_prefix.size() > kMaxRowLen) {
        return InvalidArgumentError(
            "The row_key_prefix is longer than 4KiB",
            GCP_ERROR_INFO().WithMetadata("row_key_prefix size",
                                          absl::StrFormat("%zu", row_key_prefix.size())));
      }
      
      if (row_key_prefix.empty()) {
        return InvalidArgumentError(
            "Row prefix provided is empty.",
            GCP_ERROR_INFO().WithMetadata("DropRowRange request", request.DebugString()));
      }

      // TODO: Implement prefix-based row deletion using RocksDB iterators
      return UnimplementedError(
          "row_key_prefix deletion not yet implemented for RocksDB storage",
          GCP_ERROR_INFO().WithMetadata("row_key_prefix", row_key_prefix));
    }
};


}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE2_H
