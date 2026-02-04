/**
 * @file column_family_stream.h
 * @brief RocksDB-backed column family stream for iterating cells.
 */
#include "filter.h"
#include "re2/re2.h"
#include <memory>
#include <string>
#include <vector>

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_FILTERED_TABLE_STREAM_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_FILTERED_TABLE_STREAM_H

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

/**
 * Merge cell stream over multiple column families using
 * AbstractFamilyColumnStreamImpl. Supports FamilyNameRegex and ColumnRange
 * filters by pruning streams.
 */
class StorageFitleredTableStream : public MergeCellStreams {
 public:
  /** Constructs from a vector of column family stream implementations. */
  explicit StorageFitleredTableStream(
      std::vector<std::unique_ptr<AbstractFamilyColumnStreamImpl>> cf_streams)
      : MergeCellStreams(CreateCellStreams(std::move(cf_streams))) {}

  /** Applies filter; prunes streams that don't match FamilyNameRegex or
   * ColumnRange. */
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
  /** Builds CellStream vector from column family stream implementations. */
  static std::vector<CellStream> CreateCellStreams(
      std::vector<std::unique_ptr<AbstractFamilyColumnStreamImpl>> cf_streams) {
    std::vector<CellStream> res;
    res.reserve(cf_streams.size());
    for (auto& stream : cf_streams) {
      res.emplace_back(std::move(stream));
    }
    return res;
  }
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_FILTERED_TABLE_STREAM_H