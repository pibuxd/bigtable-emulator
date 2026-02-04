/**
 * @file column_family_stream.h
 * @brief RocksDB-backed column family stream for iterating cells.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_COLUMN_FAMILY_STREAM_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_COLUMN_FAMILY_STREAM_H

#include "absl/types/optional.h"
#include "cell_view.h"
#include "filter.h"
#include "filtered_map.h"
#include "re2/re2.h"
#include "rocksdb/iterator.h"
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class RocksDBStorage;

/**
 * RocksDB-backed column family stream; iterates over cells in a column family
 * for a row set.
 */
class RocksDBColumnFamilyStream : public AbstractFamilyColumnStreamImpl {
  friend class RocksDBStorage;

 public:
  /**
   * Constructs a column family stream.
   * @param column_family_name Name of the column family; used in returned
   * CellViews.
   * @param handle RocksDB column family handle (must remain valid for this
   * object's lifetime).
   * @param row_set Row keys to include in the stream.
   * @param db Pointer to the RocksDB storage.
   * @param table_name Table name.
   * @param prefetch_all_columns If true, prefetch all columns for each row.
   */
  RocksDBColumnFamilyStream(std::string const& column_family_name,
                            rocksdb::ColumnFamilyHandle* handle,
                            std::shared_ptr<StringRangeSet const> row_set,
                            RocksDBStorage* db, std::string const table_name,
                            bool const prefetch_all_columns);

  virtual ~RocksDBColumnFamilyStream();

  bool ApplyFilter(InternalFilter const& internal_filter) override {
    return true;
  }
  bool HasValue() const override;
  CellView const& Value() const override;
  bool Next(NextMode mode) override;
  std::string const& column_family_name() const override {
    return column_family_name_;
  }

 private:
  using TColumnRow = std::map<std::chrono::milliseconds, std::string>;
  using TColumnFamilyRow = std::map<std::string, TColumnRow>;

  std::string column_family_name_;
  std::string table_name_;
  bool const prefetch_all_columns_;
  rocksdb::ColumnFamilyHandle* handle_;
  std::shared_ptr<StringRangeSet const> row_ranges_;
  std::vector<std::shared_ptr<re2::RE2 const>> row_regexes_;
  mutable StringRangeSet column_ranges_;
  std::vector<std::shared_ptr<re2::RE2 const>> column_regexes_;
  mutable TimestampRangeSet timestamp_ranges_;
  RocksDBStorage* db_;
  mutable rocksdb::Iterator* row_iter_;
  mutable std::string row_key_;
  mutable absl::optional<TColumnFamilyRow> row_data_;

  void NextRow() const;
  void InitializeIfNeeded() const;
  bool PointToFirstCellAfterColumnChange() const;
  bool PointToFirstCellAfterRowChange() const;

  mutable absl::optional<
      RegexFiteredMapView<StringRangeFilteredMapView<TColumnFamilyRow>>>
      columns_;
  mutable absl::optional<TimestampRangeFilteredMapView<TColumnRow>> cells_;
  mutable absl::optional<RegexFiteredMapView<
      StringRangeFilteredMapView<TColumnFamilyRow>>::const_iterator>
      column_it_;
  mutable absl::optional<
      TimestampRangeFilteredMapView<TColumnRow>::const_iterator>
      cell_it_;
  mutable absl::optional<CellView> cur_value_;
  mutable bool initialized_{false};
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_COLUMN_FAMILY_STREAM_H
