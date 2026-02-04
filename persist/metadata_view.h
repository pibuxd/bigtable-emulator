/**
 * @file storage.h
 * @brief Abstract storage interface and helpers for the Bigtable emulator.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_METADATA_VIEW_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_METADATA_VIEW_H
 
#include "persist/logging.h"
#include "persist/proto/storage.pb.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {


class CachedTablesMetadataView {
public:
    using meta_t = std::tuple<std::string, storage::TableMeta>;
    using container_t = std::vector<meta_t>;
private:
    container_t data_;
public:

    template<typename T>
    CachedTablesMetadataView(T abstract_metadata_view) {
        container_t tmp;
        std::transform(abstract_metadata_view.begin(), abstract_metadata_view.end(), std::back_inserter(tmp), [](auto el) -> auto { return el; });
        data_ = tmp;
    };
    
    container_t::const_iterator begin() const {
        return data_.cbegin();
    };
    container_t::const_iterator end() const {
        return data_.cend();
    };

    /** Returns vector of table names in this view. */
    inline std::vector<std::string> names() const {
        std::vector<std::string> tmp;
        DBG("TablesMetadataView:names listing tables");
        std::transform(begin(), end(), std::back_inserter(tmp), [](auto el) -> auto {
            DBG(absl::StrCat("TablesMetadataView:names => ", std::get<0>(el)));
            return std::get<0>(el);
        });
        DBG("TablesMetadataView:names end");
        return tmp;
    }
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_METADATA_VIEW_H
