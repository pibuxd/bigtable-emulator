#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSIST_LOGGING_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSIST_LOGGING_H

#include "google/cloud/status.h"
#include "fmtlog-inl.h"
#include <google/bigtable/v2/data.pb.h>
#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

#define DBG(...) logd(__VA_ARGS__)
#define LERROR(...) loge(__VA_ARGS__)
#define LWARN(...) logw(__VA_ARGS__)

static inline fmtlog::LogLevel ParseLogLevel(std::string level) {
  std::transform(level.begin(), level.end(), level.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  if (level == "dbg" || level == "debug") {
    return fmtlog::DBG;
  }
  if (level == "info" || level == "inf") {
    return fmtlog::INF;
  }
  if (level == "warn" || level == "warning" || level == "wrn") {
    return fmtlog::WRN;
  }
  if (level == "err" || level == "error") {
    return fmtlog::ERR;
  }
  return fmtlog::WRN;
}

static inline void ConfigureEmulatorLogging(std::string const& log_path,
                                            std::string const& log_level) {
  fmtlog::setLogFile(log_path.c_str(), false);
  fmtlog::setLogLevel(ParseLogLevel(log_level));
  fmtlog::flushOn(fmtlog::WRN);
  fmtlog::setFlushDelay(200000000);  // 200ms
  fmtlog::setThreadName("main");
  // Start background polling to flush logs to file periodically.
  fmtlog::startPollingThread(100000000);  // 100ms
}

static inline void ShutdownEmulatorLogging() {
  fmtlog::poll(true);
  fmtlog::stopPollingThread();
  fmtlog::closeLogFile();
}

// Custom fmt formatter for status
template <>
struct fmt::formatter<google::cloud::Status>
    : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(google::cloud::Status status, FormatContext& ctx) const {
    return formatter<std::string_view>::format(status.message(), ctx);
  }
};

// Custom fmt formatter for KindCase
template <>
struct fmt::formatter<google::bigtable::v2::Value::KindCase>
    : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(google::bigtable::v2::Value::KindCase kind_case,
              FormatContext& ctx) const {
    std::string_view name = "unknown";
    switch (kind_case) {
      case google::bigtable::v2::Value::KindCase::kRawValue:
        name = "raw_value";
        break;
      case google::bigtable::v2::Value::KindCase::kRawTimestampMicros:
        name = "raw_timestamp_micros";
        break;
      case google::bigtable::v2::Value::KindCase::kBytesValue:
        name = "bytes";
        break;
      case google::bigtable::v2::Value::KindCase::kStringValue:
        name = "string";
        break;
      case google::bigtable::v2::Value::KindCase::kIntValue:
        name = "int";
        break;
      case google::bigtable::v2::Value::KindCase::kBoolValue:
        name = "bool";
        break;
      case google::bigtable::v2::Value::KindCase::kFloatValue:
        name = "float";
        break;
      case google::bigtable::v2::Value::KindCase::kTimestampValue:
        name = "timestamp";
        break;
      case google::bigtable::v2::Value::KindCase::kDateValue:
        name = "date";
        break;
      case google::bigtable::v2::Value::KindCase::kArrayValue:
        name = "array";
        break;
      case google::bigtable::v2::Value::KindCase::KIND_NOT_SET:
        name = "not_set";
        break;
    }
    return formatter<std::string_view>::format(name, ctx);
  }
};

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSIST_LOGGING_H