#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSIST_LOGGING_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSIST_LOGGING_H

#include "fmtlog-inl.h"

#define XNDBG(TEXT)                \
  if (true) {                    \
    std::cout << (TEXT) << "\n"; \
    std::cout.flush();           \
  }

#define DBG(...)                \
  if (true) {                   \
    logi(__VA_ARGS__);          \
    fmtlog::poll();             \
  }

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSIST_LOGGING_H