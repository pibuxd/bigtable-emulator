#!/bin/bash

rm -rfd /tmp/rocksdb-for-bigtable-test4 && \
bazel run :emulator --compilation_mode  dbg --sandbox_debug  --verbose_failures -- --host=127.0.0.1 --port=8888
