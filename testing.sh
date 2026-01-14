#!/bin/bash

rm -rfd /tmp/rocksdb-for-bigtable-test4 && \
/opt/homebrew/Cellar/bazel/8.5.0/libexec/bin/bazel-7.6.1-darwin-arm64 run :emulator --compilation_mode dbg --sandbox_debug --verbose_failures --noenable_bzlmod -- --host=127.0.0.1 --port=8888
