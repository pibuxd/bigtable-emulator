# Single node, Persistent Emulator for Google Bigtable

## 15 Jan 26' update

In `testing.sh` you have full debug command that uses `/tmp/rocksdb-for-bigtable-test4` as storage path (path is hardcoded in `persist/storage.h` in line ~392 in `RocksDBStorage()` constructor, you can change it if you want).

Now RocksDB closes and opens properly, so you can safely run the emulator multiple times and data will persist between restarts (it's persistent!).

You can just run `./testing.sh`
In `persist/example.h` you have some code that executes on server startup - this was for testing during implementation. Now you can ignore it or delete it.
After running `./testing.sh`, this testing code runs, then the server starts. You can kill it or send some requests (see "Connecting to the Emulator" section).

### Implementation

Main implementation is in `persist/storage.h`. The storage class is abstract as well as the row transaction model.
`PersistedTable` provides the table interface that can be supplied with any class implementing abstract `Storage` interface.

TODO: Below you have list of available implementations. Please look into those files and provide extra details. List should be changed into the table.
We offer two implementations:
* `persist/memory/storage.h` - thin-wrapper around pre-existing (non-persistent) in-memory class hierarchy implemented in `table.h`
* `persist/rocksdb/storage.h` - RocksDB implementation that uses TransactionDB to have ACID guarantees. All mutation operations go through transactions.

In `persist/proto.h` you have protobufs:
* `StorageRocksDBConfig` - for storing server configuration (e.g. path to storage directory)
* `TableMeta` - wrapper for table schema (might be useful in the future)
* `RowData` - important, here we serialize data

### MemoryStorage: How things are stored? 

Data is stored within `Table` and `ColumnFamily` hierarchy of classes inside local fields (std::map with std::string keys) that use mutexes to control access flow.

TODO: Describe hierary of composition of classes. For example in table.h in class Table there's field column_families_ mapping strings to ColumnFamily. In column_family.h you have class ColumnFamily mapping strings (rows_ field) into ColumnFamilyRow and so on. Provide this composition details here as table.

### RocksDBStorage: How things are stored? 

We have special column family called `"bte_metadata"` (configurable as many other parameters via `StorageRocksDBConfig`).
This column family has keys (table names) and values (values of type `TableMeta` that is just protobuf wrapping `TableSchema` protobuf).
That way we know what tables we have and what schemas they have.

BigTable maps the data in the following way:
`val data: Map<(string, string, string, std::chrono::milliseconds), string>`
The keys are:
`data[column_family, row_key, column_qualifier, timestamp] = value`

Now in RocksDB we have column families, but we need to somehow map all the other keys.

Data is grouped in the following way:
1. Column family corresponds to RocksDB column family (RocksDB's column family is just a group of key-value pairs)
2. Tuple of row key, column qualifier and table name correspond to row keys in RocksDB
3. For all the rest stuff we have protobuf that groups everything together (timestamps)

TODO: Please provide table describing this hierarchy mentioned above i.e how we store keys and value

This way we can read only single row each time and efficiently stream through them.

**IMPORTANT NOTE:** Protobufs does not preserve ordering of keys according to spec. In BT we have strict ordering of increasing timestamps (see `std::map<std::chrono::milliseconds, std::string, std::greater<>> cells_;` in `column_family.h`)
This means that we need to do some suboptimal stuff in `storage.h`. To stream rows we need to construct iterator. As we group the data we load one row at a time - see `RocksDBStorageRowTX::LoadRow()`. When it happens we 
need to map protobuf into C++ map to get correct ordering. I think this is avoidable somehow, but that kind of performance issue isn't our highest-priority concern right now.

### Testing

We provde different levels of abstraction for testing.
TODO: Change this list into the table provide more details based on metnioned test files
1. `persist/integration/read_test.cc` - integration test using CBT to perform operations
2. `persist/memory/storage_test.cc` - storage interface testing for specific implementation
3. `cluster_test.cc` - testing cluster interface operations for different storage implementations
4. `server_test.cc` - testing server RPC handler for different storage implementations

### What's left for the future (TODO)?

There are some things that can be added/improved:

1. **Garbage collection** - Automatic deletion of old cell versions based on `gc_rule`. See `TODO` in code.
2. **Atomic schema updates** - ModifyColumnFamilies doesn't save changes to storage
3. **Efficient bulk deletes** - DropRowRange could use RocksDB DeleteRange instead of iteration
4. **Better CF management** - Drop/Create column families sometimes requires DB restart now. Should be easy to implement.

## About

This is a single-node, persistent emulator for Google's Bigtable.

It should pass all the integration tests in Google's C++ client
repository (google-cloud-cpp), except those that must run against
production Bigtable.

## Dependencies

The Bigtable-emulator depends on `google-cloud-cpp` (which the build
tools retrieve and build automatically) and the `abseil`
library. Other dependencies such as `GRPC` are provided by
`google-cloud-cpp`.

## Building

Building the Bigtable emulator requires `bazel`.

```shell
bazel build //...
```

## Running the Unit Tests

```shell
bazel test //...
```

## Running clang-tidy with Bazel

```shell
bazel build --config clang-tidy //...
```

## Running the Emulator

```shell
bazel run :emulator -- --host=127.0.0.1 --port=8888
```

or just

```shell
bazel run :emulator
```

## Connecting to the Emulator

Install [CLI tool - cbt](https://docs.cloud.google.com/bigtable/docs/cbt-overview)

Create `~/.cbtrc` file. Example:
```
project = projekcik
instance = instancyjka
creds = whatever
```
Set env variable. Example:
```
export BIGTABLE_EMULATOR_HOST=localhost:8888
```

Now you can use `cbt` and your data will persist between emulator restarts:
```bash
cbt createtable my-table
cbt createfamily my-table cf1
cbt set my-table row1 cf1:col1=value1
cbt read my-table
# restart emulator
cbt read my-table  # data is still there
```

## Using RocksDB

Example in `server.cc` and `persist/storage.h`

## `compile_commands.json`

If you need to generate `compile_commands.json` for your tooling, run:
```shell
bazel run --config=compile-commands
```

## Development

It's a good idea to set home (`$HOME/.bazelrc` on Unixes) or system `bazelrc`
and enable compilation cache there with this line:
```
build --disk_cache=~/.cache/bazel/disk-cache
```
Note that the cache directory grows indefinitely.

### Formatting the code

```bash
# On bash you neet to enable globstar with `shopt -s globstar` first
clang-format -i -style=file -assume-filename=.clang-format **/*.cc **/*.h
```

### `compile_commands.json`

If you need to generate `compile_commands.json` for your tooling, run:
```shell
bazel run --config=compile-commands
```

## Contributing changes

See [`CONTRIBUTING.md`](/CONTRIBUTING.md) for details on how to contribute to
this project, including how to build and test your changes as well as how to
properly format your code.

## Licensing

Apache 2.0; see [`LICENSE`](/LICENSE) for details.
