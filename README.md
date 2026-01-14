# Single node, Persistent Emulator for Google Bigtable

## 15 Jan 26' update

In `testing.sh` you have full debug command that uses `/tmp/rocksdb-for-bigtable-test4` as storage path (path is hardcoded in `persist/storage.h` in line ~392 in `RocksDBStorage()` constructor, you can change it if you want).

Now RocksDB closes and opens properly, so you can safely run the emulator multiple times and data will persist between restarts (it's persistent!).

You can just run `./testing.sh`
In `persist/example.h` you have some code that executes on server startup - this was for testing during implementation. Now you can ignore it or delete it.
After running `./testing.sh`, this testing code runs, then the server starts. You can kill it or send some requests (see "Connecting to the Emulator" section).

### Implementation

Main implementation is in `persist/storage.h` and `persist/table2.h`. 
I copied `table.cc` into `persist/table2.h` - header-only (for now) class Table2 that serves as new class for all functionality.

RocksDB uses TransactionDB to have ACID guarantees. All mutation operations go through transactions.

In `persist/proto.h` you have protobufs:
* `StorageRocksDBConfig` - for storing server configuration (e.g. path to storage directory)
* `TableMeta` - wrapper for table schema (might be useful in the future)
* `RowData` - important stuff, here we serialize data

### How things are stored? 

We have special column family called `"bte_metadata"` (hardcoded in line ~394 in `persist/storage.h` in `RocksDBStorage()` constructor).
This column family has keys (table names) and values (values of type `TableMeta` that is just protobuf wrapping `TableSchema` protobuf).
That way we know what tables we have and what schemas they have.

BT maps the data in the following way:
`val data: Map<(string, string, string, std::chrono::milliseconds), string>`
The keys are:
`data[column_family, row_key, column_qualifier, timestamp] = value`

Now in RocksDB we have column families, but we need to somehow map all the other keys.

Data is grouped in the following way:
1. Column family corresponds to RocksDB column family (RocksDB's column family is just a group of key-value pairs)
2. Row keys correspond to row keys in RocksDB
3. For all the rest stuff we have protobuf that groups everything together (columns + timestamps)

So the data model looks like this:

![Storage diagram](https://github.com/pibuxd/bigtable-emulator/blob/styczynski/rewrite-cleanup/static/storage_diagram.png)

**IMPORTANT NOTE:** Protobufs does not preserve ordering of keys according to spec. In BT we have strict ordering of increasing timestamps (see `std::map<std::chrono::milliseconds, std::string, std::greater<>> cells_;` in `column_family.h`)
This means that we need to do some suboptimal stuff in `storage.h`. To stream rows we need to construct iterator. As we group the data we load one row at a time - see `RocksDBStorageRowTX::LoadRow()`. When it happens we 
need to map protobuf into C++ map to get correct ordering. I think this is avoidable somehow, but that kind of performance issue isn't our highest-priority concern right now.

### What's left for the future (TODO)?

There are some things that can be added/improved:

1. **Garbage collection** - Automatic deletion of old cell versions based on `gc_rule`. See `TODO` in code.
2. **Configurable DB path** - Now path is hardcoded, could be done via env variable
3. **Atomic schema updates** - ModifyColumnFamilies doesn't save changes to storage
4. **Efficient bulk deletes** - DropRowRange could use RocksDB DeleteRange instead of iteration
5. **Better CF management** - Drop/Create column families requires DB restart now

All TODOs are marked as `TODO:` in source code.


## About

This is a single-node, persistent emulator for Google's Bigtable.

It should pass all the integration tests in Google's C++ client
repository (google-cloud-cpp), except those that must run against
production Bigtable.

## Building

Building the Bigtable emulator requires `bazel`.

```shell
cd bigtable-emulator
bazel build //...
```
## Running the Unit Tests

```shell
bazel test //...
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


## Contributing changes

See [`CONTRIBUTING.md`](/CONTRIBUTING.md) for details on how to contribute to
this project, including how to build and test your changes as well as how to
properly format your code.

## Licensing

Apache 2.0; see [`LICENSE`](/LICENSE) for details.
