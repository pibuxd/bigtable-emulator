# Single node, Memory Only Emulator for Google Bigtable

## 10 Dec 25' handout

This section describes current progress and some details what and how to run.

In `testing.sh` you have full debug command that uses `/tmp/rocksdb-for-bigtable-test4` as storage path (path is hardcoded in `persist/storage.h` in line 376 in `RocksDBStorage()` constructor).
For some reason unknown to me I can't properly close the rocksDB and it fails on second lunch (oops!).
So what I do I remove the directory each time (it's not veryu persistent, is it?).
This probably occurs due to not properly doing Storage::Close() and Storage::Open(), but we will work it out later.

For now you can just run `./testing.sh`
In `persist/example.h` you have some code that is executed at the start of the server. What is reason for that? We have long way until test will pass, so for now I was manually testing each operation and `example.h` contains 
some example code to add a table, remove it, add some rows, then stream them.
After you run `./testing.sh`, this testing code runs, then we run the actual server. You can either kill it or send some requests (see "Connecting to the Emulator" section)

### Implementation

The main implementation resides in `persist/storage.h`. There are some TODOs. `RocksDBStorageRowTX` is missing many transaction methods, but it should be clear eanough what needs to be implemented.
I roughly copied `table.cc` into `persist/table2.h` - header-only (for now) class Table2 that serves as new class for migrating existing funcationality.


In `persist/proto.h` you have some protobufs?
* `StorageRocksDBConfig` is for storing server configuration (e.g. path to storage directory)
* `TableMeta` is useless at this point as it wraps only a schema for table (maybe it will be useful in the future?)
* `RowData` is important stuff

### What was done and what needs to be done?

1. There's method `FindLegacyTable()` in `cluster.cc` that does what `FindTable()` did. It returns old class `Table`
2. All other methods in `cluster.cc` uses new storage and `Table2` classes
3. You need to find all occurences using `FindLegacyTable()` and migrate them to use new class (see `server.cc`)
4. All those migrations will require implementing missing methods in `persist/storage.h` in `RocksDBStorageRowTX`
5. There is a code to stream rows, get and update rows so all other operations shouldn't be that complicated to add.

Usually when I wanted to migrate new method I did this:
1. Go to `server.cc` and find use case of `FindLegacyTable()`.
2. Go through `table.cc` as it then calls `column_family.cc` and collect all the code into one giant method.
3. Then add this method to the `Table2` class and replace each instance of `_rows[something][xyz].push_back()` with correspondiong code calling `Storage` abstract class

### How things are stored? 

We have special column family called `"bte_metadata"` (hardcoded in line 377 in `persist/storage.h` in `RocksDBStorage()` constructor)
This column family has keys (table names) and values (values of type `TableMeta` that is just protobuf wrapping `TableSchema` protobuf).
That way we know what tables we have and what schemas they have.

The BT maps the data in the following way:
`val data: Map<(string, string, string, std::chrono::milliseconds), string>`
The keys are:
`data[column_family, row_key, column_qualifier, timestamp] = value`
Now in RocksDB we have column families, but we need to somehow map all the other keys.

Data is grouped in the following way:
1. Column family corresponds to RocksDB column family (RocksDB's column family is just a group of key-value pairs)
2. Row keys corresponds to row keys in RocksDB
3. For all the rest stuff we have protobuf that groups everything together (columns + timestamps)

So the data models looks like this:

![Storage diagram](https://github.com/pibuxd/bigtable-emulator/blob/styczynski/rewrite-cleanup/static/storage_diagram.png)

**VERY IMPORTANT NOTE:** Protobufs does not preserve ordering of keys according to spec. In BT we have strict ordering of increasing timestamps (see `std::map<std::chrono::milliseconds, std::string, std::greater<>> cells_;` in `column_family.h`)
This means that we need to do some suboptimal stuff in `storage.h`. To stream rows we need to construct iterator. As we group the data we load one row at the time see `RocksDBStorageRowTX::LoadRow()`. When it happens we 
need to map protobuf into C++ map to get correct ordering. I think this is avoidable somehow, but that kind of performance issue isn't our highest-priority concert right now.


## About

This is a single-node, non-persistent emulator for Google's Bigtable.

It should pass all the integration tests in Google's C++ client
repository (google-cloud-cpp), except those that must run against
production Bigtable.

## Building

Building the Bigtable emulator requires `bazel`.

```shell
cd bigtable-emulator
bazel build "..."
```
## Running the Unit Tests

```shell
bazel test "..."
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


## Using RocksDB

Example in `server.cc`


## Contributing changes

See [`CONTRIBUTING.md`](/CONTRIBUTING.md) for details on how to contribute to
this project, including how to build and test your changes as well as how to
properly format your code.

## Licensing

Apache 2.0; see [`LICENSE`](/LICENSE) for details.
