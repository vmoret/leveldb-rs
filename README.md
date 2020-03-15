# leveldb

LevelDB binding for [rustlang](https://www.rust-lang.org).

This is heavily based on the work of [skade/leveldb](https://github.com/skade/leveldb).

The main reasons for creating this library were:

1. Get a feel of the usage of the libc library.
2. Personally I prefer the raw binary keys, so removed the dependency on `db-key`.
3. Add _BloomFilterPolicyV2_ support.

## TODO

- Add generic _FilterPolicy_ support (currently commented out in `leveldb-sys`).
- Bugfixing...
