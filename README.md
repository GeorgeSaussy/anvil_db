# README

AnvilDB is an embedded key-value store that is optimized for fast writes and sequential scans.
It's design is based on [LevelDB](https://github.com/google/leveldb) and
[RocksDB](https://github.com/facebook/rocksdb). Like those two database's,
AnvilDB uses an LSM-Tree with a write-ahead log and a stack of SSTable files
comprising the underlying storage layer. However, whereas RocksDB is forked from
LevelDB, AnvilDB is a implemented from scratch in Rust.

## Contributing

This library uses the MIT license, a permissive license.
See the [`LICENSE.txt`](./LICENCE.txt) file in this directory for more details.

If you read the code, most of the vocabulary used is taken from the
[BigTable paper](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf),
with the main exception being that the files that the database uses are called
"SSTables" in the BigTable paper, but they are called "SSTs" throughout this code base.

Feel free to raise GitHub issues on this repo, but please reach out to me
before you put too much work into opening a PR.
