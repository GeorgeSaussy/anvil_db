# Design Notes

## WAL Format

The byte format of the WAL is specified using a C-like syntax. See RFC 8446,
which documents TLS 1.3, for use of a similar syntax. If there are
any ambiguities, defer to the AnvilDB source code.

The WAL is a simple log-structured file. It has a header and then a sequence of
entry blocks. The header consists four bytes, the two byte magic number `0x4753`
and then a two byte version number. The version number is currently `0x0000`.
While the query API may not always be backwards compatible, AnvilDB will
always be able to parse older WAL files.

```c
struct  {
    uint16_t magic = 0x4753;
    uint16_t version = 0x0000;
    EntryBlock entry_blocks[];
} WAL;
```

Each entry block has the following format:

```c
struct {
    VarInt64 entry_count;
    Entry entries[EntryBlock.entry_count];
    uint32_t checksum;
} EntryBlock;
```

Both `entry_count` and `checksum` are little-endian.
The checksum is a simple CRC32 checksum of all bytes previous bytes in the
file. So if the checksum is at byte `i`, then the checksum is calculated over
the bytes `0..i-1`, including the header. The initial checksum is `1337`.

The `opaque` type is used to `Entry` format. An `opaque` is a length prefixed
byte array. The length is encoded as a `VarInt64`. Likewise `opaque_utf8` is
a length prefixed ASCII string, not including the null terminator.

```c
struct {
    VarInt64 length;
    uint8_t data[opaque.length];
} opaque;

struct {
    VarInt64 length;
    uint8_t data[opaque_utf8.length];
} opaque_utf8;
```

Each entry has the following format:

```c
enum {
    // set a key value pair
    set(0),
    // remove a key value pair, specified by the key
    remove(1),
    manifest_snapshot(5),
    manifest_update(7),
} EntryTag;

struct  {
    // omitted
} ManifestSnapshot;

struct {
    // omitted
} ManifestUpdate;


struct {
    EntryTag type;
    uint32_t value_size;
    select (Entry.type) {
        case set:
            opaque key;
            opaque value;
        case remove:
            opaque key;
        case manifest_snapshot:
            ManifestSnapshot;
        case manifest_update:
            ManifestUpdate;
    }
} Entry;
```

Garbage collection is not tracked in the WAL. Instead, if WAL has been minor
compacted, or an SST has been merge or major compacted, then it should be
deleted once the database has been restarted and the WAL has been loaded.
If an existing WAL file has not been minor compacted, it cannot be deleted.
If an existing SST file has not been major compacted, it cannot be deleted.
If there are present WAL or SST files that are not tracked in the WAL,
the database should be considered corrupt and should not be opened. AnvilDB
will alert the user who can then decide how to handle the error.

To recover the memtable from the WAL, for each entry block in order, if the
checksum of the entry block is correct, play forward the entries in the block.
If the checksum is incorrect, ignore the block and all subsequent blocks.

## SST Format

Each SST is broken into blocks. There are two types of blocks:

1. Data blocks, which contain the actual key-value pairs.
2. Index blocks, which contain the offsets of the data blocks.

All blocks in an SST are data blocks except for the last block, which is an
index block.
