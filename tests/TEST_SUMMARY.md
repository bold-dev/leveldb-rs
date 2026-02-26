# Integration Test Summary

## How to Run

### Prerequisites

- Rust toolchain (stable)
- ~100 MB disk space for generated test databases

### 1. Clone the repo

```bash
git clone https://github.com/bold-dev/leveldb-rs.git
cd leveldb-rs
```

### 2. Generate test databases

The test databases must be generated before running the integration tests. The generator is a standalone crate inside the repo (`testdb-generator/`) that uses the **original** rusty-leveldb v3 from crates.io (with write support) to create valid databases, then applies targeted corruptions.

```bash
cd testdb-generator
cargo run
cd ..
```

This creates 15 databases in `testdata/`:
- 5 valid databases (basic, large keys, large values, deleted keys, multilevel)
- 6 randomly corrupted databases (flipped bytes, truncated, missing files)
- 4 targeted size-field corruptions with correct CRCs (inflated block handle, WAL, manifest, snappy)

Note: the generator is excluded from the workspace (it depends on the original crate, not our fork) and has its own `Cargo.lock`.

### 3. Run integration tests

```bash
cargo test --test testdata_integration -- --test-threads=1
```

---

## Common Checks (applied to every test)

Every integration test performs the following checks regardless of what it specifically tests:

| Check | Description |
|-------|-------------|
| **Exclusive file lock** | Before opening the DB, the test acquires an exclusive `flock` on the LOCK file, simulating another process (e.g. a LevelDB writer) holding the database lock. SafeDB must operate correctly without attempting to acquire or interfere with this lock. |
| **No file modifications** | Before and after the test, a snapshot is taken of every file in the database directory (path, size, modification time). The test asserts that no files were created, deleted, resized, or modified. |
| **SafeDB API** | All tests use `SafeDB` (the production async API), not `DB::open_read_only`. This means every open and get is wrapped in `catch_unwind`, converting panics into errors. |
| **Graceful error handling** | For corrupt/invalid databases, the test verifies the operation either returns an error or succeeds — but never crashes the process. |

## Test Results

### Valid Databases

| Test | What it tests | Original v3 | Our Fork |
|------|--------------|-------------|----------|
| `test_basic_db_read_all_keys` | Open a 20-key DB and read every key, verify each value matches | Pass | Pass |
| `test_basic_db_missing_key_returns_none` | Query a key that doesn't exist, verify `None` is returned | Pass | Pass |
| `test_large_keys_db_read_all` | Spot-check ~100 keys across a 100,000-key DB | Pass | Pass |
| `test_large_keys_db_first_and_last` | Read the first and last key in a 100,000-key DB | Pass | Pass |
| `test_large_values_db_read` | Read 10 keys with 1MB values each, verify length | Pass | Pass |
| `test_deleted_keys_db` | Verify deleted (even) keys return `None`, surviving (odd) keys return correct values across 1,000 keys | Pass | Pass |
| `test_multilevel_db_read` | Read keys across 10 compaction batches (50,000 total keys, multiple SSTable levels) | Pass | Pass |

### Random Corruption

| Test | What it tests | Original v3 | Our Fork |
|------|--------------|-------------|----------|
| `test_corrupt_sst_data_does_not_panic` | SSTable with 10 flipped bytes in the middle — reads should error or return wrong data, not crash | Pass | Pass |
| `test_corrupt_manifest_does_not_panic` | MANIFEST with 10 flipped bytes — open should fail or reads should error, not crash | Pass | Pass |
| `test_corrupt_wal_does_not_panic` | WAL log with 10 flipped bytes — open should fail or reads should error, not crash | Pass | Pass |
| `test_truncated_sst_does_not_panic` | SSTable truncated to half its size — footer magic check fails | **PANIC** (`assert_eq!` in `Footer::decode`) | Pass (fixed: returns `None`) |
| `test_missing_sst_does_not_panic` | SSTable file deleted but still referenced by MANIFEST — reads should error | Pass | Pass |
| `test_missing_current_fails_to_open` | CURRENT file deleted — DB cannot open | Pass | Pass |

### Targeted Size-Field Corruption (MAX_ALLOC_SIZE Guards)

These corruptions are crafted to **bypass CRC checksums** so they reach the allocation code paths. Each one modifies a specific size field and recomputes the CRC (or targets a structure with no CRC).

| Test | What it tests | Corrupted field | Claimed size | Original v3 | Our Fork |
|------|--------------|----------------|-------------|-------------|----------|
| `test_inflated_block_handle_does_not_allocate` | SSTable footer's index block handle modified to claim a massive block size (no CRC on footer) | Footer index BlockHandle size varint | 500 GB | **SIGABRT** (`memory allocation of 536870912000 bytes failed`) | Pass (guard in `read_table_block`) |
| `test_inflated_manifest_key_len_does_not_allocate` | MANIFEST version edit's length-prefixed key varint inflated, log record CRC recomputed | VersionEdit key length varint | 500 GB | **SIGABRT** (`memory allocation of 536870912000 bytes failed`) | Pass (guard in `read_length_prefixed`) |
| `test_inflated_wal_length_does_not_allocate` | Crafted WAL with 130 valid fragments (correct CRCs) that accumulate past 4MB | Accumulated multi-fragment record size | 4.2 MB | Pass (4MB is harmless) | Pass (guard at 4MB limit) |
| `test_inflated_snappy_len_does_not_allocate` | Snappy decompressed-length header overwritten in-place inside a compressed SSTable block, block CRC recomputed | Snappy decompressed length varint | 500 GB | **PANIC** (slice OOB in `block.rs`) | Pass (`catch_unwind` catches panic) |

### Summary

|  | Original v3 | Our Fork |
|--|-------------|----------|
| **Total tests** | 17 | 17 |
| **Pass** | 13 | 17 |
| **SIGABRT (OOM)** | 2 | 0 |
| **PANIC** | 2 | 0 |
