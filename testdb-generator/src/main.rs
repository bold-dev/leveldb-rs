use rand::Rng;
use rusty_leveldb::{Options, DB};
use std::fs;
use std::path::{Path, PathBuf};

const OUTPUT_DIR: &str = "../testdata";

fn main() {
    let output = PathBuf::from(OUTPUT_DIR);
    if output.exists() {
        fs::remove_dir_all(&output).expect("failed to clean output dir");
    }
    fs::create_dir_all(&output).expect("failed to create output dir");

    println!("Generating test databases in {}", output.display());

    gen_basic_db(&output);
    gen_large_keys_db(&output);
    gen_large_values_db(&output);
    gen_deleted_keys_db(&output);
    gen_multilevel_db(&output);

    // Corrupt variants are based on copies of basic_db
    gen_corrupt_sst_data(&output);
    gen_corrupt_manifest(&output);
    gen_corrupt_wal(&output);
    gen_truncated_sst(&output);
    gen_missing_sst(&output);
    gen_missing_current(&output);

    // Targeted size-field corruptions to test MAX_ALLOC_SIZE guards
    gen_inflated_block_handle(&output);
    gen_inflated_wal_length(&output);
    gen_inflated_manifest_key_len(&output);
    gen_inflated_snappy_len(&output);

    println!("Done! All test databases generated.");
}

/// Open a new DB at the given path with default options.
fn open_db(path: &Path) -> DB {
    let mut opt = Options::default();
    opt.create_if_missing = true;
    DB::open(path, opt).expect("failed to open db")
}

// ---------------------------------------------------------------------------
// Valid databases
// ---------------------------------------------------------------------------

fn gen_basic_db(output: &Path) {
    let path = output.join("basic_db");
    println!("  basic_db ...");
    let mut db = open_db(&path);
    for i in 0..20 {
        let key = format!("key_{:04}", i);
        let val = format!("value_{:04}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    // Force data into SST files
    db.compact_range(b"key_0000", b"key_9999").unwrap();
    drop(db);
    // Remove LOCK file so our read-only crate can open it
    let _ = fs::remove_file(path.join("LOCK"));
}

fn gen_large_keys_db(output: &Path) {
    let path = output.join("large_keys_db");
    println!("  large_keys_db (100k keys) ...");
    let mut db = open_db(&path);
    for i in 0..100_000 {
        let key = format!("key_{:08}", i);
        let val = format!("val_{:08}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
        // Flush periodically to create multiple SSTable files
        if i % 10_000 == 9_999 {
            db.flush().unwrap();
        }
    }
    db.flush().unwrap();
    drop(db);
    let _ = fs::remove_file(path.join("LOCK"));
}

fn gen_large_values_db(output: &Path) {
    let path = output.join("large_values_db");
    println!("  large_values_db (100 x 1MB values) ...");
    let mut db = open_db(&path);
    let mut rng = rand::thread_rng();
    for i in 0..100 {
        let key = format!("bigkey_{:04}", i);
        let val: Vec<u8> = (0..1_000_000).map(|_| rng.gen::<u8>()).collect();
        db.put(key.as_bytes(), &val).unwrap();
        if i % 10 == 9 {
            db.flush().unwrap();
        }
    }
    db.flush().unwrap();
    drop(db);
    let _ = fs::remove_file(path.join("LOCK"));
}

fn gen_deleted_keys_db(output: &Path) {
    let path = output.join("deleted_keys_db");
    println!("  deleted_keys_db ...");
    let mut db = open_db(&path);
    // Write 1000 keys
    for i in 0..1000 {
        let key = format!("delkey_{:04}", i);
        let val = format!("delval_{:04}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    // Delete even-numbered keys
    for i in (0..1000).step_by(2) {
        let key = format!("delkey_{:04}", i);
        db.delete(key.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    drop(db);
    let _ = fs::remove_file(path.join("LOCK"));
}

fn gen_multilevel_db(output: &Path) {
    let path = output.join("multilevel_db");
    println!("  multilevel_db ...");
    let mut db = open_db(&path);
    // Write in batches to encourage multiple levels
    for batch in 0..10 {
        for i in 0..5000 {
            let key = format!("ml_{:02}_{:06}", batch, i);
            let val = format!("mlval_{:02}_{:06}", batch, i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.flush().unwrap();
        // Trigger compaction by writing overlapping ranges
        db.compact_range(
            format!("ml_{:02}_", batch).as_bytes(),
            format!("ml_{:02}_~", batch).as_bytes(),
        )
        .unwrap();
    }
    drop(db);
    let _ = fs::remove_file(path.join("LOCK"));
}

// ---------------------------------------------------------------------------
// Corrupt database variants (based on copies of basic_db)
// ---------------------------------------------------------------------------

/// Copy a directory recursively.
fn copy_dir(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir(&src_path, &dst_path);
        } else {
            fs::copy(&src_path, &dst_path).unwrap();
        }
    }
}

/// Find the first file matching a glob-like extension in a directory.
fn find_file_with_ext(dir: &Path, ext: &str) -> Option<PathBuf> {
    fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.extension()
                .map(|e| e == ext)
                .unwrap_or(false)
        })
}

/// Find a file whose name starts with the given prefix.
fn find_file_with_prefix(dir: &Path, prefix: &str) -> Option<PathBuf> {
    fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with(prefix))
                .unwrap_or(false)
        })
}

/// Flip some bytes in the middle of a file.
fn corrupt_file(path: &Path) {
    let mut data = fs::read(path).expect("failed to read file for corruption");
    if data.len() > 20 {
        let mid = data.len() / 2;
        for i in 0..10 {
            data[mid + i] ^= 0xFF;
        }
    }
    fs::write(path, &data).expect("failed to write corrupted file");
}

fn make_corrupt_copy(output: &Path, name: &str) -> PathBuf {
    let src = output.join("basic_db");
    let dst = output.join(name);
    copy_dir(&src, &dst);
    dst
}

fn gen_corrupt_sst_data(output: &Path) {
    println!("  corrupt_sst_data ...");
    let dst = make_corrupt_copy(output, "corrupt_sst_data");
    // Try .ldb first, then .sst
    if let Some(sst) = find_file_with_ext(&dst, "ldb").or_else(|| find_file_with_ext(&dst, "sst"))
    {
        corrupt_file(&sst);
    } else {
        eprintln!("    WARNING: no .ldb/.sst file found to corrupt");
    }
}

fn gen_corrupt_manifest(output: &Path) {
    println!("  corrupt_manifest ...");
    let dst = make_corrupt_copy(output, "corrupt_manifest");
    if let Some(manifest) = find_file_with_prefix(&dst, "MANIFEST") {
        corrupt_file(&manifest);
    } else {
        eprintln!("    WARNING: no MANIFEST file found to corrupt");
    }
}

fn gen_corrupt_wal(output: &Path) {
    println!("  corrupt_wal ...");
    let dst = make_corrupt_copy(output, "corrupt_wal");
    if let Some(log) = find_file_with_ext(&dst, "log") {
        corrupt_file(&log);
    } else {
        eprintln!("    WARNING: no .log file found to corrupt");
    }
}

fn gen_truncated_sst(output: &Path) {
    println!("  truncated_sst ...");
    let dst = make_corrupt_copy(output, "truncated_sst");
    if let Some(sst) = find_file_with_ext(&dst, "ldb").or_else(|| find_file_with_ext(&dst, "sst"))
    {
        let data = fs::read(&sst).unwrap();
        fs::write(&sst, &data[..data.len() / 2]).unwrap();
    } else {
        eprintln!("    WARNING: no .ldb/.sst file found to truncate");
    }
}

fn gen_missing_sst(output: &Path) {
    println!("  missing_sst ...");
    let dst = make_corrupt_copy(output, "missing_sst");
    if let Some(sst) = find_file_with_ext(&dst, "ldb").or_else(|| find_file_with_ext(&dst, "sst"))
    {
        fs::remove_file(&sst).unwrap();
    } else {
        eprintln!("    WARNING: no .ldb/.sst file found to delete");
    }
}

fn gen_missing_current(output: &Path) {
    println!("  missing_current ...");
    let dst = make_corrupt_copy(output, "missing_current");
    let current = dst.join("CURRENT");
    if current.exists() {
        fs::remove_file(&current).unwrap();
    } else {
        eprintln!("    WARNING: no CURRENT file found to delete");
    }
}

// ---------------------------------------------------------------------------
// Targeted size-field corruptions (test MAX_ALLOC_SIZE guards)
//
// These corruptions are designed to bypass CRC checks so they actually reach
// the allocation code paths. Each one recomputes the CRC after modifying the
// size field.
// ---------------------------------------------------------------------------

const MAGIC_FOOTER: [u8; 8] = [87, 251, 128, 139, 36, 117, 71, 219];
const CRC32C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

fn crc32c_digest(chunks: &[&[u8]]) -> u32 {
    let mut digest = CRC32C.digest();
    for chunk in chunks {
        digest.update(chunk);
    }
    digest.finalize()
}

fn mask_crc(c: u32) -> u32 {
    (c.wrapping_shr(15) | c.wrapping_shl(17)).wrapping_add(0xa282ead8)
}

/// Encode a u64 as a varint (LEB128).
fn encode_varint(mut val: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    loop {
        let mut byte = (val & 0x7F) as u8;
        val >>= 7;
        if val != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if val == 0 {
            break;
        }
    }
    buf
}

/// Find the end of a varint starting at `pos` in `data`.
fn varint_end(data: &[u8], pos: usize) -> usize {
    let mut i = pos;
    while i < data.len() {
        if data[i] & 0x80 == 0 {
            return i + 1;
        }
        i += 1;
    }
    i
}

/// Decode a varint starting at `pos` in `data`. Returns (value, end_position).
fn decode_varint(data: &[u8], pos: usize) -> (u64, usize) {
    let mut val: u64 = 0;
    let mut shift = 0;
    let mut i = pos;
    while i < data.len() {
        val |= ((data[i] & 0x7F) as u64) << shift;
        shift += 7;
        if data[i] & 0x80 == 0 {
            return (val, i + 1);
        }
        i += 1;
    }
    (val, i)
}

/// Inflated SSTable block handle via footer modification.
///
/// The footer has NO CRC — just a magic number. We modify the index block's
/// size in the footer to claim it's 500GB. When the table reader tries to load
/// the index block, it calls read_table_block which allocates based on this size.
fn gen_inflated_block_handle(output: &Path) {
    println!("  inflated_block_handle ...");
    let dst = make_corrupt_copy(output, "inflated_block_handle");
    let sst = match find_file_with_ext(&dst, "ldb").or_else(|| find_file_with_ext(&dst, "sst")) {
        Some(p) => p,
        None => {
            eprintln!("    WARNING: no .ldb/.sst file found");
            return;
        }
    };

    let mut data = fs::read(&sst).unwrap();
    let len = data.len();
    if len < 48 {
        eprintln!("    WARNING: SST file too small for footer");
        return;
    }

    // Footer layout (last 48 bytes):
    //   [metaindex_handle (2 varints), index_handle (2 varints), padding to 40B, magic (8B)]
    // The footer has NO CRC — only the magic number for validation.
    let footer_start = len - 48;

    // Parse metaindex handle to preserve it
    let (_, mi_off_end) = decode_varint(&data, footer_start);
    let (_, mi_size_end) = decode_varint(&data, mi_off_end);

    // Parse index handle — keep offset, inflate size
    let (idx_offset, idx_off_end) = decode_varint(&data, mi_size_end);

    // Build new footer: metaindex handle (unchanged) + index handle (inflated) + padding + magic
    let mut new_footer = [0u8; 48];
    let prefix_len = idx_off_end - footer_start;
    new_footer[..prefix_len].copy_from_slice(&data[footer_start..idx_off_end]);

    // Encode the original index offset + huge size
    let idx_offset_bytes = encode_varint(idx_offset);
    let huge_size = encode_varint(500 * 1024 * 1024 * 1024_u64);
    let handle_start = mi_size_end - footer_start;
    let handle_end = handle_start + idx_offset_bytes.len() + huge_size.len();
    new_footer[handle_start..handle_start + idx_offset_bytes.len()]
        .copy_from_slice(&idx_offset_bytes);
    new_footer[handle_start + idx_offset_bytes.len()..handle_end].copy_from_slice(&huge_size);
    // Bytes handle_end..40 stay zero (padding)
    // Write magic at bytes 40-47
    new_footer[40..48].copy_from_slice(&MAGIC_FOOTER);

    data[footer_start..].copy_from_slice(&new_footer);
    fs::write(&sst, &data).unwrap();
    println!("    Modified footer: index block size set to 500GB (no CRC to fix)");
}

/// Inflated WAL: create a crafted log file with many fragments that accumulate
/// past MAX_ALLOC_SIZE (4MB). Each fragment has a correct CRC.
///
/// WAL record header: [checksum(4B LE), length(2B LE), type(1B)]
/// CRC = mask_crc(crc32c([type, data...]))
fn gen_inflated_wal_length(output: &Path) {
    println!("  inflated_wal_length ...");

    // Copy basic_db to get valid CURRENT + MANIFEST + SST files
    let path = output.join("inflated_wal_length");
    copy_dir(&output.join("basic_db"), &path);

    // Find the existing log file to replace it
    let log = match find_file_with_ext(&path, "log") {
        Some(p) => p,
        None => {
            eprintln!("    WARNING: no .log file found");
            return;
        }
    };

    // Create a crafted log file with fragments that accumulate > 4MB.
    // Each fragment fills a 32768-byte block: 7-byte header + 32761 bytes data.
    // Need ~130 fragments to exceed 4MB (130 * 32761 = 4,258,930).
    let block_size: usize = 32768;
    let header_size: usize = 7;
    let data_per_fragment = block_size - header_size; // 32761
    let num_fragments: usize = 130;

    let mut file_data = Vec::with_capacity(num_fragments * block_size);
    let fragment_data = vec![0x41u8; data_per_fragment]; // 'A' repeated

    for i in 0..num_fragments {
        let record_type: u8 = if i == 0 { 2 } else { 3 }; // First=2, Middle=3

        // CRC = mask_crc(crc32c([type, data...]))
        let crc = crc32c_digest(&[&[record_type], &fragment_data]);
        let masked = mask_crc(crc);

        // Write header: [checksum(4), length(2), type(1)]
        file_data.extend_from_slice(&masked.to_le_bytes());
        file_data.extend_from_slice(&(data_per_fragment as u16).to_le_bytes());
        file_data.push(record_type);
        // Write data
        file_data.extend_from_slice(&fragment_data);
    }

    fs::write(&log, &file_data).unwrap();
    let total_accumulated = num_fragments * data_per_fragment;
    println!(
        "    Created crafted WAL with {} fragments, accumulating {} bytes ({}MB)",
        num_fragments,
        total_accumulated,
        total_accumulated / (1024 * 1024)
    );
}

/// Inflated MANIFEST key length: modify the version edit payload to claim a
/// 1GB key length, then recompute the log record CRC so it passes validation.
///
/// Log record: [checksum(4B), length(2B), type(1B), payload(length bytes)]
/// CRC = mask_crc(crc32c([type, payload...]))
fn gen_inflated_manifest_key_len(output: &Path) {
    println!("  inflated_manifest_key_len ...");
    let dst = make_corrupt_copy(output, "inflated_manifest_key_len");
    let manifest = match find_file_with_prefix(&dst, "MANIFEST") {
        Some(p) => p,
        None => {
            eprintln!("    WARNING: no MANIFEST file found");
            return;
        }
    };

    let data = fs::read(&manifest).unwrap();
    if data.len() < 10 {
        eprintln!("    WARNING: MANIFEST too small");
        return;
    }

    // Parse log record header
    let record_length = u16::from_le_bytes([data[4], data[5]]) as usize;
    let record_type = data[6];
    let payload_start = 7;
    let payload_end = payload_start + record_length;

    // Parse the VersionEdit payload: first field is a tag varint,
    // followed by a value/length varint that we'll inflate.
    let tag_end = varint_end(&data, payload_start);
    let len_start = tag_end;
    let len_end = varint_end(&data, len_start);

    // Build new payload with inflated length (1GB)
    let huge_len = encode_varint(500 * 1024 * 1024 * 1024_u64);
    let mut new_payload = Vec::new();
    new_payload.extend_from_slice(&data[payload_start..len_start]);
    new_payload.extend_from_slice(&huge_len);
    new_payload.extend_from_slice(&data[len_end..payload_end]);

    // Recompute CRC over [type, new_payload]
    let crc = crc32c_digest(&[&[record_type], &new_payload]);
    let masked = mask_crc(crc);

    // Build new file
    let mut new_data = Vec::new();
    // CRC (4 bytes LE)
    new_data.extend_from_slice(&masked.to_le_bytes());
    // Length (2 bytes LE)
    new_data.extend_from_slice(&(new_payload.len() as u16).to_le_bytes());
    // Type (1 byte)
    new_data.push(record_type);
    // Payload
    new_data.extend_from_slice(&new_payload);
    // Rest of file (other log records, if any)
    new_data.extend_from_slice(&data[payload_end..]);

    fs::write(&manifest, &new_data).unwrap();
    println!(
        "    Inflated MANIFEST key length to 500GB with correct CRC (masked: {:#010x})",
        masked
    );
}

/// Inflated Snappy decompressed length: modify the snappy header in a
/// compressed SST block IN-PLACE (same block size), then recompute the
/// block's CRC so it passes validation.
///
/// Block on disk: [compressed_data(N bytes), compress_type(1B), checksum(4B)]
/// CRC = mask_crc(crc32c([compressed_data, compress_type]))
fn gen_inflated_snappy_len(output: &Path) {
    println!("  inflated_snappy_len ...");
    let path = output.join("inflated_snappy_len");

    // Create a fresh DB with snappy compression
    let mut opt = Options::default();
    opt.create_if_missing = true;
    let mut db = DB::open(&path, opt).expect("failed to open db");
    for i in 0..50 {
        let key = format!("snappy_key_{:04}", i);
        let val = "abcdefghij".repeat(100);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    db.compact_range(b"snappy_key_0000", b"snappy_key_9999").unwrap();
    drop(db);
    let _ = fs::remove_file(path.join("LOCK"));

    let sst = match find_file_with_ext(&path, "ldb").or_else(|| find_file_with_ext(&path, "sst")) {
        Some(p) => p,
        None => {
            eprintln!("    WARNING: no .ldb/.sst file found");
            return;
        }
    };

    let mut data = fs::read(&sst).unwrap();
    let len = data.len();
    if len < 48 {
        eprintln!("    WARNING: SST too small");
        return;
    }

    // Parse footer to find the index block, then the first data block
    let footer_start = len - 48;
    let (_, mi_off_end) = decode_varint(&data, footer_start);
    let (_, mi_size_end) = decode_varint(&data, mi_off_end);
    let (idx_offset, idx_off_end) = decode_varint(&data, mi_size_end);
    let (_, _idx_size_end) = decode_varint(&data, idx_off_end);

    let ib_start = idx_offset as usize;

    // Parse first index block entry to get first data block's BlockHandle
    let (_, shared_end) = decode_varint(&data, ib_start);
    let (unshared_key_len, unshared_end) = decode_varint(&data, shared_end);
    let (_, val_len_end) = decode_varint(&data, unshared_end);
    let value_start = val_len_end + unshared_key_len as usize;
    let (block_offset, bo_end) = decode_varint(&data, value_start);
    let (block_size, _) = decode_varint(&data, bo_end);

    // First data block layout:
    //   [compressed_data (block_size bytes), compress_type (1B), checksum (4B)]
    let block_start = block_offset as usize;
    let block_data_end = block_start + block_size as usize;
    let compress_type = data[block_data_end];
    let cksum_start = block_data_end + 1;

    // Overwrite the snappy decompressed length varint IN-PLACE at block_start.
    // The huge varint (6 bytes) overwrites the original varint + some compressed data.
    // This corrupts the compressed data but the decompressed length check happens first.
    let huge_len = encode_varint(500 * 1024 * 1024 * 1024_u64);
    if huge_len.len() <= (block_data_end - block_start) {
        // Overwrite in-place
        data[block_start..block_start + huge_len.len()].copy_from_slice(&huge_len);

        // Recompute CRC over [modified block data, compress_type]
        let block_data = &data[block_start..block_data_end];
        let crc = crc32c_digest(&[block_data, &[compress_type]]);
        let masked = mask_crc(crc);
        data[cksum_start..cksum_start + 4].copy_from_slice(&masked.to_le_bytes());

        fs::write(&sst, &data).unwrap();
        println!(
            "    Overwrote snappy length at offset {} with 500GB, fixed CRC (masked: {:#010x})",
            block_start, masked
        );
    } else {
        eprintln!("    WARNING: block too small for inflated varint");
    }
}
