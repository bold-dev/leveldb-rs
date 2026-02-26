use fs2::FileExt;
use rusty_leveldb::SafeDB;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);

fn testdata_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join(name)
}

/// Snapshot of all files in a directory: path -> (size, modified time).
/// Excludes the LOCK file since we create it as part of the test.
fn snapshot_dir(dir: &Path) -> HashMap<PathBuf, (u64, std::time::SystemTime)> {
    let mut map = HashMap::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.file_name().map(|n| n == "LOCK").unwrap_or(false) {
                continue;
            }
            if let Ok(meta) = std::fs::metadata(&path) {
                map.insert(path, (meta.len(), meta.modified().unwrap()));
            }
        }
    }
    map
}

/// Assert that no files were created, deleted, or modified (excluding LOCK).
fn assert_no_modifications(before: &HashMap<PathBuf, (u64, std::time::SystemTime)>, dir: &Path) {
    let after = snapshot_dir(dir);

    for path in after.keys() {
        assert!(
            before.contains_key(path),
            "new file created: {}",
            path.display()
        );
    }

    for path in before.keys() {
        assert!(
            after.contains_key(path),
            "file deleted: {}",
            path.display()
        );
    }

    for (path, (old_size, old_mtime)) in before {
        let (new_size, new_mtime) = &after[path];
        assert_eq!(
            old_size, new_size,
            "file size changed: {} ({} -> {})",
            path.display(),
            old_size,
            new_size
        );
        assert_eq!(
            old_mtime, new_mtime,
            "file modified: {}",
            path.display()
        );
    }
}

/// Acquire an exclusive lock on the DB's LOCK file, simulating a running
/// LevelDB writer. Returns the locked File handle (lock is held until dropped).
fn lock_db(dir: &Path) -> File {
    let lock_path = dir.join("LOCK");
    let lock_file = File::create(&lock_path).expect("failed to create LOCK file");
    lock_file
        .lock_exclusive()
        .expect("failed to acquire exclusive lock");
    lock_file
}

// ---------------------------------------------------------------------------
// Valid databases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_basic_db_read_all_keys() {
    let path = testdata_path("basic_db");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    let db = SafeDB::open(path.clone(), TIMEOUT).await.expect("failed to open basic_db");
    for i in 0..20 {
        let key = format!("key_{:04}", i);
        let val = db.get(key.as_bytes()).await.expect("get failed");
        assert!(val.is_some(), "missing key: {}", key);
        let expected = format!("value_{:04}", i);
        assert_eq!(val.unwrap(), expected.as_bytes(), "wrong value for {}", key);
    }
    db.close().await.unwrap();
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_basic_db_missing_key_returns_none() {
    let path = testdata_path("basic_db");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    let db = SafeDB::open(path.clone(), TIMEOUT).await.expect("failed to open basic_db");
    let val = db.get(b"nonexistent_key").await.expect("get failed");
    assert!(val.is_none());
    db.close().await.unwrap();
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_large_keys_db_read_all() {
    let path = testdata_path("large_keys_db");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    let db = SafeDB::open(path.clone(), TIMEOUT).await.expect("failed to open large_keys_db");
    for i in (0..100_000).step_by(997) {
        let key = format!("key_{:08}", i);
        let val = db.get(key.as_bytes()).await.expect("get failed");
        assert!(val.is_some(), "missing key: {}", key);
        let expected = format!("val_{:08}", i);
        assert_eq!(val.unwrap(), expected.as_bytes(), "wrong value for {}", key);
    }
    db.close().await.unwrap();
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_large_keys_db_first_and_last() {
    let path = testdata_path("large_keys_db");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    let db = SafeDB::open(path.clone(), TIMEOUT).await.expect("failed to open large_keys_db");
    let first = db.get(b"key_00000000").await.expect("get failed");
    assert!(first.is_some(), "missing first key");
    assert_eq!(first.unwrap(), b"val_00000000");

    let last = db.get(b"key_00099999").await.expect("get failed");
    assert!(last.is_some(), "missing last key");
    assert_eq!(last.unwrap(), b"val_00099999");
    db.close().await.unwrap();
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_large_values_db_read() {
    let path = testdata_path("large_values_db");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    let db = SafeDB::open(path.clone(), TIMEOUT).await.expect("failed to open large_values_db");
    for i in (0..100).step_by(10) {
        let key = format!("bigkey_{:04}", i);
        let val = db.get(key.as_bytes()).await.expect("get failed");
        assert!(val.is_some(), "missing key: {}", key);
        assert_eq!(val.unwrap().len(), 1_000_000, "wrong value length for {}", key);
    }
    db.close().await.unwrap();
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_deleted_keys_db() {
    let path = testdata_path("deleted_keys_db");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    let db = SafeDB::open(path.clone(), TIMEOUT).await.expect("failed to open deleted_keys_db");
    for i in 0..1000 {
        let key = format!("delkey_{:04}", i);
        let val = db.get(key.as_bytes()).await.expect("get failed");
        if i % 2 == 0 {
            assert!(val.is_none(), "deleted key should be None: {}", key);
        } else {
            assert!(val.is_some(), "surviving key missing: {}", key);
            let expected = format!("delval_{:04}", i);
            assert_eq!(val.unwrap(), expected.as_bytes(), "wrong value for {}", key);
        }
    }
    db.close().await.unwrap();
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_multilevel_db_read() {
    let path = testdata_path("multilevel_db");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    let db = SafeDB::open(path.clone(), TIMEOUT).await.expect("failed to open multilevel_db");
    for batch in 0..10 {
        for i in (0..5000).step_by(499) {
            let key = format!("ml_{:02}_{:06}", batch, i);
            let val = db.get(key.as_bytes()).await.expect("get failed");
            assert!(val.is_some(), "missing key: {}", key);
            let expected = format!("mlval_{:02}_{:06}", batch, i);
            assert_eq!(val.unwrap(), expected.as_bytes(), "wrong value for {}", key);
        }
    }
    db.close().await.unwrap();
    assert_no_modifications(&before, &path);
}

// ---------------------------------------------------------------------------
// Random corruption (should error, not panic/crash)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_corrupt_sst_data_does_not_panic() {
    let path = testdata_path("corrupt_sst_data");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    if let Ok(db) = SafeDB::open(path.clone(), TIMEOUT).await {
        for i in 0..20 {
            let key = format!("key_{:04}", i);
            let _ = db.get(key.as_bytes()).await;
        }
        let _ = db.close().await;
    }
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_corrupt_manifest_does_not_panic() {
    let path = testdata_path("corrupt_manifest");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    if let Ok(db) = SafeDB::open(path.clone(), TIMEOUT).await {
        let _ = db.get(b"key_0000").await;
        let _ = db.close().await;
    }
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_corrupt_wal_does_not_panic() {
    let path = testdata_path("corrupt_wal");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    if let Ok(db) = SafeDB::open(path.clone(), TIMEOUT).await {
        for i in 0..20 {
            let key = format!("key_{:04}", i);
            let _ = db.get(key.as_bytes()).await;
        }
        let _ = db.close().await;
    }
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_truncated_sst_does_not_panic() {
    let path = testdata_path("truncated_sst");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    if let Ok(db) = SafeDB::open(path.clone(), TIMEOUT).await {
        for i in 0..20 {
            let key = format!("key_{:04}", i);
            let _ = db.get(key.as_bytes()).await;
        }
        let _ = db.close().await;
    }
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_missing_sst_does_not_panic() {
    let path = testdata_path("missing_sst");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    if let Ok(db) = SafeDB::open(path.clone(), TIMEOUT).await {
        for i in 0..20 {
            let key = format!("key_{:04}", i);
            let _ = db.get(key.as_bytes()).await;
        }
        let _ = db.close().await;
    }
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_missing_current_fails_to_open() {
    let path = testdata_path("missing_current");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    let result = SafeDB::open(path.clone(), TIMEOUT).await;
    assert!(result.is_err(), "opening DB without CURRENT file should fail");
    assert_no_modifications(&before, &path);
}

// ---------------------------------------------------------------------------
// Targeted size-field corruptions (test MAX_ALLOC_SIZE guards)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_inflated_block_handle_does_not_allocate() {
    let path = testdata_path("inflated_block_handle");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    if let Ok(db) = SafeDB::open(path.clone(), TIMEOUT).await {
        for i in 0..20 {
            let key = format!("key_{:04}", i);
            let _ = db.get(key.as_bytes()).await;
        }
        let _ = db.close().await;
    }
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_inflated_wal_length_does_not_allocate() {
    let path = testdata_path("inflated_wal_length");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    if let Ok(db) = SafeDB::open(path.clone(), TIMEOUT).await {
        for i in 0..20 {
            let key = format!("walkey_{:04}", i);
            let _ = db.get(key.as_bytes()).await;
        }
        let _ = db.close().await;
    }
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_inflated_manifest_key_len_does_not_allocate() {
    let path = testdata_path("inflated_manifest_key_len");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    if let Ok(db) = SafeDB::open(path.clone(), TIMEOUT).await {
        let _ = db.get(b"key_0000").await;
        let _ = db.close().await;
    }
    assert_no_modifications(&before, &path);
}

#[tokio::test]
async fn test_inflated_snappy_len_does_not_allocate() {
    let path = testdata_path("inflated_snappy_len");
    let _lock = lock_db(&path);
    let before = snapshot_dir(&path);
    if let Ok(db) = SafeDB::open(path.clone(), TIMEOUT).await {
        for i in 0..50 {
            let key = format!("snappy_key_{:04}", i);
            let _ = db.get(key.as_bytes()).await;
        }
        let _ = db.close().await;
    }
    assert_no_modifications(&before, &path);
}
