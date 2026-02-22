use crate::env::{path_to_str, Env, FileLock, Logger, RandomAccess};
use crate::env_common::{micros, sleep_for};
use crate::error::{err, Result, Status, StatusCode};

use std::fs;
use std::io::{self, Read};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct PosixDiskEnv;

impl Default for PosixDiskEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl PosixDiskEnv {
    pub fn new() -> PosixDiskEnv {
        PosixDiskEnv
    }
}

/// map_err_with_name annotates an io::Error with information about the operation and the file.
fn map_err_with_name(method: &'static str, f: &Path, e: io::Error) -> Status {
    let mut s = Status::from(e);
    s.err = format!("{}: {}: {}", method, s.err, path_to_str(f));
    s
}

impl Env for PosixDiskEnv {
    fn open_sequential_file(&self, p: &Path) -> Result<Box<dyn Read>> {
        Ok(Box::new(
            fs::OpenOptions::new()
                .read(true)
                .open(p)
                .map_err(|e| map_err_with_name("open (seq)", p, e))?,
        ))
    }
    fn open_random_access_file(&self, p: &Path) -> Result<Box<dyn RandomAccess>> {
        fs::OpenOptions::new()
            .read(true)
            .open(p)
            .map(|f| {
                let b: Box<dyn RandomAccess> = Box::new(f);
                b
            })
            .map_err(|e| map_err_with_name("open (randomaccess)", p, e))
    }
    fn open_writable_file(&self, p: &Path) -> Result<Box<dyn std::io::Write>> {
        err(StatusCode::NotSupported, &format!("open (write): read-only env: {}", path_to_str(p)))
    }
    fn open_appendable_file(&self, p: &Path) -> Result<Box<dyn std::io::Write>> {
        err(StatusCode::NotSupported, &format!("open (append): read-only env: {}", path_to_str(p)))
    }

    fn exists(&self, p: &Path) -> Result<bool> {
        Ok(p.exists())
    }
    fn children(&self, p: &Path) -> Result<Vec<PathBuf>> {
        let dir_reader = fs::read_dir(p).map_err(|e| map_err_with_name("children", p, e))?;
        let filenames = dir_reader
            .map(|r| match r {
                Ok(_) => {
                    let direntry = r.unwrap();
                    Path::new(&direntry.file_name()).to_owned()
                }
                Err(_) => Path::new("").to_owned(),
            })
            .filter(|s| !s.as_os_str().is_empty());
        Ok(Vec::from_iter(filenames))
    }
    fn size_of(&self, p: &Path) -> Result<usize> {
        let meta = fs::metadata(p).map_err(|e| map_err_with_name("size_of", p, e))?;
        Ok(meta.len() as usize)
    }

    fn delete(&self, p: &Path) -> Result<()> {
        err(StatusCode::NotSupported, &format!("delete: read-only env: {}", path_to_str(p)))
    }
    fn mkdir(&self, p: &Path) -> Result<()> {
        err(StatusCode::NotSupported, &format!("mkdir: read-only env: {}", path_to_str(p)))
    }
    fn rmdir(&self, p: &Path) -> Result<()> {
        err(StatusCode::NotSupported, &format!("rmdir: read-only env: {}", path_to_str(p)))
    }
    fn rename(&self, old: &Path, new: &Path) -> Result<()> {
        err(StatusCode::NotSupported, &format!("rename: read-only env: {} -> {}", path_to_str(old), path_to_str(new)))
    }

    fn lock(&self, p: &Path) -> Result<FileLock> {
        err(StatusCode::NotSupported, &format!("lock: read-only env: {}", path_to_str(p)))
    }
    fn unlock(&self, _: FileLock) -> Result<()> {
        Ok(())
    }

    fn new_logger(&self, p: &Path) -> Result<Logger> {
        err(StatusCode::NotSupported, &format!("new_logger: read-only env: {}", path_to_str(p)))
    }

    fn micros(&self) -> u64 {
        micros()
    }

    fn sleep_for(&self, micros: u32) {
        sleep_for(micros);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_read_only_ops_fail() {
        let env = PosixDiskEnv::new();
        let p = Path::new("testfile.xyz");
        assert!(env.open_writable_file(p).is_err());
        assert!(env.open_appendable_file(p).is_err());
        assert!(env.delete(p).is_err());
        assert!(env.mkdir(p).is_err());
        assert!(env.rmdir(p).is_err());
        assert!(env.rename(p, Path::new("other.xyz")).is_err());
        assert!(env.lock(p).is_err());
    }

    #[test]
    fn test_read_ops_work() {
        // Create a real file using std::fs directly, then read it via PosixDiskEnv.
        let path = Path::new("diskenv_test_read.tmp");
        std::fs::File::create(path).unwrap().write_all(b"hello").unwrap();

        let env = PosixDiskEnv::new();
        assert!(env.exists(path).unwrap());
        assert_eq!(env.size_of(path).unwrap(), 5);
        assert!(env.open_sequential_file(path).is_ok());
        assert!(env.open_random_access_file(path).is_ok());

        std::fs::remove_file(path).unwrap();
    }
}
