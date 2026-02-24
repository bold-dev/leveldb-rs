//! rusty-leveldb is a reimplementation of LevelDB in pure rust. It depends only on a few crates,
//! and is very close to the original, implementation-wise. The external API is relatively small
//! and should be easy to use.
//!
//! ```no_run
//! use rusty_leveldb::{DB, Options};
//!
//! let opt = Options::default();
//! let mut db = DB::open_read_only("mydatabase", opt).unwrap();
//!
//! if let Some(val) = db.get(b"Hello") {
//!     println!("got: {:?}", val);
//! }
//! ```
//!

#![allow(dead_code)]

extern crate integer_encoding;
extern crate rand;
extern crate snap;

#[cfg(test)]
#[macro_use]
extern crate time_test;

#[macro_use]
pub mod infolog;

mod block;
mod block_builder;
mod blockhandle;
mod cache;
mod cmp;
mod crc;
mod disk_env;
mod env_common;
mod error;
mod filter;
mod filter_block;
mod key_types;
mod log;
mod mem_env;
mod memtable;
mod merging_iter;
mod options;
mod snapshot;
mod table_block;
mod table_builder;
mod table_cache;
mod table_reader;
mod test_util;
mod types;
mod version;
mod version_edit;
mod version_set;
mod write_batch;

mod db_impl;
mod db_iter;

pub mod compressor;
pub mod env;

pub use cmp::{Cmp, DefaultCmp};
pub use compressor::{Compressor, CompressorId};
pub use db_impl::{SafeDB, DB};
pub use db_iter::DBIterator;
pub use disk_env::PosixDiskEnv;
pub use error::{Result, Status, StatusCode};
pub use filter::{BloomPolicy, FilterPolicy};
pub use mem_env::MemEnv;
pub use options::{in_memory, CompressorList, Options};
pub use types::LdbIterator;
pub use write_batch::WriteBatch;
