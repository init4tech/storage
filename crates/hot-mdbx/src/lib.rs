//! Implementation of the hot key-value storage using MDBX as the underlying
//! database.
//!
//! ## Notes on implementation
//!
//! This module provides an implementation of the [`HotKv`] trait using MDBX as
//! the underlying database. It includes functionality for opening and
//! managing the MDBX environment, handling read-only and read-write
//! transactions, and managing database tables.
//!
//! The [`DatabaseEnv`] struct encapsulates the MDBX environment and provides
//! methods for starting transactions. The [`DatabaseArguments`] struct
//! allows for configuring various parameters of the database environment,
//! such as geometry, sync mode, and maximum readers.
//!
//! ### Table Metadata
//!
//! This implementation uses the default MDBX table to store metadata about
//! each table, including whether it uses dual keys or fixed-size values. This
//! metadata is cached in memory for efficient access during the lifetime of
//! the environment. Each time a table is opened, its metadata is checked
//! against the cached values to ensure consistency.
//!
//! Rought Edges:
//! - The cache does not respect dropped transactions. Creating multiple tables
//!   with the same name but different metadata in different transactions
//!   may lead to inconsistencies.
//! - Tables created outside of this implementation (e.g., via external tools)
//!   will not have their metadata cached, which may lead to inconsistencies if
//!   the same table is later opened with different metadata.
//!
//! Overall, we do NOT recommend using this to open existing databases that
//! were not created and managed by this implementation.

#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    unreachable_pub,
    clippy::missing_const_for_fn,
    rustdoc::all
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg))]

use parking_lot::RwLock;
use signet_libmdbx::{
    Environment, EnvironmentFlags, Geometry, Mode, Ro, RoSync, Rw, RwSync, SyncMode, ffi,
    sys::{HandleSlowReadersReturnCode, PageSize},
};
use std::{
    collections::HashMap,
    ops::{Deref, Range},
    path::Path,
    sync::Arc,
};

mod cursor;
pub use cursor::{Cursor, CursorRo, CursorRoSync, CursorRw, CursorRwSync};

mod db_info;
pub use db_info::{FixedSizeInfo, FsiCache};

mod error;
pub use error::MdbxError;

mod lock;
pub use lock::{StorageLock, StorageLockError};

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

mod tx;
pub use tx::Tx;

mod utils;

use signet_hot::model::{HotKv, HotKvError};

/// 1 KB in bytes
pub const KILOBYTE: usize = 1024;
/// 1 MB in bytes
pub const MEGABYTE: usize = KILOBYTE * 1024;
/// 1 GB in bytes
pub const GIGABYTE: usize = MEGABYTE * 1024;
/// 1 TB in bytes
pub const TERABYTE: usize = GIGABYTE * 1024;

/// MDBX allows up to 32767 readers (`MDBX_READERS_LIMIT`), but we limit it to slightly below that
const DEFAULT_MAX_READERS: u64 = 32_000;

/// Space that a read-only transaction can occupy until the warning is emitted.
/// See [`signet_libmdbx::EnvironmentBuilder::set_handle_slow_readers`] for more
/// information.
const MAX_SAFE_READER_SPACE: usize = 10 * GIGABYTE;

/// Environment used when opening a MDBX environment. Read-only or Read-write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseEnvKind {
    /// Read-only MDBX environment.
    RO,
    /// Read-write MDBX environment.
    RW,
}

impl DatabaseEnvKind {
    /// Returns `true` if the environment is read-write.
    pub const fn is_rw(&self) -> bool {
        matches!(self, Self::RW)
    }
}

/// Arguments for database initialization.
#[derive(Clone, Debug)]
pub struct DatabaseArguments {
    /// Database geometry settings.
    geometry: Geometry<Range<usize>>,

    /// Open environment in exclusive/monopolistic mode. If [None], the default value is used.
    ///
    /// This can be used as a replacement for `MDB_NOLOCK`, which don't supported by MDBX. In this
    /// way, you can get the minimal overhead, but with the correct multi-process and multi-thread
    /// locking.
    ///
    /// If `true` = open environment in exclusive/monopolistic mode or return `MDBX_BUSY` if
    /// environment already used by other process. The main feature of the exclusive mode is the
    /// ability to open the environment placed on a network share.
    ///
    /// If `false` = open environment in cooperative mode, i.e. for multi-process
    /// access/interaction/cooperation. The main requirements of the cooperative mode are:
    /// - Data files MUST be placed in the LOCAL file system, but NOT on a network share.
    /// - Environment MUST be opened only by LOCAL processes, but NOT over a network.
    /// - OS kernel (i.e. file system and memory mapping implementation) and all processes that
    ///   open the given environment MUST be running in the physically single RAM with
    ///   cache-coherency. The only exception for cache-consistency requirement is Linux on MIPS
    ///   architecture, but this case has not been tested for a long time).
    ///
    /// This flag affects only at environment opening but can't be changed after.
    exclusive: Option<bool>,
    /// MDBX allows up to 32767 readers (`MDBX_READERS_LIMIT`). This arg is to configure the max
    /// readers.
    max_readers: Option<u64>,
    /// Defines the synchronization strategy used by the MDBX database when writing data to disk.
    ///
    /// This determines how aggressively MDBX ensures data durability versus prioritizing
    /// performance. The available modes are:
    ///
    /// - [`SyncMode::Durable`]: Ensures all transactions are fully flushed to disk before they are
    ///   considered committed.   This provides the highest level of durability and crash safety
    ///   but may have a performance cost.
    /// - [`SyncMode::SafeNoSync`]: Skips certain fsync operations to improve write performance.
    ///   This mode still maintains database integrity but may lose the most recent transactions if
    ///   the system crashes unexpectedly.
    ///
    /// Choose `Durable` if consistency and crash safety are critical (e.g., production
    /// environments). Choose `SafeNoSync` if performance is more important and occasional data
    /// loss is acceptable (e.g., testing or ephemeral data).
    sync_mode: SyncMode,
}

impl Default for DatabaseArguments {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseArguments {
    /// Create new database arguments with given client version.
    pub fn new() -> Self {
        Self {
            geometry: Geometry {
                size: Some(0..(8 * TERABYTE)),
                growth_step: Some(4 * GIGABYTE as isize),
                shrink_threshold: Some(0),
                page_size: Some(PageSize::Set(utils::default_page_size())),
            },
            exclusive: None,
            max_readers: None,
            sync_mode: SyncMode::Durable,
        }
    }

    /// Sets the upper size limit of the db environment, the maximum database size in bytes.
    pub const fn with_geometry_max_size(mut self, max_size: Option<usize>) -> Self {
        if let Some(max_size) = max_size {
            self.geometry.size = Some(0..max_size);
        }
        self
    }

    /// Sets the database page size value.
    pub const fn with_geometry_page_size(mut self, page_size: Option<usize>) -> Self {
        if let Some(size) = page_size {
            self.geometry.page_size = Some(PageSize::Set(size));
        }

        self
    }

    /// Sets the database sync mode.
    pub const fn with_sync_mode(mut self, sync_mode: Option<SyncMode>) -> Self {
        if let Some(sync_mode) = sync_mode {
            self.sync_mode = sync_mode;
        }

        self
    }

    /// Configures the database growth step in bytes.
    pub const fn with_growth_step(mut self, growth_step: Option<usize>) -> Self {
        if let Some(growth_step) = growth_step {
            self.geometry.growth_step = Some(growth_step as isize);
        }
        self
    }

    /// Set the mdbx exclusive flag.
    pub const fn with_exclusive(mut self, exclusive: Option<bool>) -> Self {
        self.exclusive = exclusive;
        self
    }

    /// Set `max_readers` flag.
    pub const fn with_max_readers(mut self, max_readers: Option<u64>) -> Self {
        self.max_readers = max_readers;
        self
    }

    /// Open a read-only database at `path` with the current arguments
    pub fn open_ro(self, path: &Path) -> Result<DatabaseEnv, MdbxError> {
        DatabaseEnv::open(path, DatabaseEnvKind::RO, self)
    }

    /// Open a read-write database at `path` with the current arguments
    pub fn open_rw(self, path: &Path) -> Result<DatabaseEnv, MdbxError> {
        DatabaseEnv::open(path, DatabaseEnvKind::RW, self)
    }
}

/// MDBX database environment. Wraps the low-level [Environment], and
/// implements the [`HotKv`] trait.

#[derive(Debug)]
pub struct DatabaseEnv {
    /// Libmdbx-sys environment.
    inner: Environment,
    /// Cached FixedSizeInfo for tables.
    ///
    /// Important: Do not manually close these DBIs, like via `mdbx_dbi_close`.
    /// More generally, do not dynamically create, re-open, or drop tables at
    /// runtime. It's better to perform table creation and migration only once
    /// at startup.
    fsi_cache: FsiCache,

    /// Write lock for when dealing with a read-write environment.
    _lock_file: Option<StorageLock>,
}

impl DatabaseEnv {
    /// Opens the database at the specified path with the given `EnvKind`.
    /// Acquires a lock file if opening in read-write mode.
    pub fn open(
        path: &Path,
        kind: DatabaseEnvKind,
        args: DatabaseArguments,
    ) -> Result<Self, MdbxError> {
        let _lock_file = if kind.is_rw() { Some(StorageLock::try_acquire(path)?) } else { None };

        let mut inner_env = Environment::builder();

        let mode = match kind {
            DatabaseEnvKind::RO => Mode::ReadOnly,
            DatabaseEnvKind::RW => {
                // enable writemap mode in RW mode
                inner_env.write_map();
                Mode::ReadWrite { sync_mode: args.sync_mode }
            }
        };

        inner_env.set_max_dbs(256);
        inner_env.set_geometry(args.geometry);

        fn is_current_process(id: u32) -> bool {
            #[cfg(unix)]
            {
                id == std::os::unix::process::parent_id() || id == std::process::id()
            }

            #[cfg(not(unix))]
            {
                id == std::process::id()
            }
        }

        extern "C" fn handle_slow_readers(
            _env: *const ffi::MDBX_env,
            _txn: *const ffi::MDBX_txn,
            process_id: ffi::mdbx_pid_t,
            thread_id: ffi::mdbx_tid_t,
            read_txn_id: u64,
            gap: std::ffi::c_uint,
            space: usize,
            retry: std::ffi::c_int,
        ) -> HandleSlowReadersReturnCode {
            if space > MAX_SAFE_READER_SPACE {
                let message = if is_current_process(process_id as u32) {
                    "Current process has a long-lived database transaction that grows the database file."
                } else {
                    "External process has a long-lived database transaction that grows the database file. \
                     Use shorter-lived read transactions or shut down the node."
                };
                tracing::warn!(
                    target: "storage::db::mdbx",
                    ?process_id,
                    ?thread_id,
                    ?read_txn_id,
                    ?gap,
                    ?space,
                    ?retry,
                    "{message}"
                )
            }

            HandleSlowReadersReturnCode::ProceedWithoutKillingReader
        }
        inner_env.set_handle_slow_readers(handle_slow_readers);

        inner_env.set_flags(EnvironmentFlags {
            mode,
            // We disable readahead because it improves performance for linear scans, but
            // worsens it for random access (which is our access pattern outside of sync)
            no_rdahead: true,
            coalesce: true,
            exclusive: args.exclusive.unwrap_or_default(),
            ..Default::default()
        });
        // Configure more readers
        inner_env.set_max_readers(args.max_readers.unwrap_or(DEFAULT_MAX_READERS));
        // This parameter sets the maximum size of the "reclaimed list", and the unit of measurement
        // is "pages". Reclaimed list is the list of freed pages that's populated during the
        // lifetime of DB transaction, and through which MDBX searches when it needs to insert new
        // record with overflow pages. The flow is roughly the following:
        // 0. We need to insert a record that requires N number of overflow pages (in consecutive
        //    sequence inside the DB file).
        // 1. Get some pages from the freelist, put them into the reclaimed list.
        // 2. Search through the reclaimed list for the sequence of size N.
        // 3. a. If found, return the sequence.
        // 3. b. If not found, repeat steps 1-3. If the reclaimed list size is larger than
        //    the `rp augment limit`, stop the search and allocate new pages at the end of the file:
        //    https://github.com/paradigmxyz/reth/blob/2a4c78759178f66e30c8976ec5d243b53102fc9a/crates/storage/libmdbx-rs/mdbx-sys/libmdbx/mdbx.c#L11479-L11480.
        //
        // Basically, this parameter controls for how long do we search through the freelist before
        // trying to allocate new pages. Smaller value will make MDBX to fallback to
        // allocation faster, higher value will force MDBX to search through the freelist
        // longer until the sequence of pages is found.
        //
        // The default value of this parameter is set depending on the DB size. The bigger the
        // database, the larger is `rp augment limit`.
        // https://github.com/paradigmxyz/reth/blob/2a4c78759178f66e30c8976ec5d243b53102fc9a/crates/storage/libmdbx-rs/mdbx-sys/libmdbx/mdbx.c#L10018-L10024.
        //
        // Previously, MDBX set this value as `256 * 1024` constant. Let's fallback to this,
        // because we want to prioritize freelist lookup speed over database growth.
        // https://github.com/paradigmxyz/reth/blob/fa2b9b685ed9787636d962f4366caf34a9186e66/crates/storage/libmdbx-rs/mdbx-sys/libmdbx/mdbx.c#L16017.
        inner_env.set_rp_augment_limit(256 * 1024);

        let fsi_cache = Arc::new(RwLock::new(HashMap::new()));
        let env = Self { inner: inner_env.open(path)?, fsi_cache, _lock_file };

        Ok(env)
    }

    /// Start a new read-only transaction.
    pub fn tx(&self) -> Result<Tx<Ro>, MdbxError> {
        self.inner
            .begin_ro_unsync()
            .map(|tx| Tx::new(tx, self.fsi_cache.clone()))
            .map_err(MdbxError::Mdbx)
    }

    /// Start a new read-write transaction.
    pub fn tx_rw(&self) -> Result<Tx<Rw>, MdbxError> {
        self.inner
            .begin_rw_unsync()
            .map(|tx| Tx::new(tx, self.fsi_cache.clone()))
            .map_err(MdbxError::Mdbx)
    }

    /// Start a new read-only synchronous transaction.
    pub fn tx_sync(&self) -> Result<Tx<RoSync>, MdbxError> {
        self.inner
            .begin_ro_sync()
            .map(|tx| Tx::new(tx, self.fsi_cache.clone()))
            .map_err(MdbxError::Mdbx)
    }

    /// Start a new read-write synchronous transaction.
    pub fn tx_rw_sync(&self) -> Result<Tx<RwSync>, MdbxError> {
        self.inner
            .begin_rw_sync()
            .map(|tx| Tx::new(tx, self.fsi_cache.clone()))
            .map_err(MdbxError::Mdbx)
    }
}

impl Deref for DatabaseEnv {
    type Target = Environment;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl HotKv for DatabaseEnv {
    type RoTx = Tx<Ro>;
    type RwTx = Tx<Rw>;

    fn reader(&self) -> Result<Self::RoTx, HotKvError> {
        self.tx().map_err(HotKvError::from_err)
    }

    fn writer(&self) -> Result<Self::RwTx, HotKvError> {
        self.tx_rw().map_err(HotKvError::from_err)
    }
}
