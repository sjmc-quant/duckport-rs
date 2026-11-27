//! DuckDB backend: owns the single DuckDB database and manages its connections.
//!
//! Phase 1 only needs the read-side: an r2d2 pool of N reader connections.
//! Phase 2 will add a single-writer connection dedicated to write RPCs.
//!
//! We route all DuckDB work through `tokio::task::spawn_blocking` because the
//! DuckDB FFI is synchronous.

use anyhow::{Context, Result};
use duckdb::{Config, DuckdbConnectionManager};
use r2d2::Pool;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::watermark::WatermarkHub;

pub type ReadPool = Pool<DuckdbConnectionManager>;
pub type ReadConn = r2d2::PooledConnection<DuckdbConnectionManager>;

/// Shared backend handle. Cheap to clone.
#[derive(Clone)]
pub struct Backend {
    inner: Arc<BackendInner>,
}

struct BackendInner {
    db_path: PathBuf,
    read_pool: ReadPool,
    /// Dedicated writer connection. Held behind a Mutex so every write RPC serialises.
    /// DuckDB is single-writer anyway, so serialising here mirrors the engine's model
    /// and lets us use a stable `Connection` for BEGIN/.../COMMIT sequences.
    writer: Arc<Mutex<duckdb::Connection>>,
    /// Monotonic catalog epoch. Bumped by every write RPC. Surfaced as
    /// `CatalogVersion.version` so DuckDB Airport clients refresh their cached catalog
    /// after DDL/DML without needing to DETACH/ATTACH.
    catalog_epoch: AtomicU64,
    /// Last published `data.watermark`, refreshed after every write.
    watermarks: WatermarkHub,
}

impl Backend {
    pub fn open(
        db_path: impl AsRef<Path>,
        read_pool_size: u32,
        duckdb_threads: u32,
        duckdb_memory_limit: &str,
    ) -> Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();

        // `duckdb::Config` is not Clone, so we build it from scratch each time it's
        // consumed. Tiny closure keeps pool + writer in sync.
        let build_cfg = || -> Result<Config> {
            let mut cfg = Config::default();
            if duckdb_threads > 0 {
                cfg = cfg
                    .threads(duckdb_threads as i64)
                    .context("set duckdb threads")?;
            }
            if !duckdb_memory_limit.is_empty() {
                cfg = cfg
                    .with("memory_limit", duckdb_memory_limit)
                    .context("set duckdb memory_limit")?;
            }
            Ok(cfg)
        };

        let is_memory = db_path == PathBuf::from(":memory:");
        let manager = if is_memory {
            DuckdbConnectionManager::memory_with_flags(build_cfg()?)
                .context("open in-memory duckdb")?
        } else {
            if let Some(parent) = db_path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).ok();
                }
            }
            DuckdbConnectionManager::file_with_flags(&db_path, build_cfg()?)
                .context("open duckdb file")?
        };

        let read_pool = Pool::builder()
            .max_size(read_pool_size)
            .build(manager)
            .context("build duckdb read pool")?;

        // Derive the writer from the pool's root connection via `try_clone()`.
        //
        // IMPORTANT: calling `duckdb::Connection::open(path)` twice in the same process
        // creates TWO independent DuckDB Database instances that don't share MVCC state
        // (confirmed in probe_conn). A write on one is invisible to the other until the
        // file is closed and reopened. `Connection::try_clone()` instead creates a new
        // connection on the SAME already-opened database, which is what we want: the
        // writer and every pool reader see each other's committed changes.
        //
        // `DuckdbConnectionManager::connect()` internally `try_clone`s from the root
        // connection it holds, so a pool connection is already such a clone. We take one
        // from the pool, clone it for the writer, then drop our temporary handle so the
        // original returns to the pool.
        let (duckdb_version, writer_conn): (String, duckdb::Connection) = {
            let root = read_pool.get().context("acquire initial duckdb conn")?;
            let v: String = root.query_row("SELECT version()", [], |r| r.get(0))?;
            let writer = root
                .try_clone()
                .context("clone writer connection from pool")?;
            (v, writer)
        };

        info!(
            db_path = %db_path.display(),
            duckdb_version = %duckdb_version,
            read_pool_size,
            "duckdb backend ready"
        );

        Ok(Self {
            inner: Arc::new(BackendInner {
                db_path,
                read_pool,
                writer: Arc::new(Mutex::new(writer_conn)),
                catalog_epoch: AtomicU64::new(1),
                watermarks: WatermarkHub::default(),
            }),
        })
    }

    /// Current catalog epoch. Returned verbatim as `CatalogVersion.version`.
    pub fn catalog_epoch(&self) -> u64 {
        self.inner.catalog_epoch.load(Ordering::Acquire)
    }

    /// Bumps the catalog epoch and returns the new value. Called after any write RPC.
    ///
    /// Bumping on pure DML (INSERT/UPDATE/DELETE) is conservative — DuckDB will
    /// re-fetch the catalog even though no structural change happened. That's cheap
    /// and simpler than parsing SQL to decide.
    pub fn bump_catalog_epoch(&self) -> u64 {
        self.inner.catalog_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Watermark feed for `duckport.watch_watermark`.
    pub fn watermarks(&self) -> &WatermarkHub {
        &self.inner.watermarks
    }

    pub fn db_path(&self) -> &Path {
        &self.inner.db_path
    }

    /// Acquire a reader connection from the pool. Blocks briefly if the pool is exhausted.
    pub fn reader(&self) -> Result<ReadConn> {
        self.inner
            .read_pool
            .get()
            .context("acquire duckdb read connection")
    }

    /// Run a blocking closure against a reader connection on tokio's blocking pool.
    pub async fn with_reader<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&ReadConn) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let conn = this.reader()?;
            f(&conn)
        })
        .await
        .context("duckdb blocking task panicked")?
    }

    /// Run a blocking closure holding the exclusive writer lock.
    ///
    /// Calls serialise: only one writer closure executes at a time. Use this for any
    /// statement that mutates the catalog or data (INSERT/UPDATE/DELETE/DDL/BEGIN/COMMIT).
    ///
    /// The closure must commit or roll back before it returns. After a successful one
    /// the watermark feed is refreshed under the same lock, so watchers are told about
    /// a new `duck_time` only once its data is readable, and in commit order.
    pub async fn with_writer<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut duckdb::Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let writer = self.inner.writer.clone();
        let watermarks = self.inner.watermarks.clone();
        tokio::task::spawn_blocking(move || {
            // `blocking_lock` is safe here because we're on the blocking thread pool,
            // never on a tokio worker.
            let mut guard = writer.blocking_lock();
            let out = f(&mut guard);
            if out.is_ok() {
                watermarks.refresh(&guard);
            }
            out
        })
        .await
        .context("duckdb writer task panicked")?
    }

    /// Flush the WAL into the database file. Intended for graceful shutdown after
    /// in-flight RPCs have drained; connections are released when `Backend` drops.
    pub async fn checkpoint(&self) -> Result<()> {
        info!(
            db_path = %self.inner.db_path.display(),
            "checkpointing DuckDB before shutdown"
        );
        self.with_writer(|conn| {
            conn.execute_batch("CHECKPOINT")?;
            Ok(())
        })
        .await
        .context("CHECKPOINT during shutdown")?;
        info!("DuckDB checkpoint complete");
        Ok(())
    }
}
