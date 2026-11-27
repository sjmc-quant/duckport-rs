//! Watermark change feed behind the `duckport.watch_watermark` DoAction.
//!
//! Plugins move `data.watermark` with plain SQL, so the server cannot tell from a
//! request which write changed it. Instead `Backend::with_writer` calls
//! [`WatermarkHub::refresh`] after every successful write, still holding the writer
//! lock: the table is read back (one row per business table) and, when a `duck_time`
//! differs from the last published state, every open watch stream gets the new state.
//!
//! The feed carries state, not events. A stream starts with the current watermarks
//! and a slow reader only ever sees the latest ones, so reconnecting loses nothing.
//! Streams hold no DuckDB connection.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::Result as FlightResult;
use bytes::Bytes;
use futures::stream;
use serde::Serialize;
use tokio::sync::watch;
use tokio::time::{interval_at, Instant, MissedTickBehavior};
use tonic::Status;
use tracing::debug;

use crate::write_plane::DuckportStream;

/// The current watermarks are sent again after this long without a change, so a
/// reader can tell a quiet server from a dead connection.
const HEARTBEAT: Duration = Duration::from_secs(15);

/// Open watch streams allowed at once.
const MAX_WATCHERS: usize = 256;

const READ_SQL: &str = "SELECT table_name, strftime(duck_time, '%Y-%m-%dT%H:%M:%S.%f') \
                        FROM data.watermark ORDER BY table_name";

/// One row of `data.watermark`. `duck_time` is UTC, `None` while the table has none.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Watermark {
    pub table_name: String,
    pub duck_time: Option<String>,
}

#[derive(Serialize)]
struct Message<'a> {
    watermarks: &'a [Watermark],
}

#[derive(Clone, Default)]
struct State {
    rows: Arc<Vec<Watermark>>,
    /// Set once at shutdown. Open streams end and new ones are refused, so they do
    /// not hold up tonic's graceful drain.
    closed: bool,
}

/// Cheap to clone; all clones publish to and read from the same state.
#[derive(Clone)]
pub struct WatermarkHub {
    inner: Arc<Inner>,
}

struct Inner {
    tx: watch::Sender<State>,
    watchers: AtomicUsize,
}

/// Counts one open stream; released when the stream is dropped.
struct Slot(Arc<Inner>);

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.watchers.fetch_sub(1, Ordering::AcqRel);
    }
}

struct Subscription {
    rx: watch::Receiver<State>,
    heartbeat: Duration,
    tick: tokio::time::Interval,
    started: bool,
    _slot: Slot,
}

impl Default for WatermarkHub {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                tx: watch::Sender::new(State::default()),
                watchers: AtomicUsize::new(0),
            }),
        }
    }
}

impl WatermarkHub {
    /// Read `data.watermark` on `conn` and publish it when a `duck_time` changed.
    ///
    /// Call with the writer lock held and no transaction open, so states are
    /// published in commit order. A read failure (the table does not exist yet)
    /// leaves the published state as it is.
    pub fn refresh(&self, conn: &duckdb::Connection) {
        let rows = match read(conn) {
            Ok(rows) => rows,
            Err(err) => {
                debug!(%err, "watermark refresh skipped");
                return;
            }
        };
        self.inner.tx.send_if_modified(|state| {
            if *state.rows == rows {
                return false;
            }
            state.rows = Arc::new(rows);
            true
        });
    }

    /// End every open stream and refuse new ones.
    pub fn close(&self) {
        self.inner.tx.send_modify(|state| state.closed = true);
    }

    /// A stream of the current watermarks: once now, then after each change.
    pub fn subscribe(&self) -> Result<DuckportStream<FlightResult>, Status> {
        self.subscribe_with(HEARTBEAT)
    }

    fn subscribe_with(&self, heartbeat: Duration) -> Result<DuckportStream<FlightResult>, Status> {
        let rx = self.inner.tx.subscribe();
        if rx.borrow().closed {
            return Err(Status::unavailable("server is shutting down"));
        }
        if self.inner.watchers.fetch_add(1, Ordering::AcqRel) >= MAX_WATCHERS {
            self.inner.watchers.fetch_sub(1, Ordering::AcqRel);
            return Err(Status::resource_exhausted(format!(
                "too many watermark watchers (limit {MAX_WATCHERS})"
            )));
        }
        let mut tick = interval_at(Instant::now() + heartbeat, heartbeat);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let sub = Subscription {
            rx,
            heartbeat,
            tick,
            started: false,
            _slot: Slot(Arc::clone(&self.inner)),
        };

        Ok(Box::pin(stream::unfold(sub, |mut sub| async move {
            if sub.started {
                tokio::select! {
                    changed = sub.rx.changed() => changed.ok()?,
                    _ = sub.tick.tick() => {}
                }
            }
            sub.started = true;
            let state = sub.rx.borrow_and_update().clone();
            if state.closed {
                return None;
            }
            sub.tick.reset_after(sub.heartbeat);
            Some((encode(&state.rows), sub))
        })))
    }
}

fn read(conn: &duckdb::Connection) -> duckdb::Result<Vec<Watermark>> {
    let mut stmt = conn.prepare(READ_SQL)?;
    let rows = stmt.query_map([], |row| {
        Ok(Watermark {
            table_name: row.get(0)?,
            duck_time: row.get(1)?,
        })
    })?;
    rows.collect()
}

fn encode(rows: &[Watermark]) -> Result<FlightResult, Status> {
    serde_json::to_vec(&Message { watermarks: rows })
        .map(|body| FlightResult {
            body: Bytes::from(body),
        })
        .map_err(|e| Status::internal(format!("encode watermarks: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio::time::timeout;

    const SOON: Duration = Duration::from_secs(5);
    const QUIET: Duration = Duration::from_millis(200);

    fn conn() -> duckdb::Connection {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE SCHEMA data;
             CREATE TABLE data.watermark (
                 table_name VARCHAR PRIMARY KEY, duck_time TIMESTAMP, updated_at TIMESTAMP);
             INSERT INTO data.watermark VALUES
                 ('b_1m', TIMESTAMP '2026-01-01 00:01:00', now()), ('a_1m', NULL, now());",
        )
        .unwrap();
        conn
    }

    fn decode(item: Result<FlightResult, Status>) -> serde_json::Value {
        serde_json::from_slice(&item.unwrap().body).unwrap()
    }

    #[tokio::test]
    async fn starts_with_the_current_state_then_follows_duck_time() {
        let conn = conn();
        let hub = WatermarkHub::default();
        hub.refresh(&conn);
        let mut stream = hub.subscribe_with(Duration::from_secs(3600)).unwrap();

        let first = decode(timeout(SOON, stream.next()).await.unwrap().unwrap());
        assert_eq!(
            first,
            serde_json::json!({"watermarks": [
                {"table_name": "a_1m", "duck_time": null},
                {"table_name": "b_1m", "duck_time": "2026-01-01T00:01:00.000000"},
            ]})
        );

        // updated_at alone is not a watermark change
        conn.execute_batch(
            "UPDATE data.watermark SET updated_at = TIMESTAMP '2030-01-01 00:00:00'",
        )
        .unwrap();
        hub.refresh(&conn);
        assert!(timeout(QUIET, stream.next()).await.is_err());

        conn.execute_batch(
            "UPDATE data.watermark SET duck_time = TIMESTAMP '2026-01-01 00:02:00'
             WHERE table_name = 'b_1m'",
        )
        .unwrap();
        hub.refresh(&conn);
        let next = decode(timeout(SOON, stream.next()).await.unwrap().unwrap());
        assert_eq!(
            next["watermarks"][1]["duck_time"],
            "2026-01-01T00:02:00.000000"
        );
    }

    #[tokio::test]
    async fn a_slow_reader_gets_only_the_latest_state() {
        let conn = conn();
        let hub = WatermarkHub::default();
        hub.refresh(&conn);
        let mut stream = hub.subscribe_with(Duration::from_secs(3600)).unwrap();
        decode(stream.next().await.unwrap());

        for minute in 2..=5 {
            conn.execute_batch(&format!(
                "UPDATE data.watermark SET duck_time = TIMESTAMP '2026-01-01 00:0{minute}:00'
                 WHERE table_name = 'b_1m'"
            ))
            .unwrap();
            hub.refresh(&conn);
        }
        let next = decode(timeout(SOON, stream.next()).await.unwrap().unwrap());
        assert_eq!(
            next["watermarks"][1]["duck_time"],
            "2026-01-01T00:05:00.000000"
        );
        assert!(timeout(QUIET, stream.next()).await.is_err());
    }

    #[tokio::test]
    async fn repeats_the_state_when_nothing_changes() {
        let conn = conn();
        let hub = WatermarkHub::default();
        hub.refresh(&conn);
        let mut stream = hub.subscribe_with(Duration::from_millis(50)).unwrap();
        let first = decode(stream.next().await.unwrap());
        let again = decode(timeout(SOON, stream.next()).await.unwrap().unwrap());
        assert_eq!(first, again);
    }

    #[tokio::test]
    async fn close_ends_streams_and_refuses_new_ones() {
        let hub = WatermarkHub::default();
        let mut stream = hub.subscribe_with(Duration::from_secs(3600)).unwrap();
        decode(stream.next().await.unwrap());
        hub.close();
        assert!(timeout(SOON, stream.next()).await.unwrap().is_none());
        let refused = hub.subscribe().err().unwrap();
        assert_eq!(refused.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn watcher_slots_are_released_when_a_stream_is_dropped() {
        let hub = WatermarkHub::default();
        let streams: Vec<_> = (0..MAX_WATCHERS)
            .map(|_| hub.subscribe().unwrap())
            .collect();
        let refused = hub.subscribe().err().unwrap();
        assert_eq!(refused.code(), tonic::Code::ResourceExhausted);
        drop(streams);
        assert!(hub.subscribe().is_ok());
    }

    #[test]
    fn a_missing_table_keeps_the_published_state() {
        let hub = WatermarkHub::default();
        hub.refresh(&conn());
        hub.refresh(&duckdb::Connection::open_in_memory().unwrap());
        assert_eq!(hub.inner.tx.borrow().rows.len(), 2);
    }
}
