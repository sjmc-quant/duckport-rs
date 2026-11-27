//! DoAction handlers for `duckport.*` custom actions.

use anyhow::{anyhow, Context, Result};
use arrow_flight::{Action, Result as FlightResult};
use bytes::Bytes;
use futures::stream;
use serde::Serialize;
use tonic::{Response, Status};
use tracing::{debug, info, warn};

use crate::backend::Backend;
use crate::retention;

use super::proto::{
    ExecuteRequest, ExecuteResponse, ExecuteTransactionRequest, ExecuteTransactionResponse,
    PingResponse, RetentionRunRequest,
};
use super::{truncate_for_log, DuckportStream};

/// Prefix every custom action type shares. Anything else is delegated to airport.
pub const PREFIX: &str = "duckport.";

/// All action types this plane exposes. Used by `list_actions`.
pub fn advertised() -> Vec<(&'static str, &'static str)> {
    vec![
        ("duckport.ping", "Identity + health probe"),
        ("duckport.execute", "Execute a single write statement"),
        (
            "duckport.execute_transaction",
            "Execute multiple statements atomically (BEGIN/COMMIT or ROLLBACK)",
        ),
        (
            "duckport.retention_run",
            "Run data retention now ({\"table\"?, \"dry_run\"?})",
        ),
        (
            "duckport.watch_watermark",
            "Stream data.watermark: the current state, then again after each duck_time change",
        ),
    ]
}

/// Entry point: dispatch on `action.r#type`. Caller must have already verified
/// the `duckport.` prefix; unknown `duckport.*` types return `invalid_argument`.
pub async fn handle(
    backend: &Backend,
    catalog_name: &str,
    retention: &retention::Settings,
    action: Action,
) -> Result<Response<DuckportStream<FlightResult>>, Status> {
    let kind = action.r#type.clone();
    debug!(action = %kind, body_len = action.body.len(), "duckport action");
    let body = action.body;

    if kind == "duckport.watch_watermark" {
        return Ok(Response::new(backend.watermarks().subscribe()?));
    }

    let result_bytes = match kind.as_str() {
        "duckport.ping" => ping(backend, catalog_name).await.map_err(internal_err)?,
        "duckport.execute" => execute(backend, &body).await?,
        "duckport.execute_transaction" => execute_transaction(backend, &body).await?,
        "duckport.retention_run" => retention_run(backend, retention, &body).await?,
        other => {
            return Err(Status::invalid_argument(format!(
                "unknown duckport action: {other}"
            )));
        }
    };

    single_result(result_bytes)
}

fn single_result(bytes: Bytes) -> Result<Response<DuckportStream<FlightResult>>, Status> {
    let item: std::result::Result<FlightResult, Status> = Ok(FlightResult { body: bytes });
    let stream = stream::iter(std::iter::once(item));
    Ok(Response::new(Box::pin(stream)))
}

fn json_bytes<T: Serialize>(value: &T) -> Result<Bytes, Status> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|e| Status::internal(format!("encode response: {e}")))
}

fn parse_json<T: for<'de> serde::Deserialize<'de>>(body: &[u8]) -> Result<T, Status> {
    serde_json::from_slice(body)
        .map_err(|e| Status::invalid_argument(format!("invalid JSON body: {e}")))
}

fn internal_err(err: anyhow::Error) -> Status {
    Status::internal(format!("{err:#}"))
}

async fn ping(backend: &Backend, catalog_name: &str) -> Result<Bytes> {
    let duckdb_version = backend
        .with_reader(|conn| Ok(conn.query_row("SELECT version()", [], |r| r.get::<_, String>(0))?))
        .await
        .context("read duckdb version")?;
    let resp = PingResponse {
        server: "duckport",
        server_version: env!("CARGO_PKG_VERSION"),
        catalog: catalog_name.to_string(),
        duckdb_version,
    };
    Ok(Bytes::from(serde_json::to_vec(&resp)?))
}

async fn execute(backend: &Backend, body: &[u8]) -> Result<Bytes, Status> {
    let req: ExecuteRequest = parse_json(body)?;
    if req.sql.trim().is_empty() {
        return Err(Status::invalid_argument("sql cannot be empty"));
    }

    let sql = req.sql;
    let sql_for_log = sql.clone();
    let rows = backend
        .with_writer(move |conn| Ok(conn.execute(&sql, [])? as u64))
        .await
        .map_err(|e| {
            warn!(err = ?e, "duckport.execute failed");
            Status::internal(format!("{e:#}"))
        })?;

    let epoch = backend.bump_catalog_epoch();
    info!(
        rows,
        epoch,
        sql = %truncate_for_log(&sql_for_log, 200),
        "duckport.execute ok"
    );

    json_bytes(&ExecuteResponse {
        rows_affected: rows,
    })
}

async fn execute_transaction(backend: &Backend, body: &[u8]) -> Result<Bytes, Status> {
    let req: ExecuteTransactionRequest = parse_json(body)?;
    if req.statements.is_empty() {
        return Err(Status::invalid_argument("statements cannot be empty"));
    }
    for (i, s) in req.statements.iter().enumerate() {
        if s.trim().is_empty() {
            return Err(Status::invalid_argument(format!("statement[{i}] is empty")));
        }
    }

    let statements = req.statements;
    let count_statements = statements.len();
    let counts = backend
        .with_writer(move |conn| run_transaction(conn, statements))
        .await
        .map_err(|e| {
            warn!(err = ?e, "duckport.execute_transaction failed");
            Status::internal(format!("{e:#}"))
        })?;

    let epoch = backend.bump_catalog_epoch();
    info!(
        statements = count_statements,
        total_rows = counts.iter().sum::<u64>(),
        epoch,
        "duckport.execute_transaction ok"
    );

    json_bytes(&ExecuteTransactionResponse {
        rows_affected: counts,
    })
}

async fn retention_run(
    backend: &Backend,
    settings: &retention::Settings,
    body: &[u8],
) -> Result<Bytes, Status> {
    let req: RetentionRunRequest = if body.iter().all(u8::is_ascii_whitespace) {
        RetentionRunRequest::default()
    } else {
        parse_json(body)?
    };
    let dry_run = req.dry_run.unwrap_or(settings.dry_run);
    let report = retention::run(backend, settings, req.table, dry_run)
        .await
        .map_err(|e| {
            warn!(err = ?e, "duckport.retention_run failed");
            Status::failed_precondition(format!("{e:#}"))
        })?;
    info!(
        run_id = %report.run_id,
        dry_run,
        tables = report.tables.len(),
        "duckport.retention_run ok"
    );
    json_bytes(&report)
}

/// Drives a DuckDB write transaction by hand using `execute_batch`.
///
/// We avoid `duckdb::Connection::transaction()` because it creates a `Transaction<'_>`
/// that borrows the connection for the lifetime of the guard — awkward in our
/// `FnOnce(&mut Connection) -> Result<T>` closure. `BEGIN`/`COMMIT`/`ROLLBACK` via
/// `execute_batch` is equivalent: DuckDB uses snapshot isolation and a single writer,
/// so nested transactions are not a concern here.
fn run_transaction(conn: &mut duckdb::Connection, statements: Vec<String>) -> Result<Vec<u64>> {
    conn.execute_batch("BEGIN TRANSACTION")
        .context("BEGIN TRANSACTION")?;

    let body = (|| -> Result<Vec<u64>> {
        let mut counts = Vec::with_capacity(statements.len());
        for (i, sql) in statements.iter().enumerate() {
            let n = conn
                .execute(sql, [])
                .with_context(|| format!("statement[{i}] failed: sql={sql}"))?;
            counts.push(n as u64);
        }
        Ok(counts)
    })();

    match body {
        Ok(counts) => {
            conn.execute_batch("COMMIT").context("COMMIT")?;
            Ok(counts)
        }
        Err(err) => {
            if let Err(rbk) = conn.execute_batch("ROLLBACK") {
                warn!(err = ?rbk, "ROLLBACK failed after statement error");
            }
            Err(err.context(anyhow!("transaction rolled back")))
        }
    }
}

