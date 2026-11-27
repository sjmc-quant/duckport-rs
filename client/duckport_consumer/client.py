"""
DuckportConsumer — Python consumer client for duckport-rs.

Connects to duckport-rs via Arrow Flight and provides high-level query methods
that mirror the old FlightClient interface (get_market / get_symbol / get_exginfo).

**Plan A:** Resample SQL runs only against DuckDB 主表（`data.kline_...`）。`pqt_path`、
`redundancy_hours` 与 Parquet / hybrid 路径已弃用，仅为兼容旧调用保留；传入非默认值时会打日志提示。
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

from duckport_consumer.resample import (
    build_duckdb_resample_sql,
    build_symbol_count_sql,
    halve_range,
    plan_resample_ranges,
)

logger = logging.getLogger("duckport_consumer")

WATCH_WATERMARK_ACTION = "duckport.watch_watermark"
# One watch call lasts at most this long, then it is reopened. A reopened stream starts
# with the current watermarks, so nothing is missed and a dead connection cannot hang us.
_WATCH_CALL_SECONDS = 60.0
# Polling interval against servers that predate the watch action, and retry pause.
_WATCH_POLL_SECONDS = 1.0


def _utc(ts) -> Optional[datetime]:
    """A duck_time (UTC wall clock) or a caller's datetime as an aware UTC datetime."""
    if ts is None:
        return None
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def normalize_flight_location(addr: str) -> str:
    """Accept ``host:port`` or a full ``grpc://`` / ``grpc+tls://`` URI."""
    if addr.startswith("grpc://") or addr.startswith("grpc+tls://"):
        return addr
    return f"grpc://{addr}"


def resolve_token(token: Optional[str] = None) -> str:
    """Use an explicit token, otherwise ``DUCKPORT_TOKEN``. Empty means no auth header."""
    if token is not None:
        return token
    return os.environ.get("DUCKPORT_TOKEN", "")


class _BearerMiddleware(flight.ClientMiddleware):
    def __init__(self, token: str) -> None:
        self._value = f"Bearer {token}".encode()

    def sending_headers(self):
        return {b"authorization": self._value}


class _BearerFactory(flight.ClientMiddlewareFactory):
    def __init__(self, token: str) -> None:
        self._token = token

    def start_call(self, info):
        return _BearerMiddleware(self._token)


def open_flight_client(addr: str, token: Optional[str] = None, **kwargs):
    """Flight client. Loopback does not require a token; send one only when set."""
    location = normalize_flight_location(addr)
    resolved = resolve_token(token)
    if resolved:
        kwargs["middleware"] = [_BearerFactory(resolved)]
    return flight.FlightClient(location, **kwargs)

# others_perp: USD-M perpetuals of every quote asset except USDT, in one table.
# quote_volume there is in each symbol's own quote asset (data.exginfo.quote_asset).
_KLINE_MARKETS = ("usdt_perp", "usdt_spot", "others_perp")
_PERP_MARKETS = ("usdt_perp", "others_perp")
_EQUITY_UNDERLYING_TYPES = ("EQUITY", "HK_EQUITY", "KR_EQUITY", "CN_EQUITY")
_ASSET_CLASS_RE = re.compile(r"^[A-Za-z0-9_]+$")
_ASSET_CLASS_MARKETS_ERROR = (
    "asset_class is only supported for perp markets: " + ", ".join(_PERP_MARKETS)
)


_ASSET_COLUMNS = ("contract_type", "underlying_type")


def _asset_class_clause(asset_class: str) -> str:
    """Predicate on ``contract_type`` / ``underlying_type`` for an asset_class value.

    ``crypto`` / ``tradifi`` select contract types. ``EQUITY`` also includes
    HK, KR and CN equity. Any other value matches ``underlying_type`` exactly.
    The two columns have the same names and values in ``data.exginfo`` and on
    the rows of a perp kline table, so the clause serves both.
    """
    if not _ASSET_CLASS_RE.fullmatch(asset_class):
        raise ValueError(f"Invalid asset_class: {asset_class!r}")
    key = asset_class.upper()
    if key == "CRYPTO":
        return "contract_type = 'PERPETUAL'"
    if key in ("TRADIFI", "TRADFI"):
        return "contract_type = 'TRADIFI_PERPETUAL'"
    if key == "EQUITY":
        listed = ", ".join(f"'{name}'" for name in _EQUITY_UNDERLYING_TYPES)
        return (
            "contract_type = 'TRADIFI_PERPETUAL' "
            f"AND underlying_type IN ({listed})"
        )
    return f"underlying_type = '{key}'"


def perp_asset_where(market: str, asset_class: Optional[str] = None) -> str:
    """WHERE fragment for data.exginfo rows of a perp market."""
    if market not in _PERP_MARKETS:
        raise ValueError(_ASSET_CLASS_MARKETS_ERROR)
    where = f"market = '{market}'"
    if asset_class is None:
        return where
    if not isinstance(asset_class, str):
        raise ValueError(f"Invalid asset_class: {asset_class!r}")
    return f"{where} AND {_asset_class_clause(asset_class)}"


def usdt_perp_asset_where(asset_class: Optional[str] = None) -> str:
    """WHERE fragment for data.exginfo rows of market usdt_perp."""
    return perp_asset_where("usdt_perp", asset_class)


def build_exginfo_sql(
    schema: str,
    market: str,
    asset_class: Optional[str] = None,
) -> str:
    """SQL for get_exginfo. Omitting asset_class keeps the original statement."""
    if market not in _KLINE_MARKETS:
        raise ValueError(f"Unsupported market: {market}")
    if asset_class is None:
        return f"SELECT * FROM {schema}.exginfo WHERE market = '{market}'"
    return (
        f"SELECT * FROM {schema}.exginfo WHERE {perp_asset_where(market, asset_class)}"
    )


def _resample_extra_predicate(
    schema: str,
    market: str,
    asset_class: Optional[str],
    row_columns: bool = False,
) -> Optional[str]:
    """asset_class predicate shared by the resample and its symbol-count query.

    ``row_columns``: the kline table carries ``contract_type`` /
    ``underlying_type`` on every row (binance-ingestor >= 0.7.0), so the rows are
    filtered directly. This covers delisted symbols and gives each row the class
    it had at its own time. Without the columns the symbols come from
    ``data.exginfo``, which only knows what trades now.
    """
    if asset_class is None:
        return None
    where = perp_asset_where(market, asset_class)       # also validates market and value
    if row_columns:
        return f"({where.split(' AND ', 1)[1]})"
    return (
        "symbol IN ("
        f"SELECT symbol FROM {schema}.exginfo "
        f"WHERE {perp_asset_where(market, asset_class)}"
        ")"
    )


def build_market_kline_sql(
    schema: str,
    suffix: str,
    market: str,
    interval: int,
    offset: int,
    begin: datetime,
    end: datetime,
    kline_interval_minutes: int,
    symbol: Optional[str] = None,
    asset_class: Optional[str] = None,
    *,
    end_inclusive: bool = True,
    row_columns: bool = False,
) -> str:
    """Resample SQL. asset_class filters a perp market, on the rows' own asset
    columns when ``row_columns`` is set and through exginfo otherwise.

    ``end_inclusive`` is False on every chunk except the last. Those chunks
    stop on a resample-bucket boundary so a bucket is not split in half.
    """
    if market not in _KLINE_MARKETS:
        raise ValueError(f"Unsupported market: {market}")
    return build_duckdb_resample_sql(
        f"{schema}.{market}{suffix}",
        interval,
        offset,
        begin,
        end,
        kline_interval_minutes,
        symbol,
        extra_predicate=_resample_extra_predicate(schema, market, asset_class, row_columns),
        end_inclusive=end_inclusive,
    )


# A duckport.query DoGet whose result has no rows fails with this message.
_EMPTY_RESULT = "Server never sent a data message"


def _is_oom(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "out of memory" in text or "could not allocate block" in text


class DuckportConsumer:
    """Consumer client for duckport-rs gRPC database service.

    Args:
        addr: duckport-rs gRPC address (host:port).
        schema: DuckDB schema name where tables live (e.g. "data").
        kline_interval_minutes: Base K-line interval in minutes (default 1).
        suffix: Table name suffix derived from kline_interval (e.g. "_1m").
        pqt_path: *Deprecated (Plan A).* Ignored. Previously used for hybrid
            `read_parquet` + hot table queries; all resample now uses DuckDB only.
        redundancy_hours: *Deprecated (Plan A).* Ignored.
        token: Bearer token for non-loopback access. Defaults to ``DUCKPORT_TOKEN``.
            Empty means the client sends no Authorization header. Loopback servers
            do not require a token.
    """

    def __init__(
        self,
        addr: str = "localhost:50051",
        schema: str = "data",
        kline_interval_minutes: int = 1,
        suffix: str = "_1m",
        pqt_path: Optional[str] = None,
        redundancy_hours: int = 1,
        token: Optional[str] = None,
    ):
        self._token = resolve_token(token)
        self._location = normalize_flight_location(addr)
        self._client = open_flight_client(self._location, token=self._token)
        self._schema = schema
        self._kline_interval_minutes = kline_interval_minutes
        self._suffix = suffix
        self._pqt_path = pqt_path
        self._redundancy_hours = redundancy_hours
        self._plan_a_hybrid_warned = False
        self._asset_columns: dict[str, bool] = {}
        self._watch_supported = True
        logger.info("DuckportConsumer connected to %s", self._location)
        if pqt_path is not None or redundancy_hours != 1:
            self._log_plan_a_deprecation()

    def _log_plan_a_deprecation(self) -> None:
        if self._plan_a_hybrid_warned:
            return
        self._plan_a_hybrid_warned = True
        logger.warning(
            "Plan A: pqt_path / redundancy_hours 已弃用（混合 Parquet+热表查询已关闭）。"
            "resample 仅查询 DuckDB 主表；可移除这些参数。 "
            f"pqt_path={self._pqt_path!r} redundancy_hours={self._redundancy_hours}"
        )

    # ------------------------------------------------------------------
    # Low-level RPC methods
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pa.Table:
        """Execute arbitrary read-only SQL via the custom duckport.query DoGet.

        Returns the full result as a PyArrow Table.
        """
        ticket_data = json.dumps({"type": "duckport.query", "sql": sql})
        ticket = flight.Ticket(ticket_data.encode("utf-8"))
        reader = self._client.do_get(ticket)
        return reader.read_all()

    def ping(self) -> dict:
        """Health check via duckport.ping DoAction."""
        action = flight.Action("duckport.ping", b"")
        results = list(self._client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def execute(self, sql: str) -> dict:
        """Execute a single DDL/DML statement via duckport.execute DoAction."""
        body = json.dumps({"sql": sql}).encode("utf-8")
        action = flight.Action("duckport.execute", body)
        results = list(self._client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def execute_transaction(self, statements: list[str]) -> dict:
        """Execute multiple statements atomically via duckport.execute_transaction."""
        body = json.dumps({"statements": statements}).encode("utf-8")
        action = flight.Action("duckport.execute_transaction", body)
        results = list(self._client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def read_duck_time(self, market: str) -> Optional[datetime]:
        """Read the latest duck_time for a market from config_dict."""
        try:
            table = self.query(
                f"SELECT value FROM {self._schema}.config_dict "
                f"WHERE key = '{market}_duck_time'"
            )
            if table.num_rows == 0:
                return None
            ts_str = table.column("value")[0].as_py()
            return pd.to_datetime(ts_str).tz_localize(tz=timezone.utc)
        except Exception as e:
            logger.warning("Failed to read duck_time for %s: %s", market, e)
            return None

    def watch_watermark(self, timeout: Optional[float] = None) -> Iterator[dict]:
        """Subscribe to ``data.watermark`` via the duckport.watch_watermark DoAction.

        Yields ``{table_name: duck_time}`` (aware UTC, None when unset): the current
        state at once, then again after each duck_time change, and every 15 s when
        nothing changes. ``timeout`` bounds the whole call in seconds and ends it with
        ``FlightTimedOutError``. The stream also ends when the server shuts down.
        """
        options = flight.FlightCallOptions(timeout=timeout)
        action = flight.Action(WATCH_WATERMARK_ACTION, b"")
        for result in self._client.do_action(action, options=options):
            message = json.loads(result.body.to_pybytes())
            yield {w["table_name"]: _utc(w["duck_time"]) for w in message["watermarks"]}

    def wait_duck_time(
        self,
        market: str,
        after: Optional[datetime] = None,
        timeout: Optional[float] = None,
    ) -> Optional[datetime]:
        """Block until the watermark of ``{market}{suffix}`` is later than ``after``.

        Returns the new duck_time (aware UTC); its data is already readable. With
        ``after=None`` the current duck_time is returned as soon as the table has one.
        Returns None when ``timeout`` seconds pass first. A naive ``after`` is UTC.

        The server pushes the change, so there is no polling delay. Against a server
        without duckport.watch_watermark this polls ``data.watermark`` once a second.
        """
        table = f"{market}{self._suffix}"
        after = _utc(after)
        deadline = None if timeout is None else time.monotonic() + timeout

        def left(cap: float) -> float:
            return cap if deadline is None else min(cap, deadline - time.monotonic())

        def reached(duck_time: Optional[datetime]) -> bool:
            return duck_time is not None and (after is None or duck_time > after)

        while left(1.0) > 0:
            if not self._watch_supported:
                duck_time = self._read_watermark(table)
                if reached(duck_time):
                    return duck_time
                time.sleep(max(0.0, left(_WATCH_POLL_SECONDS)))
                continue
            try:
                for watermarks in self.watch_watermark(timeout=left(_WATCH_CALL_SECONDS)):
                    if reached(watermarks.get(table)):
                        return watermarks[table]
            except flight.FlightTimedOutError:
                continue
            except flight.FlightUnavailableError as e:
                logger.warning("watermark watch lost (%s); reconnecting", e)
            except pa.ArrowInvalid as e:
                if "unknown duckport action" not in str(e):
                    raise
                logger.warning("server has no %s; polling data.watermark instead",
                               WATCH_WATERMARK_ACTION)
                self._watch_supported = False
                continue
            # the stream ended (server shutting down) or the server is unreachable
            time.sleep(max(0.0, left(_WATCH_POLL_SECONDS)))
        return None

    def _read_watermark(self, table: str) -> Optional[datetime]:
        result = self.query(
            f"SELECT count(*) AS n, max(duck_time) AS duck_time "
            f"FROM {self._schema}.watermark WHERE table_name = '{table}'"
        )
        return _utc(result.column("duck_time")[0].as_py())

    # ------------------------------------------------------------------
    # High-level query methods (compatible with old FlightClient)
    # ------------------------------------------------------------------

    def get_market(
        self,
        market: str,
        interval: int = 60,
        offset: int = 0,
        begin: Optional[datetime] = None,
        end: Optional[datetime] = None,
        *,
        asset_class: Optional[str] = None,
    ) -> pa.Table:
        """Resample K-line data for all symbols in a market.

        Mirrors old FlightClient.get_market(). ``asset_class`` applies only to
        the perp markets (``usdt_perp``, ``others_perp``). Omit it to keep the original mixed result. ``crypto``
        and ``tradifi`` select contract types. ``EQUITY`` also includes HK, KR
        and CN equity. Other values match ``exginfo.underlying_type``.

        A wide window is queried in several bucket-aligned pieces and returned
        as one table. ``query()`` is not split.
        """
        return self._resample_query(
            market, interval, offset, begin, end, asset_class=asset_class,
        )

    def get_symbol(
        self,
        market: str,
        symbol: str,
        interval: int = 60,
        offset: int = 0,
        begin: Optional[datetime] = None,
        end: Optional[datetime] = None,
        *,
        asset_class: Optional[str] = None,
    ) -> pa.Table:
        """Resample K-line data for a single symbol.

        Mirrors old FlightClient.get_symbol(). See :meth:`get_market` for
        ``asset_class``.
        """
        return self._resample_query(
            market, interval, offset, begin, end, symbol,
            asset_class=asset_class,
        )

    def get_exginfo(
        self,
        market: str,
        *,
        asset_class: Optional[str] = None,
    ) -> pa.Table:
        """Fetch exchange info for a market.

        Mirrors old FlightClient.get_exginfo(). ``usdt_perp`` and ``usdt_spot``
        without ``asset_class`` are unchanged. On a perp market, ``asset_class``
        restricts rows the same way as :meth:`get_market`.
        """
        return self.query(build_exginfo_sql(self._schema, market, asset_class))

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resample_query(
        self,
        market: str,
        interval: int,
        offset: int,
        begin: Optional[datetime],
        end: Optional[datetime],
        symbol: Optional[str] = None,
        asset_class: Optional[str] = None,
    ) -> pa.Table:
        if self._pqt_path is not None or self._redundancy_hours != 1:
            self._log_plan_a_deprecation()
        if market not in _KLINE_MARKETS:
            raise ValueError(f"Unsupported market: {market}")
        if interval <= 0:
            raise ValueError(f"interval must be positive, got {interval}")
        if 60 % interval != 0:
            raise ValueError(f"interval {interval} must divide 60 evenly")
        if interval % self._kline_interval_minutes != 0:
            raise ValueError(
                f"interval {interval} must be a multiple of "
                f"kline_interval_minutes ({self._kline_interval_minutes})"
            )
        if offset >= interval or offset % self._kline_interval_minutes != 0:
            raise ValueError(
                f"Offset must be a multiple of {self._kline_interval_minutes} "
                f"and less than interval ({interval})"
            )

        if begin is None:
            begin = datetime.now(tz=timezone.utc) - timedelta(days=90)
        elif isinstance(begin, str):
            begin = pd.to_datetime(begin)
            begin = begin.tz_convert(timezone.utc) if begin.tzinfo is not None else begin.tz_localize(timezone.utc)
        if end is None:
            end = datetime.now(tz=timezone.utc)
        elif isinstance(end, str):
            end = pd.to_datetime(end)
            end = end.tz_convert(timezone.utc) if end.tzinfo is not None else end.tz_localize(timezone.utc)

        row_columns = asset_class is not None and self._has_asset_columns(market)
        symbol_count = self._count_resample_symbols(
            market, begin, end, symbol, asset_class, row_columns,
        )
        ranges = plan_resample_ranges(begin, end, interval, offset, symbol_count)
        logger.info(
            "resample (Plan A: DuckDB only) market=%s interval=%d offset=%d begin=%s end=%s symbol=%s asset_class=%s symbols=%d chunks=%d",
            market, interval, offset, begin, end, symbol, asset_class,
            symbol_count, len(ranges),
        )
        tables = [
            table for table in (
                self._query_resample_range(
                    market, interval, offset, lo, hi, inclusive, symbol, asset_class,
                    row_columns=row_columns,
                )
                for lo, hi, inclusive in ranges
            ) if table is not None
        ]
        if not tables:
            # what the server answers for a query without rows
            raise flight.FlightInternalError(f"{_EMPTY_RESULT} (no rows in the range)")
        if len(tables) == 1:
            return tables[0]
        return pa.concat_tables(tables)

    def _has_asset_columns(self, market: str) -> bool:
        """Whether ``market``'s kline table has the per-row asset columns.

        Tables written by binance-ingestor < 0.7.0 do not; asset_class then goes
        through exginfo as before. The answer is kept for this client's lifetime.
        """
        known = self._asset_columns.get(market)
        if known is not None:
            return known
        if market not in _PERP_MARKETS:
            return False
        names = ", ".join(f"'{name}'" for name in _ASSET_COLUMNS)
        try:
            table = self.query(
                "SELECT count(*) AS n FROM information_schema.columns "
                f"WHERE table_schema = '{self._schema}' "
                f"AND table_name = '{market}{self._suffix}' AND column_name IN ({names})"
            )
            found = int(table.column("n")[0].as_py() or 0) == len(_ASSET_COLUMNS)
        except Exception as e:
            logger.warning("asset column check failed for %s, using exginfo: %s", market, e)
            return False
        self._asset_columns[market] = found
        return found

    def _count_resample_symbols(
        self,
        market: str,
        begin: datetime,
        end: datetime,
        symbol: Optional[str],
        asset_class: Optional[str],
        row_columns: bool = False,
    ) -> int:
        if symbol is not None:
            return 1
        sql = build_symbol_count_sql(
            f"{self._schema}.{market}{self._suffix}",
            begin,
            end,
            extra_predicate=_resample_extra_predicate(
                self._schema, market, asset_class, row_columns,
            ),
        )
        table = self.query(sql)
        if table.num_rows == 0:
            return 0
        value = table.column("n")[0].as_py()
        return int(value or 0)

    def _query_resample_range(
        self,
        market: str,
        interval: int,
        offset: int,
        begin: datetime,
        end: datetime,
        end_inclusive: bool,
        symbol: Optional[str],
        asset_class: Optional[str],
        depth: int = 0,
        *,
        row_columns: bool = False,
    ) -> Optional[pa.Table]:
        """One chunk of a resample; None when it has no rows.

        The server answers a zero-row query with an error, not an empty table. A chunk
        without rows (before a listing, an asset class that did not exist yet) must not
        fail the chunks that have them.
        """
        sql = build_market_kline_sql(
            self._schema,
            self._suffix,
            market,
            interval,
            offset,
            begin,
            end,
            self._kline_interval_minutes,
            symbol,
            asset_class,
            end_inclusive=end_inclusive,
            row_columns=row_columns,
        )
        try:
            return self.query(sql)
        except Exception as exc:
            halves = None
            if _is_oom(exc) and depth < 16:
                halves = halve_range(begin, end, end_inclusive, interval, offset)
            if halves is None:
                if _EMPTY_RESULT in str(exc):
                    return None
                logger.error("resample query failed: %s", exc)
                raise
            logger.warning(
                "resample chunk out of memory, halving %s .. %s", begin, end,
            )
            parts = [
                part for part in (
                    self._query_resample_range(
                        market, interval, offset, lo, hi, inclusive,
                        symbol, asset_class, depth + 1, row_columns=row_columns,
                    )
                    for lo, hi, inclusive in halves
                ) if part is not None
            ]
            return pa.concat_tables(parts) if parts else None

    def close(self):
        """Close the Flight client connection."""
        try:
            self._client.close()
            logger.info("DuckportConsumer connection closed")
        except Exception as e:
            logger.warning("Error closing connection: %s", e)
