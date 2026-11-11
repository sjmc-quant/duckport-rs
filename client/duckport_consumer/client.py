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
from datetime import datetime, timedelta, timezone
from typing import Optional

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


def _asset_class_clause(asset_class: str) -> str:
    """Extra exginfo predicate for a perp market's asset_class value.

    ``crypto`` / ``tradifi`` select contract types. ``EQUITY`` also includes
    HK, KR and CN equity. Any other value matches ``underlying_type`` exactly.
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
) -> Optional[str]:
    """Symbol predicate shared by the resample and its symbol-count query."""
    if asset_class is None:
        return None
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
) -> str:
    """Resample SQL. asset_class filters a perp market's symbols via exginfo.

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
        extra_predicate=_resample_extra_predicate(schema, market, asset_class),
        end_inclusive=end_inclusive,
    )


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

        symbol_count = self._count_resample_symbols(
            market, begin, end, symbol, asset_class,
        )
        ranges = plan_resample_ranges(begin, end, interval, offset, symbol_count)
        logger.info(
            "resample (Plan A: DuckDB only) market=%s interval=%d offset=%d begin=%s end=%s symbol=%s asset_class=%s symbols=%d chunks=%d",
            market, interval, offset, begin, end, symbol, asset_class,
            symbol_count, len(ranges),
        )
        tables = [
            self._query_resample_range(
                market, interval, offset, lo, hi, inclusive, symbol, asset_class,
            )
            for lo, hi, inclusive in ranges
        ]
        if len(tables) == 1:
            return tables[0]
        return pa.concat_tables(tables)

    def _count_resample_symbols(
        self,
        market: str,
        begin: datetime,
        end: datetime,
        symbol: Optional[str],
        asset_class: Optional[str],
    ) -> int:
        if symbol is not None:
            return 1
        sql = build_symbol_count_sql(
            f"{self._schema}.{market}{self._suffix}",
            begin,
            end,
            extra_predicate=_resample_extra_predicate(
                self._schema, market, asset_class,
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
    ) -> pa.Table:
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
        )
        try:
            return self.query(sql)
        except Exception as exc:
            halves = None
            if _is_oom(exc) and depth < 16:
                halves = halve_range(begin, end, end_inclusive, interval, offset)
            if halves is None:
                logger.error("resample query failed: %s", exc)
                raise
            logger.warning(
                "resample chunk out of memory, halving %s .. %s", begin, end,
            )
            left, right = halves
            return pa.concat_tables([
                self._query_resample_range(
                    market, interval, offset, left[0], left[1], left[2],
                    symbol, asset_class, depth + 1,
                ),
                self._query_resample_range(
                    market, interval, offset, right[0], right[1], right[2],
                    symbol, asset_class, depth + 1,
                ),
            ])

    def close(self):
        """Close the Flight client connection."""
        try:
            self._client.close()
            logger.info("DuckportConsumer connection closed")
        except Exception as e:
            logger.warning("Error closing connection: %s", e)
