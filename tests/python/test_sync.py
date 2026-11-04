#!/usr/bin/env python3
"""Planner and SQL tests for scripts/sync.py. No duckport-server required."""

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts"))

from duckport_flight import flight_client_kwargs
from reconcile.specs import TableSpec
from sync import (
    Catalog, Column, Plan, build_plans, compare_columns, create_table_sql, initial_window,
    is_safe_default, iter_batches, merge_sql, page_rows_for, shrink_window, watermark_sql,
)

T0 = datetime(2024, 1, 1)
T1 = datetime(2024, 6, 1)
T2 = datetime(2024, 12, 1)


def col(name, data_type="VARCHAR", nullable=True, default=None):
    return Column(name, data_type, nullable, default)


def kline_cols():
    return [
        col("open_time", "TIMESTAMP", False),
        col("symbol", "VARCHAR", False),
        col("close", "DOUBLE", True),
    ]


def wm(duck, start=None, ingestor="bn", time_column="open_time"):
    return {
        "duck_time": duck, "start_time": start, "ingestor": ingestor,
        "time_column": time_column, "max_lag_seconds": 900,
    }


def spec(table, dedup, time_column="open_time", kind="append"):
    return TableSpec(
        table=table, plugin="writer", kind=kind, columns={}, dedup_key=tuple(dedup),
        time_column=time_column, write_mode="anti_join",
    )


def catalog(tables, columns, watermarks=None, specs=None, keys=None, write_modes=None):
    return Catalog(
        tables=set(tables),
        columns=columns,
        watermarks=watermarks or {},
        specs=specs or {},
        spec_rows={},
        write_modes=write_modes or {},
        keys=keys or {},
    )


def by_name(plans):
    return {plan.name: plan for plan in plans}


def test_flight_token_override():
    assert flight_client_kwargs("") == {}
    assert flight_client_kwargs(None) == {}
    kwargs = flight_client_kwargs("secret")
    assert kwargs["middleware"][0]._token == "secret"


def test_column_order_type_and_nulls():
    source = kline_cols()
    assert compare_columns(source, list(source)) == []
    assert compare_columns(source, [col("open_time", "timestamp", False), *source[1:]]) == []
    swapped = [source[1], source[0], source[2]]
    assert any("列顺序不一致" in item for item in compare_columns(source, swapped))
    changed = [source[0], col("symbol", "INTEGER", False), source[2]]
    assert any("类型不一致" in item for item in compare_columns(source, changed))
    nulls = [source[0], col("symbol", "VARCHAR", True), source[2]]
    assert any("空值约束不一致" in item for item in compare_columns(source, nulls))
    assert any("目标缺少列" in item for item in compare_columns(source, source[:2]))
    assert any("目标多出列" in item for item in compare_columns(source[:2], source))


def test_mode1_aligns_only_matching_tables():
    columns = {"usdt_perp_1m": kline_cols(), "only_source": kline_cols(), "exginfo": [col("market"), col("symbol")]}
    source = catalog(
        ["usdt_perp_1m", "only_source", "exginfo", "watermark"],
        columns,
        {"usdt_perp_1m": wm(T1, T0)},
    )
    target = catalog(
        ["usdt_perp_1m", "exginfo", "watermark"],
        {"usdt_perp_1m": kline_cols(), "exginfo": [col("market"), col("symbol")]},
        {"usdt_perp_1m": wm(T0, T0, ingestor="local")},
    )
    plans = by_name(build_plans(source, target, None))
    assert "only_source" not in plans
    assert plans["usdt_perp_1m"].action == "copy"
    assert plans["usdt_perp_1m"].lower == T0
    assert plans["usdt_perp_1m"].upper == T1
    assert plans["exginfo"].action == "skip"
    sql = watermark_sql(plans["usdt_perp_1m"])[0]
    assert "UPDATE data.watermark" in sql
    assert "ingestor" not in sql.split("SET", 1)[1]


def test_watermark_does_not_move_backward_and_null_lower_is_open():
    columns = {"usdt_perp_1m": kline_cols()}
    source = catalog(["usdt_perp_1m"], columns, {"usdt_perp_1m": wm(T1)})
    ahead = catalog(["usdt_perp_1m"], columns, {"usdt_perp_1m": wm(T2)})
    equal = catalog(["usdt_perp_1m"], columns, {"usdt_perp_1m": wm(T1)})
    empty = catalog(["usdt_perp_1m"], columns, {})
    assert build_plans(source, ahead, None)[0].action == "skip"
    assert build_plans(source, equal, None)[0].action == "skip"
    fresh = build_plans(source, empty, None)[0]
    assert fresh.action == "copy"
    assert fresh.lower is None
    assert fresh.upper == T1
    assert "INSERT INTO data.watermark" in watermark_sql(fresh)[0]


def test_schema_mismatch_rejects_before_copy():
    source = catalog(
        ["usdt_perp_1m"],
        {"usdt_perp_1m": kline_cols()},
        {"usdt_perp_1m": wm(T1)},
    )
    target = catalog(
        ["usdt_perp_1m"],
        {"usdt_perp_1m": [kline_cols()[1], kline_cols()[0], kline_cols()[2]]},
        {"usdt_perp_1m": wm(T0)},
    )
    plan = build_plans(source, target, None)[0]
    assert plan.action == "reject"
    assert plan.differences

    typed = catalog(
        ["usdt_perp_1m"],
        {"usdt_perp_1m": [kline_cols()[0], col("symbol", "INTEGER", False), kline_cols()[2]]},
        {"usdt_perp_1m": wm(T0)},
    )
    assert build_plans(source, typed, None)[0].action == "reject"
    nullable = catalog(
        ["usdt_perp_1m"],
        {"usdt_perp_1m": [kline_cols()[0], col("symbol", "VARCHAR", True), kline_cols()[2]]},
        {"usdt_perp_1m": wm(T0)},
    )
    assert build_plans(source, nullable, None)[0].action == "reject"


def test_registered_dedup_must_match_only_when_both_sides_have_specs():
    columns = {"custom": [col("id", "INTEGER", False), col("ts", "TIMESTAMP", False)]}
    left = spec("custom", ("id",), "ts")
    right = spec("custom", ("ts",), "ts")
    source = catalog(["custom"], columns, {"custom": wm(T1, time_column="ts")}, {"custom": left})
    both = catalog(["custom"], columns, {"custom": wm(T0, time_column="ts")}, {"custom": right})
    rejected = build_plans(source, both, None)[0]
    assert rejected.action == "reject"
    assert any("去重键不一致" in item for item in rejected.differences)

    target = catalog(["custom"], columns, {"custom": wm(T0, time_column="ts")})
    # No builtin spec for "custom"; the source registration is the dedup key.
    assert build_plans(source, target, None)[0].action == "copy"


def test_mode2_creates_missing_table_and_checks_existing():
    columns = {"usdt_perp_funding": [
        col("funding_time", "TIMESTAMP", False), col("symbol", "VARCHAR", False),
    ]}
    source = catalog(
        ["usdt_perp_funding"],
        columns,
        {"usdt_perp_funding": wm(T1, T0, time_column="funding_time")},
    )
    missing = build_plans(source, catalog([], {}), ["usdt_perp_funding"])[0]
    assert missing.action == "create"
    assert missing.upper == T1
    assert missing.copy_all is False
    created = create_table_sql(missing.name, missing.columns)
    assert "PRIMARY KEY" not in created
    assert '"funding_time" TIMESTAMP NOT NULL' in created

    no_time = catalog(["usdt_perp_funding"], columns, {})
    whole = build_plans(no_time, catalog([], {}), ["usdt_perp_funding"])[0]
    assert whole.action == "create" and whole.copy_all is True
    assert watermark_sql(whole) == []

    bad = catalog(
        ["usdt_perp_funding"],
        {"usdt_perp_funding": [col("funding_time", "VARCHAR", False), col("symbol", "VARCHAR", False)]},
        {},
    )
    existing = build_plans(source, bad, ["usdt_perp_funding"])[0]
    assert existing.action == "reject"
    assert build_plans(source, catalog([], {}), ["missing"])[0].action == "reject"


def test_primary_key_is_a_note_not_a_rejection():
    columns = {"usdt_perp_1m": kline_cols()}
    source = catalog(["usdt_perp_1m"], columns, {"usdt_perp_1m": wm(T1)}, keys={"usdt_perp_1m": {"PRIMARY KEY"}})
    target = catalog(["usdt_perp_1m"], columns, {"usdt_perp_1m": wm(T0)})
    plan = build_plans(source, target, None)[0]
    assert plan.action == "copy"
    assert any("不拦截" in note for note in plan.notes)


def test_page_and_window_sizes():
    assert page_rows_for(0, 0) == 200_000
    assert page_rows_for(1000, 1000 * 1000) == 12 * 1024 * 1024 // 1000
    assert page_rows_for(10, 100) == 200_000
    assert initial_window(timedelta(minutes=1)) == timedelta(hours=1)
    assert initial_window(timedelta(hours=8)) == timedelta(days=1)
    assert shrink_window(timedelta(hours=2), 10_000, 100) < timedelta(hours=2)
    assert shrink_window(timedelta(seconds=2), 10_000, 1) == timedelta(seconds=1)


def test_merge_sql_window_and_duckdb():
    columns = tuple(kline_cols())
    keyed = Plan(
        name="usdt_perp_1m", action="copy", dedup_key=("open_time", "symbol"),
        time_column="open_time", columns=columns,
    )
    sql = merge_sql(keyed, "main._sync_usdt_perp_1m", T0, T1)
    assert "ANTI JOIN" in sql
    assert "BETWEEN" in sql
    assert "ON CONFLICT" not in sql.upper()

    unkeyed_time = Plan(
        name="announcements", action="copy", dedup_key=("catalog_id", "code"),
        time_column="release_time",
        columns=(col("catalog_id", "INTEGER", False), col("code"), col("release_time", "TIMESTAMP", False)),
    )
    assert "BETWEEN" not in merge_sql(unkeyed_time, "staging", T0, T1)

    import duckdb
    con = duckdb.connect()
    con.execute("CREATE SCHEMA data")
    con.execute("CREATE TABLE data.usdt_perp_1m (open_time TIMESTAMP NOT NULL, symbol VARCHAR NOT NULL, close DOUBLE)")
    con.execute("CREATE TABLE staging (open_time TIMESTAMP NOT NULL, symbol VARCHAR NOT NULL, close DOUBLE)")
    con.execute(
        "INSERT INTO data.usdt_perp_1m VALUES (TIMESTAMP '2024-01-01 00:00:00', 'A', 1), "
        "(TIMESTAMP '2024-01-01 00:01:00', 'A', 2)"
    )
    con.execute(
        "INSERT INTO staging VALUES "
        "(TIMESTAMP '2024-01-01 00:00:00', 'A', 9), "
        "(TIMESTAMP '2024-01-01 00:01:00', 'A', 9), "
        "(TIMESTAMP '2024-01-01 00:02:00', 'B', 3), "
        "(TIMESTAMP '2024-01-01 00:02:00', 'B', 4)"
    )
    con.execute(merge_sql(keyed, "staging", datetime(2024, 1, 1), datetime(2024, 1, 1, 0, 2)))
    con.execute(merge_sql(keyed, "staging", datetime(2024, 1, 1), datetime(2024, 1, 1, 0, 2)))
    rows = con.execute("SELECT close FROM data.usdt_perp_1m WHERE symbol = 'B'").fetchall()
    assert len(rows) == 1 and rows[0][0] in (3, 4)
    assert con.execute("SELECT count(*) FROM data.usdt_perp_1m").fetchone()[0] == 3
    kept = con.execute("SELECT close FROM data.usdt_perp_1m WHERE symbol = 'A' ORDER BY open_time").fetchall()
    assert kept == [(1,), (2,)]

    con.execute(
        "CREATE TABLE data.watermark ("
        "table_name VARCHAR PRIMARY KEY, ingestor VARCHAR, max_lag_seconds INTEGER, "
        "time_column VARCHAR, start_time TIMESTAMP, duck_time TIMESTAMP, updated_at TIMESTAMP)"
    )
    con.execute(
        "INSERT INTO data.watermark VALUES ('usdt_perp_1m', 'local', 1, 'open_time', "
        "TIMESTAMP '2024-01-01', TIMESTAMP '2024-02-01', TIMESTAMP '2024-02-01')"
    )
    plan = build_plans(
        catalog(["usdt_perp_1m"], {"usdt_perp_1m": kline_cols()},
                {"usdt_perp_1m": wm(T1, datetime(2020, 1, 1), ingestor="remote")}),
        catalog(["usdt_perp_1m"], {"usdt_perp_1m": kline_cols()},
                {"usdt_perp_1m": wm(datetime(2024, 2, 1), T0, ingestor="local")}),
        None,
    )[0]
    update = watermark_sql(plan)[0]
    con.execute(update)
    row = con.execute(
        "SELECT ingestor, start_time, duck_time FROM data.watermark WHERE table_name = 'usdt_perp_1m'"
    ).fetchone()
    assert row[0] == "local"
    assert row[1] == T0
    assert row[2] == T1

    # Insert path: a new table, then the same statement again must not replace ingestor or start_time.
    fresh = build_plans(
        catalog(["usdt_perp_funding"], {"usdt_perp_funding": [
            col("funding_time", "TIMESTAMP", False), col("symbol", "VARCHAR", False),
        ]}, {"usdt_perp_funding": wm(T1, datetime(2020, 1, 1), ingestor="remote", time_column="funding_time")}),
        catalog([], {}),
        ["usdt_perp_funding"],
    )[0]
    insert = watermark_sql(fresh)[0]
    con.execute(insert)
    con.execute(
        "UPDATE data.watermark SET ingestor = 'local', start_time = TIMESTAMP '2024-03-01' "
        "WHERE table_name = 'usdt_perp_funding'"
    )
    con.execute(insert)
    inserted = con.execute(
        "SELECT ingestor, start_time, duck_time FROM data.watermark WHERE table_name = 'usdt_perp_funding'"
    ).fetchone()
    assert inserted[0] == "local"
    assert inserted[1] == datetime(2024, 3, 1)
    assert inserted[2] == T1


def test_mode2_keeps_column_defaults():
    import duckdb
    exginfo = [
        col("market", "VARCHAR", True), col("symbol", "VARCHAR", True),
        col("created_time", "TIMESTAMP", True, "CURRENT_TIMESTAMP"),
    ]
    source = catalog(["exginfo"], {"exginfo": exginfo}, {})
    plan = build_plans(source, catalog([], {}), ["exginfo"])[0]
    assert plan.action == "create"
    con = duckdb.connect()
    con.execute("CREATE SCHEMA data")
    con.execute(create_table_sql(plan.name, plan.columns))
    # The plugin refreshes exginfo without naming created_time.
    con.execute("INSERT INTO data.exginfo (market, symbol) VALUES ('usdt_perp', 'A')")
    assert con.execute("SELECT created_time IS NOT NULL FROM data.exginfo").fetchone()[0]
    assert con.execute(
        "SELECT column_default FROM duckdb_columns() WHERE table_name = 'exginfo' AND column_name = 'created_time'"
    ).fetchone()[0] == "CURRENT_TIMESTAMP"
    # Staging receives every column, so it carries no defaults.
    assert "DEFAULT" not in create_table_sql("_sync_exginfo", plan.columns, schema="main", defaults=False)

    # Defaults as duckdb_columns() prints them round-trip through the created table.
    printed = ["0", "-1.5", "'it''s'", "NULL", "CAST('t' AS BOOLEAN)", "CAST('2020-01-01' AS TIMESTAMP)",
               "now()", "current_date", "get_current_timestamp()"]
    for expr in printed:
        assert is_safe_default(expr), expr
    for expr in ("nextval('s')", "main.list_value('x')", "(1 + 2)", "0); DROP TABLE data.x; --"):
        assert not is_safe_default(expr), expr

    seq = catalog(["ids"], {"ids": [col("id", "BIGINT", False, "nextval('s')"), col("ts", "TIMESTAMP", False)]}, {})
    seq.specs["ids"] = spec("ids", ("id",), "ts")
    rejected = build_plans(seq, catalog([], {}), ["ids"])[0]
    assert rejected.action == "reject"
    assert "nextval" in rejected.reason

    # Align mode does not create tables, so an uncopyable default does not block it.
    target = catalog(["ids"], {"ids": seq.columns["ids"]}, {}, specs={"ids": spec("ids", ("id",), "ts")})
    seq.watermarks["ids"] = wm(T1, time_column="ts")
    assert build_plans(seq, target, None)[0].action == "copy"


class _DuckSource:
    """query_arrow over an in-process DuckDB, standing in for the source Flight."""

    def __init__(self, con):
        self.con = con
        self.sql = []

    def query_arrow(self, sql):
        self.sql.append(sql)
        result = self.con.execute(sql).arrow()
        return result.read_all() if hasattr(result, "read_all") else result


def test_keyset_pages_read_every_row():
    import duckdb
    con = duckdb.connect()
    con.execute("CREATE SCHEMA data")
    # Inserted out of key order, as an unkeyed table filled by several writers would be.
    con.execute(
        "CREATE TABLE data.refs AS SELECT (i * 7919) % 1000 AS k, "
        "TIMESTAMP '2024-01-01' + to_seconds((i % 7)::BIGINT) AS ts, i AS v FROM range(1000) r(i)"
    )
    source = _DuckSource(con)
    columns = (col("k", "BIGINT", False), col("ts", "TIMESTAMP", False), col("v", "BIGINT", False))

    single = Plan(name="refs", action="create", dedup_key=("k",), columns=columns, copy_all=True)
    keys = [k for batch in iter_batches(source, single, 100) for k in batch.column("k").to_pylist()]
    assert sorted(keys) == list(range(1000))
    assert len(keys) == 1000
    assert all("ORDER BY" in sql for sql in source.sql)

    # Two-column key, as in the fallback after the time window cannot shrink further.
    composite = Plan(name="refs", action="create", dedup_key=("ts", "k"), columns=columns, copy_all=True)
    pairs = [
        pair for batch in iter_batches(source, composite, 64)
        for pair in zip(batch.column("ts").to_pylist(), batch.column("k").to_pylist())
    ]
    assert len(pairs) == 1000 and len(set(pairs)) == 1000


def main():
    test_flight_token_override()
    test_column_order_type_and_nulls()
    test_mode1_aligns_only_matching_tables()
    test_watermark_does_not_move_backward_and_null_lower_is_open()
    test_schema_mismatch_rejects_before_copy()
    test_registered_dedup_must_match_only_when_both_sides_have_specs()
    test_mode2_creates_missing_table_and_checks_existing()
    test_primary_key_is_a_note_not_a_rejection()
    test_page_and_window_sizes()
    test_merge_sql_window_and_duckdb()
    test_mode2_keeps_column_defaults()
    test_keyset_pages_read_every_row()
    print("ok")


if __name__ == "__main__":
    main()
