#!/usr/bin/env python3
"""Copy business tables from one duckport into the local one.

Two modes, both read-only on the source:

    sync.py --from HOST:PORT
        Tables that exist on both sides and pass a column check. Copy rows
        after the target watermark up to the source watermark, then set the
        target duck_time to the source duck_time.

    sync.py --from HOST:PORT --table NAME [--table NAME]
        That source table. Create it on the target when it is missing, then
        fill it up to the source watermark. An existing table is aligned the
        same way as the first mode.

An existing table is rejected when the source keeps no row at or before the
target watermark (retention, or a later start): moving the watermark would
hide the span in between. Fill that span on the target first.

Rows are written in time order, one transaction per batch. The watermark
moves in that same transaction once 1000 rows have been written since the
previous move, to the batch's last timestamp. One timestamp is never split,
so a move can cover more than 1000 rows. An interrupted run keeps the last
move; rows written after it are copied again and skipped by the anti-join.
The run always finishes by setting the watermark to the source duck_time.

Only rows, the watermark, the table spec and the write mode are copied. The
source's retention policy is not: the target keeps its own.

Run ``duckport sync`` on the target machine. Writes are refused from the
public internet. Exit 0 when nothing was rejected, 1 when a table was rejected
or a copy failed, 2 when the command itself could not run.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight

from duckport_flight import Duckport, lit, q, text
from reconcile.specs import builtin_spec, registered_spec

PUBLIC_BYTES = 12 * 1024 * 1024
LAN_ROWS = 200_000
# One threshold for every table. The page cap still splits a write, and a
# timestamp is never split, so one move may cover more rows than this.
WATERMARK_EVERY_ROWS = 1000
MAX_WINDOW = timedelta(days=7)
MIN_WINDOW = timedelta(seconds=1)

SYSTEM_TABLES = {
    "config_dict", "watermark", "retention_policy", "retention_log", "table_specs", "upgrade_log",
}
IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
SAFE_TYPE = re.compile(r"[A-Za-z0-9_\[\],() ]+")
# Column defaults as duckdb_columns() prints them. Only literals and clock
# functions are copied; anything else (nextval, macros) is rejected at plan time.
_LITERAL = r"(?:'(?:[^']|'')*'|-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)"
SAFE_DEFAULT = re.compile(
    rf"(?:NULL|{_LITERAL}|CAST\({_LITERAL} AS [A-Za-z0-9_\[\],() ]+\)"
    r"|current_timestamp|current_date|current_time|now\(\)|get_current_timestamp\(\)|today\(\))",
    re.IGNORECASE,
)
KLINE_TABLE = re.compile(r"^(usdt_perp|usdt_spot)_\d+[mhd]$")

SPEC_FIELDS = (
    "table_name", "writer", "writer_version", "kind", "time_column", "period_seconds",
    "dedup_key", "write_mode", "columns", "column_defaults", "string_checks", "legacy_columns",
)
ARRAY_FIELDS = {"dedup_key", "legacy_columns"}


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    nullable: bool
    default: object = None


@dataclass
class Catalog:
    tables: set = field(default_factory=set)
    columns: dict = field(default_factory=dict)
    watermarks: dict = field(default_factory=dict)
    specs: dict = field(default_factory=dict)
    spec_rows: dict = field(default_factory=dict)
    write_modes: dict = field(default_factory=dict)
    keys: dict = field(default_factory=dict)
    kline_interval: str | None = None


@dataclass
class Plan:
    name: str
    action: str  # copy, create, skip, reject
    reason: str = ""
    differences: tuple = ()
    notes: tuple = ()
    lower: datetime | None = None
    upper: datetime | None = None
    copy_all: bool = False
    dedup_key: tuple = ()
    time_column: str | None = None
    kind: str = "append"
    columns: tuple = ()
    source_watermark: dict | None = None
    target_watermark: dict | None = None
    spec_row: dict | None = None
    write_mode: str | None = None
    write_spec: bool = False
    set_write_mode: bool = False


def as_naive(value):
    """UTC-naive datetime. None stays None."""
    if value is None:
        return None
    if isinstance(value, str):
        text_value = value.strip().replace("T", " ")
        if text_value.endswith("Z"):
            text_value = text_value[:-1]
        for mark in ("+", "-"):
            idx = text_value.find(mark, 11)
            if idx != -1:
                text_value = text_value[:idx]
                break
        return datetime.fromisoformat(text_value)
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    raise TypeError(f"unsupported timestamp {value!r}")


def fmt(value):
    return as_naive(value).strftime("%Y-%m-%d %H:%M:%S.%f")


def fmt_opt(value):
    if value is None:
        return "无"
    return fmt(value)


def nullable_of(value):
    if isinstance(value, str):
        return value.strip().upper() in {"YES", "TRUE", "1"}
    return bool(value)


def normalize_type(value):
    return " ".join(str(value).strip().split()).casefold()


def is_business(name):
    return name not in SYSTEM_TABLES and not name.endswith("__pk_old") and not name.endswith("__nopk")


def page_rows_for(num_rows, nbytes, cap_rows=LAN_ROWS, cap_bytes=PUBLIC_BYTES):
    """Rows that stay under the public DoGet ceiling and the LAN chunk size."""
    if not num_rows:
        return cap_rows
    per_row = max(1, math.ceil(nbytes / num_rows))
    return max(1, min(cap_rows, cap_bytes // per_row))


def initial_window(period):
    if period is None:
        return timedelta(hours=1)
    window = period * 60
    if window < timedelta(minutes=1):
        return timedelta(minutes=1)
    if window > timedelta(days=1):
        return timedelta(days=1)
    return window


def shrink_window(window, rows, page_rows):
    shrunk = window * (page_rows / max(rows, 1))
    if shrunk >= window:
        shrunk = window / 2
    if shrunk < MIN_WINDOW:
        return MIN_WINDOW
    return shrunk


def is_page_too_big(exc):
    text_value = str(exc).lower()
    return "resource_exhausted" in text_value or "deadline_exceeded" in text_value or "timeout" in text_value


def compare_columns(source, target):
    """Column order, type and nullability. Empty means the layouts match."""
    diffs = []
    source_names = [c.name for c in source]
    target_names = [c.name for c in target]
    source_set = set(source_names)
    target_set = set(target_names)
    missing = [name for name in source_names if name not in target_set]
    extra = [name for name in target_names if name not in source_set]
    if missing:
        diffs.append("目标缺少列：" + ", ".join(missing))
    if extra:
        diffs.append("目标多出列：" + ", ".join(extra))
    if not missing and not extra and source_names != target_names:
        diffs.append("列顺序不一致：源 " + ", ".join(source_names) + "；目标 " + ", ".join(target_names))
    target_by_name = {c.name: c for c in target}
    for column in source:
        other = target_by_name.get(column.name)
        if other is None:
            continue
        if normalize_type(column.data_type) != normalize_type(other.data_type):
            diffs.append(f"类型不一致 {column.name}：源 {column.data_type}，目标 {other.data_type}")
        if column.nullable != other.nullable:
            src_null = "可空" if column.nullable else "NOT NULL"
            dst_null = "可空" if other.nullable else "NOT NULL"
            diffs.append(f"空值约束不一致 {column.name}：源 {src_null}，目标 {dst_null}")
    return diffs


def layout_notes(source_columns, target_columns, source_keys, target_keys):
    notes = []
    if set(source_keys or ()) != set(target_keys or ()):
        src = ", ".join(sorted(source_keys or ())) or "无"
        dst = ", ".join(sorted(target_keys or ())) or "无"
        notes.append(f"主键或唯一约束不同：源 {src}，目标 {dst}；不拦截")
    target_defaults = {c.name: c.default for c in target_columns}
    for column in source_columns:
        if column.name not in target_defaults:
            continue
        if (column.default or None) != (target_defaults[column.name] or None):
            notes.append(
                f"列默认值不同 {column.name}：源 {column.default}，目标 {target_defaults[column.name]}；不拦截"
            )
    return notes


def spec_key_diff(source_spec, target_spec):
    diffs = []
    if tuple(source_spec.dedup_key) != tuple(target_spec.dedup_key):
        diffs.append(f"去重键不一致：源 {list(source_spec.dedup_key)}，目标 {list(target_spec.dedup_key)}")
    if (source_spec.time_column or None) != (target_spec.time_column or None):
        diffs.append(f"时间列不一致：源 {source_spec.time_column}，目标 {target_spec.time_column}")
    return diffs


def _reject(name, reason, differences=(), notes=()):
    return Plan(name=name, action="reject", reason=reason, differences=tuple(differences), notes=tuple(notes))


def _skip(name, reason, notes=()):
    return Plan(name=name, action="skip", reason=reason, notes=tuple(notes))


def _watermark_value(row, key):
    if not row or row.get(key) is None:
        return None
    return as_naive(row[key])


def _choose_spec(name, source, target, target_exists):
    """(spec, errors). Compare dedup keys only when both sides registered a spec."""
    errors = []
    if name in source.specs and source.specs[name] is None:
        return None, ["源表规格无法解析"]
    if target_exists and name in target.specs and target.specs[name] is None:
        return None, ["目标表规格无法解析"]
    source_registered = source.specs[name] if name in source.specs else None
    target_registered = target.specs[name] if target_exists and name in target.specs else None
    if source_registered is not None and target_registered is not None:
        errors.extend(spec_key_diff(source_registered, target_registered))
    spec = source_registered or builtin_spec(name) or target_registered
    return spec, errors


def _align_plan(name, source, target, columns, spec, notes):
    if spec.kind == "snapshot" and not spec.time_column:
        return _skip(name, "没有水位的快照表不在这个模式里处理", notes)
    dedup = tuple(spec.dedup_key)
    needed = list(dedup)
    if spec.time_column and spec.time_column not in needed:
        needed.append(spec.time_column)
    missing_source = [item for item in needed if item not in {c.name for c in columns}]
    missing_target = [item for item in needed if item not in {c.name for c in target.columns.get(name, ())}]
    if missing_source or missing_target:
        detail = []
        if missing_source:
            detail.append("源缺少 " + ", ".join(missing_source))
        if missing_target:
            detail.append("目标缺少 " + ", ".join(missing_target))
        return _reject(name, "去重键或时间列不在表中：" + "；".join(detail), notes=notes)
    if not dedup:
        return _reject(name, "没有去重键", notes=notes)

    try:
        source_duck = _watermark_value(source.watermarks.get(name), "duck_time")
        target_duck = _watermark_value(target.watermarks.get(name), "duck_time")
    except (TypeError, ValueError) as exc:
        return _reject(name, f"无法解析水位：{exc}", notes=notes)

    if source_duck is None:
        if spec.kind == "snapshot" or not spec.time_column:
            return _skip(name, "没有水位的快照表不在这个模式里处理", notes)
        return _skip(name, "源没有 duck_time", notes)
    if not spec.time_column:
        return _reject(name, "有水位但没有时间列，无法按水位追平", notes=notes)
    if target_duck is not None and target_duck >= source_duck:
        return _skip(name, "目标水位已经不早于源", notes)

    write_mode = source.write_modes.get(name) or spec.write_mode
    return Plan(
        name=name, action="copy", lower=target_duck, upper=source_duck,
        dedup_key=dedup, time_column=spec.time_column, kind=spec.kind or "append",
        columns=tuple(columns), source_watermark=source.watermarks.get(name),
        target_watermark=target.watermarks.get(name), spec_row=source.spec_rows.get(name),
        write_mode=write_mode, notes=tuple(notes),
        write_spec=name not in target.specs and name in source.spec_rows,
        set_write_mode=bool(write_mode) and name not in target.write_modes,
    )


def _create_plan(name, source, columns, spec):
    dedup = tuple(spec.dedup_key)
    if not dedup:
        return _reject(name, "没有去重键")
    have = {c.name for c in columns}
    missing = [item for item in dedup if item not in have]
    if spec.time_column and spec.time_column not in have:
        missing.append(spec.time_column)
    if missing:
        return _reject(name, "去重键或时间列不在表中：源缺少 " + ", ".join(missing))
    unsafe = [f"{c.name} = {c.default}" for c in columns if c.default is not None and not is_safe_default(c.default)]
    if unsafe:
        return _reject(name, "列默认值无法照抄，请先在目标手工建表：" + "；".join(unsafe))
    try:
        source_duck = _watermark_value(source.watermarks.get(name), "duck_time")
    except (TypeError, ValueError) as exc:
        return _reject(name, f"无法解析水位：{exc}")
    if source_duck is not None and not spec.time_column:
        return _reject(name, "有水位但没有时间列，无法只复制到源水位")
    write_mode = source.write_modes.get(name) or spec.write_mode or "anti_join"
    return Plan(
        name=name, action="create",
        upper=source_duck, copy_all=source_duck is None,
        dedup_key=dedup, time_column=spec.time_column, kind=spec.kind or "append",
        columns=tuple(columns), source_watermark=source.watermarks.get(name),
        spec_row=source.spec_rows.get(name), write_mode=write_mode,
        write_spec=name in source.spec_rows, set_write_mode=True,
        reason="复制全部行，不写水位" if source_duck is None else "",
    )


def build_plans(source: Catalog, target: Catalog, requested: list[str] | None) -> list[Plan]:
    """Decide what each candidate table will do. This does not talk to Flight."""
    if requested is None:
        names = sorted(name for name in source.tables & target.tables if is_business(name))
        mode_create = False
    else:
        names = list(requested)
        mode_create = True

    plans = []
    for name in names:
        if not IDENT.match(name):
            plans.append(_reject(name, "表名只能包含字母、数字和下划线"))
            continue
        if name not in source.tables or not is_business(name):
            plans.append(_reject(name, "源上没有这张业务表"))
            continue
        source_columns = source.columns.get(name) or []
        if not source_columns:
            plans.append(_reject(name, "源上没有列信息"))
            continue
        target_exists = name in target.tables
        if mode_create and not target_exists:
            spec, errors = _choose_spec(name, source, target, False)
            if errors:
                plans.append(_reject(name, "；".join(errors)))
                continue
            if spec is None:
                plans.append(_reject(name, "没有去重键"))
                continue
            plans.append(_create_plan(name, source, source_columns, spec))
            continue
        if not target_exists:
            continue
        target_columns = target.columns.get(name) or []
        notes = layout_notes(source_columns, target_columns, source.keys.get(name), target.keys.get(name))
        diffs = compare_columns(source_columns, target_columns)
        spec, errors = _choose_spec(name, source, target, True)
        diffs.extend(errors)
        if diffs:
            plans.append(_reject(name, "结构不一致", diffs, notes))
            continue
        if spec is None:
            plans.append(_reject(name, "没有去重键", notes=notes))
            continue
        plans.append(_align_plan(name, source, target, source_columns, spec, notes))
    return plans


def safe_type(data_type):
    if not data_type or not SAFE_TYPE.fullmatch(str(data_type).strip()):
        raise ValueError(f"unexpected column type {data_type!r}")
    return str(data_type).strip()


def is_safe_default(expr):
    return bool(SAFE_DEFAULT.fullmatch(str(expr).strip()))


def create_table_sql(table, columns, schema="data", defaults=True):
    """Columns, NOT NULL and defaults. Writers that omit a defaulted column
    (exginfo.created_time) rely on the default being there. Staging tables
    receive every column and skip defaults."""
    parts = []
    for column in columns:
        part = f"{q(column.name)} {safe_type(column.data_type)}"
        if defaults and column.default is not None:
            if not is_safe_default(column.default):
                raise ValueError(f"unexpected default for {column.name}: {column.default!r}")
            part += f" DEFAULT {str(column.default).strip()}"
        if not column.nullable:
            part += " NOT NULL"
        parts.append(part)
    return f"CREATE TABLE {schema}.{q(table)} ({', '.join(parts)})"


def sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"non-finite value {value!r}")
        return repr(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP {lit(fmt(value))}"
    if isinstance(value, (bytes, bytearray)):
        raise ValueError("binary value cannot be inlined")
    return text(str(value))


def time_predicates(time_column, lower_excl=None, upper_incl=None, start_incl=None, end_excl=None):
    column = q(time_column)
    preds = []
    if lower_excl is not None:
        preds.append(f"{column} > {sql_literal(lower_excl)}")
    if upper_incl is not None:
        preds.append(f"{column} <= {sql_literal(upper_incl)}")
    if start_incl is not None:
        preds.append(f"{column} >= {sql_literal(start_incl)}")
    if end_excl is not None:
        preds.append(f"{column} < {sql_literal(end_excl)}")
    return preds


def select_sql(plan, predicates, limit=None, order=None):
    columns = ", ".join(q(column.name) for column in plan.columns)
    sql = f"SELECT {columns} FROM data.{q(plan.name)}"
    if predicates:
        sql += " WHERE " + " AND ".join(predicates)
    if order:
        sql += " ORDER BY " + ", ".join(q(name) for name in order)
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql


def write_order(plan):
    """Time column first, then the rest of the dedup key. Without a time column, the dedup key."""
    if not plan.time_column:
        return list(plan.dedup_key)
    return [plan.time_column] + [name for name in plan.dedup_key if name != plan.time_column]


def merge_sql(plan, staging, batch_lo=None, batch_hi=None):
    """Insert staged rows whose dedup key is not already in the target.

    The target-side time window is only applied when the time column is part of
    the dedup key. Otherwise a key that sits outside this batch's timestamps
    would be missed and inserted again.
    """
    keys = [q(name) for name in plan.dedup_key]
    key_list = ", ".join(keys)
    columns = [q(column.name) for column in plan.columns]
    column_list = ", ".join(columns)
    where = [f"{key} IS NOT NULL" for key in keys]
    if plan.time_column:
        where.append(f"{q(plan.time_column)} IS NOT NULL")
    distinct = (
        f"SELECT DISTINCT ON ({key_list}) {column_list} FROM {staging} "
        f"WHERE {' AND '.join(where)} ORDER BY {key_list}"
    )
    join_on = " AND ".join(f"s.{key} = t.{key}" for key in keys)
    if plan.time_column in plan.dedup_key and batch_lo is not None and batch_hi is not None:
        anti = (
            f"(SELECT {key_list} FROM data.{q(plan.name)} WHERE {q(plan.time_column)} BETWEEN "
            f"{sql_literal(batch_lo)} AND {sql_literal(batch_hi)}) t"
        )
    else:
        anti = f"(SELECT {key_list} FROM data.{q(plan.name)}) t"
    selected = ", ".join(f"s.{column}" for column in columns)
    order = ", ".join(f"s.{q(name)}" for name in write_order(plan))
    return (
        f"INSERT INTO data.{q(plan.name)} ({column_list}) "
        f"SELECT {selected} FROM ({distinct}) s ANTI JOIN {anti} ON {join_on} ORDER BY {order}"
    )


def _array_sql(values):
    if values is None:
        return "NULL"
    return "[" + ", ".join(text(str(item)) for item in values) + "]::VARCHAR[]"


def spec_upsert_sql(row):
    values = []
    for name in SPEC_FIELDS:
        value = row.get(name)
        if name in ARRAY_FIELDS:
            values.append(_array_sql(value))
        elif name == "period_seconds":
            values.append("NULL" if value is None else str(int(value)))
        elif isinstance(value, (dict, list)):
            values.append(text(json.dumps(value, ensure_ascii=True, sort_keys=True)))
        else:
            values.append("NULL" if value is None else text(str(value)))
    columns = ", ".join(SPEC_FIELDS)
    return (
        f"INSERT OR REPLACE INTO data.table_specs ({columns}) VALUES ({', '.join(values)})"
    )


def watermark_sql(plan, duck=None):
    """Statements that move the target watermark to ``duck`` (default: the source
    watermark). Empty when the plan writes no watermark."""
    source = plan.source_watermark or {}
    if plan.copy_all:
        return []
    duck = plan.upper if duck is None else duck
    if duck is None:
        return []
    if plan.action == "copy" and plan.target_watermark is not None:
        sets = [f"duck_time = {sql_literal(duck)}", "updated_at = CURRENT_TIMESTAMP"]
        if source.get("time_column"):
            sets.append(f"time_column = COALESCE(time_column, {text(str(source['time_column']))})")
        if source.get("start_time") is not None:
            sets.append(f"start_time = COALESCE(start_time, {sql_literal(as_naive(source['start_time']))})")
        return [
            f"UPDATE data.watermark SET {', '.join(sets)} WHERE table_name = {text(plan.name)}"
        ]
    start = source.get("start_time")
    ingestor = source.get("ingestor")
    time_column = source.get("time_column") or plan.time_column
    return [(
        "INSERT INTO data.watermark (table_name, ingestor, max_lag_seconds, time_column, start_time, "
        "duck_time, updated_at) VALUES ("
        f"{text(plan.name)}, {'NULL' if ingestor is None else text(str(ingestor))}, "
        f"{'NULL' if source.get('max_lag_seconds') is None else str(int(source['max_lag_seconds']))}, "
        f"{'NULL' if not time_column else text(str(time_column))}, "
        f"{sql_literal(as_naive(start)) if start is not None else 'NULL'}, {sql_literal(duck)}, "
        "CURRENT_TIMESTAMP) ON CONFLICT (table_name) DO UPDATE SET "
        "duck_time = excluded.duck_time, "
        "time_column = COALESCE(time_column, excluded.time_column), "
        "start_time = COALESCE(start_time, excluded.start_time), "
        "updated_at = excluded.updated_at"
    )]


def write_mode_sql(plan):
    if not plan.set_write_mode or not plan.write_mode:
        return []
    return [(
        "INSERT INTO data.config_dict (key, value) VALUES ("
        f"{text('write_mode:' + plan.name)}, {text(plan.write_mode)}) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value"
    )]


def describe(plan):
    if plan.action == "reject":
        return "拒绝  " + plan.reason
    if plan.action == "skip":
        return "跳过  " + plan.reason
    if plan.action == "create":
        if plan.copy_all:
            return "建表  复制全部行，不写水位"
        return f"建表  复制到源水位 {fmt(plan.upper)}"
    left = "起点" if plan.lower is None else fmt(plan.lower)
    return f"复制  {left} 之后到 {fmt(plan.upper)}"


def find_gap(source, plan):
    """First source row after the target watermark when the source keeps nothing
    at or before it, else None. The watermark only moves forward, so such a span
    would never be filled afterwards."""
    if plan.action != "copy" or plan.lower is None or not plan.time_column:
        return None
    column = q(plan.time_column)
    table = f"data.{q(plan.name)}"
    if source.query(f"SELECT 1 AS hit FROM {table} WHERE {column} <= {sql_literal(plan.lower)} LIMIT 1"):
        return None
    preds = time_predicates(plan.time_column, plan.lower, plan.upper)
    rows = source.query(
        f"SELECT {column} AS first FROM {table} WHERE {' AND '.join(preds)} ORDER BY {column} LIMIT 1"
    )
    if not rows or rows[0].get("first") is None:
        return None
    return as_naive(rows[0]["first"])


def check_gaps(source, plans):
    """Reject copy plans whose range starts after the target watermark."""
    checked = []
    for plan in plans:
        try:
            first = find_gap(source, plan)
        except (flight.FlightError, pa.ArrowException, OSError, ValueError) as exc:
            checked.append(_reject(plan.name, f"无法检查缺口：{str(exc).splitlines()[0]}", notes=plan.notes))
            continue
        if first is None:
            checked.append(plan)
            continue
        gap = f"源上最早只有 {fmt(first)}，目标水位 {fmt(plan.lower)}，中间这段源上没有；先在目标补齐这段再同步"
        checked.append(_reject(plan.name, "目标水位之后有缺口", (gap,), plan.notes))
    return checked


def print_plans(plans):
    if not plans:
        print("没有需要处理的表")
        return
    for plan in plans:
        print(f"{plan.name}  {describe(plan)}")
        for diff in plan.differences:
            print(f"  {diff}")
        for note in plan.notes:
            print(f"  {note}")


def local_writers(plans, source, target):
    """(instances, plugins) that write the tables about to change on the target.

    Instances come from the target watermark; plugins from the target's table
    spec, else the source's, else the builtin spec. The CLI stops only local
    ingestors that match one of them.
    """
    instances, plugins = set(), set()
    for plan in plans:
        if plan.action not in ("copy", "create"):
            continue
        ingestor = (target.watermarks.get(plan.name) or {}).get("ingestor")
        if ingestor:
            instances.add(str(ingestor))
        writer = (target.spec_rows.get(plan.name) or source.spec_rows.get(plan.name) or {}).get("writer")
        if not writer:
            builtin = builtin_spec(plan.name)
            writer = builtin.plugin if builtin else None
        if writer:
            plugins.add(str(writer))
    return sorted(instances), sorted(plugins)


def summarize(plans):
    return {
        "actions": sum(plan.action in ("copy", "create") for plan in plans),
        "rejects": sum(plan.action == "reject" for plan in plans),
        "skips": sum(plan.action == "skip" for plan in plans),
    }


def try_query(dp, sql):
    try:
        return dp.query(sql)
    except (flight.FlightError, pa.ArrowException, OSError, ValueError):
        return None


def load_catalog(dp) -> Catalog:
    catalog = Catalog()
    tables = dp.query("SELECT table_name FROM duckdb_tables() WHERE schema_name = 'data'")
    catalog.tables = {row["table_name"] for row in tables}
    for row in dp.query(
        "SELECT table_name, column_name, data_type, is_nullable, column_default FROM duckdb_columns() "
        "WHERE schema_name = 'data' ORDER BY table_name, column_index"
    ):
        catalog.columns.setdefault(row["table_name"], []).append(Column(
            row["column_name"], row["data_type"], nullable_of(row["is_nullable"]), row.get("column_default"),
        ))
    if "watermark" in catalog.tables:
        for row in dp.query(
            "SELECT table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time "
            "FROM data.watermark"
        ):
            catalog.watermarks[row["table_name"]] = row
    if "table_specs" in catalog.tables:
        for row in dp.query("SELECT * FROM data.table_specs"):
            name = row.get("table_name")
            if not name:
                continue
            catalog.spec_rows[name] = row
            try:
                catalog.specs[name] = registered_spec(row)
            except (TypeError, ValueError, KeyError, json.JSONDecodeError):
                catalog.specs[name] = None
    if "config_dict" in catalog.tables:
        for row in dp.query("SELECT key, value FROM data.config_dict"):
            key = row.get("key") or ""
            if key.startswith("write_mode:"):
                catalog.write_modes[key.split(":", 1)[1]] = row.get("value")
            elif key == "kline_interval":
                catalog.kline_interval = row.get("value")
    constraints = try_query(
        dp,
        "SELECT table_name, constraint_type FROM duckdb_constraints() "
        "WHERE schema_name = 'data' AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')",
    )
    if constraints:
        for row in constraints:
            catalog.keys.setdefault(row["table_name"], set()).add(row["constraint_type"])
    return catalog


def normalize_batch(table, columns):
    """Drop timezone from timestamps and project columns into target order."""
    if table.num_rows == 0:
        return table
    for index, field in enumerate(table.schema):
        if pa.types.is_timestamp(field.type) and field.type.tz is not None:
            cast = pc.cast(table.column(index), pa.timestamp(field.type.unit))
            table = table.set_column(index, field.name, cast)
    names = [column.name for column in columns]
    return table.select(names)


def batch_time_bounds(table, time_column):
    if not time_column or time_column not in table.column_names or table.num_rows == 0:
        return None, None
    column = table.column(time_column)
    lo = pc.min(column).as_py()
    hi = pc.max(column).as_py()
    if lo is None or hi is None:
        return None, None
    return as_naive(lo), as_naive(hi)


def _read_limited(source, plan, predicates, limit, order=None):
    return source.query_arrow(select_sql(plan, predicates, limit=limit, order=order))


def _sample_page_rows(source, plan, predicates):
    limit = 200
    while True:
        try:
            sample = _read_limited(source, plan, predicates, limit)
            return page_rows_for(sample.num_rows, sample.nbytes)
        except (flight.FlightError, pa.ArrowException, OSError) as exc:
            if is_page_too_big(exc) and limit > 1:
                limit = max(1, limit // 5)
                continue
            raise


def _iter_keyset(source, plan, predicates, order, page_rows):
    """Pages ordered by ``order``. The next page starts after the last row's key,
    so every page must be read in that same order or rows are skipped."""
    last = None
    rows = page_rows
    while True:
        page_preds = list(predicates)
        if last is not None:
            left = ", ".join(q(name) for name in order)
            right = ", ".join(sql_literal(value) for value in last)
            page_preds.append(f"({left}) > ({right})")
        try:
            table = _read_limited(source, plan, page_preds, rows, order)
        except (flight.FlightError, pa.ArrowException, OSError) as exc:
            if is_page_too_big(exc) and rows > 1:
                rows = max(1, rows // 2)
                continue
            raise
        table = normalize_batch(table, plan.columns)
        if table.num_columns == 0 or table.num_rows == 0:
            return
        yield table
        if table.num_rows < rows:
            return
        last = tuple(table.column(name)[table.num_rows - 1].as_py() for name in order)
        for value in last:
            if value is None:
                raise RuntimeError(f"{plan.name} 分页键含空值，无法继续")


def _period_of(name):
    spec = builtin_spec(name)
    return spec.period if spec else None


def time_slices(table, time_column, page_rows):
    """Cut a time-sorted table into slices of about ``page_rows``. A cut never
    falls inside one timestamp, so every row up to a slice's last time is in
    that slice or an earlier one."""
    column = table.column(time_column)
    start, total = 0, table.num_rows
    while start < total:
        end = min(start + page_rows, total)
        if end < total:
            end = pc.sum(pc.less_equal(column, column[end - 1])).as_py()
        yield table.slice(start, end - start)
        start = end


def iter_batches(source, plan, page_rows):
    """Yield (batch, through) in write order. ``through`` is the time every
    source row up to which is in this batch or an earlier one, or None when a
    batch cannot vouch for that (no time column, or keyset pages)."""
    order = write_order(plan)
    if not plan.time_column:
        for batch in _iter_keyset(source, plan, [], order, page_rows):
            yield batch, None
        return

    bounds = time_predicates(plan.time_column, plan.lower, None if plan.copy_all else plan.upper)
    column = q(plan.time_column)
    where = (" WHERE " + " AND ".join(bounds)) if bounds else ""
    span = source.query(
        f"SELECT min({column}) AS lo, max({column}) AS hi FROM data.{q(plan.name)}{where}"
    )
    if not span or span[0].get("lo") is None:
        return
    cursor = as_naive(span[0]["lo"])
    end = as_naive(span[0]["hi"])
    window = initial_window(_period_of(plan.name))
    while cursor <= end:
        nxt = cursor + window
        predicates = time_predicates(
            plan.time_column, plan.lower, None if plan.copy_all else plan.upper, cursor, nxt,
        )
        try:
            table = source.query_arrow(select_sql(plan, predicates))
        except (flight.FlightError, pa.ArrowException, OSError) as exc:
            if is_page_too_big(exc) and window > MIN_WINDOW:
                window = max(MIN_WINDOW, window / 2)
                continue
            if is_page_too_big(exc):
                for batch in _iter_keyset(source, plan, predicates, order, page_rows):
                    yield batch, None
                cursor = nxt
                continue
            raise
        table = normalize_batch(table, plan.columns)
        used = window
        if table.num_rows > page_rows * 4 and window > MIN_WINDOW:
            window = shrink_window(window, table.num_rows, page_rows)
            continue
        if table.num_rows:
            table = table.sort_by([(name, "ascending") for name in order])
            for batch in time_slices(table, plan.time_column, page_rows):
                yield batch, as_naive(batch.column(plan.time_column)[batch.num_rows - 1].as_py())
        if table.num_rows < max(1, page_rows // 4):
            window = min(window * 2, MAX_WINDOW)
        cursor += used


def apply_plan(source, target, plan):
    staging = f"_sync_{plan.name}"
    staging_sql_name = f"main.{q(staging)}"
    bounds = []
    if plan.time_column and not plan.copy_all:
        bounds = time_predicates(plan.time_column, plan.lower, plan.upper)
    elif plan.time_column and plan.lower is not None:
        bounds = time_predicates(plan.time_column, plan.lower, None)
    page_rows = _sample_page_rows(source, plan, bounds)
    statements = [
        f"DROP TABLE IF EXISTS {staging_sql_name}",
        create_table_sql(staging, plan.columns, schema="main", defaults=False),
    ]
    if plan.action == "create":
        statements.insert(0, create_table_sql(plan.name, plan.columns))
    target.transaction(statements)
    read_rows = 0
    since_watermark = 0
    try:
        for batch, through in iter_batches(source, plan, page_rows):
            if batch.num_rows == 0:
                continue
            batch = normalize_batch(batch, plan.columns)
            lo, hi = batch_time_bounds(batch, plan.time_column)
            target.execute(f"TRUNCATE {staging_sql_name}")
            target.append("main", staging, batch)
            since_watermark += batch.num_rows
            # Keyset pages have no timestamp to vouch for, so they only count
            # toward the next move. The rows and that move commit together.
            moved = []
            if through is not None and since_watermark >= WATERMARK_EVERY_ROWS:
                moved = watermark_sql(plan, through)
                if moved:
                    since_watermark = 0
            target.transaction([
                merge_sql(plan, staging_sql_name, lo, hi),
                *moved,
                f"TRUNCATE {staging_sql_name}",
            ])
            read_rows += batch.num_rows
            reached = f"  水位 {fmt(through)}" if moved else ""
            print(f"  {plan.name}  已读取 {read_rows:,} 行{reached}", flush=True)
        meta = []
        if plan.write_spec and plan.spec_row:
            meta.append(spec_upsert_sql(plan.spec_row))
        meta.extend(write_mode_sql(plan))
        meta.extend(watermark_sql(plan))
        if meta:
            target.transaction(meta)
    finally:
        try:
            target.execute(f"DROP TABLE IF EXISTS {staging_sql_name}")
        except (flight.FlightError, pa.ArrowException, OSError) as exc:
            print(f"  未能删除 {staging}：{exc}", file=sys.stderr)
    return read_rows


def confirm(args, plans):
    pending = sum(plan.action in ("copy", "create") for plan in plans)
    if pending == 0 or args.dry_run or args.preflight or args.yes:
        return True
    if not sys.stdin.isatty():
        print("非交互终端请加 --yes", file=sys.stderr)
        return False
    answer = input(f"将同步 {pending} 张表，继续？[y/N] ")
    return answer.strip().lower() in {"y", "yes"}


def kline_interval_sql(source, target, done_names):
    if target.kline_interval or not source.kline_interval:
        return []
    if not any(KLINE_TABLE.match(name) for name in done_names):
        return []
    return [(
        "INSERT INTO data.config_dict (key, value) VALUES ("
        f"{text('kline_interval')}, {text(str(source.kline_interval))}) "
        "ON CONFLICT (key) DO NOTHING"
    )]


def run(args):
    source_token = args.from_token if args.from_token is not None else os.getenv("DUCKPORT_SYNC_FROM_TOKEN", "")
    source = Duckport(args.source, token=source_token)
    target = Duckport(args.addr)
    source_ping = source.action("duckport.ping")
    target_ping = target.action("duckport.ping")
    print(f"源 {args.source}  duckport {source_ping.get('server_version', '?')}")
    print(f"目标 {args.addr}  duckport {target_ping.get('server_version', '?')}")
    print("模式 " + ("指定源表" if args.table else "追平已有表"))

    source_catalog = load_catalog(source)
    target_catalog = load_catalog(target)
    plans = build_plans(source_catalog, target_catalog, args.table or None)
    plans = check_gaps(source, plans)
    print_plans(plans)
    counts = summarize(plans)
    print(
        f"将处理 {counts['actions']} 张表，跳过 {counts['skips']} 张，拒绝 {counts['rejects']} 张"
    )
    if args.status_file:
        instances, plugins = local_writers(plans, source_catalog, target_catalog)
        with open(args.status_file, "w") as handle:
            json.dump({**counts, "ingestors": instances, "writers": plugins}, handle)
    if args.dry_run or args.preflight:
        return 1 if counts["rejects"] else 0
    if not confirm(args, plans):
        print("已取消")
        return 1 if counts["rejects"] else 0

    failures = 0
    done = []
    for plan in plans:
        if plan.action not in ("copy", "create"):
            continue
        print(f"开始 {plan.name}", flush=True)
        try:
            rows = apply_plan(source, target, plan)
        except (flight.FlightError, pa.ArrowException, OSError, RuntimeError, ValueError) as exc:
            failures += 1
            print(f"{plan.name}  失败  {str(exc).splitlines()[0]}", file=sys.stderr)
            continue
        done.append(plan.name)
        print(f"{plan.name}  完成  读取 {rows:,} 行  duck_time {fmt_opt(plan.upper if not plan.copy_all else None)}")
    interval = kline_interval_sql(source_catalog, target_catalog, done)
    if interval:
        target.transaction(interval)
    if failures or counts["rejects"]:
        return 1
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--from", dest="source", required=True, help="source duckport HOST:PORT")
    parser.add_argument("--from-token", default=None, help="bearer token for the source; default DUCKPORT_SYNC_FROM_TOKEN")
    parser.add_argument("--addr", default="127.0.0.1:50051", help="target duckport, usually the local server")
    parser.add_argument("--table", action="append", default=[], help="source table to create or align; repeatable")
    parser.add_argument("--dry-run", action="store_true", help="print the plan only")
    parser.add_argument("--preflight", action="store_true", help="print the plan only; used by the duckport CLI")
    parser.add_argument("--yes", action="store_true", help="do not ask before writing")
    parser.add_argument("--status-file", help="write action counts as JSON")
    args = parser.parse_args(argv)
    try:
        return run(args)
    except (flight.FlightError, pa.ArrowException, OSError, RuntimeError, ValueError) as exc:
        print(f"sync failed: {str(exc).splitlines()[0]}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
