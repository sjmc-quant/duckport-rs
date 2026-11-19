#!/usr/bin/env python3
"""Plugins register their table specs; duckport doctor reads them.

Runs the real init_schema of binance-ingestor and binance-funding against the server,
then checks that every registered spec parses and agrees with the built-in spec duckport-rs
keeps for older plugin versions (a newer plugin may add columns that have a default), and that doctor finds nothing to fix.

Prerequisites:
  DUCKPORT_DB_PATH=/tmp/specs.db cargo run --bin duckport-server    (fresh database)
  BINANCE_INGESTOR_SRC / BINANCE_FUNDING_SRC: plugin checkouts (default: next to this repo)
"""

import os
import sys

import pyarrow.flight as flight

from reconcile_fixtures import ADDR, NEW, SERVER, doctor, hr, reset, tx

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..", "..")
for env, default in (("BINANCE_INGESTOR_SRC", "duckport-binance-ingestor"),
                     ("BINANCE_FUNDING_SRC", "duckport-binance-funding")):
    sys.path.insert(0, os.getenv(env, os.path.join(ROOT, "..", default)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from binance_funding.duckport_client import DuckportClient as FundingClient  # noqa: E402
from binance_ingestor.duckport_client import DuckportClient as KlineClient  # noqa: E402
from duckport_flight import Duckport  # noqa: E402
from reconcile.specs import builtin_spec  # noqa: E402
from reconcile.state import collect  # noqa: E402


def main():
    client = flight.FlightClient(SERVER)
    reset(client)
    tx(client, "DROP TABLE IF EXISTS data.usdt_perp_5m", "DROP TABLE IF EXISTS data.usdt_spot_5m",
       "DROP TABLE IF EXISTS data.exginfo", "DROP TABLE IF EXISTS data.usdt_perp_funding")

    hr("plugins register their specs in init_schema")
    os.environ["INGESTOR_NAME"] = "bn5m"
    kline = KlineClient(ADDR, schema="data", interval="5m")
    kline.init_schema(["usdt_perp", "usdt_spot"], "5m", {"usdt_perp", "usdt_spot"}, start_date="2026-10-01")
    os.environ["INGESTOR_NAME"] = "binance-funding"
    FundingClient(ADDR).init_schema(start_date="2026-10-01")

    state = collect(Duckport(ADDR), [])
    assert not state.bad_specs, state.bad_specs
    assert set(state.registered) == {"usdt_perp_5m", "usdt_spot_5m", "exginfo", "usdt_perp_funding"}, \
        set(state.registered)
    for name, spec in state.registered.items():
        builtin = builtin_spec(name)
        print(f"  {name}: writer={spec.plugin} v{spec.writer_version} kind={spec.kind} period={spec.period}")
        assert spec.source == "registered" and spec.write_mode == "anti_join"
        assert spec.plugin == builtin.plugin and spec.kind == builtin.kind
        assert spec.dedup_key == builtin.dedup_key and spec.time_column == builtin.time_column
        assert spec.period == builtin.period, (name, spec.period, builtin.period)
        # A newer plugin may add columns (binance-ingestor 0.7.0: contract_type / underlying_type
        # on the perp tables). Each of them needs a default, so that rows written before it get a value.
        assert set(builtin.columns) <= set(spec.columns), (name, set(builtin.columns) - set(spec.columns))
        added = set(spec.columns) - set(builtin.columns)
        assert added <= set(spec.column_defaults), (name, added - set(spec.column_defaults))
        assert spec.string_checks == builtin.string_checks, (name, spec.string_checks, builtin.string_checks)
        assert spec.legacy_columns == builtin.legacy_columns
        assert {k: v for k, v in spec.column_defaults.items() if k not in added} == builtin.column_defaults

    hr("doctor on freshly initialised plugin tables: nothing to fix")
    instances = [dict(NEW[0], instance="bn5m"), NEW[1]]
    rc, f = doctor(instances)
    assert rc == 0, rc
    assert not [x for x in f if x["level"] != "hint"], f

    tx(client, "DROP TABLE data.usdt_perp_5m", "DROP TABLE data.usdt_spot_5m", "DROP TABLE data.exginfo",
       "DROP TABLE data.usdt_perp_funding", "DROP TABLE IF EXISTS main._staging_usdt_perp_5m",
       "DROP TABLE IF EXISTS main._staging_usdt_spot_5m", "DROP TABLE IF EXISTS main._staging_exginfo",
       "DROP TABLE IF EXISTS main._staging_usdt_perp_funding")
    reset(client)
    print("\nALL TABLE SPEC TESTS PASSED")


if __name__ == "__main__":
    main()
