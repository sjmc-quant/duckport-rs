#!/usr/bin/env python3
"""duckport.watch_watermark and DuckportConsumer.wait_duck_time, end to end.

Run through tests/python/run_with_server.sh (needs DUCKPORT_ADDR).
"""

import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import pyarrow.flight as flight

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(HERE, "..", "..", "client")))

from duckport_consumer import DuckportConsumer  # noqa: E402

ADDR = os.getenv("DUCKPORT_ADDR", "127.0.0.1:50071")
MARKET = "watch_test"
TABLE = f"{MARKET}_1m"
T0 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)


def minute(n: int) -> datetime:
    return T0 + timedelta(minutes=n)


def watermark_sql(n: int) -> str:
    return (
        f"INSERT INTO data.watermark (table_name, time_column, duck_time) "
        f"VALUES ('{TABLE}', 'open_time', TIMESTAMP '{minute(n):%Y-%m-%d %H:%M:%S}') "
        f"ON CONFLICT (table_name) DO UPDATE SET duck_time = excluded.duck_time"
    )


def later(seconds: float, fn, *args) -> threading.Thread:
    def run():
        time.sleep(seconds)
        fn(*args)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def main():
    writer = DuckportConsumer(addr=ADDR)
    reader = DuckportConsumer(addr=ADDR)

    print("== the action is advertised")
    assert "duckport.watch_watermark" in [a.type for a in reader._client.list_actions()]

    print("== the first message is the current state")
    writer.execute_transaction(
        [
            "CREATE TABLE IF NOT EXISTS data.watch_test_1m (open_time TIMESTAMP, v INTEGER)",
            "DELETE FROM data.watch_test_1m",
            f"INSERT INTO data.watch_test_1m VALUES (TIMESTAMP '{minute(1):%Y-%m-%d %H:%M:%S}', 1)",
            watermark_sql(1),
        ]
    )
    started = time.monotonic()
    first = next(reader.watch_watermark(timeout=10))
    assert first[TABLE] == minute(1), first
    assert time.monotonic() - started < 2
    assert reader.wait_duck_time(MARKET, timeout=5) == minute(1)
    assert reader.wait_duck_time(MARKET, after=minute(0), timeout=5) == minute(1)

    print("== a commit wakes the waiter, and its data is readable")
    def write_minute_2():
        writer.execute_transaction(
            [
                f"INSERT INTO data.watch_test_1m VALUES (TIMESTAMP '{minute(2):%Y-%m-%d %H:%M:%S}', 2)",
                watermark_sql(2),
            ]
        )

    thread = later(1.0, write_minute_2)
    started = time.monotonic()
    got = reader.wait_duck_time(MARKET, after=minute(1).replace(tzinfo=None), timeout=20)
    waited = time.monotonic() - started
    thread.join()
    assert got == minute(2), got
    assert 0.9 < waited < 3, waited
    assert reader.query("SELECT max(v) AS v FROM data.watch_test_1m").column("v")[0].as_py() == 2
    print(f"   woke {waited - 1.0:.3f}s after the write was issued")

    print("== a single-statement execute also notifies")
    thread = later(0.5, writer.execute, watermark_sql(3))
    assert reader.wait_duck_time(MARKET, after=minute(2), timeout=20) == minute(3)
    thread.join()

    print("== a rolled-back transaction does not")
    def rolled_back():
        try:
            writer.execute_transaction([watermark_sql(9), "SELECT * FROM data.no_such_table"])
        except flight.FlightError:
            pass

    thread = later(0.3, rolled_back)
    started = time.monotonic()
    assert reader.wait_duck_time(MARKET, after=minute(3), timeout=2) is None
    assert 1.9 < time.monotonic() - started < 4
    thread.join()

    print("== another table's watermark does not end the wait")
    thread = later(
        0.3,
        writer.execute,
        "INSERT INTO data.watermark (table_name, time_column, duck_time) "
        "VALUES ('watch_other_1m', 'open_time', TIMESTAMP '2030-01-01 00:00:00') "
        "ON CONFLICT (table_name) DO UPDATE SET duck_time = excluded.duck_time",
    )
    assert reader.wait_duck_time(MARKET, after=minute(3), timeout=2) is None
    thread.join()

    print("== a table without a watermark waits, then times out")
    assert reader.wait_duck_time("watch_missing", timeout=1) is None

    print("== several watchers all get the change")
    watchers = [DuckportConsumer(addr=ADDR) for _ in range(5)]
    results = [None] * len(watchers)

    def wait(i):
        results[i] = watchers[i].wait_duck_time(MARKET, after=minute(3), timeout=20)

    threads = [threading.Thread(target=wait, args=(i,)) for i in range(len(watchers))]
    for t in threads:
        t.start()
    time.sleep(0.5)
    writer.execute(watermark_sql(4))
    for t in threads:
        t.join()
    assert results == [minute(4)] * len(watchers), results

    print("== a server without the action is polled instead")
    old = DuckportConsumer(addr=ADDR)
    old._watch_supported = False
    thread = later(0.5, writer.execute, watermark_sql(5))
    assert old.wait_duck_time(MARKET, after=minute(4), timeout=20) == minute(5)
    thread.join()
    assert old.wait_duck_time(MARKET, after=minute(5), timeout=1.5) is None

    print("== the raw stream is plain JSON")
    raw = flight.FlightClient(f"grpc://{ADDR}")
    options = flight.FlightCallOptions(timeout=10)
    body = next(iter(raw.do_action(flight.Action("duckport.watch_watermark", b""), options=options)))
    message = json.loads(body.body.to_pybytes())
    row = next(w for w in message["watermarks"] if w["table_name"] == TABLE)
    assert row == {"table_name": TABLE, "duck_time": "2026-01-01T00:05:00.000000"}, row

    writer.execute_transaction(
        [
            "DROP TABLE data.watch_test_1m",
            "DELETE FROM data.watermark WHERE table_name IN ('watch_test_1m', 'watch_other_1m')",
        ]
    )
    print("ALL PASSED")


if __name__ == "__main__":
    main()
