"""Markets the consumer client accepts. Builds SQL only; needs no server.

    python tests/python/test_consumer_markets.py
"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "client"))

from duckport_consumer.client import (  # noqa: E402
    build_exginfo_sql, build_market_kline_sql, perp_asset_where, usdt_perp_asset_where,
)

BEGIN, END = datetime(2026, 11, 1), datetime(2026, 11, 2)


def kline_sql(market, **kw):
    return build_market_kline_sql("data", "_1m", market, 5, 0, BEGIN, END, 1, **kw)


def raises(fn, text):
    try:
        fn()
    except ValueError as e:
        assert text in str(e), str(e)
        return
    raise AssertionError(f"expected ValueError containing {text!r}")


def test_kline_markets():
    for market in ("usdt_perp", "usdt_spot", "others_perp"):
        assert f"data.{market}_1m" in kline_sql(market)
    for market in ("usdc_perp", "coin_perp", "others_spot"):
        raises(lambda: kline_sql(market), "Unsupported market")
        raises(lambda: build_exginfo_sql("data", market), "Unsupported market")


def test_exginfo_without_asset_class_is_the_plain_statement():
    for market in ("usdt_perp", "usdt_spot", "others_perp"):
        assert build_exginfo_sql("data", market) == f"SELECT * FROM data.exginfo WHERE market = '{market}'"


def test_asset_class_on_both_perp_markets():
    assert usdt_perp_asset_where("crypto") == "market = 'usdt_perp' AND contract_type = 'PERPETUAL'"
    assert perp_asset_where("usdt_perp", "crypto") == usdt_perp_asset_where("crypto")
    assert perp_asset_where("others_perp", "tradifi") == \
        "market = 'others_perp' AND contract_type = 'TRADIFI_PERPETUAL'"
    assert "market = 'others_perp' AND contract_type = 'PERPETUAL'" in kline_sql("others_perp", asset_class="crypto")
    assert "market = 'others_perp'" in build_exginfo_sql("data", "others_perp", "tradifi")
    # usdt_perp statements are what they were
    assert build_exginfo_sql("data", "usdt_perp", "crypto") == \
        "SELECT * FROM data.exginfo WHERE market = 'usdt_perp' AND contract_type = 'PERPETUAL'"


def test_asset_class_is_refused_on_spot():
    raises(lambda: kline_sql("usdt_spot", asset_class="crypto"), "only supported for perp markets")
    raises(lambda: build_exginfo_sql("data", "usdt_spot", "crypto"), "only supported for perp markets")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
