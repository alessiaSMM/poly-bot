# app/trade_collector.py

import json
import time
import requests
from datetime import datetime, timezone

TRADES_LOG = "state/trades_log.jsonl"

# Endpoint CLOB trades (realtime-like polling)
TRADES_ENDPOINT = "https://clob.polymarket.com/trades"
POLL_INTERVAL = 5  # secondi

last_seen_ts = 0


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def fetch_latest_trades():
    global last_seen_ts

    params = {
        "limit": 100,
        "sort": "timestamp",
        "order": "desc",
    }

    r = requests.get(TRADES_ENDPOINT, params=params, timeout=10)
    r.raise_for_status()
    trades = r.json()

    new_trades = []
    for t in trades:
        ts = int(t.get("timestamp", 0))
        if ts > last_seen_ts:
            new_trades.append(t)

    if new_trades:
        last_seen_ts = max(int(t["timestamp"]) for t in new_trades)

    return new_trades


def append_trades(trades):
    if not trades:
        return

    with open(TRADES_LOG, "a") as f:
        for t in trades:
            record = {
                "ts": int(t["timestamp"]),
                "ts_iso": datetime.fromtimestamp(
                    int(t["timestamp"]) / 1000, tz=timezone.utc
                ).isoformat(),
                "maker": t.get("maker"),
                "side": t.get("side"),
                "size": float(t.get("size", 0)),
                "price": float(t.get("price", 0)),
                "conditionId": t.get("conditionId"),
                "outcome": t.get("outcome"),
            }
            f.write(json.dumps(record) + "\n")


def main():
    print("📡 Trade Collector avviato (REAL-TIME)")
    print("📝 Log:", TRADES_LOG)

    while True:
        try:
            trades = fetch_latest_trades()
            append_trades(trades)
            if trades:
                print(f"➕ Nuovi trade salvati: {len(trades)} | {utc_now_iso()}")
        except Exception as e:
            print("❌ Errore collector:", e)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
