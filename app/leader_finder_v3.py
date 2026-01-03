#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
leader_finder_v3.py

Analisi batch dei trade Polymarket.
- Legge state/trades_log.jsonl
- Finestra mobile: ultime 24 ore
- Identifica balene e trader attivi
"""

import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

STATE_FILE = Path("state/trades_log.jsonl")
OUTPUT_LEADERS = Path("state/auto_leaders.json")
OUTPUT_REPORT = Path("state/leaders_report.json")

WINDOW_SECONDS = 24 * 60 * 60  # 24h


def load_recent_trades():
    if not STATE_FILE.exists():
        print("❌ Nessun file trade trovato")
        return []

    now = time.time()
    cutoff = now - WINDOW_SECONDS

    recent_trades = []

    with STATE_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                t = json.loads(line)
            except Exception:
                continue

            ts_ms = t.get("timestamp")
            if ts_ms is None:
                continue

            ts = ts_ms / 1000
            if ts >= cutoff:
                recent_trades.append(t)

    return recent_trades


def analyze_trades(trades):
    stats = defaultdict(lambda: {
        "volume": 0.0,
        "trade_count": 0,
        "markets": set(),
    })

    for t in trades:
        wallet = t.get("proxyWallet")
        if not wallet:
            continue

        size = t.get("size") or 0
        price = t.get("price") or 0
        condition = t.get("conditionId")

        volume = float(size) * float(price)

        s = stats[wallet]
        s["volume"] += volume
        s["trade_count"] += 1
        if condition:
            s["markets"].add(condition)

    # normalizza
    results = []
    for wallet, s in stats.items():
        results.append({
            "wallet": wallet,
            "volume_24h": round(s["volume"], 2),
            "trade_count": s["trade_count"],
            "markets_count": len(s["markets"]),
        })

    # ordina per volume
    results.sort(key=lambda x: x["volume_24h"], reverse=True)
    return results


def main():
    print("🔍 LeaderFinder avviato")
    print("🕒 Finestra: ultime 24 ore")

    trades = load_recent_trades()
    print(f"📊 Trade analizzati: {len(trades)}")

    leaders = analyze_trades(trades)

    OUTPUT_LEADERS.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_LEADERS.open("w", encoding="utf-8") as f:
        json.dump(leaders, f, indent=2)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_hours": 24,
        "total_trades": len(trades),
        "leaders_found": len(leaders),
        "top_10": leaders[:10],
    }

    with OUTPUT_REPORT.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"✅ LeaderFinder completato — leader trovati: {len(leaders)}")
    if leaders:
        print(f"🥇 Top wallet: {leaders[0]['wallet']} (vol={leaders[0]['volume_24h']})")


if __name__ == "__main__":
    main()
