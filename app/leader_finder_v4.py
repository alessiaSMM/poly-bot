#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
leader_finder_v4.py

Analisi batch Polymarket:
- finestra reale 24h
- selezione balene / trader qualificati
- individuazione trade copiabili (ultimi 15 min)
- DRY RUN (nessuna esecuzione)
"""

import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# =====================
# Config
# =====================

STATE_TRADES = Path("state/trades_log.jsonl")
STATE_COPIED = Path("state/copied_trades.json")

OUT_LEADERS = Path("state/auto_leaders.json")
OUT_CANDIDATES = Path("state/copy_candidates.json")
OUT_REPORT = Path("state/leaders_report.json")

WINDOW_24H = 24 * 60 * 60
WINDOW_15M = 15 * 60

# criteri
WHALE_VOLUME = 50_000
WHALE_TRADES = 5
WHALE_MARKETS = 2

TRADER_VOLUME = 1_000
TRADER_TRADES = 3
TRADER_MARKETS = 2


# =====================
# Utility
# =====================

def now_ts():
    return time.time()


def load_json(path, default):
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


# =====================
# Load trades
# =====================

def load_recent_trades():
    if not STATE_TRADES.exists():
        return []

    cutoff = now_ts() - WINDOW_24H
    trades = []

    with STATE_TRADES.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                t = json.loads(line)
            except Exception:
                continue

            ts_ms = t.get("timestamp")
            if not ts_ms:
                continue

            ts = ts_ms / 1000
            if ts >= cutoff:
                trades.append(t)

    return trades


# =====================
# Aggregate stats
# =====================

def aggregate(trades):
    stats = defaultdict(lambda: {
        "volume": 0.0,
        "trade_count": 0,
        "markets": set(),
    })

    for t in trades:
        w = t.get("proxyWallet")
        if not w:
            continue

        size = float(t.get("size") or 0)
        price = float(t.get("price") or 0)
        cond = t.get("conditionId")

        stats[w]["volume"] += size * price
        stats[w]["trade_count"] += 1
        if cond:
            stats[w]["markets"].add(cond)

    return stats


# =====================
# Leader selection
# =====================

def select_leaders(stats):
    whales = []
    traders = []

    for wallet, s in stats.items():
        markets = len(s["markets"])

        if (
            s["volume"] >= WHALE_VOLUME
            and s["trade_count"] >= WHALE_TRADES
            and markets >= WHALE_MARKETS
        ):
            whales.append(wallet)

        elif (
            s["volume"] >= TRADER_VOLUME
            and s["trade_count"] >= TRADER_TRADES
            and markets >= TRADER_MARKETS
        ):
            traders.append(wallet)

    if whales:
        return "WHALES", whales
    else:
        return "TRADERS", traders


# =====================
# Copy candidates
# =====================

def find_copy_candidates(trades, leaders, copied_state):
    cutoff_15m = now_ts() - WINDOW_15M
    candidates = []

    for t in trades:
        w = t.get("proxyWallet")
        if w not in leaders:
            continue

        ts_ms = t["timestamp"]
        ts = ts_ms / 1000

        if ts < cutoff_15m:
            continue

        last_copied = copied_state.get(w, 0)
        if ts_ms <= last_copied:
            continue

        candidates.append(t)

        # DRY RUN: aggiorniamo comunque lo stato
        copied_state[w] = ts_ms

    return candidates


# =====================
# Main
# =====================

def main():
    print("🔍 LeaderFinder v4 avviato")
    print("🕒 Finestra: ultime 24h / trade ultimi 15m")

    trades = load_recent_trades()
    print(f"📊 Trade analizzati: {len(trades)}")

    stats = aggregate(trades)
    mode, leaders = select_leaders(stats)

    print(f"🏷 Modalità selezione: {mode}")
    print(f"👥 Leader selezionati: {len(leaders)}")

    copied_state = load_json(STATE_COPIED, {})
    candidates = find_copy_candidates(trades, set(leaders), copied_state)

    print(f"📋 Trade copiabili (dry-run): {len(candidates)}")

    # output
    save_json(OUT_LEADERS, leaders)
    save_json(OUT_CANDIDATES, candidates)
    save_json(STATE_COPIED, copied_state)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "leaders_count": len(leaders),
        "candidates_count": len(candidates),
        "window_hours": 24,
    }

    save_json(OUT_REPORT, report)

    print("✅ LeaderFinder v4 completato")


if __name__ == "__main__":
    main()
