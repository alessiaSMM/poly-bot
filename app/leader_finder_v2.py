#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LeaderFinder v5
────────────────────────────────────────────────────────────
OBIETTIVO:
- Analizzare i trade GLOBALI delle ultime 24 ore
- Identificare:
  STEP 1 → BALENE (criteri STRICT)
  STEP 2 → TRADER ATTIVI QUALIFICATI (criteri più permissivi)
- Salvare:
  - lista wallet leader (state/auto_leaders.json)
  - report dettagliato (state/leaders_report.json)

IMPORTANTE:
- Nessun loop infinito
- Nessuna copia trade
- Solo ANALISI e SELEZIONE
- Refresh previsto ogni 15 minuti via scheduler esterno
"""

import requests
import json
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────

CLOB_TRADES_URL = "https://data-api.polymarket.com/trades"
STATE_DIR = "state"
AUTO_LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")
REPORT_FILE = os.path.join(STATE_DIR, "leaders_report.json")

BATCH_SIZE = 1000
MAX_OFFSET = 100_000

LOOKBACK_HOURS = 24
CUTOFF = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

# STEP 1 — BALENE (criteri STRICT)
WHALE_MIN_VOLUME = 50_000      # USDC totali nelle 24h
WHALE_MIN_TRADES = 200
WHALE_MIN_MARKETS = 5

# STEP 2 — TRADER QUALIFICATI
QUAL_MIN_VOLUME = 1_000
QUAL_MIN_TRADES = 5
QUAL_ALLOWED_KEYWORDS = [
    "election", "president", "senate", "parliament",  # politica
    "nba", "nfl", "nhl", "mlb", "soccer", "football",  # sport
    "vs.", "win", "spread", "total"
]

# ────────────────────────────────────────────────────────────
# UTILS
# ────────────────────────────────────────────────────────────

def ensure_state_dir():
    os.makedirs(STATE_DIR, exist_ok=True)

def parse_timestamp(ts):
    """
    Parsing robusto timestamp (secondi / millisecondi / ISO)
    """
    try:
        if isinstance(ts, (int, float)):
            if ts > 1e12:
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        if isinstance(ts, str):
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None
    return None

def fetch_trades():
    """
    Fetch trade globali ordinati DESC per timestamp.
    Stop quando si va oltre le 24h.
    """
    all_trades = []
    offset = 0

    print("📡 Raccolta trade GLOBALI (ultime 24h)...")

    while offset < MAX_OFFSET:
        params = {
            "limit": BATCH_SIZE,
            "offset": offset,
            "sort": "timestamp",
            "order": "desc"
        }
        r = requests.get(CLOB_TRADES_URL, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()

        if not batch:
            break

        first_ts = parse_timestamp(batch[0].get("timestamp"))
        last_ts = parse_timestamp(batch[-1].get("timestamp"))

        print(
            f"   Batch offset={offset} | "
            f"first={first_ts} | last={last_ts}"
        )

        for t in batch:
            ts = parse_timestamp(t.get("timestamp"))
            if not ts:
                continue
            if ts < CUTOFF:
                return all_trades
            all_trades.append(t)

        offset += BATCH_SIZE

    return all_trades

# ────────────────────────────────────────────────────────────
# MAIN LOGIC
# ────────────────────────────────────────────────────────────

def main():
    ensure_state_dir()

    print("🔍 LeaderFinder v5 avviato")
    print(f"🕒 Finestra analisi: ultime {LOOKBACK_HOURS} ore")
    print("=" * 80)

    trades = fetch_trades()
    print(f"✅ Trade raccolti entro 24h: {len(trades)}")

    if not trades:
        print("❌ Nessun trade rilevato")
        json.dump([], open(AUTO_LEADERS_FILE, "w"), indent=2)
        return

    # Aggregazione per wallet
    wallets = defaultdict(lambda: {
        "volume": 0.0,
        "trades": 0,
        "markets": set(),
        "trade_samples": [],
        "market_samples": {}
    })

    for t in trades:
        wallet = t.get("maker") or t.get("taker")
        if not wallet:
            continue

        amount = float(t.get("amount", 0))
        wallets[wallet]["volume"] += amount
        wallets[wallet]["trades"] += 1

        cond_id = t.get("conditionId")
        title = t.get("question")
        slug = t.get("marketSlug")

        if cond_id:
            wallets[wallet]["markets"].add(cond_id)
            if cond_id not in wallets[wallet]["market_samples"]:
                wallets[wallet]["market_samples"][cond_id] = {
                    "title": title,
                    "slug": slug
                }

        if len(wallets[wallet]["trade_samples"]) < 25:
            wallets[wallet]["trade_samples"].append({
                "side": t.get("side"),
                "amount": amount,
                "price": t.get("price"),
                "timestamp": t.get("timestamp"),
                "question": title,
                "outcome": t.get("outcome")
            })

    # ─────────────────────────────────────────────
    # STEP 1 — BALENE
    # ─────────────────────────────────────────────
    print("\n🎯 STEP 1: BALENE (criteri STRICT)")
    print("=" * 80)

    whales = []

    for w, d in wallets.items():
        if (
            d["volume"] >= WHALE_MIN_VOLUME and
            d["trades"] >= WHALE_MIN_TRADES and
            len(d["markets"]) >= WHALE_MIN_MARKETS
        ):
            whales.append((w, d))

    whales.sort(key=lambda x: x[1]["volume"], reverse=True)

    if whales:
        print(f"🐋 BALENE TROVATE: {len(whales)}")
    else:
        print("🚨 NESSUNA BALENA TROVATA")

    leaders = []
    report = []

    for wallet, d in whales:
        leaders.append(wallet)
        report.append({
            "wallet": wallet,
            "type": "whale",
            "volume_24h": round(d["volume"], 2),
            "trades_24h": d["trades"],
            "markets_count": len(d["markets"]),
            "markets": list(d["market_samples"].values())[:15],
            "trades": d["trade_samples"]
        })

    # ─────────────────────────────────────────────
    # STEP 2 — TRADER QUALIFICATI
    # ─────────────────────────────────────────────
    if not leaders:
        print("\n⬇️⬇️⬇️ TARGET ABBASSATO ⬇️⬇️⬇️")
        print("🎯 STEP 2: TRADER ATTIVI QUALIFICATI")
        print("=" * 80)

        qualified = []

        for w, d in wallets.items():
            if d["volume"] < QUAL_MIN_VOLUME or d["trades"] < QUAL_MIN_TRADES:
                continue

            text_blob = " ".join(
                (v.get("title") or "").lower()
                for v in d["market_samples"].values()
            )

            if any(k in text_blob for k in QUAL_ALLOWED_KEYWORDS):
                qualified.append((w, d))

        qualified.sort(key=lambda x: x[1]["volume"], reverse=True)

        if qualified:
            print(f"✅ TRADER QUALIFICATI: {len(qualified)}")
        else:
            print("❌ Nessun trader qualificato trovato")

        for wallet, d in qualified:
            leaders.append(wallet)
            report.append({
                "wallet": wallet,
                "type": "qualified",
                "volume_24h": round(d["volume"], 2),
                "trades_24h": d["trades"],
                "markets_count": len(d["markets"]),
                "markets": list(d["market_samples"].values())[:15],
                "trades": d["trade_samples"]
            })

    # ─────────────────────────────────────────────
    # OUTPUT
    # ─────────────────────────────────────────────
    json.dump(leaders, open(AUTO_LEADERS_FILE, "w"), indent=2)
    json.dump(report, open(REPORT_FILE, "w"), indent=2)

    print("\n📊 CLASSIFICA WALLET (TOP per volume 24h)")
    print("=" * 80)
    ranking = sorted(wallets.items(), key=lambda x: x[1]["volume"], reverse=True)[:10]
    for i, (w, d) in enumerate(ranking, 1):
        print(
            f"{i:02d}. {w} | "
            f"volume={d['volume']:.2f} | "
            f"trades={d['trades']} | "
            f"mercati={len(d['markets'])}"
        )

    print("\n📌 Salvato:", AUTO_LEADERS_FILE)
    print("📌 Report:", REPORT_FILE)
    print("✅ LeaderFinder v5 completato")

# ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    main()
