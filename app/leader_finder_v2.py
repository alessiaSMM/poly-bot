"""
LeaderFinder v6
================

OBIETTIVO
---------
Identificare balene o trader qualificati su Polymarket
valutandoli su una finestra MOBILE di 24 ORE,
con refresh periodico (15 minuti).

IMPORTANTE
----------
- I TRADER vengono valutati su 24h rolling window
- I TRADE nuovi (futuro copy) saranno solo quelli
  successivi all’ultimo refresh
- Questo script NON esegue trade
- Questo script NON resta in ascolto (no loop infinito)

È pensato per essere:
- eseguito ogni 15 minuti (cron / scheduler)
- idempotente
- deterministico
"""

import json
import os
import time
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# =========================
# CONFIGURAZIONE
# =========================

TRADES_API = "https://data-api.polymarket.com/trades"
STATE_DIR = "state"
TRADES_STATE_FILE = os.path.join(STATE_DIR, "trades_rolling_24h.json")
LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")
REPORT_FILE = os.path.join(STATE_DIR, "leaders_report.json")

ROLLING_WINDOW_HOURS = 24
REFRESH_MINUTES = 15

# CRITERI BALENE (STEP 1)
WHALE_MIN_VOLUME = 50_000     # USDC su 24h
WHALE_MIN_TRADES = 50

# CRITERI TRADER QUALIFICATI (STEP 2)
QUAL_MIN_VOLUME = 1_000       # USDC su 24h
QUAL_MIN_TRADES = 5

MAX_PAGES = 120               # limite difensivo API
PAGE_SIZE = 1000

os.makedirs(STATE_DIR, exist_ok=True)

# =========================
# UTILITY TIME
# =========================

def now_utc():
    return datetime.now(timezone.utc)

def parse_ts(ts):
    """
    Parsing robusto timestamp Polymarket.
    """
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(ts, str):
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return None

# =========================
# LOAD / SAVE STATE
# =========================

def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# =========================
# FETCH TRADE RECENTI
# =========================

def fetch_recent_trades(cutoff):
    """
    Scarica trade globali dal più recente all’indietro
    finché non si scende sotto il cutoff (24h).
    """
    print("📡 Raccolta trade GLOBALI...")
    trades = []
    offset = 0

    for page in range(MAX_PAGES):
        params = {
            "limit": PAGE_SIZE,
            "offset": offset,
            "order": "desc"
        }
        r = requests.get(TRADES_API, params=params, timeout=20)
        r.raise_for_status()
        batch = r.json()

        if not batch:
            break

        first_ts = parse_ts(batch[0]["timestamp"])
        last_ts = parse_ts(batch[-1]["timestamp"])

        print(
            f"   Batch offset={offset} | "
            f"first={first_ts} | last={last_ts}"
        )

        for t in batch:
            ts = parse_ts(t["timestamp"])
            if ts and ts >= cutoff:
                trades.append(t)
            else:
                return trades  # stop netto

        offset += PAGE_SIZE
        time.sleep(0.1)

    return trades

# =========================
# MAIN LOGIC
# =========================

def main():
    print("🔍 LeaderFinder v6 avviato")

    now = now_utc()
    cutoff = now - timedelta(hours=ROLLING_WINDOW_HOURS)

    print(f"🕒 Rolling window: ultime {ROLLING_WINDOW_HOURS}h")
    print(f"⏱️  Refresh previsto: ogni {REFRESH_MINUTES} min")
    print(f"📌 Cutoff UTC: {cutoff}")

    # -------------------------
    # 1. Carica stato precedente
    # -------------------------
    rolling_trades = load_json(TRADES_STATE_FILE, [])

    # Filtra solo quelli ancora validi
    rolling_trades = [
        t for t in rolling_trades
        if parse_ts(t["timestamp"]) >= cutoff
    ]

    # -------------------------
    # 2. Fetch nuovi trade
    # -------------------------
    new_trades = fetch_recent_trades(cutoff)

    # Deduplica (tx_hash + log_index se presenti)
    seen = {
        (t.get("tx_hash"), t.get("log_index"))
        for t in rolling_trades
    }

    fresh = []
    for t in new_trades:
        key = (t.get("tx_hash"), t.get("log_index"))
        if key not in seen:
            fresh.append(t)
            seen.add(key)

    print(f"➕ Trade nuovi aggiunti: {len(fresh)}")

    rolling_trades.extend(fresh)

    # Salva rolling window aggiornata
    save_json(TRADES_STATE_FILE, rolling_trades)

    print(f"📊 Trade totali in finestra 24h: {len(rolling_trades)}")

    # -------------------------
    # 3. Aggregazione per wallet
    # -------------------------
    stats = defaultdict(lambda: {
        "volume": 0.0,
        "trades": 0,
        "markets": set(),
        "recent": []
    })

    for t in rolling_trades:
        wallet = t["maker"]
        size = float(t.get("size", 0))
        price = float(t.get("price", 0))
        notional = size * price

        stats[wallet]["volume"] += notional
        stats[wallet]["trades"] += 1
        stats[wallet]["markets"].add(t.get("condition_id"))
        stats[wallet]["recent"].append(t)

    # -------------------------
    # 4. STEP 1 — BALENE
    # -------------------------
    whales = []
    for w, s in stats.items():
        if (
            s["volume"] >= WHALE_MIN_VOLUME
            and s["trades"] >= WHALE_MIN_TRADES
        ):
            whales.append((w, s))

    leaders = []
    report = {}

    if whales:
        print(f"🐋 BALENE TROVATE: {len(whales)}")
        for w, s in sorted(
            whales, key=lambda x: x[1]["volume"], reverse=True
        ):
            print(
                f"👑 {w} | volume 24h={s['volume']:.2f} | "
                f"trade={s['trades']} | mercati={len(s['markets'])}"
            )
            leaders.append(w)
            report[w] = s
    else:
        print("🚨 Nessuna balena trovata")

        # -------------------------
        # 5. STEP 2 — QUALIFICATI
        # -------------------------
        print("⬇️ TARGET ABBASSATO: TRADER ATTIVI QUALIFICATI")

        qualified = []
        for w, s in stats.items():
            if (
                s["volume"] >= QUAL_MIN_VOLUME
                and s["trades"] >= QUAL_MIN_TRADES
            ):
                qualified.append((w, s))

        if not qualified:
            print("❌ Nessun trader qualificato trovato")
        else:
            print(f"✅ Trader qualificati: {len(qualified)}")
            for w, s in sorted(
                qualified, key=lambda x: x[1]["volume"], reverse=True
            ):
                print(
                    f"👤 {w} | volume 24h={s['volume']:.2f} | "
                    f"trade={s['trades']} | mercati={len(s['markets'])}"
                )
                leaders.append(w)
                report[w] = s

    # -------------------------
    # 6. Output finale
    # -------------------------
    save_json(LEADERS_FILE, leaders)
    save_json(REPORT_FILE, report)

    print("📌 Salvato:", LEADERS_FILE)
    print("📌 Report:", REPORT_FILE)
    print("✅ LeaderFinder v6 completato")


if __name__ == "__main__":
    main()
