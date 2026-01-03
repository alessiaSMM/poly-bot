import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set

import requests

# =========================
# ENDPOINTS
# =========================
DATA_TRADES_URL = "https://data-api.polymarket.com/trades"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"

# =========================
# STATE / OUTPUT
# =========================
STATE_DIR = "state"
AUTO_LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")
LEADERS_REPORT_FILE = os.path.join(STATE_DIR, "leaders_report.json")

# =========================
# WINDOW
# =========================
LOOKBACK_HOURS = 24

# =========================
# STEP 1 – BALENE
# =========================
MIN_WHALE_VOLUME_USDC = 50_000
MIN_WHALE_TRADES = 20
MIN_WHALE_DISTINCT_MARKETS = 3

# =========================
# STEP 2 – QUALIFICATI
# =========================
MIN_TRADER_VOLUME_USDC = 1_000
MIN_TRADER_TRADES = 5
MIN_TRADER_DISTINCT_MARKETS = 2

ALLOWED_CATEGORIES_STEP2 = {
    "Politics", "US-current-affairs", "World",
    "Geopolitics", "Sports", "Sport", "Tech"
}

# =========================
# PERFORMANCE
# =========================
TRADES_LIMIT = 1000
TRADES_MAX_OFFSET = 10_000
REQ_TIMEOUT = 20
SLEEP_HTTP = 0.05
SLEEP_GAMMA = 0.03

# =========================
# UTILS
# =========================
def ensure_state_dir():
    os.makedirs(STATE_DIR, exist_ok=True)

def utc_now():
    return datetime.now(timezone.utc)

def ts_to_dt(ts_ms: int):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

def fmt_local(dt):
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def safe_float(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d

def side_badge(s):
    s = (s or "").upper()
    if s == "BUY":
        return "🟢 BUY"
    if s == "SELL":
        return "🔴 SELL"
    return "⚪️ ?"

# =========================
# 1) FETCH TRADES GLOBALI
# =========================
def fetch_trades_global_24h(cutoff):
    print("📡 Raccolta trade GLOBALI (ordinati per timestamp DESC)...")
    trades = []
    offset = 0
    ordering_supported = True

    while offset <= TRADES_MAX_OFFSET:
        params = {
            "limit": TRADES_LIMIT,
            "offset": offset,
        }

        if ordering_supported:
            params["sort"] = "timestamp"
            params["order"] = "desc"

        r = requests.get(DATA_TRADES_URL, params=params, timeout=REQ_TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError(f"Errore Data API: {r.status_code}")

        batch = r.json()
        if not batch:
            break

        # diagnostica batch
        first_ts = batch[0].get("timestamp")
        last_ts = batch[-1].get("timestamp")
        if first_ts and last_ts:
            print(
                f"   Batch offset={offset} | "
                f"first={fmt_local(ts_to_dt(first_ts))} | "
                f"last={fmt_local(ts_to_dt(last_ts))}"
            )

        stop = False
        for t in batch:
            ts = t.get("timestamp")
            if not ts:
                continue
            dt = ts_to_dt(int(ts))
            if dt < cutoff:
                stop = True
                break
            trades.append(t)

        if stop:
            break

        # se il primo batch sembra troppo vecchio, disabilita ordering
        if offset == 0 and first_ts:
            if ts_to_dt(first_ts) < cutoff:
                print("⚠️ WARNING: trade non ordinati correttamente, retry senza sort")
                ordering_supported = False
                offset = 0
                trades.clear()
                continue

        offset += TRADES_LIMIT
        time.sleep(SLEEP_HTTP)

    print(f"✅ Trade entro 24h raccolti: {len(trades)}")
    return trades

# =========================
# 2) GAMMA CHECK
# =========================
def gamma_market_by_condition_id(cid):
    params = {"conditionId": cid}
    r = requests.get(GAMMA_MARKETS_URL, params=params, timeout=REQ_TIMEOUT)
    if r.status_code != 200:
        return None
    data = r.json()
    if isinstance(data, list) and data:
        return data[0]
    if isinstance(data, dict):
        return data
    return None

def market_is_active(m):
    if not m:
        return False
    if m.get("closed") is True:
        return False
    if m.get("archived") is True:
        return False
    if "active" in m and m.get("active") is False:
        return False
    return True

# =========================
# 3) AGGREGATION
# =========================
def normalize_wallet(t):
    return (t.get("proxyWallet") or t.get("wallet") or "").lower() or None

def aggregate_wallets(trades, active_meta):
    wallets = {}

    for t in trades:
        cid = t.get("conditionId")
        if cid not in active_meta:
            continue

        w = normalize_wallet(t)
        if not w:
            continue

        m = active_meta[cid]
        vol = safe_float(t.get("size")) * safe_float(t.get("price"))
        ts = t.get("timestamp")
        dt = ts_to_dt(ts)

        e = wallets.setdefault(w, {
            "wallet": w,
            "volume": 0.0,
            "trades": 0,
            "markets": set(),
            "categories": set(),
            "recent": []
        })

        e["volume"] += vol
        e["trades"] += 1
        e["markets"].add(cid)
        e["categories"].add(m.get("category") or "—")

        e["recent"].append({
            "when": fmt_local(dt),
            "side": side_badge(t.get("side")),
            "vol": round(vol, 2),
            "price": t.get("price"),
            "outcome": t.get("outcome"),
            "question": m.get("question"),
            "category": m.get("category")
        })

    for e in wallets.values():
        e["markets_count"] = len(e["markets"])
        e["recent"].sort(key=lambda x: x["when"], reverse=True)

    return wallets

# =========================
# MAIN
# =========================
def main():
    ensure_state_dir()
    cutoff = utc_now() - timedelta(hours=LOOKBACK_HOURS)

    print("🔍 LeaderFinder v4.1 avviato")
    print(f"🕒 Cutoff UTC: {cutoff.strftime('%Y-%m-%d %H:%M:%S')}")

    trades = fetch_trades_global_24h(cutoff)
    if not trades:
        print("❌ Nessun trade recente rilevato (impossibile ma ora verificato)")
        return

    condition_ids = {t["conditionId"] for t in trades if t.get("conditionId")}
    print(f"🔎 conditionId unici dai trade: {len(condition_ids)}")

    active_meta = {}
    for i, cid in enumerate(condition_ids, 1):
        m = gamma_market_by_condition_id(cid)
        if market_is_active(m):
            active_meta[cid] = m
        if i % 100 == 0:
            print(f"   Gamma check: {i}/{len(condition_ids)} | attivi={len(active_meta)}")
        time.sleep(SLEEP_GAMMA)

    print(f"✅ Mercati ancora attivi: {len(active_meta)}")

    wallets = aggregate_wallets(trades, active_meta)
    print(f"👛 Wallet unici: {len(wallets)}")

    whales = [
        w for w in wallets.values()
        if w["volume"] >= MIN_WHALE_VOLUME_USDC
        and w["trades"] >= MIN_WHALE_TRADES
        and w["markets_count"] >= MIN_WHALE_DISTINCT_MARKETS
    ]

    if whales:
        whales.sort(key=lambda x: x["volume"], reverse=True)
        print(f"🐋 BALENE TROVATE: {len(whales)}")
        save_json(AUTO_LEADERS_FILE, [w["wallet"] for w in whales])
        save_json(LEADERS_REPORT_FILE, whales)
        return

    print("🚨 Nessuna balena trovata → STEP 2")

    qualified = [
        w for w in wallets.values()
        if w["volume"] >= MIN_TRADER_VOLUME_USDC
        and w["trades"] >= MIN_TRADER_TRADES
        and w["markets_count"] >= MIN_TRADER_DISTINCT_MARKETS
        and set(w["categories"]) & ALLOWED_CATEGORIES_STEP2
    ]

    if not qualified:
        print("❌ Nessun trader qualificato trovato")
        return

    qualified.sort(key=lambda x: x["volume"], reverse=True)
    print(f"✅ Trader qualificati: {len(qualified)}")

    save_json(AUTO_LEADERS_FILE, [w["wallet"] for w in qualified])
    save_json(LEADERS_REPORT_FILE, qualified)

if __name__ == "__main__":
    main()
