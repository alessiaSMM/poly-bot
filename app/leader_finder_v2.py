import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone

# =========================
# CONFIGURAZIONE
# =========================

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
TRADES_URL = "https://data-api.polymarket.com/trades"

STATE_DIR = "state"
WHALES_DIR = os.path.join(STATE_DIR, "whales")
AUTO_LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")

MAX_MARKETS_TO_SCAN = 40
TRADE_LOOKBACK_HOURS = 24

# Criteri BALENA (NON MODIFICATI)
MIN_WHALE_VOLUME_USDC = 50_000
MIN_WHALE_TRADES = 20

REQUEST_TIMEOUT = 15

# =========================
# UTILS
# =========================

def ensure_dirs():
    os.makedirs(STATE_DIR, exist_ok=True)
    os.makedirs(WHALES_DIR, exist_ok=True)

def utc_now():
    return datetime.now(timezone.utc)

def parse_ts(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

def fmt_dt(dt: datetime) -> str:
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")

def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# =========================
# FETCH DATI
# =========================

def fetch_active_markets(limit=MAX_MARKETS_TO_SCAN):
    resp = requests.get(
        GAMMA_MARKETS_URL,
        params={"limit": limit},
        timeout=REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()

def fetch_trades(condition_id: str):
    resp = requests.get(
        TRADES_URL,
        params={"conditionId": condition_id},
        timeout=REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()

# =========================
# LOGICA BALENA
# =========================

def scan_for_whales():
    print("🔍 LeaderFinder v2.1 avviato")
    print("🎯 STEP 1: RICERCA BALENE (ultime 24h – criteri invariati)")
    print("====================================")

    markets = fetch_active_markets()
    print(f"📊 Mercati analizzati: {len(markets)}")

    cutoff = utc_now() - timedelta(hours=TRADE_LOOKBACK_HOURS)
    wallet_stats = {}

    for idx, m in enumerate(markets, 1):
        if idx % 10 == 0:
            print(f"🔎 Scansione mercati: {idx}/{len(markets)}")

        condition_id = m.get("conditionId")
        question = m.get("question", "—")
        category = m.get("category", "—")

        if not condition_id:
            continue

        try:
            trades = fetch_trades(condition_id)
        except Exception:
            continue

        for t in trades:
            ts_ms = t.get("timestamp")
            if not ts_ms:
                continue

            trade_dt = parse_ts(ts_ms)
            if trade_dt < cutoff:
                continue

            wallet = t.get("maker") or t.get("taker")
            if not wallet:
                continue

            size = float(t.get("size", 0))
            price = float(t.get("price", 0))
            volume = size * price

            if wallet not in wallet_stats:
                wallet_stats[wallet] = {
                    "volume": 0.0,
                    "trades": [],
                }

            wallet_stats[wallet]["volume"] += volume
            wallet_stats[wallet]["trades"].append({
                "market": question,
                "category": category,
                "size": size,
                "price": price,
                "volume": volume,
                "timestamp": fmt_dt(trade_dt),
                "conditionId": condition_id,
                "side": t.get("side", "—")
            })

    # =========================
    # VALUTAZIONE BALENE
    # =========================

    for wallet, stats in wallet_stats.items():
        total_volume = stats["volume"]
        trade_count = len(stats["trades"])

        if total_volume >= MIN_WHALE_VOLUME_USDC and trade_count >= MIN_WHALE_TRADES:
            print("🐋 BALENA TROVATA")
            print(f"👑 Wallet:          {wallet}")
            print(f"💰 Volume stimato:  {total_volume:.2f} USDC")
            print(f"🔁 Trade recenti:   {trade_count}")
            print("📋 DETTAGLIO TRADE (ultime 24h):")
            print("------------------------------------")

            for tr in stats["trades"]:
                emoji = "🟢 BUY" if tr["side"] == "buy" else "🔴 SELL"
                print(
                    f"{emoji} | {tr['market']} | "
                    f"{tr['size']:.2f} @ {tr['price']} | "
                    f"{tr['timestamp']} | {tr['category']}"
                )

            # Persistenza
            whale_path = os.path.join(WHALES_DIR, f"{wallet}.json")
            save_json(whale_path, {
                "wallet": wallet,
                "total_volume": total_volume,
                "trade_count": trade_count,
                "trades": stats["trades"],
                "window_hours": TRADE_LOOKBACK_HOURS,
                "saved_at": fmt_dt(utc_now())
            })

            save_json(AUTO_LEADERS_FILE, [wallet])

            print("------------------------------------")
            print(f"💾 Salvato storico in {whale_path}")
            print(f"📌 Leader attivo scritto in {AUTO_LEADERS_FILE}")
            return

    print("❌ Nessuna balena trovata nelle ultime 24 ore")
    save_json(AUTO_LEADERS_FILE, [])

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    ensure_dirs()
    scan_for_whales()
