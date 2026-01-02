import os
import json
import time
import requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta

# ============================================================
# LEADER FINDER v2 – CON STAMPA TRADE DETTAGLIATI
# ============================================================

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
DATA_TRADES_URL = "https://data-api.polymarket.com/trades"

REQUEST_TIMEOUT = 12
MARKETS_LIMIT = 300
MAX_MARKETS_TO_SCAN = 40
TRADES_LIMIT = 100

WHALE_MIN_TOTAL_VOLUME = 50_000.0
WHALE_MIN_TRADES = 3
WHALE_LOOKBACK_HOURS = 72

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)
LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")

ITALY_TZ = timezone(timedelta(hours=1))


# ------------------------------------------------------------
# UTILS
# ------------------------------------------------------------

def now_ts() -> int:
    return int(time.time())

def hours_ago_ts(hours: int) -> int:
    return now_ts() - hours * 3600

def ts_to_str(ts: int) -> str:
    utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    ita = utc.astimezone(ITALY_TZ)
    return f"{utc.strftime('%Y-%m-%d %H:%M:%S')} UTC | {ita.strftime('%Y-%m-%d %H:%M:%S')} IT"

def save_leaders(leaders):
    with open(LEADERS_FILE, "w") as f:
        json.dump(leaders, f, indent=2)

def http_get(url, params):
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


# ------------------------------------------------------------
# FETCH
# ------------------------------------------------------------

def fetch_markets():
    params = {
        "limit": MARKETS_LIMIT,
        "active": True,
        "closed": False,
        "archived": False,
    }
    return http_get(GAMMA_MARKETS_URL, params)

def fetch_trades(condition_id):
    params = {
        "limit": TRADES_LIMIT,
        "market": condition_id,
    }
    return http_get(DATA_TRADES_URL, params)


# ------------------------------------------------------------
# CORE
# ------------------------------------------------------------

def main():
    print("🔍 LeaderFinder v2 avviato")
    print("🎯 STEP 1: RICERCA BALENE (criteri STRICT – tutte le categorie)")

    markets = fetch_markets()
    cutoff = hours_ago_ts(WHALE_LOOKBACK_HOURS)

    stats = defaultdict(lambda: {
        "volume": 0.0,
        "trades": [],
    })

    for idx, m in enumerate(markets[:MAX_MARKETS_TO_SCAN], start=1):
        condition_id = m.get("conditionId")
        if not condition_id:
            continue

        trades = fetch_trades(condition_id)
        for t in trades:
            ts = t.get("timestamp")
            if not ts or ts < cutoff:
                continue

            wallet = t.get("proxyWallet")
            if not wallet:
                continue

            size = float(t.get("size", 0))
            price = float(t.get("price", 0))
            volume = size * price

            stats[wallet]["volume"] += volume
            stats[wallet]["trades"].append({
                "question": m.get("question"),
                "category": m.get("category"),
                "conditionId": condition_id,
                "side": "BUY" if t.get("side") == "buy" else "SELL",
                "size": size,
                "price": price,
                "timestamp": ts,
                "outcome": t.get("outcome"),
            })

            s = stats[wallet]
            if len(s["trades"]) >= WHALE_MIN_TRADES and s["volume"] >= WHALE_MIN_TOTAL_VOLUME:
                print("\n🐋 BALENA TROVATA")
                print(f"👑 Wallet:          {wallet}")
                print(f"💰 Volume stimato:  {s['volume']:.2f} USDC")
                print(f"🔁 Trade recenti:   {len(s['trades'])}")

                print("\n📜 TRADE RECENTI (più recenti in alto)")
                print("--------------------------------------------------")

                for tr in sorted(s["trades"], key=lambda x: x["timestamp"], reverse=True)[:10]:
                    emoji = "🟢" if tr["side"] == "BUY" else "🔴"
                    print(
                        f"{emoji} {tr['side']} | {tr['question']}\n"
                        f"   📂 {tr['category']} | 🎯 {tr['outcome']}\n"
                        f"   💼 {tr['size']} @ {tr['price']} = {tr['size'] * tr['price']:.2f} USDC\n"
                        f"   🆔 {tr['conditionId']}\n"
                        f"   ⏰ {ts_to_str(tr['timestamp'])}\n"
                    )

                save_leaders([wallet])
                return

    print("❌ Nessuna balena trovata")
    save_leaders([])


if __name__ == "__main__":
    main()
