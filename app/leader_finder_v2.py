import os
import json
import requests
from datetime import datetime, timedelta, timezone

# =========================
# CONFIG
# =========================

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
TRADES_URL = "https://data-api.polymarket.com/trades"

STATE_DIR = "state"
WHALES_DIR = os.path.join(STATE_DIR, "whales")
AUTO_LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")

MAX_MARKETS_TO_SCAN = 40
LOOKBACK_HOURS = 24

# STEP 1 – BALENE (STRICT)
MIN_WHALE_VOLUME = 50_000
MIN_WHALE_TRADES = 20

# STEP 2 – TRADER ATTIVI QUALIFICATI (DOWNGRADE)
MIN_TRADER_VOLUME = 1_000      # ⬅️ RIDOTTO
MIN_TRADER_TRADES = 5
MIN_DISTINCT_MARKETS = 2
ALLOWED_CATEGORIES_STEP2 = {
    "Politics",
    "Geopolitics",
    "Sport",
    "US-current-affairs",
    "World"
}

REQUEST_TIMEOUT = 15

# =========================
# UTILS
# =========================

def ensure_dirs():
    os.makedirs(STATE_DIR, exist_ok=True)
    os.makedirs(WHALES_DIR, exist_ok=True)

def now_utc():
    return datetime.now(timezone.utc)

def parse_ts(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

def fmt(dt):
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# =========================
# FETCH
# =========================

def fetch_markets():
    r = requests.get(
        GAMMA_MARKETS_URL,
        params={"limit": MAX_MARKETS_TO_SCAN},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    return r.json()

def fetch_trades(condition_id):
    r = requests.get(
        TRADES_URL,
        params={"conditionId": condition_id},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    return r.json()

# =========================
# CORE LOGIC
# =========================

def leader_finder():
    print("🔍 LeaderFinder v2.3 avviato")
    print("🎯 STEP 1: RICERCA BALENE (24h, criteri STRICT)")
    print("=" * 40)

    markets = fetch_markets()
    cutoff = now_utc() - timedelta(hours=LOOKBACK_HOURS)

    stats = {}

    for i, m in enumerate(markets, 1):
        if i % 10 == 0:
            print(f"🔎 Scansione mercati: {i}/{len(markets)}")

        cid = m.get("conditionId")
        question = m.get("question", "—")
        category = m.get("category", "—")

        if not cid:
            continue

        try:
            trades = fetch_trades(cid)
        except Exception:
            continue

        for t in trades:
            ts = t.get("timestamp")
            if not ts:
                continue

            dt = parse_ts(ts)
            if dt < cutoff:
                continue

            wallet = t.get("maker") or t.get("taker")
            if not wallet:
                continue

            size = float(t.get("size", 0))
            price = float(t.get("price", 0))
            volume = size * price

            s = stats.setdefault(wallet, {
                "volume": 0.0,
                "trades": [],
                "markets": set()
            })

            s["volume"] += volume
            s["markets"].add(question)
            s["trades"].append({
                "market": question,
                "category": category,
                "volume": volume,
                "size": size,
                "price": price,
                "side": t.get("side", "—"),
                "timestamp": fmt(dt)
            })

    # =========================
    # STEP 1 – BALENE
    # =========================

    whales = []

    for wallet, s in stats.items():
        if s["volume"] >= MIN_WHALE_VOLUME and len(s["trades"]) >= MIN_WHALE_TRADES:
            whales.append((wallet, s))

    if whales:
        print("🐋 BALENE TROVATE")
        leaders = []

        for wallet, s in whales:
            print(
                f"👑 {wallet} | "
                f"volume 24h {s['volume']:.2f} USDC | "
                f"trade {len(s['trades'])}"
            )
            leaders.append(wallet)

            save_json(
                os.path.join(WHALES_DIR, f"{wallet}.json"),
                {
                    "wallet": wallet,
                    "type": "whale",
                    "volume_24h": s["volume"],
                    "trade_count": len(s["trades"]),
                    "trades": s["trades"],
                    "saved_at": fmt(now_utc())
                }
            )

        save_json(AUTO_LEADERS_FILE, leaders)
        return

    # =========================
    # STEP 2 – DOWNGRADE
    # =========================

    print("🚨 NESSUNA BALENA TROVATA")
    print("🎯 STEP 2: TRADER ATTIVI QUALIFICATI (24h, volume ≥ 1.000 USDC)")
    print("=" * 40)

    qualified = []

    for wallet, s in stats.items():
        categories = {t["category"] for t in s["trades"]}

        if (
            s["volume"] >= MIN_TRADER_VOLUME and
            len(s["trades"]) >= MIN_TRADER_TRADES and
            len(s["markets"]) >= MIN_DISTINCT_MARKETS and
            categories & ALLOWED_CATEGORIES_STEP2
        ):
            qualified.append((wallet, s))

    qualified.sort(key=lambda x: x[1]["volume"], reverse=True)

    if not qualified:
        print("❌ Nessun trader qualificato trovato")
        save_json(AUTO_LEADERS_FILE, [])
        return

    leaders = []
    for wallet, s in qualified[:3]:
        print(
            f"👤 Trader: {wallet} | "
            f"volume 24h {s['volume']:.2f} USDC | "
            f"trade {len(s['trades'])}"
        )
        leaders.append(wallet)

    save_json(AUTO_LEADERS_FILE, leaders)
    print("📌 Leader salvati in auto_leaders.json")

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    ensure_dirs()
    leader_finder()
