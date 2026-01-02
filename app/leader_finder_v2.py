import os
import json
import time
import requests
from collections import defaultdict

# ============================================================
# LEADER FINDER v2
# - STEP 1: BALENE (criteri strict, TUTTE le categorie)
# - STEP 2: TRADER QUALIFICATI (categorie filtrate)
# ============================================================

# ---------------------------
# API ENDPOINTS
# ---------------------------
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
DATA_TRADES_URL = "https://data-api.polymarket.com/trades"

# ---------------------------
# PARAMETRI GENERALI
# ---------------------------
REQUEST_TIMEOUT = 20
MARKETS_LIMIT = 300
MAX_MARKETS_TO_SCAN = 120
TRADES_LIMIT = 100

# ---------------------------
# STEP 1 – BALENE (STRICT)
# ---------------------------
WHALE_MIN_TOTAL_VOLUME = 50_000      # USDC stimati
WHALE_MIN_TRADES = 3
WHALE_LOOKBACK_HOURS = 72            # trade recenti

# ---------------------------
# STEP 2 – TRADER QUALIFICATI
# ---------------------------
TRADER_MIN_TOTAL_VOLUME = 3_000
TRADER_MIN_TRADES = 2
TRADER_LOOKBACK_HOURS = 48

ALLOWED_CATEGORIES_STEP2 = {
    "Politics",
    "Geopolitics",
    "Elections",
    "World",
    "Macro",
    "Economy",
    "US Politics",
    "Sports",
}

# ---------------------------
# PATHS
# ---------------------------
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)

LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")

# ============================================================
# UTILS
# ============================================================

def now_ts():
    return int(time.time())

def hours_ago_ts(hours):
    return now_ts() - int(hours * 3600)

def save_leaders(leaders):
    with open(LEADERS_FILE, "w") as f:
        json.dump(leaders, f, indent=2)

# ============================================================
# FETCH
# ============================================================

def fetch_active_markets():
    params = {
        "limit": MARKETS_LIMIT,
        "active": True,
        "closed": False,
        "archived": False,
    }
    r = requests.get(GAMMA_MARKETS_URL, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def fetch_trades(condition_id):
    params = {
        "limit": TRADES_LIMIT,
        "market": condition_id,
        "takerOnly": False,
    }
    r = requests.get(DATA_TRADES_URL, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

# ============================================================
# CORE LOGIC
# ============================================================

def collect_trader_stats(markets, lookback_hours):
    cutoff = hours_ago_ts(lookback_hours)

    stats = defaultdict(lambda: {
        "volume": 0.0,
        "trades": 0,
        "markets": set(),
        "categories": set(),
    })

    scanned = 0

    for m in markets:
        if scanned >= MAX_MARKETS_TO_SCAN:
            break

        condition_id = m.get("conditionId")
        if not condition_id:
            continue

        scanned += 1

        try:
            trades = fetch_trades(condition_id)
        except Exception:
            continue

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
            stats[wallet]["trades"] += 1
            stats[wallet]["markets"].add(condition_id)
            if m.get("category"):
                stats[wallet]["categories"].add(m["category"])

    return stats

# ============================================================
# STEP 1 – BALENE
# ============================================================

def find_whales(markets):
    print("🎯 STEP 1: RICERCA BALENE (criteri STRICT – tutte le categorie)")
    stats = collect_trader_stats(markets, WHALE_LOOKBACK_HOURS)

    whales = []

    for wallet, s in stats.items():
        if (
            s["volume"] >= WHALE_MIN_TOTAL_VOLUME
            and s["trades"] >= WHALE_MIN_TRADES
        ):
            whales.append((wallet, s))

    whales.sort(key=lambda x: x[1]["volume"], reverse=True)

    if whales:
        w, s = whales[0]
        print("🐋 BALENA TROVATA")
        print(f"👑 Wallet: {w}")
        print(f"💰 Volume stimato: {s['volume']:.2f} USDC")
        print(f"🔁 Trade: {s['trades']}")
        print(f"📂 Categorie: {', '.join(s['categories']) or 'varie'}")
        save_leaders([w])
        return True

    print("❌ Nessuna balena trovata con criteri STRICT")
    return False

# ============================================================
# STEP 2 – TRADER QUALIFICATI
# ============================================================

def find_qualified_trader(markets):
    print("\n🚨 TARGET ABBASSATO 🚨")
    print("🎯 STEP 2: TRADER ATTIVI E QUALIFICATI (categorie filtrate)")

    stats = collect_trader_stats(markets, TRADER_LOOKBACK_HOURS)

    candidates = []

    for wallet, s in stats.items():
        if s["trades"] < TRADER_MIN_TRADES or s["volume"] < TRADER_MIN_TOTAL_VOLUME:
            continue

        if not s["categories"] & ALLOWED_CATEGORIES_STEP2:
            continue

        candidates.append((wallet, s))

    candidates.sort(key=lambda x: x[1]["volume"], reverse=True)

    if candidates:
        w, s = candidates[0]
        print("✅ TRADER QUALIFICATO SELEZIONATO")
        print(f"👑 Wallet: {w}")
        print(f"💰 Volume stimato: {s['volume']:.2f} USDC")
        print(f"🔁 Trade: {s['trades']}")
        print(f"📂 Categorie: {', '.join(s['categories'])}")
        save_leaders([w])
        return True

    print("❌ Nessun trader qualificato trovato")
    save_leaders([])
    return False

# ============================================================
# MAIN
# ============================================================

def main():
    print("🔍 LeaderFinder v2 avviato")

    try:
        markets = fetch_active_markets()
    except Exception as e:
        print(f"❌ Errore fetch mercati: {e}")
        save_leaders([])
        return

    if not isinstance(markets, list) or not markets:
        print("❌ Nessun mercato attivo trovato")
        save_leaders([])
        return

    if find_whales(markets):
        return

    find_qualified_trader(markets)

if __name__ == "__main__":
    main()
