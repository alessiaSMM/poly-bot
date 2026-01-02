import os
import json
import requests
from collections import Counter

# ============================================================
# CONFIGURAZIONE (REALISTICA 2025)
# ============================================================

CLOB_MARKETS_URL = "https://clob.polymarket.com/markets?limit=1000"
TRADES_URL = "https://data-api.polymarket.com/trades"

# Filtri volutamente LARGHI (Polymarket oggi è poco concentrato)
MIN_LIQUIDITY = 500            # $500 minimi (molti mercati vivi sono piccoli)
TRADE_LIMIT_PER_MARKET = 200   # prendiamo più storia
TOP_WALLETS = 10               # osserviamo più balene

# Nessuna whitelist categorie (le categorizzazioni cambiano spesso)
CATEGORY_WHITELIST = None

REQUEST_TIMEOUT = 15

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)

LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")


# ============================================================
# FETCH
# ============================================================

def fetch_markets():
    r = requests.get(CLOB_MARKETS_URL, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return data.get("data", data)


def fetch_trades(market_id):
    r = requests.get(
        TRADES_URL,
        params={"market": market_id, "limit": TRADE_LIMIT_PER_MARKET},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


# ============================================================
# CORE LOGIC
# ============================================================

def find_active_leaders():
    print("🔍 LeaderFinder: analisi mercati CLOB attivi...")

    markets = fetch_markets()

    eligible_markets = []
    for m in markets:
        if m.get("status") != "open":
            continue

        liquidity = float(m.get("liquidity", 0))
        if liquidity < MIN_LIQUIDITY:
            continue

        if CATEGORY_WHITELIST is not None:
            if m.get("category") not in CATEGORY_WHITELIST:
                continue

        market_id = m.get("id")
        if market_id:
            eligible_markets.append(m)

    print(f"📊 Mercati eleggibili: {len(eligible_markets)}")

    wallet_volume = Counter()
    wallet_trades = Counter()

    for m in eligible_markets:
        market_id = m.get("id")

        try:
            trades = fetch_trades(market_id)
        except Exception:
            continue

        for t in trades:
            size = float(t.get("size", 0))

            for key in (
                "makerAddress",
                "takerAddress",
                "buyerAddress",
                "sellerAddress",
            ):
                addr = t.get(key)
                if not addr:
                    continue

                addr = addr.lower()
                wallet_volume[addr] += size
                wallet_trades[addr] += 1

    if not wallet_volume:
        print("⚠️ Nessuna attività rilevante trovata")
        with open(LEADERS_FILE, "w") as f:
            json.dump([], f)
        return []

    # Ordiniamo per volume totale (criterio principale)
    ranked = sorted(
        wallet_volume.items(),
        key=lambda x: x[1],
        reverse=True,
    )

    leaders = []
    for wallet, vol in ranked:
        if wallet_trades[wallet] < 2:
            continue  # evita wallet con trade isolato
        leaders.append(wallet)
        if len(leaders) >= TOP_WALLETS:
            break

    with open(LEADERS_FILE, "w") as f:
        json.dump(leaders, f, indent=2)

    print("👑 LeaderFinder: balene selezionate")
    for w in leaders:
        print(f"   {w}  (trade={wallet_trades[w]}, vol≈{wallet_volume[w]:.2f})")

    return leaders


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    find_active_leaders()
