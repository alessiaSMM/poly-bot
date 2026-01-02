import os
import json
import requests
from collections import Counter

# -----------------------------------------
# CONFIG
# -----------------------------------------

CLOB_MARKETS_URL = "https://clob.polymarket.com/markets?limit=1000"
TRADES_URL = "https://data-api.polymarket.com/trades"

MIN_LIQUIDITY = 10_000          # minimo $10k
TOP_WALLETS = 5                 # quante balene tenere
TRADE_LIMIT_PER_MARKET = 50

CATEGORY_WHITELIST = {
    "Politics",
    "Sports",
    "Pop Culture",
    "Crypto",
    "Economics",
}

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)
LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")


# -----------------------------------------
# UTILS
# -----------------------------------------

def fetch_markets():
    r = requests.get(CLOB_MARKETS_URL, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data.get("data", data)


def fetch_trades(market_id):
    r = requests.get(
        TRADES_URL,
        params={"market": market_id, "limit": TRADE_LIMIT_PER_MARKET},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


# -----------------------------------------
# CORE
# -----------------------------------------

def find_active_leaders():
    markets = fetch_markets()

    eligible_markets = []
    for m in markets:
        if m.get("status") != "open":
            continue
        if float(m.get("liquidity", 0)) < MIN_LIQUIDITY:
            continue
        if m.get("category") not in CATEGORY_WHITELIST:
            continue
        eligible_markets.append(m)

    wallet_volume = Counter()

    for m in eligible_markets:
        market_id = m.get("id")
        if not market_id:
            continue

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
                if addr:
                    wallet_volume[addr.lower()] += size

    top_wallets = [w for w, _ in wallet_volume.most_common(TOP_WALLETS)]

    with open(LEADERS_FILE, "w") as f:
        json.dump(top_wallets, f, indent=2)

    print("👑 LeaderFinder: balene attive selezionate")
    for w in top_wallets:
        print("   ", w)

    return top_wallets


if __name__ == "__main__":
    find_active_leaders()
