import os
import json
import requests

# ============================================================
# CONFIG – TEST MODE (ASTA BASSISSIMA)
# ============================================================

CLOB_MARKETS_URL = "https://clob.polymarket.com/markets?limit=1000"
TRADES_URL = "https://data-api.polymarket.com/trades"

TRADE_LIMIT_PER_MARKET = 50
MAX_MARKETS_TO_SCAN = 20   # basta poco per il test

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)

LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")


# ============================================================
# FETCH
# ============================================================

def fetch_markets():
    r = requests.get(CLOB_MARKETS_URL, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data.get("data", data)


def fetch_trades(market_id):
    r = requests.get(
        TRADES_URL,
        params={"market": market_id, "limit": TRADE_LIMIT_PER_MARKET},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


# ============================================================
# CORE – FIND AT LEAST ONE ACTIVE USER
# ============================================================

def find_test_leader():
    print("🔍 LeaderFinder (TEST MODE): cerco QUALUNQUE utente attivo")

    markets = fetch_markets()

    scanned = 0
    leaders = []

    for m in markets:
        if m.get("status") != "open":
            continue

        market_id = m.get("id")
        if not market_id:
            continue

        scanned += 1
        if scanned > MAX_MARKETS_TO_SCAN:
            break

        try:
            trades = fetch_trades(market_id)
        except Exception:
            continue

        for t in trades:
            # prendiamo QUALUNQUE address disponibile
            for key in ("makerAddress", "takerAddress", "buyerAddress", "sellerAddress"):
                addr = t.get(key)
                if addr:
                    leaders.append(addr.lower())
                    print("✅ Trovato utente attivo:")
                    print(f"   Wallet: {addr}")
                    print(f"   Market: {m.get('question')}")
                    print(f"   Market ID: {market_id}")
                    break
            if leaders:
                break

        if leaders:
            break

    if not leaders:
        print("❌ Nessun utente trovato (caso raro)")
        with open(LEADERS_FILE, "w") as f:
            json.dump([], f)
        return []

    with open(LEADERS_FILE, "w") as f:
        json.dump(leaders, f, indent=2)

    print("👑 LeaderFinder: leader di TEST salvato")
    return leaders


if __name__ == "__main__":
    find_test_leader()
