import os
import json
import requests

# ============================================================
# LEADER FINDER – TEST MODE
# Scopo: trovare ALMENO un utente attivo
# ============================================================

CLOB_MARKETS_URL = "https://clob.polymarket.com/markets?limit=1000"
TRADES_URL = "https://data-api.polymarket.com/trades"

TRADE_LIMIT_PER_MARKET = 50
MAX_MARKETS_TO_SCAN = 30   # basta e avanza per il test

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)

LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")


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


def find_any_active_user():
    print("🧪 LeaderFinder TEST MODE: cerco un QUALUNQUE utente attivo")

    markets = fetch_markets()

    for m in markets:
        if m.get("status") != "open":
            continue

        market_id = m.get("id")
        if not market_id:
            continue

        try:
            trades = fetch_trades(market_id)
        except Exception:
            continue

        for t in trades:
            for key in ("makerAddress", "takerAddress", "buyerAddress", "sellerAddress"):
                addr = t.get(key)
                if addr:
                    leader = addr.lower()

                    with open(LEADERS_FILE, "w") as f:
                        json.dump([leader], f, indent=2)

                    print("✅ Utente trovato (TEST MODE)")
                    print(f"   Wallet: {leader}")
                    print(f"   Mercato: {m.get('question')}")
                    print(f"   Market ID: {market_id}")
                    print("👑 Salvato in auto_leaders.json")

                    return [leader]

    print("❌ Nessun utente trovato (caso molto raro)")
    with open(LEADERS_FILE, "w") as f:
        json.dump([], f)

    return []


if __name__ == "__main__":
    find_any_active_user()
