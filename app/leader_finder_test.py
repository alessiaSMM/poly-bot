import os
import json
import requests

# ============================================================
# LEADER FINDER – TEST MODE (ASTA BASSISSIMA, MA CORRETTA)
# Scopo: trovare almeno 1 utente con trade su un mercato attivo (open)
# Fonte mercati: Gamma API
# Fonte trade: Data API (/trades) con market=conditionId
# ============================================================

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
DATA_TRADES_URL = "https://data-api.polymarket.com/trades"

# Per trovare subito segnali: prendiamo i mercati più "vivi"
MARKETS_LIMIT = 200
MAX_MARKETS_TO_SCAN = 60
TRADES_LIMIT_PER_MARKET = 50

REQUEST_TIMEOUT = 20

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)

LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")


def fetch_active_markets():
    """
    Gamma API: mercati con flag chiari.
    active=true & closed=false & archived=false -> mercati "aperti" in senso pratico.
    """
    params = {
        "limit": MARKETS_LIMIT,
        "offset": 0,
        "active": True,
        "closed": False,
        "archived": False,
        # ordinamento utile per trovare attività: volume 24h (se presente)
        "order": "volume24hr",
        "ascending": False,
    }
    r = requests.get(GAMMA_MARKETS_URL, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_trades_for_condition(condition_id: str):
    """
    Data API: /trades accetta market come lista di conditionId (comma-separated).
    """
    params = {
        "limit": TRADES_LIMIT_PER_MARKET,
        "market": condition_id,
        # proviamo ad allargare: includere anche maker+non solo taker
        "takerOnly": False,
    }
    r = requests.get(DATA_TRADES_URL, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def save_leaders(leaders):
    with open(LEADERS_FILE, "w") as f:
        json.dump(leaders, f, indent=2)


def find_any_active_user():
    print("🧪 LeaderFinder TEST MODE: cerco un QUALUNQUE utente con trade su mercati attivi")

    try:
        markets = fetch_active_markets()
    except Exception as e:
        print(f"❌ Errore fetch mercati (Gamma): {e}")
        save_leaders([])
        return []

    if not isinstance(markets, list):
        print("❌ Formato mercati inatteso (Gamma).")
        save_leaders([])
        return []

    print(f"📊 Mercati attivi letti (Gamma): {len(markets)}")

    scanned = 0

    for m in markets:
        if scanned >= MAX_MARKETS_TO_SCAN:
            break

        condition_id = m.get("conditionId")
        question = m.get("question", "")
        slug = m.get("slug", "")
        market_id = m.get("id", "")

        if not condition_id or not isinstance(condition_id, str) or not condition_id.startswith("0x"):
            continue

        scanned += 1

        try:
            trades = fetch_trades_for_condition(condition_id)
        except Exception:
            continue

        if not isinstance(trades, list) or len(trades) == 0:
            continue

        # Data API trades: campo più affidabile per "chi ha tradato"
        # proxyWallet = indirizzo profilo (wallet/proxy) dell'utente.
        for t in trades:
            wallet = t.get("proxyWallet")
            if wallet and isinstance(wallet, str) and wallet.startswith("0x"):
                leader = wallet.lower()

                save_leaders([leader])

                print("✅ Utente trovato (TEST MODE)")
                print(f"👑 Leader:      {leader}")
                print(f"🧾 Mercato:     {question}")
                print(f"🔗 Slug:        {slug}")
                print(f"🆔 Market ID:   {market_id}")
                print(f"🆔 conditionId: {condition_id}")
                print("📌 Salvato in state/auto_leaders.json")

                return [leader]

    print("❌ Nessun utente trovato: o mercati senza trade recenti, o limiti troppo bassi.")
    save_leaders([])
    return []


if __name__ == "__main__":
    find_any_active_user()
