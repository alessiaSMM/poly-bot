import os
import json
import time
import requests
from collections import defaultdict
from typing import Dict, Any, List, Optional, Tuple

# ============================================================
# LEADER FINDER v2 (OTTIMIZZATO)
# - STEP 1: BALENE (criteri strict, TUTTE le categorie)
# - STEP 2: TRADER QUALIFICATI (categorie filtrate: politics + sport)
# - PROGRESS LOG + EARLY STOP + RETRY
# ============================================================

# ---------------------------
# API ENDPOINTS
# ---------------------------
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
DATA_TRADES_URL = "https://data-api.polymarket.com/trades"

# ---------------------------
# PARAMETRI GENERALI
# ---------------------------
REQUEST_TIMEOUT = 12          # più basso per evitare "stall" lunghi
RETRIES = 2                   # retry leggero
RETRY_BACKOFF_SEC = 0.6

MARKETS_LIMIT = 300
MAX_MARKETS_TO_SCAN = 40      # ridotto: abbastanza per trovare segnali, molto più rapido
TRADES_LIMIT = 80

PROGRESS_EVERY = 10           # stampa progresso ogni N mercati scansionati

# ---------------------------
# STEP 1 – BALENE (STRICT)
# ---------------------------
WHALE_MIN_TOTAL_VOLUME = 50_000.0  # USDC stimati (size * price)
WHALE_MIN_TRADES = 3
WHALE_LOOKBACK_HOURS = 72

# ---------------------------
# STEP 2 – TRADER QUALIFICATI
# ---------------------------
TRADER_MIN_TOTAL_VOLUME = 3_000.0
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

def now_ts() -> int:
    return int(time.time())

def hours_ago_ts(hours: int) -> int:
    return now_ts() - int(hours * 3600)

def save_leaders(leaders: List[str]) -> None:
    with open(LEADERS_FILE, "w") as f:
        json.dump(leaders, f, indent=2)

def http_get_json(url: str, params: dict, timeout: int = REQUEST_TIMEOUT):
    last_err = None
    for attempt in range(RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
            else:
                raise last_err


# ============================================================
# FETCH
# ============================================================

def fetch_active_markets() -> List[Dict[str, Any]]:
    """
    Gamma API: mercati con flag chiari.
    Prendiamo solo mercati attivi, non chiusi, non archiviati.
    """
    params = {
        "limit": MARKETS_LIMIT,
        "active": True,
        "closed": False,
        "archived": False,
        # in alcune versioni Gamma, order/ascending sono accettati; se ignorati non fa danni
        "order": "volume24hr",
        "ascending": False,
    }
    data = http_get_json(GAMMA_MARKETS_URL, params=params)
    if isinstance(data, list):
        return data
    # fallback se API cambia formato
    return data.get("markets", []) if isinstance(data, dict) else []


def fetch_trades(condition_id: str) -> List[Dict[str, Any]]:
    """
    Data API: /trades con market=conditionId
    """
    params = {
        "limit": TRADES_LIMIT,
        "market": condition_id,
        "takerOnly": False,
    }
    data = http_get_json(DATA_TRADES_URL, params=params)
    return data if isinstance(data, list) else []


# ============================================================
# CORE
# ============================================================

def collect_stats_until_match(
    markets: List[Dict[str, Any]],
    lookback_hours: int,
    min_trades: int,
    min_volume: float,
    allowed_categories: Optional[set],
    label: str
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Scansiona mercati (fino a MAX_MARKETS_TO_SCAN), raccoglie trade recenti e
    ritorna appena trova un wallet che supera le soglie richieste.

    allowed_categories:
      - None => tutte le categorie
      - set  => almeno una categoria del wallet deve appartenere al set
    """
    cutoff = hours_ago_ts(lookback_hours)

    stats = defaultdict(lambda: {
        "volume": 0.0,
        "trades": 0,
        "markets": set(),
        "categories": set(),
        "example_market": None,
    })

    scanned = 0
    for m in markets:
        condition_id = m.get("conditionId")
        if not condition_id or not isinstance(condition_id, str) or not condition_id.startswith("0x"):
            continue

        scanned += 1
        if scanned % PROGRESS_EVERY == 0:
            print(f"🔎 {label} – scansione mercati: {scanned} / {MAX_MARKETS_TO_SCAN}")

        if scanned > MAX_MARKETS_TO_SCAN:
            break

        category = m.get("category")
        question = m.get("question")
        slug = m.get("slug")

        # Se abbiamo un filtro categorie, possiamo già scartare mercati fuori scope
        # (ma solo in STEP 2; in STEP 1 allowed_categories=None)
        if allowed_categories is not None:
            if category not in allowed_categories:
                continue

        try:
            trades = fetch_trades(condition_id)
        except Exception:
            continue

        if not trades:
            continue

        for t in trades:
            ts = t.get("timestamp")
            if not ts or ts < cutoff:
                continue

            wallet = t.get("proxyWallet")
            if not wallet or not isinstance(wallet, str) or not wallet.startswith("0x"):
                continue

            # volume stimato: size * price
            try:
                size = float(t.get("size", 0) or 0)
                price = float(t.get("price", 0) or 0)
            except Exception:
                continue

            vol = size * price

            s = stats[wallet]
            s["volume"] += vol
            s["trades"] += 1
            s["markets"].add(condition_id)
            if category:
                s["categories"].add(category)
            if s["example_market"] is None:
                s["example_market"] = {
                    "question": question,
                    "slug": slug,
                    "category": category,
                    "conditionId": condition_id,
                    "id": m.get("id"),
                }

            # verifica criteri
            if s["trades"] >= min_trades and s["volume"] >= min_volume:
                # se c'è filtro categorie, assicuriamoci che il wallet abbia almeno una categoria consentita
                if allowed_categories is not None and not (s["categories"] & allowed_categories):
                    continue
                return wallet.lower(), s

    return None


def step1_find_whale(markets: List[Dict[str, Any]]) -> bool:
    print("🎯 STEP 1: RICERCA BALENE (criteri STRICT – tutte le categorie)")
    res = collect_stats_until_match(
        markets=markets,
        lookback_hours=WHALE_LOOKBACK_HOURS,
        min_trades=WHALE_MIN_TRADES,
        min_volume=WHALE_MIN_TOTAL_VOLUME,
        allowed_categories=None,  # tutte
        label="STEP 1"
    )

    if not res:
        print("❌ Nessuna balena trovata con criteri STRICT")
        return False

    wallet, s = res
    ex = s.get("example_market") or {}

    print("🐋 BALENA TROVATA")
    print(f"👑 Wallet:          {wallet}")
    print(f"💰 Volume stimato:  {s['volume']:.2f} USDC")
    print(f"🔁 Trade recenti:   {s['trades']}")
    print(f"📂 Categorie:       {', '.join(sorted(s['categories'])) if s['categories'] else 'varie'}")
    print(f"🧾 Esempio mercato: {ex.get('question') or '-'}")
    save_leaders([wallet])
    return True


def step2_find_qualified(markets: List[Dict[str, Any]]) -> bool:
    print("\n" + "🚨" * 14)
    print("🚨 NESSUNA BALENA TROVATA CON CRITERI STRICT")
    print("🚨 TARGET ABBASSATO: CERCO TRADER ATTIVI E COMPETENTI (politica + sport)")
    print("🚨" * 14)

    res = collect_stats_until_match(
        markets=markets,
        lookback_hours=TRADER_LOOKBACK_HOURS,
        min_trades=TRADER_MIN_TRADES,
        min_volume=TRADER_MIN_TOTAL_VOLUME,
        allowed_categories=ALLOWED_CATEGORIES_STEP2,
        label="STEP 2"
    )

    if not res:
        print("❌ Nessun trader qualificato trovato (STEP 2)")
        save_leaders([])
        return False

    wallet, s = res
    ex = s.get("example_market") or {}

    print("✅ TRADER QUALIFICATO SELEZIONATO")
    print(f"👑 Wallet:          {wallet}")
    print(f"💰 Volume stimato:  {s['volume']:.2f} USDC")
    print(f"🔁 Trade recenti:   {s['trades']}")
    print(f"📂 Categorie:       {', '.join(sorted(s['categories'])) if s['categories'] else '-'}")
    print(f"🧾 Esempio mercato: {ex.get('question') or '-'}")
    print(f"🔗 Slug:            {ex.get('slug') or '-'}")
    print(f"🆔 conditionId:     {ex.get('conditionId') or '-'}")

    save_leaders([wallet])
    return True


def main():
    print("🔍 LeaderFinder v2 avviato")

    try:
        markets = fetch_active_markets()
    except Exception as e:
        print(f"❌ Errore fetch mercati (Gamma): {e}")
        save_leaders([])
        return

    if not markets:
        print("❌ Nessun mercato attivo trovato su Gamma")
        save_leaders([])
        return

    # STEP 1
    if step1_find_whale(markets):
        return

    # STEP 2
    step2_find_qualified(markets)


if __name__ == "__main__":
    main()
