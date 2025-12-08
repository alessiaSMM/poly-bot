import os
import json
import requests
from app.market_mapper import MarketMapper

DATA_API = "https://data-api.polymarket.com/trades"

# 👑 BALENE DA SEGUIRE
LEADERS = [
    "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
]

# Percentuale di copia in PAPER MODE
COPY_FACTOR = 0.25

# File persistente per ricordare l’ultimo trade visto
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)
STATE_FILE = os.path.join(STATE_DIR, "leaders_state.json")


# ---------------------------------------------------------------------
# STATO PERSISTENTE
# ---------------------------------------------------------------------

def load_state():
    """Carica ultimo timestamp visto per ogni leader."""
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except:
        return {}


def save_state(state):
    """Salva ultimi timestamp per ogni leader."""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print("⚠️ Errore salvataggio stato leader:", e)


last_seen = load_state()


# ---------------------------------------------------------------------
# API CALLS
# ---------------------------------------------------------------------

def fetch_trades(user):
    """Ottiene ultimi trade del wallet dal Data API."""
    params = {"user": user, "limit": 50, "takerOnly": True}
    r = requests.get(DATA_API, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------
# MARKET MAPPER
# ---------------------------------------------------------------------

market_mapper = MarketMapper()
market_mapper.refresh()


# ---------------------------------------------------------------------
# LOOP PRINCIPALE COPY-TRADER
# ---------------------------------------------------------------------

def process_leader_trades():
    """Processa nuovi trade delle balene seguite."""

    global last_seen

    if not LEADERS:
        return

    for leader in LEADERS:

        la = leader.lower()
        if la not in last_seen:
            last_seen[la] = 0

        # Ottieni trade
        try:
            trades = fetch_trades(leader)
        except Exception as e:
            print(f"❌ Errore fetch trades per {leader}: {e}")
            continue

        new_trades = 0

        # Processa dal più vecchio al più nuovo
        for t in reversed(trades):

            ts = t.get("timestamp", 0)

            # Salta se non è nuovo
            if ts <= last_seen[la]:
                continue

            # Aggiorna ultimo timestamp visto
            last_seen[la] = ts
            new_trades += 1

            # Parametri principali
            side = t.get("side")
            size = float(t.get("size", 0))
            price = float(t.get("price", 0))
            outcome = t.get("outcome")
            title = t.get("title") or t.get("question")
            market_id = t.get("marketId")
            condition_id = t.get("conditionId")

            # PAPER MODE: copia della size
            my_size = size * COPY_FACTOR

            # Arricchimento tramite Market Mapper
            market = market_mapper.get_market_from_trade(t)

            category = market.get("category", "Unknown") if market else "Unknown"
            status = market.get("status", "Unknown") if market else "Unknown"
            created_at = market.get("createdAt") if market else None

            open_date = "-"
            if created_at:
                open_date = MarketMapper.ts_to_italian(int(created_at / 1000))

            trade_date = MarketMapper.ts_to_italian(ts)

            # BUY / SELL colorati
            if side and side.upper() == "BUY":
                side_symbol = "🟢 BUY"
            elif side and side.upper() == "SELL":
                side_symbol = "🔴 SELL"
            else:
                side_symbol = side

            # ------------------------------------------------------------
            # LOG DETTAGLIATO
            # ------------------------------------------------------------
            print("====================================")
            print(f"👑 Leader:      {leader}")
            print(f"🧾 Mercato:     {title}")
            print(f"📂 Categoria:   {category}")
            print(f"🔖 Stato:       {status}")
            print(f"📅 Apertura:    {open_date}")
            print(f"🎯 Outcome:     {outcome}")
            print(f"🆔 marketId:    {market_id}")
            print(f"🆔 condId:      {condition_id}")
            print(f"💼 LUI:         {side_symbol} {size} @ {price}")
            print(f"📝 TU (PAPER):  {side_symbol} {my_size:.4f} @ {price}")
            print(f"⏰ Quando:      {trade_date}")
            print("====================================")

        # Salva stato se ci sono trade nuovi
        if new_trades > 0:
            save_state(last_seen)
