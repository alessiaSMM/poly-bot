import os
import json
import time
import requests
from app.market_mapper import MarketMapper

DATA_API = "https://data-api.polymarket.com/trades"

# 👑 BALENE DA SEGUIRE
LEADERS = [
    "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
]

# Copia il 25% dell'operazione (solo PAPER MODE)
COPY_FACTOR = 0.25

# File persistente per ricordare l’ultimo trade visto
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # root del progetto
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)
STATE_FILE = os.path.join(STATE_DIR, "leaders_state.json")


# ---------------------------------------------------------------------
# STATO PERSISTENTE
# ---------------------------------------------------------------------

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except:
        return {}


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print("⚠️ Errore salvataggio stato leader:", e)


last_seen = load_state()


# ---------------------------------------------------------------------
# FUNZIONI API
# ---------------------------------------------------------------------

def fetch_trades(user):
    params = {"user": user, "limit": 50, "takerOnly": True}
    r = requests.get(DATA_API, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------
# MARKET MAPPER (per arricchire i trade)
# ---------------------------------------------------------------------

market_mapper = MarketMapper()
market_mapper.refresh()


# ---------------------------------------------------------------------
# LOOP PRINCIPALE COPY-TRADER
# ---------------------------------------------------------------------

def process_leader_trades():
    global last_seen

    if not LEADERS:
        return

    for leader in LEADERS:

        la = leader.lower()
        if la not in last_seen:
            last_seen[la] = 0

        try:
            trades = fetch_trades(leader)
        except Exception as e:
            print(f"❌ Errore fetch trades per {leader}: {e}")
            continue

        # Data API → dal più recente al più vecchio
        # Noi li processiamo dal più vecchio al più recente
        new_trades = 0

        for t in reversed(trades):
            ts = t.get("timestamp", 0)

            # Se non è più recente, ignora
            if ts <= last_seen[la]:
                continue

            last_seen[la] = ts
            new_trades += 1

            # Parametri del trade
            side = t.get("side")
            size = float(t.get("size", 0))
            price = float(t.get("price", 0))
            outcome = t.get("outcome")
            title = t.get("title") or t.get("question")
            market_id = t.get("marketId")
            condition_id = t.get("conditionId")

            # PAPER copy-size
            my_size = size * COPY_FACTOR

            # Arricchimento con Market Mapper
            market = market_mapper.get_market_from_trade(t)

            category = market.get("category", "Unknown") if market else "Unknown"
            status = market.get("status", "Unknown") if market else "Unknown"
            created_at = market.get("createdAt") if market else None

            open_date = "-"
            if created_at:
                open_date = MarketMapper.ts_to_italian(int(created_at / 1000))

            trade_date = MarketMapper.ts_to_italian(ts)

            # ------------------------------------------------------------
            # LOG MOLTO PULITO
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
            # Colori + emoji BUY/SELL
            if side.upper() == "BUY":
            side_symbol = "🟢 BUY"
            elif side.upper() == "SELL":
            side_symbol = "🔴 SELL"
            else:
            side_symbol = side

            print(f"💼 LUI:         {side_symbol} {size} @ {price}")
            print(f"📝 TU (PAPER):  {side_symbol} {my_size:.4f} @ {price}")

            print(f"⏰ Quando:      {trade_date}")
            print("====================================")

        if new_trades > 0:
            save_state(last_seen)
