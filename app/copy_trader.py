import os
import json
import requests

from app.market_mapper import MarketMapper

# Endpoint Data-API trades ufficiale
# https://data-api.polymarket.com/trades
DATA_API = "https://data-api.polymarket.com/trades"

# 👑 Whale / leader da seguire
LEADERS = [
    "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
]

# Fattore di copia in modalità PAPER (es: 0.25 = 25% della size della whale)
COPY_FACTOR = 0.25

# Directory per stato persistente
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)
STATE_FILE = os.path.join(STATE_DIR, "leaders_state.json")

# --------------------
# STATO TIMESTAMP TRADE
# --------------------


def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print(f"⚠️ Errore salvataggio stato: {e}")


last_seen = load_state()

# --------------------
# MARKET MAPPER
# --------------------

market_mapper = MarketMapper()
market_mapper.refresh()


# --------------------
# FUNZIONI DI SUPPORTO
# --------------------


def fetch_trades(user: str) -> list:
    """
    Recupera gli ultimi trade per un utente dalla Data-API.
    Docs: https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets
    """
    params = {
        "user": user,
        "limit": 50,
        "takerOnly": True,
    }
    r = requests.get(DATA_API, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def format_ts_italian(ts_raw: int | float) -> str:
    """
    Converte il timestamp dei trade in data/ora italiana.
    La Data-API usa tipicamente millisecondi (int64).
    """
    from app.market_mapper import MarketMapper as MM

    if ts_raw is None:
        return "-"

    try:
        ts = float(ts_raw)
    except Exception:
        return "-"

    # euristica: se è > 10^12 è quasi certamente millisecondi
    if ts > 1_000_000_000_000:
        ts = ts / 1000.0

    return MM.ts_to_italian(ts)


# --------------------
# LOOP COPY-TRADER
# --------------------


def process_leader_trades() -> None:
    """
    Legge i trade delle whale, li collega ai mercati CLOB
    e stampa cosa faresti TU in modalità PAPER.
    """
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

        new_trades = 0

        # processiamo dal più vecchio al più recente
        for t in reversed(trades):
            ts_raw = t.get("timestamp", 0)
            ts_numeric = 0

            try:
                ts_numeric = float(ts_raw)
            except Exception:
                ts_numeric = 0

            if ts_numeric <= last_seen[la]:
                continue

            last_seen[la] = ts_numeric
            new_trades += 1

            # --------------- DATI TRADE ---------------
            side = (t.get("side") or "").upper()
            size = float(t.get("size", 0) or 0.0)
            price = float(t.get("price", 0) or 0.0)
            outcome = t.get("outcome", "")
            title = t.get("title") or t.get("slug") or ""
            condition_id = t.get("conditionId")
            trade_time_it = format_ts_italian(ts_raw)
            my_size = size * COPY_FACTOR

            # --------------- INFO MERCATO ---------------
            market = market_mapper.get_market_from_condition(condition_id)

            if market:
                category = market.get("category") or "Unknown"
                status = MarketMapper.infer_status(market)
                end_iso = market.get("end_date_iso")
                end_it = MarketMapper.iso_to_italian(end_iso)
                slug = market.get("market_slug") or ""
            else:
                category = "Unknown"
                status = "Unknown"
                end_it = "-"
                slug = ""

            # --------------- BUY/SELL EMOJI ---------------
            if side == "BUY":
                side_label = "🟢 BUY"
            elif side == "SELL":
                side_label = "🔴 SELL"
            else:
                side_label = side or "?"

            # --------------- OUTPUT CONTESTUALIZZATO ---------------
            print("====================================")
            print(f"👑 Leader:      {leader}")
            print(f"🧾 Mercato:     {title}")
            if slug:
                print(f"🔗 Slug:        {slug}")
            print(f"📂 Categoria:   {category}")
            print(f"🔖 Stato:       {status}")
            print(f"📅 Scadenza:    {end_it}")
            print(f"🎯 Outcome:     {outcome}")
            print(f"🆔 condId:      {condition_id}")
            print(f"💼 LUI:         {side_label} {size} @ {price}")
            print(f"📝 TU (PAPER):  {side_label} {my_size:.4f} @ {price}")
            print(f"⏰ Quando:      {trade_time_it}")
            print("====================================")

        if new_trades > 0:
            save_state(last_seen)
