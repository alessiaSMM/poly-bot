import os
import json
import requests

from app.market_mapper import MarketMapper

DATA_API = "https://data-api.polymarket.com/trades"

LEADERS = [
    "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
]

COPY_FACTOR = 0.25

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)
STATE_FILE = os.path.join(STATE_DIR, "leaders_state.json")


# -------------------------------------------------
# STATO
# -------------------------------------------------

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except:
        return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


last_seen = load_state()


# -------------------------------------------------
# MARKET MAPPER
# -------------------------------------------------

market_mapper = MarketMapper()
market_mapper.refresh()


# -------------------------------------------------
# API
# -------------------------------------------------

def fetch_trades(user):
    params = {"user": user, "limit": 50, "takerOnly": True}
    r = requests.get(DATA_API, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def trade_ts_to_it(ts):
    if ts > 1_000_000_000_000:
        ts = ts / 1000
    return MarketMapper.ts_to_italian(ts)


# -------------------------------------------------
# COPY TRADER
# -------------------------------------------------

def process_leader_trades():
    global last_seen

    for leader in LEADERS:
        key = leader.lower()
        if key not in last_seen:
            last_seen[key] = 0

        trades = fetch_trades(leader)
        new = 0

        for t in reversed(trades):
            ts = t.get("timestamp", 0)
            if ts <= last_seen[key]:
                continue

            last_seen[key] = ts
            new += 1

            side = (t.get("side") or "").upper()
            size = float(t.get("size", 0))
            price = float(t.get("price", 0))
            outcome = t.get("outcome")
            title = t.get("title") or t.get("question")
            condition_id = t.get("conditionId")

            my_size = size * COPY_FACTOR
            trade_time = trade_ts_to_it(ts)

            market = market_mapper.get_market_from_condition(condition_id)

            if market:
                category = market.get("category", "Unknown")
                status = MarketMapper.infer_status(market)
                end_it = MarketMapper.iso_to_italian(
                    market.get("end_date_iso")
                )
                market_note = ""
            else:
                category = "—"
                status = "—"
                end_it = "—"
                market_note = "⚠️ Mercato storico (non più nel CLOB)"

            if side == "BUY":
                side_label = "🟢 BUY"
            elif side == "SELL":
                side_label = "🔴 SELL"
            else:
                side_label = side

            print("====================================")
            print(f"👑 Leader:      {leader}")
            print(f"🧾 Mercato:     {title}")
            print(f"📂 Categoria:   {category}")
            print(f"🔖 Stato:       {status}")
            print(f"📅 Scadenza:    {end_it}")
            if market_note:
                print(f"{market_note}")
            print(f"🎯 Outcome:     {outcome}")
            print(f"🆔 condId:      {condition_id}")
            print(f"💼 LUI:         {side_label} {size} @ {price}")
            print(f"📝 TU (PAPER):  {side_label} {my_size:.4f} @ {price}")
            print(f"⏰ Quando:      {trade_time}")
            print("====================================")

        if new:
            save_state(last_seen)
