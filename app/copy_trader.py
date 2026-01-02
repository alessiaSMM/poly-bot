import os
import json
import requests

from app.market_mapper import MarketMapper

# ============================================================
# CONFIG
# ============================================================

DATA_API_TRADES = "https://data-api.polymarket.com/trades"

COPY_FACTOR = 0.25          # quanto copiare rispetto alla whale
TRADE_LIMIT = 50            # trade recenti per leader

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)

STATE_FILE = os.path.join(STATE_DIR, "leaders_state.json")
LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")


# ============================================================
# LEADERS
# ============================================================

def load_leaders():
    if not os.path.exists(LEADERS_FILE):
        print("⚠️ Nessuna balena trovata (auto_leaders.json mancante)")
        return []
    with open(LEADERS_FILE, "r") as f:
        return json.load(f)


LEADERS = load_leaders()


# ============================================================
# STATE
# ============================================================

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


last_seen = load_state()


# ============================================================
# MARKET MAPPER
# ============================================================

market_mapper = MarketMapper()
market_mapper.refresh()


# ============================================================
# UTILS
# ============================================================

def fetch_trades(leader):
    params = {
        "user": leader,
        "limit": TRADE_LIMIT,
        "takerOnly": True,
    }
    r = requests.get(DATA_API_TRADES, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def ts_to_italian(ts):
    if ts > 1_000_000_000_000:
        ts = ts / 1000
    return MarketMapper.ts_to_italian(ts)


# ============================================================
# CORE COPY-TRADER
# ============================================================

def process_leader_trades():
    global last_seen

    if not LEADERS:
        print("⚠️ Nessuna balena attiva caricata")
        return

    for leader in LEADERS:
        key = leader.lower()
        if key not in last_seen:
            last_seen[key] = 0

        try:
            trades = fetch_trades(leader)
        except Exception as e:
            print(f"❌ Errore fetch trade leader {leader}: {e}")
            continue

        nuovi = 0

        for trade in reversed(trades):
            ts = trade.get("timestamp", 0)
            if ts <= last_seen[key]:
                continue

            last_seen[key] = ts
            nuovi += 1

            side = (trade.get("side") or "").upper()
            size = float(trade.get("size", 0))
            price = float(trade.get("price", 0))
            outcome = trade.get("outcome")
            title = trade.get("title") or trade.get("question")
            condition_id = trade.get("conditionId")

            mio_size = size * COPY_FACTOR
            quando = ts_to_italian(ts)

            market = market_mapper.get_market_from_condition(condition_id)

            if market:
                categoria = market.get("category", "Unknown")
                stato = MarketMapper.infer_status(market)
                scadenza = MarketMapper.iso_to_italian(
                    market.get("end_date_iso")
                )
                storico = False
            else:
                categoria = "—"
                stato = "—"
                scadenza = "—"
                storico = True

            if side == "BUY":
                side_label = "🟢 BUY"
            elif side == "SELL":
                side_label = "🔴 SELL"
            else:
                side_label = side

            print("====================================")
            print(f"👑 Leader:      {leader}")
            print(f"🧾 Mercato:     {title}")
            print(f"📂 Categoria:   {categoria}")
            print(f"🔖 Stato:       {stato}")
            print(f"📅 Scadenza:    {scadenza}")
            if storico:
                print("⚠️ Mercato storico (non più presente nel CLOB)")
            print(f"🎯 Outcome:     {outcome}")
            print(f"🆔 condId:      {condition_id}")
            print(f"💼 LUI:         {side_label} {size:.2f} @ {price}")
            print(f"📝 TU (PAPER):  {side_label} {mio_size:.4f} @ {price}")
            print(f"⏰ Quando:      {quando}")
            print("====================================")

        if nuovi > 0:
            save_state(last_seen)
