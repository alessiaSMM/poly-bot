import os
import json
import time
import requests

DATA_API = "https://data-api.polymarket.com/trades"

# 👇 QUI metti le balene che vuoi seguire
LEADERS = [
    # "0x1234567890abcdef1234567890abcdef12345678",
    # "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
]

# Quanto copi del loro size (0.25 = 25%)
COPY_FACTOR = 0.25

# File dove salviamo l’ultimo timestamp visto per ogni leader (persistente)
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # root progetto
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)
STATE_FILE = os.path.join(STATE_DIR, "leaders_state.json")


def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print("⚠️ Errore salvataggio stato leader:", e)


# stato in memoria (viene caricato una volta)
last_seen = load_state()


def fetch_trades(user):
    params = {"user": user, "limit": 50, "takerOnly": True}
    r = requests.get(DATA_API, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def process_leader_trades():
    global last_seen

    if not LEADERS:
        # Nessun leader configurato, niente da fare
        return

    for leader in LEADERS:
        la = leader.lower()
        if la not in last_seen:
            last_seen[la] = 0

        try:
            trades = fetch_trades(leader)
        except Exception as e:
            print(f"❌ Errore fetch trades per {leader}:", e)
            continue

        # La Data API di solito restituisce dal più recente al più vecchio
        # quindi li processiamo al contrario
        new_trades = 0

        for t in reversed(trades):
            ts = t.get("timestamp", 0)

            if ts <= last_seen[la]:
                continue

            last_seen[la] = ts
            new_trades += 1

            side = t.get("side")
            size = float(t.get("size", 0))
            price = float(t.get("price", 0))
            title = t.get("title") or t.get("question")
            outcome = t.get("outcome")
            market_id = t.get("marketId")
            condition_id = t.get("conditionId")

            my_size = size * COPY_FACTOR

            print("====================================")
            print(f"👑 Leader:   {leader}")
            print(f"🧾 Mercato:  {title}")
            print(f"🎯 Outcome:  {outcome}")
            print(f"🆔 marketId: {market_id}")
            print(f"🆔 condId:   {condition_id}")
            print(f"💼 LUI:      {side} {size} @ {price}")
            print(f"📝 TU (PAPER): {side} {my_size:.4f} @ {price}")
            print(f"⏰ ts:       {ts}")
            print("====================================")

        if new_trades > 0:
            save_state(last_seen)
