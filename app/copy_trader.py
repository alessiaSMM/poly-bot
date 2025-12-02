import time
import requests

DATA_API = "https://data-api.polymarket.com/trades"

# 👇 QUI devi mettere i wallet che vuoi seguire
LEADERS = [
    # "0x1234567890abcdef1234567890abcdef12345678",
]

COPY_FACTOR = 0.25   # copia al 25% del volume del leader
last_seen = {}       # stato interno: ultimo timestamp per leader


def fetch_trades(user):
    params = {"user": user, "limit": 50, "takerOnly": True}
    r = requests.get(DATA_API, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def process_leader_trades():
    for leader in LEADERS:

        la = leader.lower()
        if la not in last_seen:
            last_seen[la] = 0

        trades = fetch_trades(leader)

        for t in reversed(trades):
            ts = t["timestamp"]

            if ts <= last_seen[la]:
                continue

            last_seen[la] = ts

            side = t["side"]
            size = float(t["size"])
            price = float(t["price"])
            my_size = size * COPY_FACTOR

            title = t.get("title")
            outcome = t.get("outcome")

            print("====================================")
            print(f"Leader:   {leader}")
            print(f"Mercato:  {title}")
            print(f"Outcome:  {outcome}")
            print(f"LUI:      {side} {size} @ {price}")
            print(f"TU (PAPER):  {side} {my_size:.4f} @ {price}")
            print("====================================")
