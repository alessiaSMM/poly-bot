# app/leader_finder.py

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

TRADES_LOG = "state/trades_log.jsonl"
AUTO_LEADERS = "state/auto_leaders.json"
REPORT = "state/leaders_report.json"

WINDOW_HOURS = 24
WHALE_VOLUME = 50_000
QUALIFIED_VOLUME = 1_000


def load_recent_trades():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)
    trades = []

    with open(TRADES_LOG, "r") as f:
        for line in f:
            t = json.loads(line)
            ts = datetime.fromtimestamp(t["ts"] / 1000, tz=timezone.utc)
            if ts >= cutoff:
                trades.append(t)

    return trades


def aggregate(trades):
    wallets = defaultdict(lambda: {
        "volume": 0.0,
        "count": 0,
        "markets": set(),
        "recent": []
    })

    for t in trades:
        w = t["maker"]
        notional = t["size"] * t["price"]

        wallets[w]["volume"] += notional
        wallets[w]["count"] += 1
        wallets[w]["markets"].add(t["conditionId"])

        if len(wallets[w]["recent"]) < 10:
            wallets[w]["recent"].append(t)

    return wallets


def classify(wallets):
    whales = {}
    qualified = {}

    for w, d in wallets.items():
        if d["volume"] >= WHALE_VOLUME:
            whales[w] = d
        elif d["volume"] >= QUALIFIED_VOLUME:
            qualified[w] = d

    return whales, qualified


def main():
    print("🔍 LeaderFinder avviato")
    print(f"🕒 Finestra: ultime {WINDOW_HOURS} ore")

    trades = load_recent_trades()
    print(f"📊 Trade in finestra: {len(trades)}")

    wallets = aggregate(trades)
    whales, qualified = classify(wallets)

    print("\n🐋 BALENE")
    for w, d in sorted(whales.items(), key=lambda x: -x[1]["volume"]):
        print(f"  {w} | volume={d['volume']:.2f} | trade={d['count']} | mercati={len(d['markets'])}")

    if not whales:
        print("  — nessuna balena trovata")

    print("\n🎯 TRADER QUALIFICATI")
    for w, d in sorted(qualified.items(), key=lambda x: -x[1]["volume"]):
        print(f"  {w} | volume={d['volume']:.2f} | trade={d['count']}")

    leaders = list(whales.keys()) if whales else list(qualified.keys())

    with open(AUTO_LEADERS, "w") as f:
        json.dump(leaders, f, indent=2)

    with open(REPORT, "w") as f:
        json.dump({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "window_hours": WINDOW_HOURS,
            "whales": whales,
            "qualified": qualified,
        }, f, indent=2)

    print("\n📌 Salvato:", AUTO_LEADERS)
    print("📌 Report:", REPORT)


if __name__ == "__main__":
    main()
