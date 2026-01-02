import os
import json
import requests
from datetime import datetime, timedelta, timezone

# =========================
# CONFIG
# =========================

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
TRADES_URL = "https://data-api.polymarket.com/trades"

STATE_DIR = "state"
WHALES_DIR = os.path.join(STATE_DIR, "whales")
AUTO_LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")

LOOKBACK_HOURS = 24

# STEP 1 – BALENE (STRICT)
MIN_WHALE_VOLUME = 50_000
MIN_WHALE_TRADES = 20

# STEP 2 – TRADER ATTIVI QUALIFICATI (TARGET ABBASSATO)
MIN_TRADER_VOLUME = 1_000
MIN_TRADER_TRADES = 5
MIN_DISTINCT_MARKETS = 2

ALLOWED_CATEGORIES_STEP2 = {
    "Politics",
    "US-current-affairs",
    "World",
    "Geopolitics",
    "Sport",
}

REQUEST_TIMEOUT = 20

# Pagination (Gamma /events usa limit+offset)
PAGE_LIMIT = 100
MAX_EVENT_PAGES = 200     # safety: 200*100 = 20.000 eventi (oltre ogni plausibilità)
MAX_TOTAL_MARKETS = 5000  # safety: evita esplosioni se l’API cambia comportamento

# =========================
# UTILS
# =========================

def ensure_dirs():
    os.makedirs(STATE_DIR, exist_ok=True)
    os.makedirs(WHALES_DIR, exist_ok=True)

def now_utc():
    return datetime.now(timezone.utc)

def parse_ts(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

def fmt_local(dt_utc):
    # stampa in timezone locale della VM; per Roma solitamente CET/CEST già configurato
    return dt_utc.astimezone().strftime("%Y-%m-%d %H:%M:%S")

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# =========================
# FETCH: EVENTS -> MARKETS (ATTIVI REALI)
# =========================

def fetch_all_active_markets_via_events():
    """
    Metodo raccomandato: /events?closed=false ... e poi estrazione markets dagli eventi.
    Doc: "Most efficient approach is to use the /events endpoint ... Always include closed=false"
    """
    print("📥 Caricamento mercati attivi via /events (paginato)")
    markets = []
    seen_condition_ids = set()

    offset = 0
    page = 0

    while True:
        page += 1
        if page > MAX_EVENT_PAGES:
            print("⚠️ Safety stop: troppe pagine eventi. Interrompo per evitare loop infinito.")
            break

        params = {
            "order": "id",
            "ascending": "false",
            "closed": "false",
            "active": "true",
            "archived": "false",
            "limit": PAGE_LIMIT,
            "offset": offset,
        }

        r = requests.get(GAMMA_EVENTS_URL, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        batch = r.json()

        if not batch:
            break

        # ogni event contiene una lista "markets"
        for ev in batch:
            ev_markets = ev.get("markets") or []
            for m in ev_markets:
                cid = m.get("conditionId") or m.get("condition_id") or m.get("conditionID")
                if not cid:
                    continue
                if cid in seen_condition_ids:
                    continue

                # filtro extra: scarta mercati chiusi/archiviati se presenti
                if m.get("closed") is True:
                    continue
                if m.get("archived") is True:
                    continue

                seen_condition_ids.add(cid)
                markets.append(m)

        offset += PAGE_LIMIT

        if page % 2 == 0:
            print(f"   → Eventi letti: {offset} | Mercati attivi unici: {len(markets)}")

        if len(markets) > MAX_TOTAL_MARKETS:
            print("⚠️ Safety stop: troppi mercati (oltre soglia).")
            break

    print(f"✅ Totale mercati attivi (da events): {len(markets)}")
    return markets

def fetch_trades(condition_id):
    r = requests.get(TRADES_URL, params={"conditionId": condition_id}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

# =========================
# CORE LOGIC
# =========================

def leader_finder():
    print("🔍 LeaderFinder v2.4 avviato")
    print("🎯 STEP 1: RICERCA BALENE (ultime 24h, criteri STRICT)")
    print("=" * 50)

    markets = fetch_all_active_markets_via_events()
    cutoff = now_utc() - timedelta(hours=LOOKBACK_HOURS)

    # stats per wallet
    stats = {}  # wallet -> {volume, trades[], markets:set()}

    for idx, m in enumerate(markets, 1):
        if idx % 50 == 0:
            print(f"🔎 Scansione mercati: {idx}/{len(markets)}")

        cid = m.get("conditionId") or m.get("condition_id") or m.get("conditionID")
        question = m.get("question") or m.get("title") or "—"
        category = m.get("category") or "—"

        if not cid:
            continue

        try:
            trades = fetch_trades(cid)
        except Exception:
            continue

        for t in trades:
            ts = t.get("timestamp")
            if not ts:
                continue

            dt = parse_ts(ts)
            if dt < cutoff:
                continue

            # maker/taker: prendo una delle due per "attribuire" il trade
            wallet = t.get("maker") or t.get("taker")
            if not wallet:
                continue

            size = float(t.get("size", 0) or 0)
            price = float(t.get("price", 0) or 0)
            volume = size * price

            s = stats.setdefault(wallet, {"volume": 0.0, "trades": [], "markets": set()})
            s["volume"] += volume
            s["markets"].add(question)

            s["trades"].append({
                "market": question,
                "category": category,
                "volume": volume,
                "size": size,
                "price": price,
                "side": t.get("side", "—"),
                "timestamp": fmt_local(dt),
                "conditionId": cid,
            })

    # =========================
    # STEP 1 – BALENE
    # =========================
    whales = []
    for wallet, s in stats.items():
        if s["volume"] >= MIN_WHALE_VOLUME and len(s["trades"]) >= MIN_WHALE_TRADES:
            whales.append((wallet, s))

    if whales:
        whales.sort(key=lambda x: x[1]["volume"], reverse=True)
        print("🐋 BALENE TROVATE (ultime 24h)")
        leaders = []

        for wallet, s in whales[:5]:
            print("=" * 50)
            print(f"👑 Wallet:         {wallet}")
            print(f"💰 Volume 24h:     {s['volume']:.2f} USDC")
            print(f"🔁 Trade 24h:      {len(s['trades'])}")
            print(f"🧾 Mercati distinti: {len(s['markets'])}")

            # stampa ultimi 25 trade (più recenti)
            s["trades"].sort(key=lambda tr: tr["timestamp"], reverse=True)
            print("🧾 Ultimi trade:")
            for tr in s["trades"][:25]:
                side = (tr["side"] or "").upper()
                badge = "🟢 BUY" if side == "BUY" else ("🔴 SELL" if side == "SELL" else "⚪️ ?")
                print(f"  {badge} | {tr['volume']:.2f} | {tr['price']:.6f} | {tr['timestamp']} | {tr['category']} | {tr['market']}")

            leaders.append(wallet)

            save_json(
                os.path.join(WHALES_DIR, f"{wallet}.json"),
                {
                    "wallet": wallet,
                    "type": "whale",
                    "volume_24h": s["volume"],
                    "trade_count": len(s["trades"]),
                    "distinct_markets": len(s["markets"]),
                    "trades": s["trades"],
                    "saved_at": fmt_local(now_utc()),
                },
            )

        save_json(AUTO_LEADERS_FILE, leaders)
        print("=" * 50)
        print("📌 Leader salvati in state/auto_leaders.json")
        return

    # =========================
    # STEP 2 – TRADER QUALIFICATI
    # =========================
    print("🚨 NESSUNA BALENA TROVATA")
    print("🎯 STEP 2: TRADER ATTIVI QUALIFICATI (24h, volume ≥ 1.000 USDC)")
    print("=" * 50)

    qualified = []
    for wallet, s in stats.items():
        categories = {tr["category"] for tr in s["trades"]}
        if (
            s["volume"] >= MIN_TRADER_VOLUME
            and len(s["trades"]) >= MIN_TRADER_TRADES
            and len(s["markets"]) >= MIN_DISTINCT_MARKETS
            and (categories & ALLOWED_CATEGORIES_STEP2)
        ):
            qualified.append((wallet, s))

    qualified.sort(key=lambda x: x[1]["volume"], reverse=True)

    if not qualified:
        print("❌ Nessun trader qualificato trovato")
        save_json(AUTO_LEADERS_FILE, [])
        return

    leaders = []
    for wallet, s in qualified[:5]:
        print("=" * 50)
        print(f"👤 Trader:         {wallet}")
        print(f"💰 Volume 24h:     {s['volume']:.2f} USDC")
        print(f"🔁 Trade 24h:      {len(s['trades'])}")
        print(f"🧾 Mercati distinti: {len(s['markets'])}")

        s["trades"].sort(key=lambda tr: tr["timestamp"], reverse=True)
        print("🧾 Ultimi trade:")
        for tr in s["trades"][:20]:
            side = (tr["side"] or "").upper()
            badge = "🟢 BUY" if side == "BUY" else ("🔴 SELL" if side == "SELL" else "⚪️ ?")
            print(f"  {badge} | {tr['volume']:.2f} | {tr['price']:.6f} | {tr['timestamp']} | {tr['category']} | {tr['market']}")

        leaders.append(wallet)

    save_json(AUTO_LEADERS_FILE, leaders)
    print("=" * 50)
    print("📌 Leader salvati in state/auto_leaders.json")

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    ensure_dirs()
    leader_finder()
