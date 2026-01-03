# app/leader_finder_v3.py
import os
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import requests

# =========================
# ENDPOINTS
# =========================
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
DATA_TRADES_URL = "https://data-api.polymarket.com/trades"

# =========================
# OUTPUT / STATE
# =========================
STATE_DIR = "state"
AUTO_LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")
LEADERS_REPORT_FILE = os.path.join(STATE_DIR, "leaders_report.json")

# =========================
# WINDOW
# =========================
LOOKBACK_HOURS = 24

# =========================
# STEP 1 (BALENE) – STRICT
# =========================
MIN_WHALE_VOLUME_USDC = 50_000.0
MIN_WHALE_TRADES = 20
MIN_WHALE_DISTINCT_MARKETS = 3

# =========================
# STEP 2 (QUALIFICATI) – FALLBACK
# =========================
MIN_TRADER_VOLUME_USDC = 1_000.0
MIN_TRADER_TRADES = 5
MIN_TRADER_DISTINCT_MARKETS = 2

# categorie: Step 1 = tutte; Step 2 = sottoinsieme
ALLOWED_CATEGORIES_STEP2 = {
    "Politics",
    "US Current Affairs",
    "World",
    "Geopolitics",
    "Sports",
    "Tech",
}

# =========================
# PERFORMANCE / PAGINATION
# =========================
REQ_TIMEOUT = 20

# Gamma pagination (limit+offset)
EVENTS_PAGE_LIMIT = 200
EVENTS_MAX_PAGES = 200  # safety

# Data API /trades pagination (limit+offset) – offset max 10k (doc)
TRADES_LIMIT = 1000
TRADES_MAX_OFFSET = 10_000

# Throttling leggero (rate limit /trades esiste)
SLEEP_BETWEEN_TRADE_CALLS_SEC = 0.05

# =========================
# HELPERS
# =========================
def ensure_state_dir() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def ts_ms_to_dt_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

def fmt_local(dt_utc: datetime) -> str:
    return dt_utc.astimezone().strftime("%Y-%m-%d %H:%M:%S")

def safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def save_json(path: str, data) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# =========================
# DATA MODELS
# =========================
@dataclass(frozen=True)
class MarketMeta:
    condition_id: str
    question: str
    slug: str
    event_id: int
    event_slug: str
    category: str
    end_date_iso: Optional[str]

# =========================
# GAMMA: eventi attivi -> mercati attivi
# =========================
def fetch_active_events_paginated() -> List[dict]:
    """
    Doc: usare active=true&closed=false per eventi live.
    """
    events: List[dict] = []
    offset = 0

    for page in range(1, EVENTS_MAX_PAGES + 1):
        params = {
            "active": "true",
            "closed": "false",
            "archived": "false",
            "limit": EVENTS_PAGE_LIMIT,
            "offset": offset,
            "order": "id",
            "ascending": "false",
        }
        r = requests.get(GAMMA_EVENTS_URL, params=params, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break

        events.extend(batch)
        offset += EVENTS_PAGE_LIMIT

        # log leggero
        if page % 2 == 0:
            print(f"📥 Eventi attivi letti: {len(events)}")

    return events

def derive_category_from_event(event: dict) -> str:
    """
    Event.tags = array di tag {label, slug, id}. In assenza: '—'
    """
    tags = event.get("tags") or []
    if not tags:
        return "—"
    # preferenza: label
    label = tags[0].get("label") or tags[0].get("slug")
    return label or "—"

def build_active_market_cache(events: List[dict]) -> Tuple[Dict[str, MarketMeta], Dict[int, Set[str]]]:
    """
    Ritorna:
    - condId -> MarketMeta
    - eventId -> set(condId) (per sanity check)
    """
    cond_to_meta: Dict[str, MarketMeta] = {}
    event_to_conditions: Dict[int, Set[str]] = {}

    for ev in events:
        ev_id_raw = ev.get("id")
        if ev_id_raw is None:
            continue
        try:
            ev_id = int(ev_id_raw)
        except Exception:
            continue

        ev_slug = ev.get("slug") or "—"
        category = derive_category_from_event(ev)
        end_date = ev.get("endDate") or ev.get("endDateIso") or ev.get("end_date_iso")

        markets = ev.get("markets") or []
        for m in markets:
            cid = m.get("conditionId") or m.get("condition_id") or m.get("conditionID")
            if not cid:
                continue

            # filtri extra: evitare chiusi/archiviati se presenti
            if m.get("closed") is True:
                continue
            if m.get("archived") is True:
                continue

            question = m.get("question") or m.get("title") or "—"
            slug = m.get("slug") or m.get("market_slug") or "—"

            cond_to_meta[cid] = MarketMeta(
                condition_id=cid,
                question=question,
                slug=slug,
                event_id=ev_id,
                event_slug=ev_slug,
                category=category,
                end_date_iso=end_date,
            )
            event_to_conditions.setdefault(ev_id, set()).add(cid)

    return cond_to_meta, event_to_conditions

# =========================
# DATA API: trades per eventId, stop a cutoff
# =========================
def fetch_trades_for_event_until_cutoff(event_id: int, cutoff_utc: datetime) -> List[dict]:
    """
    /trades supporta eventId + limit/offset.
    Niente filtro time lato server: stop client-side quando timestamp < cutoff.
    """
    collected: List[dict] = []
    offset = 0

    while offset <= TRADES_MAX_OFFSET:
        params = {
            "eventId": str(event_id),
            "limit": TRADES_LIMIT,
            "offset": offset,
            # default takerOnly=true da doc; lo lasciamo implicito (più "leader-like")
        }
        r = requests.get(DATA_TRADES_URL, params=params, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        batch = r.json()

        if not batch:
            break

        # trades ordinati per timestamp desc (doc)
        stop_here = False
        for t in batch:
            ts = t.get("timestamp")
            if ts is None:
                continue
            dt = ts_ms_to_dt_utc(int(ts))
            if dt < cutoff_utc:
                stop_here = True
                break
            collected.append(t)

        if stop_here:
            break

        offset += TRADES_LIMIT
        time.sleep(SLEEP_BETWEEN_TRADE_CALLS_SEC)

    return collected

# =========================
# AGGREGATION
# =========================
def compute_usdc_volume(trade: dict) -> float:
    # trade: size + price (doc)
    size = safe_float(trade.get("size"))
    price = safe_float(trade.get("price"))
    return size * price

def normalize_wallet(trade: dict) -> Optional[str]:
    # Data API /trades ritorna proxyWallet
    w = trade.get("proxyWallet") or trade.get("proxy_wallet") or trade.get("wallet")
    if not w:
        return None
    return str(w).lower()

def side_badge(side: str) -> str:
    s = (side or "").upper()
    if s == "BUY":
        return "🟢 BUY"
    if s == "SELL":
        return "🔴 SELL"
    return "⚪️ ?"

def pick_outcome(trade: dict) -> str:
    # Data API /trades include outcome + outcomeIndex
    oc = trade.get("outcome")
    if oc:
        return str(oc)
    oi = trade.get("outcomeIndex")
    if oi is None:
        return "—"
    return f"idx:{oi}"

def build_wallet_stats(
    trades: List[dict],
    active_cond_to_meta: Dict[str, MarketMeta],
) -> Dict[str, dict]:
    """
    Solo trade su mercati attivi: conditionId presente in active cache.
    """
    wallets: Dict[str, dict] = {}

    for t in trades:
        cid = t.get("conditionId")
        if not cid:
            continue
        if cid not in active_cond_to_meta:
            # vincolo richiesto: solo mercati ancora attivi
            continue

        wallet = normalize_wallet(t)
        if not wallet:
            continue

        meta = active_cond_to_meta[cid]
        vol = compute_usdc_volume(t)

        ts = t.get("timestamp")
        if ts is None:
            continue
        dt = ts_ms_to_dt_utc(int(ts))

        entry = wallets.setdefault(
            wallet,
            {
                "wallet": wallet,
                "volume_usdc": 0.0,
                "trade_count": 0,
                "distinct_markets": set(),
                "categories": set(),
                "recent_trades": [],  # list of dict
            },
        )

        entry["volume_usdc"] += vol
        entry["trade_count"] += 1
        entry["distinct_markets"].add(cid)
        entry["categories"].add(meta.category)

        entry["recent_trades"].append(
            {
                "timestamp_local": fmt_local(dt),
                "timestamp_utc_ms": int(ts),
                "side": t.get("side", "—"),
                "badge": side_badge(t.get("side", "")),
                "usdc_volume": round(vol, 6),
                "size": safe_float(t.get("size")),
                "price": safe_float(t.get("price")),
                "outcome": pick_outcome(t),
                "conditionId": cid,
                "question": meta.question,
                "slug": meta.slug,
                "eventSlug": meta.event_slug,
                "category": meta.category,
                "endDateIso": meta.end_date_iso or "—",
                "tx": t.get("transactionHash") or "—",
            }
        )

    # normalizza set -> list + sort trades desc
    for w in wallets.values():
        w["distinct_markets_count"] = len(w["distinct_markets"])
        w["distinct_markets"] = list(w["distinct_markets"])
        w["categories"] = sorted(list(w["categories"]))
        w["recent_trades"].sort(key=lambda x: x["timestamp_utc_ms"], reverse=True)

    return wallets

# =========================
# SELECTION
# =========================
def select_whales(wallets: Dict[str, dict]) -> List[dict]:
    whales = []
    for w in wallets.values():
        if (
            w["volume_usdc"] >= MIN_WHALE_VOLUME_USDC
            and w["trade_count"] >= MIN_WHALE_TRADES
            and w["distinct_markets_count"] >= MIN_WHALE_DISTINCT_MARKETS
        ):
            whales.append(w)
    whales.sort(key=lambda x: x["volume_usdc"], reverse=True)
    return whales

def select_qualified(wallets: Dict[str, dict]) -> List[dict]:
    qualified = []
    for w in wallets.values():
        if (
            w["volume_usdc"] >= MIN_TRADER_VOLUME_USDC
            and w["trade_count"] >= MIN_TRADER_TRADES
            and w["distinct_markets_count"] >= MIN_TRADER_DISTINCT_MARKETS
        ):
            cats = set(w["categories"])
            if cats & ALLOWED_CATEGORIES_STEP2:
                qualified.append(w)
    qualified.sort(key=lambda x: x["volume_usdc"], reverse=True)
    return qualified

# =========================
# PRINT
# =========================
def print_leader_block(w: dict, max_trades: int = 25) -> None:
    print("=" * 60)
    print(f"👑 Wallet:         {w['wallet']}")
    print(f"💰 Volume 24h:     {w['volume_usdc']:.2f} USDC")
    print(f"🔁 Trade 24h:      {w['trade_count']}")
    print(f"🧾 Mercati distinti: {w['distinct_markets_count']}")
    print(f"📂 Categorie:      {', '.join(w['categories']) if w['categories'] else '—'}")
    print("🧾 Trade recenti (campione):")

    for tr in w["recent_trades"][:max_trades]:
        print(
            f"  {tr['badge']} | {tr['usdc_volume']:.2f} USDC | "
            f"@ {tr['price']:.6f} | {tr['timestamp_local']} | "
            f"{tr['category']} | {tr['question']} | outcome={tr['outcome']}"
        )

# =========================
# MAIN
# =========================
def main():
    ensure_state_dir()

    print("🔍 LeaderFinder v3 avviato")
    cutoff = utc_now() - timedelta(hours=LOOKBACK_HOURS)
    print(f"🕒 Finestra: ultime {LOOKBACK_HOURS} ore (cutoff UTC: {cutoff.strftime('%Y-%m-%d %H:%M:%S')})")
    print("📌 Vincolo: solo trade 24h su mercati ANCORA attivi")

    # 1) eventi attivi -> mercati attivi (cache)
    print("📥 Caricamento eventi attivi da Gamma...")
    events = fetch_active_events_paginated()
    cond_to_meta, event_to_conditions = build_active_market_cache(events)
    active_event_ids = sorted(list(event_to_conditions.keys()))

    print(f"✅ Eventi attivi:  {len(active_event_ids)}")
    print(f"✅ Mercati attivi: {len(cond_to_meta)}")

    # 2) trades recenti per eventId (stop a cutoff)
    print("📡 Raccolta trade recenti per eventId (stop a cutoff)...")
    recent_trades: List[dict] = []
    scanned_events = 0

    for ev_id in active_event_ids:
        scanned_events += 1
        if scanned_events % 25 == 0:
            print(f"   🔎 Eventi scansionati: {scanned_events}/{len(active_event_ids)} | trade raccolti: {len(recent_trades)}")

        try:
            batch = fetch_trades_for_event_until_cutoff(ev_id, cutoff)
        except Exception:
            continue

        if batch:
            recent_trades.extend(batch)

    print(f"✅ Trade grezzi entro 24h (pre-filtro mercati attivi): {len(recent_trades)}")

    # 3) aggregazione per wallet, includendo SOLO conditionId attivi
    wallets = build_wallet_stats(recent_trades, cond_to_meta)
    print(f"✅ Wallet unici (post-filtro mercati attivi): {len(wallets)}")

    # 4) STEP 1 – balene
    print("🎯 STEP 1: BALENE (criteri STRICT, tutte le categorie)")
    whales = select_whales(wallets)

    report = {
        "generated_at_local": fmt_local(utc_now()),
        "lookback_hours": LOOKBACK_HOURS,
        "active_events": len(active_event_ids),
        "active_markets": len(cond_to_meta),
        "raw_trades_24h": len(recent_trades),
        "unique_wallets": len(wallets),
        "whales": [],
        "qualified": [],
    }

    if whales:
        print(f"🐋 BALENE TROVATE: {len(whales)}")
        leaders = []
        for w in whales[:10]:
            print_leader_block(w, max_trades=25)
            leaders.append(w["wallet"])
            report["whales"].append(w)
        save_json(AUTO_LEADERS_FILE, leaders)
        save_json(LEADERS_REPORT_FILE, report)
        print(f"📌 Salvato: {AUTO_LEADERS_FILE}")
        print(f"📌 Report:  {LEADERS_REPORT_FILE}")
        return

    # 5) STEP 2 – fallback
    print("🚨 NESSUNA BALENA TROVATA")
    print("⬇️⬇️⬇️ TARGET ABBASSATO: TRADER ATTIVI QUALIFICATI ⬇️⬇️⬇️")
    print("🎯 STEP 2: QUALIFICATI (24h, volume ≥ 1.000 USDC, categorie selezionate)")

    qualified = select_qualified(wallets)
    if not qualified:
        print("❌ Nessun trader qualificato trovato")
        save_json(AUTO_LEADERS_FILE, [])
        save_json(LEADERS_REPORT_FILE, report)
        print(f"📌 Salvato: {AUTO_LEADERS_FILE}")
        print(f"📌 Report:  {LEADERS_REPORT_FILE}")
        return

    print(f"✅ Trader qualificati trovati: {len(qualified)}")
    leaders = []
    for w in qualified[:10]:
        print_leader_block(w, max_trades=20)
        leaders.append(w["wallet"])
        report["qualified"].append(w)

    save_json(AUTO_LEADERS_FILE, leaders)
    save_json(LEADERS_REPORT_FILE, report)
    print(f"📌 Salvato: {AUTO_LEADERS_FILE}")
    print(f"📌 Report:  {LEADERS_REPORT_FILE}")

if __name__ == "__main__":
    main()
