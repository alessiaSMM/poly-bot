import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import requests

# =========================
# ENDPOINTS
# =========================
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
DATA_TRADES_URL = "https://data-api.polymarket.com/trades"

# =========================
# STATE / OUTPUT
# =========================
STATE_DIR = "state"
AUTO_LEADERS_FILE = os.path.join(STATE_DIR, "auto_leaders.json")
LEADERS_REPORT_FILE = os.path.join(STATE_DIR, "leaders_report.json")

# =========================
# WINDOW
# =========================
LOOKBACK_HOURS = 24

# =========================
# STEP 1 – BALENE (STRICT)
# =========================
MIN_WHALE_VOLUME_USDC = 50_000.0
MIN_WHALE_TRADES = 20
MIN_WHALE_DISTINCT_MARKETS = 3

# =========================
# STEP 2 – QUALIFICATI (FALLBACK)
# =========================
MIN_TRADER_VOLUME_USDC = 1_000.0
MIN_TRADER_TRADES = 5
MIN_TRADER_DISTINCT_MARKETS = 2

ALLOWED_CATEGORIES_STEP2 = {
    # aggiustabili in futuro
    "Politics",
    "US-current-affairs",
    "World",
    "Geopolitics",
    "Sports",
    "Sport",
    "Tech",
}

# =========================
# PERFORMANCE
# =========================
REQ_TIMEOUT = 20

# Data API trades pagination
TRADES_LIMIT = 1000
TRADES_MAX_OFFSET = 10_000  # spesso limite pratico

# Throttle (gentile verso API)
SLEEP_BETWEEN_HTTP_SEC = 0.03

# Gamma lookups
GAMMA_LOOKUP_SLEEP_SEC = 0.02

# =========================
# UTILS
# =========================
def ensure_state_dir() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def ts_ms_to_dt_utc(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

def fmt_local(dt_utc: datetime) -> str:
    return dt_utc.astimezone().strftime("%Y-%m-%d %H:%M:%S")

def save_json(path: str, data) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def side_badge(side: str) -> str:
    s = (side or "").upper()
    if s == "BUY":
        return "🟢 BUY"
    if s == "SELL":
        return "🔴 SELL"
    return "⚪️ ?"

# =========================
# 1) TRADES GLOBALI 24H
# =========================
def fetch_global_trades_24h(cutoff_utc: datetime) -> List[dict]:
    """
    Scarica trade globali paginando per offset.
    Si ferma quando trova timestamp < cutoff (batch ordinati per timestamp decrescente nella pratica).
    """
    print("📡 Raccolta trade GLOBALI (stop quando < cutoff)...")
    collected: List[dict] = []
    offset = 0

    while offset <= TRADES_MAX_OFFSET:
        params = {"limit": TRADES_LIMIT, "offset": offset}
        r = requests.get(DATA_TRADES_URL, params=params, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        batch = r.json()

        if not batch:
            break

        stop = False
        for t in batch:
            ts = t.get("timestamp")
            if ts is None:
                continue
            dt = ts_ms_to_dt_utc(int(ts))
            if dt < cutoff_utc:
                stop = True
                break
            collected.append(t)

        # progress
        if offset % (TRADES_LIMIT * 2) == 0:
            print(f"   → trade raccolti: {len(collected)} (offset={offset})")

        if stop:
            break

        offset += TRADES_LIMIT
        time.sleep(SLEEP_BETWEEN_HTTP_SEC)

    print(f"✅ Trade entro 24h (grezzi): {len(collected)}")
    return collected

# =========================
# 2) GAMMA: verifichiamo SOLO i conditionId visti nei trade
# =========================
def gamma_fetch_market_by_condition_id(condition_id: str) -> Optional[dict]:
    """
    Gamma non sempre documenta lo stesso parametro, quindi proviamo vari nomi.
    Se torna una lista, prendiamo il primo.
    """
    candidates = [
        {"conditionId": condition_id},
        {"condition_id": condition_id},
        {"conditionID": condition_id},
        {"condition_ids": condition_id},  # qualche API usa questo pattern
    ]

    for params in candidates:
        try:
            r = requests.get(GAMMA_MARKETS_URL, params=params, timeout=REQ_TIMEOUT)
            if r.status_code != 200:
                continue
            data = r.json()
            if isinstance(data, list):
                if not data:
                    continue
                return data[0]
            if isinstance(data, dict):
                return data
        except Exception:
            continue

    return None

def is_market_still_active(market: dict) -> bool:
    """
    Interpreta i campi più comuni.
    - closed True => non attivo
    - archived True => non attivo
    - active False => non attivo
    Se un campo manca, non blocca da solo.
    """
    if market is None:
        return False

    if market.get("closed") is True:
        return False
    if market.get("archived") is True:
        return False
    if "active" in market and market.get("active") is False:
        return False

    return True

def build_active_meta_for_condition_ids(condition_ids: Set[str]) -> Dict[str, dict]:
    """
    Per ogni conditionId visto nei trade, chiediamo a Gamma se il mercato è ancora attivo.
    Cache solo su quello che serve: niente 30k mercati.
    """
    print(f"🧠 Verifica mercati su Gamma (conditionId unici da trade): {len(condition_ids)}")
    active_meta: Dict[str, dict] = {}

    checked = 0
    for cid in condition_ids:
        checked += 1
        if checked % 200 == 0:
            print(f"   → verificati: {checked}/{len(condition_ids)} | attivi: {len(active_meta)}")

        m = gamma_fetch_market_by_condition_id(cid)
        if m and is_market_still_active(m):
            active_meta[cid] = m

        time.sleep(GAMMA_LOOKUP_SLEEP_SEC)

    print(f"✅ Mercati ancora attivi (intersezione): {len(active_meta)}")
    return active_meta

# =========================
# 3) AGGREGAZIONE WALLET
# =========================
def normalize_wallet(trade: dict) -> Optional[str]:
    # data-api spesso include proxyWallet; fallback su maker/taker se presente
    w = trade.get("proxyWallet") or trade.get("proxy_wallet") or trade.get("wallet")
    if w:
        return str(w).lower()

    # fallback: in alcune risposte ci sono maker/taker
    w2 = trade.get("maker") or trade.get("taker")
    if w2:
        return str(w2).lower()

    return None

def compute_usdc_volume(trade: dict) -> float:
    size = safe_float(trade.get("size"))
    price = safe_float(trade.get("price"))
    return size * price

def trade_outcome(trade: dict) -> str:
    oc = trade.get("outcome")
    if oc:
        return str(oc)
    oi = trade.get("outcomeIndex")
    if oi is None:
        return "—"
    return f"idx:{oi}"

def extract_category(market: dict) -> str:
    return market.get("category") or "—"

def extract_question(market: dict) -> str:
    return market.get("question") or market.get("title") or "—"

def extract_slug(market: dict) -> str:
    return market.get("slug") or market.get("market_slug") or "—"

def extract_end_date(market: dict) -> str:
    return market.get("endDateIso") or market.get("end_date_iso") or market.get("endDate") or "—"

def build_wallet_stats(trades_24h: List[dict], active_meta: Dict[str, dict]) -> Dict[str, dict]:
    wallets: Dict[str, dict] = {}

    for t in trades_24h:
        cid = t.get("conditionId")
        if not cid:
            continue

        # vincolo richiesto: solo mercati ancora attivi
        m = active_meta.get(cid)
        if not m:
            continue

        wallet = normalize_wallet(t)
        if not wallet:
            continue

        vol = compute_usdc_volume(t)
        ts = t.get("timestamp")
        if ts is None:
            continue

        dt = ts_ms_to_dt_utc(int(ts))
        side = t.get("side", "—")

        entry = wallets.setdefault(
            wallet,
            {
                "wallet": wallet,
                "volume_usdc": 0.0,
                "trade_count": 0,
                "distinct_markets": set(),
                "categories": set(),
                "recent_trades": [],
            },
        )

        entry["volume_usdc"] += vol
        entry["trade_count"] += 1
        entry["distinct_markets"].add(cid)
        entry["categories"].add(extract_category(m))

        entry["recent_trades"].append(
            {
                "timestamp_local": fmt_local(dt),
                "timestamp_utc_ms": int(ts),
                "badge": side_badge(side),
                "side": side,
                "usdc_volume": round(vol, 6),
                "size": safe_float(t.get("size")),
                "price": safe_float(t.get("price")),
                "outcome": trade_outcome(t),
                "conditionId": cid,
                "question": extract_question(m),
                "slug": extract_slug(m),
                "category": extract_category(m),
                "endDateIso": extract_end_date(m),
                "tx": t.get("transactionHash") or t.get("txHash") or "—",
            }
        )

    # normalizza
    for w in wallets.values():
        w["distinct_markets_count"] = len(w["distinct_markets"])
        w["distinct_markets"] = list(w["distinct_markets"])
        w["categories"] = sorted(list(w["categories"]))
        w["recent_trades"].sort(key=lambda x: x["timestamp_utc_ms"], reverse=True)

    return wallets

# =========================
# 4) SELEZIONE
# =========================
def select_whales(wallets: Dict[str, dict]) -> List[dict]:
    res = []
    for w in wallets.values():
        if (
            w["volume_usdc"] >= MIN_WHALE_VOLUME_USDC
            and w["trade_count"] >= MIN_WHALE_TRADES
            and w["distinct_markets_count"] >= MIN_WHALE_DISTINCT_MARKETS
        ):
            res.append(w)
    res.sort(key=lambda x: x["volume_usdc"], reverse=True)
    return res

def select_qualified(wallets: Dict[str, dict]) -> List[dict]:
    res = []
    for w in wallets.values():
        if (
            w["volume_usdc"] >= MIN_TRADER_VOLUME_USDC
            and w["trade_count"] >= MIN_TRADER_TRADES
            and w["distinct_markets_count"] >= MIN_TRADER_DISTINCT_MARKETS
        ):
            cats = set(w["categories"])
            if cats & ALLOWED_CATEGORIES_STEP2:
                res.append(w)
    res.sort(key=lambda x: x["volume_usdc"], reverse=True)
    return res

# =========================
# 5) STAMPA
# =========================
def print_leader_block(w: dict, max_trades: int = 25) -> None:
    print("=" * 70)
    print(f"👑 Wallet:           {w['wallet']}")
    print(f"💰 Volume 24h:       {w['volume_usdc']:.2f} USDC")
    print(f"🔁 Trade 24h:        {w['trade_count']}")
    print(f"🧾 Mercati distinti: {w['distinct_markets_count']}")
    print(f"📂 Categorie:        {', '.join(w['categories']) if w['categories'] else '—'}")
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
def main() -> None:
    ensure_state_dir()

    cutoff = utc_now() - timedelta(hours=LOOKBACK_HOURS)
    print("🔍 LeaderFinder v4 avviato")
    print(f"🕒 Finestra: ultime {LOOKBACK_HOURS} ore (cutoff UTC: {cutoff.strftime('%Y-%m-%d %H:%M:%S')})")
    print("📌 Vincolo: solo trade 24h su mercati ANCORA attivi")

    # 1) trades globali 24h
    trades_24h = fetch_global_trades_24h(cutoff)

    # se davvero 0, inutile proseguire
    if not trades_24h:
        print("❌ Nessun trade rilevato nelle ultime 24 ore (globale).")
        save_json(AUTO_LEADERS_FILE, [])
        save_json(LEADERS_REPORT_FILE, {
            "generated_at_local": fmt_local(utc_now()),
            "lookback_hours": LOOKBACK_HOURS,
            "raw_trades_24h": 0,
            "active_markets_checked": 0,
            "unique_wallets": 0,
            "whales": [],
            "qualified": [],
        })
        return

    # 2) conditionId unici dai trade
    condition_ids = {t.get("conditionId") for t in trades_24h if t.get("conditionId")}
    condition_ids.discard(None)

    # 3) verifica su Gamma solo quei mercati
    active_meta = build_active_meta_for_condition_ids(condition_ids)

    # 4) aggregazione per wallet (solo trade su mercati attivi)
    wallets = build_wallet_stats(trades_24h, active_meta)
    print(f"✅ Wallet unici (post-filtro mercati attivi): {len(wallets)}")

    # report base
    report = {
        "generated_at_local": fmt_local(utc_now()),
        "lookback_hours": LOOKBACK_HOURS,
        "raw_trades_24h": len(trades_24h),
        "condition_ids_seen": len(condition_ids),
        "active_markets_checked": len(condition_ids),
        "active_markets_matched": len(active_meta),
        "unique_wallets": len(wallets),
        "whales": [],
        "qualified": [],
    }

    # STEP 1
    print("🎯 STEP 1: BALENE (criteri STRICT, tutte le categorie)")
    whales = select_whales(wallets)
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

    # STEP 2
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
