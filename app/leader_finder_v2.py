import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set

import requests

# =========================
# ENDPOINTS
# =========================
DATA_TRADES_URL = "https://data-api.polymarket.com/trades"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"

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
TRADES_LIMIT = 1000
TRADES_MAX_OFFSET = 50_000  # aumentato: se serve, ma stoppiamo col cutoff
REQ_TIMEOUT = 25
SLEEP_HTTP = 0.05
SLEEP_GAMMA = 0.02

# =========================
# UTILS
# =========================
def ensure_state_dir() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

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
# TIME PARSING (FIX 1970)
# =========================
def parse_trade_datetime_utc(trade: dict) -> Optional[datetime]:
    """
    Parsing robusto per timestamp Polymarket:
    - timestamp in secondi o millisecondi
    - createdAt / created_at ISO string
    Ritorna datetime UTC.
    """
    # 1) timestamp numerico
    ts = trade.get("timestamp")
    if ts is not None and ts != "":
        try:
            ts_int = int(ts)
            # < 10^10 => secondi (es. 1700000000)
            if ts_int < 10_000_000_000:
                return datetime.fromtimestamp(ts_int, tz=timezone.utc)
            # altrimenti millisecondi (es. 1700000000000)
            return datetime.fromtimestamp(ts_int / 1000, tz=timezone.utc)
        except Exception:
            pass

    # 2) ISO strings
    for k in ("createdAt", "created_at"):
        v = trade.get(k)
        if v:
            try:
                # es: "2026-01-03T12:34:56.789Z"
                return datetime.fromisoformat(str(v).replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                continue

    return None

# =========================
# 1) TRADES GLOBALI ULTIME 24H
# =========================
def fetch_trades_global_24h(cutoff_utc: datetime) -> List[dict]:
    """
    Scarica trade globali e mantiene solo quelli >= cutoff.
    Importante: NON assumiamo unità del timestamp.
    Inoltre NON ci fidiamo ciecamente dell'ordinamento:
    - proviamo sort/order
    - se non funziona, continuiamo comunque finché troviamo trade >= cutoff,
      con una safety: max offset.
    """
    print("📡 Raccolta trade GLOBALI (con parsing timestamp robusto)...")

    collected: List[dict] = []
    offset = 0

    # proviamo ordering; se non cambia nulla, non ci rompe
    use_sort = True

    while offset <= TRADES_MAX_OFFSET:
        params = {"limit": TRADES_LIMIT, "offset": offset}
        if use_sort:
            params["sort"] = "timestamp"
            params["order"] = "desc"

        r = requests.get(DATA_TRADES_URL, params=params, timeout=REQ_TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"Errore Data API /trades: {r.status_code} - {r.text[:200]}")

        batch = r.json()
        if not batch:
            break

        # diagnostica: primi/ultimi tempi batch (con parsing robusto)
        dt_first = parse_trade_datetime_utc(batch[0])
        dt_last = parse_trade_datetime_utc(batch[-1])

        if dt_first and dt_last:
            print(
                f"   Batch offset={offset} | "
                f"first={fmt_local(dt_first)} | last={fmt_local(dt_last)}"
            )
        else:
            print(f"   Batch offset={offset} | (timestamp non parseabile su first/last)")

        # raccogliamo solo >= cutoff
        any_recent_in_batch = False
        for t in batch:
            dt = parse_trade_datetime_utc(t)
            if not dt:
                continue
            if dt >= cutoff_utc:
                collected.append(t)
                any_recent_in_batch = True

        # Se ordering è attivo e l'ultimo è già sotto cutoff, possiamo stopparci
        if use_sort and dt_last and dt_last < cutoff_utc:
            break

        # Se ordering è attivo ma vediamo che dt_first è già troppo vecchio,
        # vuol dire che sort/order non sta funzionando: disabilitiamo e riproviamo da capo.
        if offset == 0 and use_sort and dt_first and dt_first < cutoff_utc:
            print("⚠️ sort/order sembra ignorato: retry senza sort/order")
            use_sort = False
            collected.clear()
            offset = 0
            continue

        # Se non stiamo usando sort e non troviamo nulla di recente in questo batch,
        # non possiamo sapere se i prossimi offset contengono trade più recenti.
        # Però per evitare loop infinito, usiamo una regola pratica:
        # se già ai primi offset non compare nulla di recente, stoppiamo presto.
        if not use_sort and offset >= (TRADES_LIMIT * 3) and len(collected) == 0 and not any_recent_in_batch:
            print("⚠️ Nessun trade recente trovato nei primi batch senza ordering: stop di sicurezza")
            break

        offset += TRADES_LIMIT
        time.sleep(SLEEP_HTTP)

    print(f"✅ Trade entro {LOOKBACK_HOURS}h raccolti: {len(collected)}")
    return collected

# =========================
# 2) GAMMA: lookup mercati per conditionId (solo quelli visti nei trade)
# =========================
def gamma_market_by_condition_id(cid: str) -> Optional[dict]:
    """
    Gamma /markets spesso supporta filtro conditionId.
    Se torna lista, prendiamo il primo.
    """
    # Tentativi multipli (alcune varianti di parametro)
    params_list = [
        {"conditionId": cid},
        {"condition_id": cid},
        {"conditionID": cid},
    ]
    for params in params_list:
        try:
            r = requests.get(GAMMA_MARKETS_URL, params=params, timeout=REQ_TIMEOUT)
            if r.status_code != 200:
                continue
            data = r.json()
            if isinstance(data, list):
                if data:
                    return data[0]
            elif isinstance(data, dict):
                return data
        except Exception:
            continue
    return None

def market_is_active(m: dict) -> bool:
    if not m:
        return False
    if m.get("closed") is True:
        return False
    if m.get("archived") is True:
        return False
    if "active" in m and m.get("active") is False:
        return False
    return True

def build_active_meta(condition_ids: Set[str]) -> Dict[str, dict]:
    print(f"🧠 Verifica mercati su Gamma per conditionId unici: {len(condition_ids)}")
    active: Dict[str, dict] = {}

    for i, cid in enumerate(condition_ids, 1):
        m = gamma_market_by_condition_id(cid)
        if m and market_is_active(m):
            active[cid] = m

        if i % 150 == 0:
            print(f"   → Gamma check {i}/{len(condition_ids)} | attivi={len(active)}")

        time.sleep(SLEEP_GAMMA)

    print(f"✅ Mercati ancora attivi (intersezione): {len(active)}")
    return active

# =========================
# 3) AGGREGATION WALLET
# =========================
def normalize_wallet(trade: dict) -> Optional[str]:
    w = trade.get("proxyWallet") or trade.get("wallet") or trade.get("maker") or trade.get("taker")
    if not w:
        return None
    return str(w).lower()

def compute_usdc_volume(trade: dict) -> float:
    return safe_float(trade.get("size")) * safe_float(trade.get("price"))

def trade_outcome(trade: dict) -> str:
    if trade.get("outcome") is not None:
        return str(trade.get("outcome"))
    if trade.get("outcomeIndex") is not None:
        return f"idx:{trade.get('outcomeIndex')}"
    return "—"

def get_category(market: dict) -> str:
    return market.get("category") or "—"

def get_question(market: dict) -> str:
    return market.get("question") or market.get("title") or "—"

def get_slug(market: dict) -> str:
    return market.get("slug") or market.get("market_slug") or "—"

def get_end_date(market: dict) -> str:
    return market.get("endDateIso") or market.get("end_date_iso") or market.get("endDate") or "—"

def aggregate_wallets(trades_24h: List[dict], active_meta: Dict[str, dict]) -> Dict[str, dict]:
    wallets: Dict[str, dict] = {}

    for t in trades_24h:
        cid = t.get("conditionId")
        if not cid:
            continue

        # vincolo richiesto: solo mercati ancora attivi
        m = active_meta.get(cid)
        if not m:
            continue

        w = normalize_wallet(t)
        if not w:
            continue

        dt = parse_trade_datetime_utc(t)
        if not dt:
            continue

        vol = compute_usdc_volume(t)
        side = t.get("side", "—")

        e = wallets.setdefault(w, {
            "wallet": w,
            "volume_usdc": 0.0,
            "trade_count": 0,
            "distinct_markets": set(),
            "categories": set(),
            "recent_trades": []
        })

        e["volume_usdc"] += vol
        e["trade_count"] += 1
        e["distinct_markets"].add(cid)
        e["categories"].add(get_category(m))

        e["recent_trades"].append({
            "timestamp_local": fmt_local(dt),
            "timestamp_utc": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "badge": side_badge(side),
            "side": side,
            "usdc_volume": round(vol, 6),
            "size": safe_float(t.get("size")),
            "price": safe_float(t.get("price")),
            "outcome": trade_outcome(t),
            "conditionId": cid,
            "question": get_question(m),
            "slug": get_slug(m),
            "category": get_category(m),
            "endDateIso": get_end_date(m),
            "tx": t.get("transactionHash") or t.get("txHash") or "—",
        })

    # normalizza
    for e in wallets.values():
        e["distinct_markets_count"] = len(e["distinct_markets"])
        e["distinct_markets"] = list(e["distinct_markets"])
        e["categories"] = sorted(list(e["categories"]))
        e["recent_trades"].sort(key=lambda x: x["timestamp_local"], reverse=True)

    return wallets

# =========================
# 4) SELEZIONE
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
# 5) PRINT
# =========================
def print_leader(w: dict, max_trades: int = 25) -> None:
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
            f"@ {safe_float(tr['price']):.6f} | {tr['timestamp_local']} | "
            f"{tr['category']} | {tr['question']} | outcome={tr['outcome']}"
        )

# =========================
# MAIN
# =========================
def main():
    ensure_state_dir()

    cutoff = utc_now() - timedelta(hours=LOOKBACK_HOURS)
    print("🔍 LeaderFinder v4.2 avviato")
    print(f"🕒 Cutoff UTC: {cutoff.strftime('%Y-%m-%d %H:%M:%S')}")
    print("📌 Vincolo: solo trade 24h su mercati ANCORA attivi")

    trades_24h = fetch_trades_global_24h(cutoff)
    if not trades_24h:
        print("❌ Nessun trade recente rilevato (ora davvero strano).")
        save_json(AUTO_LEADERS_FILE, [])
        save_json(LEADERS_REPORT_FILE, {
            "generated_at_local": fmt_local(utc_now()),
            "lookback_hours": LOOKBACK_HOURS,
            "raw_trades_24h": 0,
            "condition_ids_seen": 0,
            "active_markets_matched": 0,
            "unique_wallets": 0,
            "whales": [],
            "qualified": [],
        })
        return

    condition_ids = {t.get("conditionId") for t in trades_24h if t.get("conditionId")}
    condition_ids.discard(None)
    print(f"🔎 conditionId unici dai trade recenti: {len(condition_ids)}")

    active_meta = build_active_meta(condition_ids)

    wallets = aggregate_wallets(trades_24h, active_meta)
    print(f"👛 Wallet unici (post-filtro attivi): {len(wallets)}")

    report = {
        "generated_at_local": fmt_local(utc_now()),
        "lookback_hours": LOOKBACK_HOURS,
        "raw_trades_24h": len(trades_24h),
        "condition_ids_seen": len(condition_ids),
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
            print_leader(w, max_trades=25)
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
        print_leader(w, max_trades=20)
        leaders.append(w["wallet"])
        report["qualified"].append(w)

    save_json(AUTO_LEADERS_FILE, leaders)
    save_json(LEADERS_REPORT_FILE, report)
    print(f"📌 Salvato: {AUTO_LEADERS_FILE}")
    print(f"📌 Report:  {LEADERS_REPORT_FILE}")

if __name__ == "__main__":
    main()
