import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import requests

# =========================
# ENDPOINTS
# =========================
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

# In v4.3: categorie disponibili SOLO se presenti nel trade payload.
# Se mancano, non blocchiamo nulla.
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
TRADES_MAX_OFFSET = 80_000  # stop per cutoff comunque
REQ_TIMEOUT = 25
SLEEP_HTTP = 0.03

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

def parse_trade_datetime_utc(trade: dict) -> Optional[datetime]:
    """
    Parsing robusto:
    - timestamp (secondi o millisecondi)
    - createdAt / created_at ISO
    """
    ts = trade.get("timestamp")
    if ts is not None and ts != "":
        try:
            ts_int = int(ts)
            if ts_int < 10_000_000_000:
                return datetime.fromtimestamp(ts_int, tz=timezone.utc)
            return datetime.fromtimestamp(ts_int / 1000, tz=timezone.utc)
        except Exception:
            pass

    for k in ("createdAt", "created_at"):
        v = trade.get(k)
        if v:
            try:
                return datetime.fromisoformat(str(v).replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                continue

    return None

def normalize_wallet(trade: dict) -> Optional[str]:
    w = trade.get("proxyWallet") or trade.get("wallet") or trade.get("maker") or trade.get("taker")
    if not w:
        return None
    return str(w).lower()

def compute_usdc_volume(trade: dict) -> float:
    # volume in USDC stimato: size * price
    return safe_float(trade.get("size")) * safe_float(trade.get("price"))

def trade_outcome(trade: dict) -> str:
    if trade.get("outcome") is not None:
        return str(trade.get("outcome"))
    if trade.get("outcomeIndex") is not None:
        return f"idx:{trade.get('outcomeIndex')}"
    return "—"

def trade_market_label(trade: dict) -> str:
    # Non sempre il trade contiene question/slug: fallback su conditionId.
    return (
        trade.get("question")
        or trade.get("title")
        or trade.get("market_question")
        or trade.get("marketTitle")
        or "—"
    )

def trade_category(trade: dict) -> str:
    return trade.get("category") or trade.get("marketCategory") or "—"

def trade_slug(trade: dict) -> str:
    return trade.get("slug") or trade.get("market_slug") or trade.get("marketSlug") or "—"

# =========================
# 1) FETCH TRADES GLOBALI 24H
# =========================
def fetch_trades_global_24h(cutoff_utc: datetime) -> List[dict]:
    print("📡 Raccolta trade GLOBALI (v4.3: niente Gamma, attivo = trade recente)")
    collected: List[dict] = []
    offset = 0
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

        dt_first = parse_trade_datetime_utc(batch[0])
        dt_last = parse_trade_datetime_utc(batch[-1])

        if dt_first and dt_last:
            print(f"   Batch offset={offset} | first={fmt_local(dt_first)} | last={fmt_local(dt_last)}")
        else:
            print(f"   Batch offset={offset} | first/last non parseabili")

        # raccogliamo solo >= cutoff
        for t in batch:
            dt = parse_trade_datetime_utc(t)
            if not dt:
                continue
            if dt >= cutoff_utc:
                collected.append(t)

        # stop rapido se sort funziona
        if use_sort and dt_last and dt_last < cutoff_utc:
            break

        # se sort non funziona (first già vecchio), disattiva e riparti
        if offset == 0 and use_sort and dt_first and dt_first < cutoff_utc:
            print("⚠️ sort/order ignorato: retry senza sort")
            use_sort = False
            collected.clear()
            offset = 0
            continue

        offset += TRADES_LIMIT
        time.sleep(SLEEP_HTTP)

    print(f"✅ Trade entro {LOOKBACK_HOURS}h raccolti: {len(collected)}")
    return collected

# =========================
# 2) AGGREGAZIONE WALLET (solo trade entro cutoff)
# =========================
def aggregate_wallets(trades_24h: List[dict]) -> Dict[str, dict]:
    wallets: Dict[str, dict] = {}

    for t in trades_24h:
        cid = t.get("conditionId")
        if not cid:
            continue

        dt = parse_trade_datetime_utc(t)
        if not dt:
            continue

        wallet = normalize_wallet(t)
        if not wallet:
            continue

        vol = compute_usdc_volume(t)
        side = t.get("side", "—")

        e = wallets.setdefault(wallet, {
            "wallet": wallet,
            "volume_usdc": 0.0,
            "trade_count": 0,
            "distinct_markets": set(),
            "categories": set(),
            "recent_trades": [],
            "example_markets": {},  # cid -> {label, slug, category}
        })

        e["volume_usdc"] += vol
        e["trade_count"] += 1
        e["distinct_markets"].add(cid)

        cat = trade_category(t)
        if cat and cat != "—":
            e["categories"].add(cat)

        # conserviamo un esempio market info per quel conditionId
        if cid not in e["example_markets"]:
            e["example_markets"][cid] = {
                "question": trade_market_label(t),
                "slug": trade_slug(t),
                "category": cat,
            }

        e["recent_trades"].append({
            "timestamp_local": fmt_local(dt),
            "badge": side_badge(side),
            "side": side,
            "usdc_volume": round(vol, 6),
            "size": safe_float(t.get("size")),
            "price": safe_float(t.get("price")),
            "outcome": trade_outcome(t),
            "conditionId": cid,
            "question": trade_market_label(t),
            "slug": trade_slug(t),
            "category": cat,
            "tx": t.get("transactionHash") or t.get("txHash") or "—",
        })

    for w in wallets.values():
        w["distinct_markets_count"] = len(w["distinct_markets"])
        w["distinct_markets"] = list(w["distinct_markets"])
        w["categories"] = sorted(list(w["categories"]))
        w["recent_trades"].sort(key=lambda x: x["timestamp_local"], reverse=True)

        # example markets list
        w["example_markets_list"] = []
        for cid, info in w["example_markets"].items():
            w["example_markets_list"].append({
                "conditionId": cid,
                "question": info.get("question", "—"),
                "slug": info.get("slug", "—"),
                "category": info.get("category", "—"),
            })

        # pulizia
        del w["example_markets"]

    return wallets

# =========================
# 3) SELEZIONE
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
            # Se abbiamo categorie, applichiamo filtro; se non le abbiamo, NON blocchiamo.
            cats = set(w["categories"])
            if not cats:
                qualified.append(w)
            else:
                if cats & ALLOWED_CATEGORIES_STEP2:
                    qualified.append(w)

    qualified.sort(key=lambda x: x["volume_usdc"], reverse=True)
    return qualified

# =========================
# 4) STAMPA
# =========================
def print_leader(w: dict, max_trades: int = 20, max_markets: int = 10) -> None:
    print("=" * 80)
    print(f"👑 Wallet:           {w['wallet']}")
    print(f"💰 Volume 24h:       {w['volume_usdc']:.2f} USDC")
    print(f"🔁 Trade 24h:        {w['trade_count']}")
    print(f"🧾 Mercati distinti: {w['distinct_markets_count']}")

    if w["categories"]:
        print(f"📂 Categorie (dal trade payload): {', '.join(w['categories'])}")
    else:
        print("📂 Categorie:        — (non presenti nel payload trade)")

    print("🧾 Mercati (campione):")
    for m in w["example_markets_list"][:max_markets]:
        print(f"  - {m['category']} | {m['question']} | slug={m['slug']} | condId={m['conditionId']}")

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

    print("🔍 LeaderFinder v4.3 avviato")
    print(f"🕒 Cutoff UTC: {cutoff.strftime('%Y-%m-%d %H:%M:%S')}")
    print("📌 Definizione: mercato 'attivo' = ha trade nelle ultime 24 ore (CLOB reality)")

    trades_24h = fetch_trades_global_24h(cutoff)
    if not trades_24h:
        print("❌ Nessun trade recente rilevato (questa volta sarebbe davvero anomalo).")
        save_json(AUTO_LEADERS_FILE, [])
        save_json(LEADERS_REPORT_FILE, {
            "generated_at_local": fmt_local(utc_now()),
            "lookback_hours": LOOKBACK_HOURS,
            "raw_trades_24h": 0,
            "unique_wallets": 0,
            "whales": [],
            "qualified": [],
        })
        return

    condition_ids = {t.get("conditionId") for t in trades_24h if t.get("conditionId")}
    condition_ids.discard(None)
    print(f"🔎 conditionId unici nei trade 24h: {len(condition_ids)}")

    wallets = aggregate_wallets(trades_24h)
    print(f"👛 Wallet unici (trade 24h): {len(wallets)}")

    report = {
        "generated_at_local": fmt_local(utc_now()),
        "lookback_hours": LOOKBACK_HOURS,
        "raw_trades_24h": len(trades_24h),
        "condition_ids_seen": len(condition_ids),
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
            print_leader(w, max_trades=25, max_markets=15)
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
    print("🎯 STEP 2: QUALIFICATI (24h, volume ≥ 1.000 USDC, categorie preferite se disponibili)")

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
        print_leader(w, max_trades=20, max_markets=10)
        leaders.append(w["wallet"])
        report["qualified"].append(w)

    save_json(AUTO_LEADERS_FILE, leaders)
    save_json(LEADERS_REPORT_FILE, report)
    print(f"📌 Salvato: {AUTO_LEADERS_FILE}")
    print(f"📌 Report:  {LEADERS_REPORT_FILE}")

if __name__ == "__main__":
    main()
