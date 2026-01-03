#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
trade_collector_rtds.py

Collector real-time per Polymarket RTDS:
- Connessione: wss://ws-live-data.polymarket.com
- Sottoscrizione: topic="activity", type="trades"
- Output: append-only JSONL su state/trades_log.jsonl

Fonti:
- RTDS message/subscribe structure e ping guidance (docs Polymarket)  :contentReference[oaicite:2]{index=2}
- Topic activity/trades + schema payload Trade (repo Polymarket real-time-data-client) :contentReference[oaicite:3]{index=3}
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque, Dict, Optional

from websocket import WebSocketApp  # websocket-client


RTDS_URL_DEFAULT = "wss://ws-live-data.polymarket.com"
DEFAULT_LOG_PATH = "state/trades_log.jsonl"


@dataclass(frozen=True)
class Subscription:
    topic: str = "activity"
    msg_type: str = "trades"
    # RTDS filters: stringa JSON, es: {"event_slug":"..."} oppure {"market_slug":"..."}
    filters: Optional[str] = None
    # Auth opzionale: RTDS supporta clob_auth per alcuni topic; per activity/trades non risulta necessario.
    # Se lo imposti, verrà comunque inviato nel subscribe.
    clob_key: Optional[str] = None
    clob_secret: Optional[str] = None
    clob_passphrase: Optional[str] = None


class GracefulKiller:
    def __init__(self) -> None:
        self._stop = threading.Event()
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, *_: Any) -> None:
        self._stop.set()

    @property
    def stop_requested(self) -> bool:
        return self._stop.is_set()


class JsonlWriter:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def append(self, obj: Dict[str, Any]) -> None:
        line = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")


class LruSeen:
    """
    Dedup leggero (utile in reconnessioni / replay): conserva gli ultimi N token.
    Token tipico: transactionHash + timestamp + proxyWallet + conditionId + side + size + price.
    """

    def __init__(self, maxlen: int = 50000) -> None:
        self._maxlen = maxlen
        self._dq: Deque[str] = deque(maxlen=maxlen)
        self._set = set()
        self._lock = threading.Lock()

    def add_if_new(self, token: str) -> bool:
        with self._lock:
            if token in self._set:
                return False
            if len(self._dq) == self._maxlen:
                # espulsione manuale per mantenere set coerente con deque(maxlen)
                old = self._dq.popleft()
                self._set.discard(old)
            self._dq.append(token)
            self._set.add(token)
            return True


def now_ms() -> int:
    return int(time.time() * 1000)


def build_subscribe_payload(sub: Subscription) -> Dict[str, Any]:
    sub_obj: Dict[str, Any] = {
        "topic": sub.topic,
        "type": sub.msg_type,
    }
    if sub.filters:
        sub_obj["filters"] = sub.filters

    if sub.clob_key and sub.clob_secret and sub.clob_passphrase:
        sub_obj["clob_auth"] = {
            "key": sub.clob_key,
            "secret": sub.clob_secret,
            "passphrase": sub.clob_passphrase,
        }

    return {
        "action": "subscribe",
        "subscriptions": [sub_obj],
    }


def safe_json_loads(s: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(s)
    except Exception:
        return None


def extract_trade_minimal(message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalizza un messaggio RTDS in un record coerente per analisi successive.
    Struttura RTDS: {topic,type,timestamp,payload} :contentReference[oaicite:4]{index=4}
    Schema payload Trade dal client ufficiale :contentReference[oaicite:5]{index=5}
    """
    if not isinstance(message, dict):
        return None

    topic = message.get("topic")
    msg_type = message.get("type")
    ts = message.get("timestamp")
    payload = message.get("payload")

    if topic != "activity" or msg_type != "trades":
        return None
    if not isinstance(payload, dict):
        return None

    # Campi utili per leader-finding (proxyWallet, conditionId, side, size, price, timestamp, ecc.)
    record: Dict[str, Any] = {
        "received_at": now_ms(),
        "topic": topic,
        "type": msg_type,
        "timestamp": ts,
        "payload": payload,  # conservazione raw
        # estrazioni “flat” (più comode in batch)
        "proxyWallet": payload.get("proxyWallet"),
        "conditionId": payload.get("conditionId"),
        "side": payload.get("side"),
        "size": payload.get("size"),
        "price": payload.get("price"),
        "outcome": payload.get("outcome"),
        "outcomeIndex": payload.get("outcomeIndex"),
        "asset": payload.get("asset"),
        "slug": payload.get("slug"),
        "eventSlug": payload.get("eventSlug"),
        "title": payload.get("title"),
        "transactionHash": payload.get("transactionHash"),
    }
    return record


def make_dedup_token(rec: Dict[str, Any]) -> str:
    # token stabile: transazione + trade timestamp + wallet + mercato + parametri principali
    return "|".join(
        str(x)
        for x in (
            rec.get("transactionHash"),
            rec.get("timestamp"),
            rec.get("proxyWallet"),
            rec.get("conditionId"),
            rec.get("side"),
            rec.get("size"),
            rec.get("price"),
        )
    )


class RTDSTradeCollector:
    def __init__(
        self,
        url: str,
        subscription: Subscription,
        log_path: str,
        ping_interval_s: int = 5,
        reconnect_min_s: float = 1.0,
        reconnect_max_s: float = 60.0,
        dedup_max: int = 50000,
        verbose: bool = True,
    ) -> None:
        self.url = url
        self.subscription = subscription
        self.writer = JsonlWriter(log_path)
        self.ping_interval_s = ping_interval_s
        self.reconnect_min_s = reconnect_min_s
        self.reconnect_max_s = reconnect_max_s
        self.verbose = verbose

        self._killer = GracefulKiller()
        self._ws: Optional[WebSocketApp] = None
        self._ping_thread: Optional[threading.Thread] = None
        self._connected = threading.Event()
        self._seen = LruSeen(maxlen=dedup_max)

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(msg, flush=True)

    def _start_ping_loop(self) -> None:
        def _loop() -> None:
            # RTDS consiglia ping regolari (idealmente ogni 5s) :contentReference[oaicite:6]{index=6}
            while not self._killer.stop_requested:
                if self._connected.is_set() and self._ws and self._ws.sock:
                    try:
                        # ping frame WebSocket (non JSON) -> generalmente accettato dai server
                        self._ws.sock.ping(b"keepalive")
                    except Exception:
                        # la reconnessione scatta via on_error/on_close
                        pass
                time.sleep(self.ping_interval_s)

        self._ping_thread = threading.Thread(target=_loop, name="ping-loop", daemon=True)
        self._ping_thread.start()

    def _on_open(self, ws: WebSocketApp) -> None:
        self._connected.set()
        self._log("📡 Connesso a RTDS")

        payload = build_subscribe_payload(self.subscription)
        ws.send(json.dumps(payload))
        self._log(
            f"✅ Subscribe inviato: topic={self.subscription.topic} type={self.subscription.msg_type}"
            + (f" filters={self.subscription.filters}" if self.subscription.filters else "")
        )

    def _on_message(self, ws: WebSocketApp, message: str) -> None:
        obj = safe_json_loads(message)
        if obj is None:
            return

        rec = extract_trade_minimal(obj)
        if rec is None:
            return

        token = make_dedup_token(rec)
        if not self._seen.add_if_new(token):
            return

        self.writer.append(rec)

    def _on_error(self, ws: WebSocketApp, error: Any) -> None:
        self._connected.clear()
        self._log(f"❌ Errore WS: {error}")

    def _on_close(self, ws: WebSocketApp, status_code: Any, msg: Any) -> None:
        self._connected.clear()
        self._log(f"🔌 Connessione chiusa: code={status_code} msg={msg}")

    def run_forever(self) -> None:
        self._start_ping_loop()

        backoff = self.reconnect_min_s
        while not self._killer.stop_requested:
            self._ws = WebSocketApp(
                self.url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

            self._log(f"🌐 Connessione: {self.url}")
            try:
                # ping già gestito a livello applicativo (thread dedicato)
                self._ws.run_forever(ping_interval=None, ping_timeout=None)
            except Exception as e:
                self._log(f"❌ Eccezione run_forever: {e}")

            if self._killer.stop_requested:
                break

            # reconnessione con backoff esponenziale
            self._log(f"🔁 Reconnect tra {backoff:.1f}s ...")
            time.sleep(backoff)
            backoff = min(self.reconnect_max_s, backoff * 2)

        self._log("🛑 Stop richiesto, uscita pulita.")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Polymarket RTDS Activity Trades Collector -> JSONL")
    p.add_argument("--url", default=RTDS_URL_DEFAULT, help="RTDS websocket url")
    p.add_argument("--log", default=DEFAULT_LOG_PATH, help="Path file JSONL append-only")
    p.add_argument(
        "--filters",
        default=os.getenv("POLY_RTDS_FILTERS", ""),
        help='Filtri RTDS (stringa JSON). Esempi: \'{"event_slug":"..."}\' oppure \'{"market_slug":"..."}\'',
    )
    p.add_argument("--ping", type=int, default=5, help="Intervallo ping (secondi)")
    p.add_argument("--quiet", action="store_true", help="Riduce output console")

    # CLOB auth opzionale (per topic protetti; qui non necessario, però consentito)
    p.add_argument("--clob-key", default=os.getenv("POLY_CLOB_KEY", ""), help="CLOB API key")
    p.add_argument("--clob-secret", default=os.getenv("POLY_CLOB_SECRET", ""), help="CLOB API secret")
    p.add_argument("--clob-passphrase", default=os.getenv("POLY_CLOB_PASSPHRASE", ""), help="CLOB API passphrase")

    return p.parse_args()


def main() -> int:
    args = parse_args()

    filters = args.filters.strip() or None
    # normalizzazione: se input non parseable -> abort con messaggio chiaro
    if filters is not None:
        if safe_json_loads(filters) is None:
            print("❌ --filters non valido: serve una stringa JSON (es: '{\"event_slug\":\"...\"}')", file=sys.stderr)
            return 2

    sub = Subscription(
        topic="activity",
        msg_type="trades",
        filters=filters,
        clob_key=args.clob_key.strip() or None,
        clob_secret=args.clob_secret.strip() or None,
        clob_passphrase=args.clob_passphrase.strip() or None,
    )

    collector = RTDSTradeCollector(
        url=args.url,
        subscription=sub,
        log_path=args.log,
        ping_interval_s=args.ping,
        verbose=not args.quiet,
    )

    collector.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
