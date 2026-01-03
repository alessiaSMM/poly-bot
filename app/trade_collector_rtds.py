#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
trade_collector_rtds.py

Collector RTDS Polymarket.
Interruzione GARANTITA con Ctrl+C (hard-exit controllato).
"""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, Optional

from websocket import WebSocketApp


RTDS_URL = "wss://ws-live-data.polymarket.com"
LOG_PATH = "state/trades_log.jsonl"


# ==========================
# Utility
# ==========================

class JsonlWriter:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

    def append(self, obj: Dict[str, Any]) -> None:
        with self.lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")


class Deduplicator:
    def __init__(self, maxlen: int = 50000) -> None:
        self.queue = deque(maxlen=maxlen)
        self.set = set()

    def is_new(self, key: str) -> bool:
        if key in self.set:
            return False
        if len(self.queue) == self.queue.maxlen:
            old = self.queue.popleft()
            self.set.discard(old)
        self.queue.append(key)
        self.set.add(key)
        return True


def extract_trade(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if msg.get("topic") != "activity" or msg.get("type") != "trades":
        return None

    payload = msg.get("payload")
    if not isinstance(payload, dict):
        return None

    return {
        "timestamp": msg.get("timestamp"),
        "received_at": int(time.time() * 1000),
        "proxyWallet": payload.get("proxyWallet"),
        "conditionId": payload.get("conditionId"),
        "side": payload.get("side"),
        "size": payload.get("size"),
        "price": payload.get("price"),
        "outcome": payload.get("outcome"),
        "transactionHash": payload.get("transactionHash"),
        "raw": payload,
    }


def trade_key(t: Dict[str, Any]) -> str:
    return "|".join(str(t.get(k)) for k in (
        "transactionHash",
        "timestamp",
        "proxyWallet",
        "conditionId",
        "side",
        "size",
        "price",
    ))


# ==========================
# WebSocket thread
# ==========================

def ws_thread(writer: JsonlWriter, dedup: Deduplicator) -> None:
    def on_open(ws: WebSocketApp) -> None:
        print("📡 Connesso a RTDS", flush=True)
        ws.send(json.dumps({
            "action": "subscribe",
            "subscriptions": [{
                "topic": "activity",
                "type": "trades",
            }]
        }))
        print("✅ Subscribe activity/trades inviato", flush=True)

    def on_message(ws: WebSocketApp, message: str) -> None:
        try:
            data = json.loads(message)
        except Exception:
            return

        trade = extract_trade(data)
        if not trade:
            return

        key = trade_key(trade)
        if not dedup.is_new(key):
            return

        writer.append(trade)

    def on_error(ws: WebSocketApp, error: Any) -> None:
        print(f"❌ WS error: {error}", flush=True)

    def on_close(ws: WebSocketApp, *_: Any) -> None:
        print("🔌 Connessione chiusa", flush=True)

    ws = WebSocketApp(
        RTDS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    ws.run_forever(ping_interval=5, ping_timeout=2)


# ==========================
# Main
# ==========================

def main() -> None:
    print(f"🌐 Connessione: {RTDS_URL}", flush=True)

    writer = JsonlWriter(LOG_PATH)
    dedup = Deduplicator()

    t = threading.Thread(
        target=ws_thread,
        args=(writer, dedup),
        daemon=True
    )
    t.start()

    def hard_exit(signum: int, frame: Any) -> None:
        print("\n🛑 Ctrl+C → uscita forzata", flush=True)
        os._exit(0)  # ← UNICA SOLUZIONE AFFIDABILE

    signal.signal(signal.SIGINT, hard_exit)
    signal.signal(signal.SIGTERM, hard_exit)

    # Thread principale dorme
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
