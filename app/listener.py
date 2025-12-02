import asyncio
import json
import websockets

RTDS_URL = "wss://ws-live-data.polymarket.com"

# messaggio di subscribe preso dalla doc ufficiale RTDS Crypto Prices (Binance source)
SUBSCRIBE_MSG = {
    "action": "subscribe",
    "subscriptions": [
        {
            "topic": "crypto_prices",
            "type": "update",
            # "filters": "btcusdt,ethusdt"  # opzionale, se vuoi filtrare
        }
    ]
}

async def polymarket_listener(callback):
    while True:
        try:
            print(f"Mi collego a {RTDS_URL} ...")
            async with websockets.connect(RTDS_URL, ping_interval=10) as ws:
                print("✅ Connesso a RTDS (crypto_prices)")

                # invio subscribe come da documentazione
                await ws.send(json.dumps(SUBSCRIBE_MSG))
                print("📨 Subscribe inviato a RTDS")

                # ricevo messaggi in loop
                while True:
                    raw = await ws.recv()
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        print("Messaggio non JSON:", raw)
                        continue

                    # log minimale
                    print("EVENTO:", data.get("topic"), data.get("type"), data.get("payload"))

                    # callback utente (per il modulo analisi & filtri)
                    await callback(data)

        except Exception as e:
            print("Errore WebSocket RTDS:", e)
            print("Riprovo tra 5 secondi...")
            await asyncio.sleep(5)
