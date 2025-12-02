import asyncio
import json
import websockets

RTDS_URL = "wss://ws-live-data.polymarket.com"

SUBSCRIPTION = {
    "action": "subscribe",
    "subscriptions": [
        {"topic": "crypto_prices", "type": "update"}
    ]
}

async def start_rtds_listener(callback=None):
    """
    Listener RTDS ufficiale Polymarket.
    Se callback è definita, ogni messaggio viene passato a quella funzione.
    """
    while True:
        try:
            print("🔵 Connessione a RTDS...")
            async with websockets.connect(
                RTDS_URL,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:

                await ws.send(json.dumps(SUBSCRIPTION))
                print("🟢 Sottoscritto a crypto_prices")

                while True:
                    msg = await ws.recv()

# Ignora messaggi non JSON (es. "CONNECTED", "PING", "PONG")
if not msg.startswith("{"):
    # print(f"Ignorato messaggio non JSON: {msg}")
    continue

try:
    data = json.loads(msg)
except Exception as e:
    print("⚠️ Errore json.loads:", e)
    print("Messaggio ricevuto:", msg)
    continue


                    if data.get("topic") == "crypto_prices":
                        if callback:
                            callback(data)
                        else:
                            print(data)

        except Exception as e:
            print("❌ RTDS errore:", e)
            print("🔄 Riconnessione tra 3s...")
            await asyncio.sleep(3)

