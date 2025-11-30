import asyncio
import json
import websockets

POLY_WS_URL = "wss://strapi-mtcg.polymarket.com/updates"

async def polymarket_listener(callback):
    """
    Listener A.1 — ascolta il feed WebSocket di Polymarket.
    Chiama 'callback' a ogni update del mercato.
    """
    try:
        async with websockets.connect(POLY_WS_URL, ping_interval=20) as ws:
            print("Connesso al feed Polymarket WebSocket")

            # Sottoscrizione agli aggiornamenti dei mercati
            subscribe_msg = {
                "type": "subscribe",
                "channels": ["markets"]
            }
            await ws.send(json.dumps(subscribe_msg))

            while True:
                msg = await ws.recv()
                data = json.loads(msg)

                # Filtra solo gli eventi utili
                if "type" in data and data["type"] == "market_update":
                    await callback(data)

    except Exception as e:
        print("Errore WebSocket:", e)
        await asyncio.sleep(2)
        return await polymarket_listener(callback)


# Test locale (non usato in produzione)
async def test():
    async def on_update(event):
        print("Update mercato:", str(event)[:200])
    await polymarket_listener(on_update)


if __name__ == "__main__":
    asyncio.run(test())
