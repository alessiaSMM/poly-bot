import asyncio
import json
import websockets

POLY_WS_URL = "wss://api.polymarket.com/markets/ws"

async def polymarket_listener(callback):
    """
    Listener A.1 - ascolta il feed WebSocket dei mercati Polymarket.
    """
    try:
        async with websockets.connect(POLY_WS_URL, ping_interval=20) as ws:
            print("Connesso al feed ufficiale Polymarket WebSocket")

            # Sottoscrizione base (Polymarket invia tutti i market updates)
            subscribe_msg = {
                "type": "subscribe",
                "channels": ["markets"]
            }
            await ws.send(json.dumps(subscribe_msg))

            while True:
                msg = await ws.recv()
                data = json.loads(msg)

                if "type" in data:
                    await callback(data)

    except Exception as e:
        print("Errore WebSocket:", e)
        await asyncio.sleep(5)
        return await polymarket_listener(callback)


async def test():
    async def on_update(event):
        print("Evento:", str(event)[:200])
    await polymarket_listener(on_update)


if __name__ == "__main__":
    asyncio.run(test())
