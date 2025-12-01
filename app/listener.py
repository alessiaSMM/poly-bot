import asyncio
import json
import websockets

POLY_WS_URL = "wss://prod-events.polymarket.com/events"

async def polymarket_listener(callback):
    try:
        async with websockets.connect(POLY_WS_URL, ping_interval=20) as ws:
            print("Connesso al feed Polymarket")
            subscribe = {
                "type": "subscribe",
                "channels": ["markets"]
            }
            await ws.send(json.dumps(subscribe))

            while True:
                msg = await ws.recv()
                data = json.loads(msg)
                await callback(data)

    except Exception as e:
        print("Errore WebSocket:", e)
        await asyncio.sleep(5)
        return await polymarket_listener(callback)
