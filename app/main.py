import asyncio
import threading
from .listener import polymarket_listener
from .config import *
from .kms_signer import build_kms_client_from_json, kms_key_resource, KMSSigner
from .server import app

async def on_market_event(event):
    print("EVENTO:", str(event)[:200])

async def bot_loop():
    sa_json = GCP_SA_JSON
    kms_client = build_kms_client_from_json(sa_json)
    signer = KMSSigner(kms_client, kms_key_resource())
    print("Indirizzo KMS:", signer.get_eth_address())
    await polymarket_listener(on_market_event)

def start_flask():
    app.run(host="0.0.0.0", port=10000)

def main():
    t = threading.Thread(target=start_flask, daemon=True)
    t.start()
    asyncio.run(bot_loop())

if __name__ == "__main__":
    main()
