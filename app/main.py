import asyncio
from .listener import polymarket_listener
from .kms_signer import (
    KMSSigner, build_kms_client_from_json, kms_key_resource
)
from .config import GCP_SA_JSON
import os

async def handle_market_update(event):
    """
    Qui entra il punto A.1 (Analisi & Filtri).
    Questo è il punto dove TU decidi cosa è utile.
    """
    market_id = event.get("market_id")
    prob = event.get("prob")

    print(f"💡 Update mercato {market_id}: prob={prob}")


async def main_async():
    sa_json = os.getenv("GCP_SA_JSON")
    kms_client = build_kms_client_from_json(sa_json)
    signer = KMSSigner(kms_client, kms_key_resource())

    print("Indirizzo KMS:", signer.get_eth_address())

    await polymarket_listener(handle_market_update)


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
