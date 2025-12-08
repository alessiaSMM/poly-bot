import asyncio
from app.listener import start_rtds_listener
from app.copy_trader import process_leader_trades

async def bot_loop():
    print("🔥 Polybot avviato (modalità PAPER - nessun trade reale)")
    print("📊 RTDS + Copy-trader attivi")

    # RTDS (puoi anche toglierlo se ti dà fastidio)
   # rtds_task = asyncio.create_task(start_rtds_listener())

    # Copy-trader loop
    while True:
        process_leader_trades()
        await asyncio.sleep(5)

def main():
    asyncio.run(bot_loop())

if __name__ == "__main__":
    main()
