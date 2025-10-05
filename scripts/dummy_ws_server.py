# scripts/dummy_ws_server.py
import asyncio, json, time
import websockets

async def handler(ws, path):
    print("Client connected!")  # 👈 لاگ اضافه
    while True:
        msg = {
            "type": "pair_created",
            "liquidity_usd": 200000,
            "token_address": "0xDEADBEEF",
            "timestamp": int(time.time())
        }
        await ws.send(json.dumps(msg))
        await asyncio.sleep(2)

async def main():
    print("Dummy WS server listening on ws://0.0.0.0:8765")  # 👈 لاگ اضافه
    server = await websockets.serve(handler, "0.0.0.0", 8765)
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
