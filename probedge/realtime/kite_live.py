import asyncio, time, random
async def live_tick_stream(symbols: list[str]):
    while True:
        ts = time.time()
        batch = [(s, ts, 100.0 + random.random()) for s in symbols]
        yield batch
        await asyncio.sleep(1.0)
