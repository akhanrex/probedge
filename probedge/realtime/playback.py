import asyncio, time, random
async def playback_stream(symbols: list[str]):
    while True:
        ts = time.time()
        batch = [(s, ts, 100.0 + random.random()) for s in symbols]
        yield batch
        await asyncio.sleep(0.5)
