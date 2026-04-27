#!/usr/bin/env python3
import asyncio
import aiohttp
import json
import os
import time
import signal
from datetime import datetime
from collections import defaultdict

URL = "https://jameteam.com/templates/api/inline-templates/create"
RPS_TARGET = int(os.getenv("RPS_TARGET", "500"))
DURATION = int(os.getenv("DURATION_SECONDS", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "100"))
TIMEOUT = int(os.getenv("TIMEOUT", "5"))

INIT_DATA = "query_id=AAG0PbMlAAAAALQ9syVoq6PZ&user=%7B%22id%22%3A632503732%2C%22first_name%22%3A%22%F0%9F%91%89%F0%9F%8F%BB%F0%9F%91%8C%F0%9F%8F%BB%F0%9F%A5%B5%22%2C%22last_name%22%3A%22%22%2C%22username%22%3A%22rekrut%22%2C%22language_code%22%3A%22ru%22%2C%22is_premium%22%3Atrue%2C%22allows_write_to_pm%22%3Atrue%2C%22photo_url%22%3A%22https%3A%5C%2F%5C%2Ft.me%5C%2Fi%5C%2Fuserpic%5C%2F320%5C%2F3Rh7rfuUzLDv9psEiz8liMd9OP75rDao7HhypSIsBzY.svg%22%7D&auth_date=1777306880&signature=aXEJ_cdQeD1VzeHW7uCtwCajfLe0t-7UBBSwtSny8UtYkL9pkJxRm4SkapgfPG6mWCe3SgzAzowPk4kAJPJADg&hash=d43ecfd675d80451bd926be34d19eb56c6ca8e5508ecee1dde918287a349d50a"

DATA_OBJ = {
    "name": "FASFASFASFS",
    "text": "FASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFS",
    "button_text": "FASFASFASFS",
    "referral_text": "FASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFS",
    "webapp_button_text": "FASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFS",
    "nft_urls": "https://google.com\nhttps://google.com\nhttps://google.com",
    "inline_button_text": "FASFASFASFSFASFASFASFSFASFASFASFSFASFASFASFS",
    "inline_button_url": "https://google.com",
    "one_time": True,
    "remove_photo": False,
    "current_photo": ""
}
DATA_JSON = json.dumps(DATA_OBJ, ensure_ascii=False)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36 Edg/147.0.0.0",
    "Origin": "https://jameteam.com",
    "Referer": "https://jameteam.com/templates/",
    "Accept": "*/*",
    "Accept-Language": "ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

running = True
total = 0
success = 0
failed = 0
times = []
statuses = defaultdict(int)
errors = defaultdict(int)
start_time = 0

def make_form():
    form = aiohttp.FormData()
    form.add_field('initData', INIT_DATA)
    form.add_field('data', DATA_JSON)
    return form

async def send_request(session, req_id):
    global total, success, failed, times, statuses, errors
    start = time.perf_counter()
    form = make_form()
    try:
        async with session.post(URL, data=form, headers=HEADERS, timeout=TIMEOUT) as resp:
            dur = (time.perf_counter() - start) * 1000
            body = await resp.text()
            total += 1
            times.append(dur)
            statuses[resp.status] += 1
            if 200 <= resp.status < 300:
                success += 1
                if req_id % 50 == 0:
                    print(f"[{datetime.now():%H:%M:%S}] ✅ #{req_id} {resp.status} {dur:.0f}ms | {body[:100]}")
            else:
                failed += 1
                print(f"[{datetime.now():%H:%M:%S}] ⚠️ #{req_id} {resp.status} {dur:.0f}ms | {body[:200]}")
    except asyncio.TimeoutError:
        total += 1
        failed += 1
        errors["Timeout"] += 1
        print(f"[{datetime.now():%H:%M:%S}] ❌ #{req_id} TIMEOUT")
    except Exception as e:
        total += 1
        failed += 1
        errors[type(e).__name__] += 1
        if total % 20 == 0:
            print(f"[{datetime.now():%H:%M:%S}] ❌ #{req_id} {type(e).__name__}: {str(e)[:100]}")

async def worker(worker_id, rate_per_sec):
    interval = 1.0 / rate_per_sec
    next_time = time.perf_counter()
    async with aiohttp.ClientSession() as session:
        while running:
            next_time += interval
            req_id = total + 1
            await send_request(session, req_id)
            sleep = next_time - time.perf_counter()
            if sleep > 0:
                await asyncio.sleep(sleep)
            else:
                next_time = time.perf_counter()

async def stats_reporter():
    last_total = 0
    last_time = time.perf_counter()
    while running:
        await asyncio.sleep(5)
        now = time.perf_counter()
        elapsed = now - start_time
        cur_total = total
        if elapsed > 0 and cur_total > last_total:
            avg_rps = cur_total / elapsed
            cur_rps = (cur_total - last_total) / (now - last_time) if now - last_time else 0
            success_rate = success / cur_total * 100 if cur_total else 0
            print(f"\n{'='*70}")
            print(f"[{datetime.now():%H:%M:%S}] ⏱️ {elapsed:.0f}с | Всего: {cur_total:,} | RPS: {cur_rps:.0f} (ср:{avg_rps:.0f}) | Цель: {RPS_TARGET}")
            print(f"✅ Успех: {success:,} ({success_rate:.1f}%) | ❌ Ошибок: {failed:,}")
            if times:
                sorted_t = sorted(times[-500:])
                p95 = sorted_t[int(len(sorted_t)*0.95)] if sorted_t else 0
                print(f"⏱️  p95: {p95:.0f}ms")
            if errors:
                print("❌ Ошибки:", dict(list(errors.items())[:3]))
            print(f"{'='*70}")
            last_total, last_time = cur_total, now

async def main():
    global running, start_time
    print("█" * 70)
    print("🔥 НАГРУЗОЧНЫЙ ТЕСТ (inline-templates/create) multipart/form-data")
    print(f"🎯 URL: {URL}")
    print(f"⚡ RPS={RPS_TARGET} | Воркеров={MAX_WORKERS} | Таймаут={TIMEOUT}с")
    if DURATION:
        print(f"⏱️ Длительность: {DURATION}с")
    else:
        print("⏱️ Длительность: бесконечно (Ctrl+C)")
    print("█" * 70)

    workers_cnt = min(MAX_WORKERS, max(5, RPS_TARGET // 10))
    rate_per_worker = RPS_TARGET / workers_cnt
    print(f"🔧 Активировано воркеров: {workers_cnt} | RPS на воркера: {rate_per_worker:.2f}\n")
    tasks = [asyncio.create_task(worker(i, rate_per_worker)) for i in range(workers_cnt)]
    tasks.append(asyncio.create_task(stats_reporter()))
    start_time = time.perf_counter()
    if DURATION > 0:
        await asyncio.sleep(DURATION)
        running = False
    await asyncio.gather(*tasks, return_exceptions=True)

def shutdown():
    global running
    running = False
    print("\n🛑 Остановка...")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        elapsed = time.perf_counter() - start_time if start_time else 0
        print("\n" + "█" * 70)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        if elapsed:
            print(f"⏰ Время: {elapsed:.1f}с | Запросов: {total:,} | RPS: {total/elapsed:.1f}")
        if total:
            print(f"✅ Успех: {success:,} ({success/total*100:.1f}%) | ❌ Ошибок: {failed:,}")
            print("Статусы:", dict(sorted(statuses.items(), key=lambda x: x[1], reverse=True)))
            if errors:
                print("Ошибки:", dict(errors))
        print("█" * 70)
        loop.close()
