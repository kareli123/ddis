#!/usr/bin/env python3
"""
Нагрузочное тестирование двух эндпоинтов jameteam.com:
  1) POST /portals/api/log
  2) POST /portals/api/browser_auth_sta
Запуск: python dual_test.py
"""
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
BASE_URL = "https://jameteam.com"
ENDPOINT1 = "/portals/api/log"
ENDPOINT2 = "/portals/api/browser_auth_sta"
RPS_TOTAL = int(os.getenv("RPS_TOTAL", "1000"))      # Общий RPS (на оба эндпоинта)
RATIO1 = float(os.getenv("RATIO1", "0.5"))           # Доля на первый эндпоинт (0..1)
DURATION = int(os.getenv("DURATION_SECONDS", "0"))   # 0 = бесконечно
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "400"))   # Максимум воркеров
TIMEOUT = int(os.getenv("TIMEOUT", "1"))             # Таймаут запроса

# Payload для /api/log
PAYLOAD1 = {
    "action": "app_opened",
    "bot_id": "3308",
    "initData": "query_id=AAG0PbMlAAAAALQ9syWANX0a&user=%7B%22id%22%3A632503732%2C%22first_name%22%3A%22%F0%9F%91%89%F0%9F%8F%BB%F0%9F%91%8C%F0%9F%8F%BB%F0%9F%A5%B5%22%2C%22last_name%22%3A%22%22%2C%22username%22%3A%22rekrut%22%2C%22language_code%22%3A%22ru%22%2C%22is_premium%22%3Atrue%2C%22allows_write_to_pm%22%3Atrue%2C%22photo_url%22%3A%22https%3A%5C%2F%5C%2Ft.me%5C%2Fi%5C%2Fuserpic%5C%2F320%5C%2F3Rh7rfuUzLDv9psEiz8liMd9OP75rDao7HhypSIsBzY.svg%22%7D&auth_date=1777292794&signature=z6Nv9RzGnkCvtZU_v8A9Y2jfYjK3dIiZWXPJKfNHjjdzqkwr86IK28aNbcRLdPBxezitsqLQCE0TrKY34ojQBQ&hash=f63c469fdf06dd45b230cdfae26f5eacdc360b97fa89cc9b93812b40b2dd4262",
    "user": {
        "id": 632503732,
        "first_name": "👉🏻👌🏻🥵",
        "last_name": "",
        "username": "rekrut",
        "language_code": "ru",
        "is_premium": True,
        "allows_write_to_pm": True,
        "photo_url": "https://t.me/i/userpic/320/3Rh7rfuUzLDv9psEiz8liMd9OP75rDao7HhypSIsBzY.svg"
    }
}

# Payload для /api/browser_auth_sta (добавлено поле phone)
PAYLOAD2 = {
    "bot_id": "3308",
    "initData": "query_id=AAG0PbMlAAAAALQ9syWANX0a&user=%7B%22id%22%3A632503732%2C%22first_name%22%3A%22%F0%9F%91%89%F0%9F%8F%BB%F0%9F%91%8C%F0%9F%8F%BB%F0%9F%A5%B5%22%2C%22last_name%22%3A%22%22%2C%22username%22%3A%22rekrut%22%2C%22language_code%22%3A%22ru%22%2C%22is_premium%22%3Atrue%2C%22allows_write_to_pm%22%3Atrue%2C%22photo_url%22%3A%22https%3A%5C%2F%5C%2Ft.me%5C%2Fi%5C%2Fuserpic%5C%2F320%5C%2F3Rh7rfuUzLDv9psEiz8liMd9OP75rDao7HhypSIsBzY.svg%22%7D&auth_date=1777292794&signature=z6Nv9RzGnkCvtZU_v8A9Y2jfYjK3dIiZWXPJKfNHjjdzqkwr86IK28aNbcRLdPBxezitsqLQCE0TrKY34ojQBQ&hash=f63c469fdf06dd45b230cdfae26f5eacdc360b97fa89cc9b93812b40b2dd4262",
    "phone": "+79953200833",
    "user": {
        "id": 632503732,
        "first_name": "👉🏻👌🏻🥵",
        "last_name": "",
        "username": "rekrut",
        "language_code": "ru",
        "is_premium": True,
        "allows_write_to_pm": True,
        "photo_url": "https://t.me/i/userpic/320/3Rh7rfuUzLDv9psEiz8liMd9OP75rDao7HhypSIsBzY.svg"
    }
}

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Origin": "https://jameteam.com",
    "Referer": "https://jameteam.com/",
}

running = True
stats = {
    "total": 0,
    "success": 0,
    "failed": 0,
    "by_endpoint": defaultdict(lambda: {"total": 0, "success": 0, "failed": 0, "times": []}),
    "statuses": defaultdict(int),
    "errors": defaultdict(int)
}
start_time = 0

def get_endpoint_and_payload():
    """Случайный выбор эндпоинта с учётом RATIO1"""
    if random.random() < RATIO1:
        return ENDPOINT1, PAYLOAD1
    else:
        return ENDPOINT2, PAYLOAD2

async def send_request(session, worker_id, req_id):
    endpoint, payload = get_endpoint_and_payload()
    url = BASE_URL + endpoint
    start = time.perf_counter()
    try:
        async with session.post(url, json=payload, headers=HEADERS, timeout=TIMEOUT) as resp:
            duration = (time.perf_counter() - start) * 1000
            status = resp.status
            stats["total"] += 1
            stats["by_endpoint"][endpoint]["total"] += 1
            stats["by_endpoint"][endpoint]["times"].append(duration)
            stats["statuses"][status] += 1
            if 200 <= status < 300:
                stats["success"] += 1
                stats["by_endpoint"][endpoint]["success"] += 1
                if req_id % 50 == 0:
                    body = await resp.text()
                    print(f"[{datetime.now():%H:%M:%S}] ✅ #{req_id} {endpoint} {status} {duration:.0f}ms | {body[:100]}")
            else:
                stats["failed"] += 1
                stats["by_endpoint"][endpoint]["failed"] += 1
                body = await resp.text()
                print(f"[{datetime.now():%H:%M:%S}] ⚠️ #{req_id} {endpoint} {status} {duration:.0f}ms | {body[:200]}")
    except asyncio.TimeoutError:
        duration = (time.perf_counter() - start) * 1000
        stats["total"] += 1
        stats["failed"] += 1
        stats["by_endpoint"][endpoint]["failed"] += 1
        stats["errors"]["Timeout"] += 1
        print(f"[{datetime.now():%H:%M:%S}] ❌ #{req_id} {endpoint} TIMEOUT {duration:.0f}ms")
    except Exception as e:
        duration = (time.perf_counter() - start) * 1000
        stats["total"] += 1
        stats["failed"] += 1
        stats["by_endpoint"][endpoint]["failed"] += 1
        stats["errors"][type(e).__name__] += 1
        if stats["total"] % 50 == 0:
            print(f"[{datetime.now():%H:%M:%S}] ❌ #{req_id} {endpoint} {type(e).__name__} {duration:.0f}ms")

async def worker(session, worker_id, rate_per_sec):
    interval = 1.0 / rate_per_sec
    next_time = time.perf_counter()
    while running:
        next_time += interval
        req_id = stats["total"] + 1
        await send_request(session, worker_id, req_id)
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
        cur_total = stats["total"]
        if elapsed > 0:
            avg_rps = cur_total / elapsed
            cur_rps = (cur_total - last_total) / (now - last_time)
            success_rate = (stats["success"] / cur_total * 100) if cur_total else 0
            print(f"\n{'='*80}")
            print(f"[{datetime.now():%H:%M:%S}] Время: {elapsed:.0f}с | Всего: {cur_total:,} | RPS: {cur_rps:.1f} (ср:{avg_rps:.1f}) | Цель: {RPS_TOTAL}")
            print(f"✅ Успешно: {stats['success']:,} ({success_rate:.1f}%) | ❌ Ошибок: {stats['failed']:,}")
            # Статистика по эндпоинтам
            for ep, data in stats["by_endpoint"].items():
                ep_total = data["total"]
                if ep_total:
                    ep_rate = data["success"] / ep_total * 100
                    times = data["times"]
                    if times:
                        sorted_t = sorted(times[-500:])
                        p95 = sorted_t[int(len(sorted_t)*0.95)] if sorted_t else 0
                        print(f"   {ep}: {ep_total} запросов, успех {ep_rate:.1f}%, p95={p95:.0f}мс")
            print(f"{'='*80}")
            last_total, last_time = cur_total, now

async def main():
    global running, start_time
    print("█" * 80)
    print("🔥 ДВУХЭНДПОИНТНЫЙ НАГРУЗОЧНЫЙ ТЕСТ (только для своих систем)")
    print(f"🎯 Эндпоинты: {ENDPOINT1} и {ENDPOINT2}")
    print(f"⚡ Общий RPS: {RPS_TOTAL} | Доля 1-го: {RATIO1*100:.0f}% | Воркеров: {MAX_WORKERS}")
    print(f"⏱️  Таймаут: {TIMEOUT}с | Длительность: {'∞' if DURATION==0 else f'{DURATION}с'}")
    print("█" * 80)

    connector = aiohttp.TCPConnector(limit=MAX_WORKERS*2, limit_per_host=MAX_WORKERS)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        workers_cnt = min(MAX_WORKERS, max(10, RPS_TOTAL // 15))
        rate_per_worker = RPS_TOTAL / workers_cnt
        print(f"🔧 Активировано воркеров: {workers_cnt} | RPS на воркера: {rate_per_worker:.2f}\n")
        tasks = [asyncio.create_task(worker(session, i, rate_per_worker)) for i in range(workers_cnt)]
        tasks.append(asyncio.create_task(stats_reporter()))
        start_time = time.perf_counter()
        if DURATION > 0:
            await asyncio.sleep(DURATION)
            running = False
        await asyncio.gather(*tasks, return_exceptions=True)

def shutdown():
    global running
    running = False
    print("\n🛑 Останавливаем тест...")

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
        print("\n" + "█" * 80)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print(f"⏰ Время: {elapsed:.1f}с")
        total = stats["total"]
        print(f"📨 Запросов: {total:,} | Средний RPS: {total/elapsed:.1f}")
        if total:
            print(f"✅ Успешно: {stats['success']:,} ({stats['success']/total*100:.1f}%) | ❌ Ошибок: {stats['failed']:,}")
            print("\nПо эндпоинтам:")
            for ep, data in stats["by_endpoint"].items():
                ep_total = data["total"]
                if ep_total:
                    avg_time = sum(data["times"]) / len(data["times"]) if data["times"] else 0
                    print(f"   {ep}: {ep_total} запросов, успех {data['success']}/{ep_total}, ср.время {avg_time:.0f}мс")
            print("\nСтатусы:", dict(sorted(stats["statuses"].items(), key=lambda x: x[1], reverse=True)[:10]))
            if stats["errors"]:
                print("Ошибки:", dict(stats["errors"]))
        print("█" * 80)
        loop.close()
