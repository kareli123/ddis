#!/usr/bin/env python3
"""
Load testing for https://jameteam.com/portals/api/log
Usage: python jametest.py
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
TARGET_URL = os.getenv("TARGET_URL", "https://jameteam.com/portals/api/log")
RPS_TARGET = int(os.getenv("RPS", "1000"))          # Целевой RPS
DURATION = int(os.getenv("DURATION_SECONDS", "0"))  # 0 = бесконечно
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "300"))  # Параллельных воркеров
TIMEOUT = int(os.getenv("TIMEOUT", "1"))            # Таймаут запроса (сек)
SHOW_RESPONSES = os.getenv("SHOW_RESPONSES", "0") == "1"  # Показывать тело ответа (замедляет)

# Шаблон payload (можно менять под свои нужды)
PAYLOAD_TEMPLATE = {
    "action": "app_opened",
    "bot_id": "3308",
    "initData": "query_id=AAG0PbMlAAAAALQ9syVavF9o&user=%7B%22id%22%3A632503732%2C%22first_name%22%3A%22%F0%9F%91%89%F0%9F%8F%BB%F0%9F%91%8C%F0%9F%8F%BB%F0%9F%A5%B5%22%2C%22last_name%22%3A%22%22%2C%22username%22%3A%22rekrut%22%2C%22language_code%22%3A%22ru%22%2C%22is_premium%22%3Atrue%2C%22allows_write_to_pm%22%3Atrue%2C%22photo_url%22%3A%22https%3A%5C%2F%5C%2Ft.me%5C%2Fi%5C%2Fuserpic%5C%2F320%5C%2F3Rh7rfuUzLDv9psEiz8liMd9OP75rDao7HhypSIsBzY.svg%22%7D&auth_date=1777292324&signature=7KcOWCMcyOtcpC-hZXiEDTFMbuyQbiV_Jjn_V_189w5t7vF7vMDGYZhHo25d56V-sfuOTmro3z6cHbfO09IhCQ&hash=1e9b72af90179540e0611d334428a7b66a855af11d543b27d3136fd1afe76081",
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

# Заголовки (можно подобрать реалистичные)
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Origin": "https://jameteam.com",
    "Referer": "https://jameteam.com/",
}

# Глобальные счётчики
running = True
total_requests = 0
successful = 0
failed = 0
response_times = []
status_counts = defaultdict(int)
error_counts = defaultdict(int)
start_time = 0

def get_payload():
    """Вернуть копию payload (можно делать небольшие вариации)"""
    # Для максимальной нагрузки используем один и тот же payload
    # Если нужно разнообразие, раскомментируйте код ниже
    return PAYLOAD_TEMPLATE.copy()

async def send_request(session, worker_id, req_id):
    global total_requests, successful, failed, response_times, status_counts, error_counts
    start = time.perf_counter()
    try:
        async with session.post(TARGET_URL, json=get_payload(), headers=HEADERS, timeout=TIMEOUT) as resp:
            duration_ms = (time.perf_counter() - start) * 1000
            status = resp.status
            total_requests += 1
            response_times.append(duration_ms)
            status_counts[status] += 1
            
            if 200 <= status < 300:
                successful += 1
                if SHOW_RESPONSES and req_id % 50 == 0:
                    body = await resp.text()
                    print(f"[{datetime.now():%H:%M:%S}] ✅ #{req_id} {status} {duration_ms:.0f}ms | {body[:200]}")
            else:
                failed += 1
                body = await resp.text()
                print(f"[{datetime.now():%H:%M:%S}] ⚠️ #{req_id} {status} {duration_ms:.0f}ms | {body[:200]}")
    except asyncio.TimeoutError:
        duration_ms = (time.perf_counter() - start) * 1000
        total_requests += 1
        failed += 1
        error_counts["Timeout"] += 1
        print(f"[{datetime.now():%H:%M:%S}] ❌ #{req_id} TIMEOUT {duration_ms:.0f}ms")
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000
        total_requests += 1
        failed += 1
        error_counts[type(e).__name__] += 1
        if total_requests % 50 == 0:
            print(f"[{datetime.now():%H:%M:%S}] ❌ #{req_id} {type(e).__name__} {duration_ms:.0f}ms")

async def worker(session, worker_id, rate_per_sec):
    interval = 1.0 / rate_per_sec
    next_time = time.perf_counter()
    while running:
        next_time += interval
        global total_requests
        req_id = total_requests + 1
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
        cur_total = total_requests
        if elapsed > 0 and cur_total > last_total:
            avg_rps = cur_total / elapsed
            cur_rps = (cur_total - last_total) / (now - last_time)
            success_rate = (successful / cur_total * 100) if cur_total else 0
            
            # Перцентили
            if response_times:
                sorted_times = sorted(response_times[-1000:])
                p50 = sorted_times[len(sorted_times)//2] if sorted_times else 0
                p95 = sorted_times[int(len(sorted_times)*0.95)] if sorted_times else 0
            else:
                p50 = p95 = 0
            
            print(f"\n{'='*70}")
            print(f"[{datetime.now():%H:%M:%S}] Время: {elapsed:.0f}с | Запросов: {cur_total:,} | RPS: {cur_rps:.1f} (ср:{avg_rps:.1f}) | Цель: {RPS_TARGET}")
            print(f"✅ Успешно: {successful:,} ({success_rate:.1f}%) | ❌ Ошибок: {failed:,}")
            print(f"⏱️  Время отклика: p50={p50:.0f}мс, p95={p95:.0f}мс")
            if status_counts:
                top_status = sorted(status_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                print("Статусы: " + "  ".join([f"{k}:{v}" for k,v in top_status]))
            if error_counts:
                top_err = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:3]
                print("Ошибки: " + "  ".join([f"{k}:{v}" for k,v in top_err]))
            if cur_rps < RPS_TARGET * 0.7 and elapsed > 10:
                print("⚡ Предупреждение: RPS ниже целевого! Увеличьте MAX_WORKERS или используйте более мощный сервер.")
            print(f"{'='*70}")
            last_total, last_time = cur_total, now

async def main():
    global running, start_time
    print("█" * 70)
    print("🔥 НАГРУЗОЧНОЕ ТЕСТИРОВАНИЕ (только для своих систем)")
    print(f"🎯 Цель: {TARGET_URL}")
    print(f"⚡ Целевой RPS: {RPS_TARGET} | Воркеров: {MAX_WORKERS}")
    print(f"⏱️  Таймаут: {TIMEOUT}с | Длительность: {'∞' if DURATION==0 else f'{DURATION}с'}")
    print("█" * 70)

    connector = aiohttp.TCPConnector(limit=MAX_WORKERS*2, limit_per_host=MAX_WORKERS)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        workers_cnt = min(MAX_WORKERS, max(10, RPS_TARGET // 15))
        rate_per_worker = RPS_TARGET / workers_cnt
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
        print("\n" + "█" * 70)
        print("📊 ИТОГОВАЯ СТАТИСТИКА")
        print(f"⏰ Время: {elapsed:.1f}с")
        print(f"📨 Запросов: {total_requests:,} | Средний RPS: {total_requests/elapsed:.1f}")
        if total_requests:
            print(f"✅ Успешно: {successful:,} ({successful/total_requests*100:.1f}%) | ❌ Ошибок: {failed:,}")
            if response_times:
                avg = sum(response_times) / len(response_times)
                print(f"⏱️  Среднее время: {avg:.0f}мс")
                sorted_times = sorted(response_times)
                print(f"   p50: {sorted_times[len(sorted_times)//2]:.0f}мс | p95: {sorted_times[int(len(sorted_times)*0.95)]:.0f}мс | p99: {sorted_times[int(len(sorted_times)*0.99)]:.0f}мс")
            print("Статусы:", dict(sorted(status_counts.items(), key=lambda x: x[1], reverse=True)))
            if error_counts:
                print("Ошибки:", dict(error_counts))
        print("█" * 70)
        loop.close()
