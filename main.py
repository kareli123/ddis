#!/usr/bin/env python3
"""
MEGA LOAD TESTER — для экстремальной нагрузки на свой сервер
Запуск: python mega_load.py
Остановка: Ctrl+C
"""

import asyncio
import aiohttp
import json
import os
import time
import random
import signal
import sys
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List

# ========== КОНФИГУРАЦИЯ (меняйте под свои нужды) ==========
TARGET_URL = os.getenv("TARGET_URL", "https://mrkt-verification.xyz/api/auth/telegram")
RPS_TARGET = int(os.getenv("RPS", "2000"))          # Целевой RPS (стартуйте с 500)
DURATION = int(os.getenv("DURATION_SECONDS", "0"))  # 0 = бесконечно, или число секунд
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "200"))  # Максимум параллельных воркеров
REQUEST_TIMEOUT = int(os.getenv("TIMEOUT", "2"))    # Таймаут запроса (сек)
MAX_ERRORS = int(os.getenv("MAX_ERRORS", "1000"))   # Остановка при стольких ошибках подряд

# Заголовки с ротацией User-Agent для реалистичности
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 Chrome/119.0.0.0 Mobile Safari/537.36",
]

BASE_HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Origin": "https://mrkt-verification.xyz",
    "Referer": "https://mrkt-verification.xyz/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

PAYLOAD = {"initData": ""}

# Глобальные счётчики
running = True
total_requests = 0
successful_requests = 0
failed_requests = 0
consecutive_errors = 0
response_times: List[float] = []
status_counts = defaultdict(int)
error_counts = defaultdict(int)
start_time = 0

@dataclass
class Stats:
    rps: float = 0
    avg_response_ms: float = 0
    p95_ms: float = 0
    p99_ms: float = 0
    success_rate: float = 0

def get_headers():
    headers = BASE_HEADERS.copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)
    return headers

async def send_request(session, worker_id, req_id):
    global total_requests, successful_requests, failed_requests, consecutive_errors
    global response_times, status_counts, error_counts
    
    start = time.perf_counter()
    headers = get_headers()
    
    try:
        async with session.post(TARGET_URL, json=PAYLOAD, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            duration_ms = (time.perf_counter() - start) * 1000
            status = resp.status
            
            total_requests += 1
            response_times.append(duration_ms)
            status_counts[status] += 1
            
            if 200 <= status < 300:
                successful_requests += 1
                consecutive_errors = 0
                # Логируем каждый 50-й успешный запрос (для снижения вывода)
                if req_id % 50 == 0:
                    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] ✅ #{req_id} | {status} | {duration_ms:.0f}ms | W{worker_id}")
            else:
                failed_requests += 1
                consecutive_errors += 1
                # Логируем каждую ошибку
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] ⚠️ #{req_id} | {status} | {duration_ms:.0f}ms | W{worker_id}")
                # Показываем тело ответа при ошибке (первые 200 символов)
                try:
                    body = await resp.text()
                    print(f"    📦 Ответ: {body[:200].replace(chr(10), ' ')}")
                except:
                    pass
                
    except asyncio.TimeoutError:
        duration_ms = (time.perf_counter() - start) * 1000
        total_requests += 1
        failed_requests += 1
        consecutive_errors += 1
        error_counts["Timeout"] += 1
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] ❌ #{req_id} | TIMEOUT | {duration_ms:.0f}ms")
        
    except aiohttp.ClientError as e:
        duration_ms = (time.perf_counter() - start) * 1000
        total_requests += 1
        failed_requests += 1
        consecutive_errors += 1
        error_name = type(e).__name__
        error_counts[error_name] += 1
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] ❌ #{req_id} | {error_name} | {duration_ms:.0f}ms")
        
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000
        total_requests += 1
        failed_requests += 1
        consecutive_errors += 1
        error_counts["Other"] += 1
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] ❌ #{req_id} | {type(e).__name__}: {str(e)[:50]}")

async def worker(session, worker_id, rate_per_sec):
    """Каждый воркер отправляет запросы с заданной частотой"""
    interval = 1.0 / rate_per_sec
    next_time = time.perf_counter()
    req_id = worker_id  # начальный id, потом будем использовать глобальный счётчик
    
    while running:
        next_time += interval
        # Глобальный счётчик запросов (для уникальных ID)
        global total_requests
        req_id = total_requests + 1
        
        await send_request(session, worker_id, req_id)
        
        # Проверка на слишком много последовательных ошибок
        if consecutive_errors > MAX_ERRORS:
            print(f"\n⚠️  СЛИШКОМ МНОГО ОШИБОК ПОДРЯД ({consecutive_errors}). ОСТАНОВКА.")
            global running
            running = False
            break
        
        sleep_time = next_time - time.perf_counter()
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)
        else:
            # Если отстаём, сбрасываем расписание
            next_time = time.perf_counter()

async def stats_reporter():
    """Периодический вывод статистики"""
    last_total = 0
    last_time = time.perf_counter()
    
    while running:
        await asyncio.sleep(5)
        now = time.perf_counter()
        elapsed = now - start_time
        current_total = total_requests
        
        if elapsed > 0:
            avg_rps = current_total / elapsed
            current_rps = (current_total - last_total) / 5 if (now - last_time) > 0 else 0
            success_rate = (successful_requests / current_total * 100) if current_total > 0 else 0
            
            # Вычисляем перцентили
            if response_times:
                sorted_times = sorted(response_times[-1000:])  # последние 1000 замеров
                p50 = sorted_times[len(sorted_times)//2] if sorted_times else 0
                p95 = sorted_times[int(len(sorted_times)*0.95)] if sorted_times else 0
                p99 = sorted_times[int(len(sorted_times)*0.99)] if sorted_times else 0
            else:
                p50 = p95 = p99 = 0
            
            print(f"\n{'='*80}")
            print(f"🔥 СТАТИСТИКА [{datetime.now().strftime('%H:%M:%S')}] | В работе: {elapsed:.0f}с")
            print(f"📊 Запросов: {current_total:,} | RPS тек: {current_rps:.1f} | RPS ср: {avg_rps:.1f} | Цель: {RPS_TARGET}")
            print(f"✅ Успех: {successful_requests:,} ({success_rate:.1f}%) | ❌ Ошибок: {failed_requests:,}")
            print(f"⏱️  Время ответа (мс) — p50: {p50:.0f} | p95: {p95:.0f} | p99: {p99:.0f}")
            
            if status_counts:
                top_status = sorted(status_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                print(f"📈 Статусы: " + " | ".join([f"{k}: {v}" for k, v in top_status]))
            
            if error_counts:
                top_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:3]
                print(f"⚠️  Ошибки: " + " | ".join([f"{k}: {v}" for k, v in top_errors]))
            
            if current_rps < RPS_TARGET * 0.7 and elapsed > 10:
                print(f"\n⚡ НЕДОСТАТОЧНАЯ МОЩНОСТЬ! Текущий RPS {current_rps:.0f} < {RPS_TARGET}")
                print(f"   ➜ Увеличьте MAX_WORKERS или используйте более мощный сервер")
            
            print(f"{'='*80}\n")
            
            last_total = current_total
            last_time = now

async def main():
    global running, start_time
    
    print(f"\n{'█'*80}")
    print(f"🔥 MEGA LOAD TESTER — ЭКСТРЕМАЛЬНАЯ НАГРУЗКА")
    print(f"{'█'*80}")
    print(f"🎯 Цель: {TARGET_URL}")
    print(f"⚡ Целевой RPS: {RPS_TARGET}")
    print(f"🔧 Воркеров: {MAX_WORKERS}")
    print(f"⏱️  Таймаут: {REQUEST_TIMEOUT}с")
    print(f"🛑 Лимит ошибок: {MAX_ERRORS}")
    print(f"📦 Payload: {PAYLOAD}")
    print(f"{'█'*80}\n")
    
    # Настройка соединений для максимальной производительности
    connector = aiohttp.TCPConnector(
        limit=MAX_WORKERS * 2,           # Общий лимит соединений
        limit_per_host=MAX_WORKERS,      # Лимит на хост
        ttl_dns_cache=30,                # Кеш DNS
        use_dns_cache=True,
        force_close=False,
        enable_cleanup_closed=True
    )
    
    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT,
        connect=REQUEST_TIMEOUT,
        sock_read=REQUEST_TIMEOUT
    )
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Рассчитываем оптимальное количество воркеров
        # Чем больше воркеров, тем точнее RPS, но выше накладные расходы
        workers_count = min(MAX_WORKERS, max(10, RPS_TARGET // 15))
        rate_per_worker = RPS_TARGET / workers_count
        
        print(f"🔧 Активировано воркеров: {workers_count}")
        print(f"🔧 RPS на воркера: {rate_per_worker:.2f}\n")
        print("🚀 ЗАПУЩЕНА МАКСИМАЛЬНАЯ НАГРУЗКА! Для остановки нажмите Ctrl+C\n")
        
        tasks = []
        for i in range(workers_count):
            tasks.append(asyncio.create_task(worker(session, i, rate_per_worker)))
        
        tasks.append(asyncio.create_task(stats_reporter()))
        
        start_time = time.perf_counter()
        
        if DURATION > 0:
            await asyncio.sleep(DURATION)
            running = False
        
        await asyncio.gather(*tasks, return_exceptions=True)

def signal_handler():
    global running
    if running:
        print("\n\n🛑 Получен сигнал остановки. Завершаем работу...")
        running = False

if __name__ == "__main__":
    # Настройка сигналов для graceful shutdown
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        # Финальная статистика
        elapsed = time.perf_counter() - start_time if start_time else 0
        print(f"\n{'█'*80}")
        print(f"📈 ИТОГОВАЯ СТАТИСТИКА")
        print(f"{'█'*80}")
        print(f"⏰ Время работы: {elapsed:.1f} сек")
        print(f"📨 Всего запросов: {total_requests:,}")
        if total_requests > 0:
            print(f"✅ Успешных: {successful_requests:,} ({successful_requests/total_requests*100:.1f}%)")
            print(f"❌ Ошибок: {failed_requests:,} ({failed_requests/total_requests*100:.1f}%)")
            print(f"⚡ Средний RPS: {total_requests/elapsed:.1f}")
            print(f"🎯 Целевой RPS: {RPS_TARGET}")
            print(f"📈 Эффективность: {total_requests/elapsed/RPS_TARGET*100:.1f}%")
            
            if response_times:
                sorted_times = sorted(response_times)
                print(f"\n⏱️  Время ответа (мс):")
                print(f"   Среднее: {sum(response_times)/len(response_times):.0f}")
                print(f"   Медиана (p50): {sorted_times[len(sorted_times)//2]:.0f}")
                print(f"   p95: {sorted_times[int(len(sorted_times)*0.95)]:.0f}")
                print(f"   p99: {sorted_times[int(len(sorted_times)*0.99)]:.0f}")
                print(f"   Максимум: {max(response_times):.0f}")
            
            print(f"\n📊 Распределение статусов:")
            for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"   {status}: {count} ({count/total_requests*100:.1f}%)")
            
            if error_counts:
                print(f"\n⚠️  Типы ошибок:")
                for err, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True):
                    print(f"   {err}: {count}")
        print(f"{'█*'80}")
        loop.close()
