#!/usr/bin/env python3
import asyncio
import aiohttp
import os
import time
import signal
import json
import random
from datetime import datetime
from collections import defaultdict

URL = "https://betalab.forum/api/qr-start"

# Настройки через переменные окружения
RPS_TARGET = int(os.getenv("RPS_TARGET", "1200"))        # запросов в секунду
DURATION = int(os.getenv("DURATION_SECONDS", "0"))      # 0 = бесконечно
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "100"))      # параллельных задач
TIMEOUT = int(os.getenv("TIMEOUT", "5"))                # таймаут запроса (сек)

# Префиксы для генерации имён
PREFIXES = ["bloodparty", "rekrut", "shadow", "darkwolf", "phantom", "night", "storm", "thunder", "rage", "chaos"]
SUFFIXES = ["", "_god", "_king", "_warrior", "_hunter", "_legend", "_beast", "_ghost"]

def generate_random_user():
    """Генерирует случайного пользователя"""
    user_id = random.randint(10000, 99999999)
    
    prefix = random.choice(PREFIXES)
    number = random.randint(1, 9999)
    suffix = random.choice(SUFFIXES)
    
    username = f"{prefix}_{number}{suffix}"
    
    # Иногда делаем простые имена
    if random.random() < 0.3:
        username = f"user_{random.randint(1, 99999)}"
    
    return {
        "user_id": user_id,
        "username": username
    }

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
}

running = True
total = 0
success = 0
failed = 0
times = []
statuses = defaultdict(int)
errors = defaultdict(int)
responses_log = []  # храним последние 30 ответов для вывода
start_time = 0

def log_response(req_id, status, duration_ms, body, payload, error=None):
    """Форматированный вывод лога каждого запроса"""
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    
    if error:
        print(f"[{timestamp}] ❌ #{req_id} | ОШИБКА: {error} | {duration_ms}ms | user_id={payload.get('user_id', '?')}, username={payload.get('username', '?')}")
    elif 200 <= status < 300:
        preview = body[:200].replace('\n', ' ') if body else "empty"
        print(f"[{timestamp}] ✅ #{req_id} | {status} | {duration_ms}ms | [{payload.get('user_id')}] @{payload.get('username')} | Ответ: {preview}")
        if len(body) > 200:
            print(f"[{timestamp}]    ... (ещё {len(body)-200} символов)")
    else:
        preview = body[:300].replace('\n', ' ') if body else "empty"
        print(f"[{timestamp}] ⚠️ #{req_id} | {status} | {duration_ms}ms | [{payload.get('user_id')}] @{payload.get('username')} | Ответ: {preview}")

async def send_request(session, req_id):
    global total, success, failed, times, statuses, errors, responses_log
    
    payload = generate_random_user()
    start_t = time.perf_counter()
    
    try:
        async with session.post(URL, json=payload, headers=HEADERS, timeout=TIMEOUT) as resp:
            dur = (time.perf_counter() - start_t) * 1000
            body = await resp.text()
            total += 1
            times.append(dur)
            statuses[resp.status] += 1
            
            # Сохраняем последние ответы
            responses_log.append({
                "id": req_id,
                "status": resp.status,
                "body": body[:500],
                "time": dur,
                "user_id": payload["user_id"],
                "username": payload["username"]
            })
            if len(responses_log) > 30:
                responses_log.pop(0)
            
            if 200 <= resp.status < 300:
                success += 1
                # Логируем каждый запрос (можно изменить на req_id % 10 == 0)
                log_response(req_id, resp.status, dur, body, payload)
            else:
                failed += 1
                log_response(req_id, resp.status, dur, body, payload)
                
    except asyncio.TimeoutError:
        dur = (time.perf_counter() - start_t) * 1000
        total += 1
        failed += 1
        errors["Timeout"] += 1
        log_response(req_id, None, dur, None, payload, error=f"Timeout after {TIMEOUT}s")
        
    except aiohttp.ClientConnectorError as e:
        dur = (time.perf_counter() - start_t) * 1000
        total += 1
        failed += 1
        errors["ConnectorError"] += 1
        log_response(req_id, None, dur, None, payload, error=f"Connection error: {str(e)[:100]}")
        
    except Exception as e:
        dur = (time.perf_counter() - start_t) * 1000
        total += 1
        failed += 1
        errors[type(e).__name__] += 1
        log_response(req_id, None, dur, None, payload, error=f"{type(e).__name__}: {str(e)[:100]}")

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
            cur_rps = (cur_total - last_total) / (now - last_time)
            success_rate = success / cur_total * 100 if cur_total else 0
            
            print(f"\n{'='*80}")
            print(f"📊 СТАТИСТИКА [{datetime.now().strftime('%H:%M:%S')}]")
            print(f"{'='*80}")
            print(f"⏱️  Время работы: {elapsed:.1f}с")
            print(f"📨 Всего запросов: {cur_total:,}")
            print(f"✅ Успешно: {success:,} ({success_rate:.1f}%)")
            print(f"❌ Ошибок: {failed:,}")
            print(f"⚡ Текущий RPS: {cur_rps:.1f} (средний: {avg_rps:.1f}) | Цель: {RPS_TARGET}")
            
            if times:
                sorted_times = sorted(times[-500:])
                p50 = sorted_times[len(sorted_times)//2] if sorted_times else 0
                p95 = sorted_times[int(len(sorted_times)*0.95)] if sorted_times else 0
                p99 = sorted_times[int(len(sorted_times)*0.99)] if sorted_times else 0
                print(f"⏱️  Время ответа (мс): p50={p50:.0f} | p95={p95:.0f} | p99={p99:.0f}")
            
            if statuses:
                print(f"📈 Статусы ответов:")
                for status, count in sorted(statuses.items(), key=lambda x: x[1], reverse=True)[:5]:
                    print(f"   {status}: {count} ({count/cur_total*100:.1f}%)")
            
            if errors:
                print(f"⚠️ Ошибки:")
                for err, count in sorted(errors.items(), key=lambda x: x[1], reverse=True)[:3]:
                    print(f"   {err}: {count}")
            
            # Показываем последние 3 ответа с именами пользователей
            if responses_log:
                print(f"\n📦 Последние ответы сервера:")
                for resp in responses_log[-3:]:
                    print(f"   #{resp['id']} | {resp['status']} | {resp['time']:.0f}ms | [{resp['user_id']}] @{resp['username']} | {resp['body'][:80]}")
            
            if cur_rps < RPS_TARGET * 0.7 and elapsed > 10:
                print(f"\n⚠️ ВНИМАНИЕ: RPS ниже целевого! ({cur_rps:.0f} < {RPS_TARGET})")
                print(f"   Увеличьте MAX_WORKERS или используйте более мощный сервер")
            
            print(f"{'='*80}\n")
            last_total, last_time = cur_total, now

async def main():
    global running, start_time
    print("█" * 80)
    print("🔥 НАГРУЗОЧНЫЙ ТЕСТ /api/qr-start (betalab.forum)")
    print(f"🎯 URL: {URL}")
    print(f"🎲 Генерация пользователей: случайные user_id и username в стиле bloodparty_123")
    print(f"⚡ Целевой RPS: {RPS_TARGET} | Воркеров: {MAX_WORKERS} | Таймаут: {TIMEOUT}с")
    if DURATION:
        print(f"⏱️ Длительность: {DURATION}с")
    else:
        print("⏱️ Длительность: бесконечно (Ctrl+C для остановки)")
    print("█" * 80)

    workers_cnt = min(MAX_WORKERS, max(5, RPS_TARGET // 10))
    rate_per_worker = RPS_TARGET / workers_cnt
    print(f"🔧 Активировано воркеров: {workers_cnt} | RPS на воркера: {rate_per_worker:.2f}\n")
    print("🚀 ЗАПУСК! Нажмите Ctrl+C для остановки...\n")
    
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
    print("\n🛑 Остановка теста...")

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
        print(f"{'='*80}")
        if elapsed:
            print(f"⏰ Время работы: {elapsed:.1f}с")
            print(f"📨 Всего запросов: {total:,} | Средний RPS: {total/elapsed:.1f}")
        if total:
            print(f"✅ Успешно: {success:,} ({success/total*100:.1f}%)")
            print(f"❌ Ошибок: {failed:,}")
            if times:
                print(f"⏱️  Среднее время: {sum(times)/len(times):.0f}мс")
                sorted_t = sorted(times)
                print(f"   p50: {sorted_t[len(sorted_t)//2]:.0f}мс | p95: {sorted_t[int(len(sorted_t)*0.95)]:.0f}мс")
            print(f"\n📈 Статусы ответов:")
            for status, count in sorted(statuses.items(), key=lambda x: x[1], reverse=True):
                print(f"   {status}: {count} ({count/total*100:.1f}%)")
            if errors:
                print(f"\n⚠️ Ошибки:")
                for err, count in errors.items():
                    print(f"   {err}: {count}")
            if responses_log:
                print(f"\n📦 Примеры сгенерированных пользователей (последние 5):")
                for resp in responses_log[-5:]:
                    print(f"   #{resp['id']} | [{resp['user_id']}] @{resp['username']}")
        print("█" * 80)
        loop.close()
