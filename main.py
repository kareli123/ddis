import asyncio
import aiohttp
from collections import defaultdict
import sys

# ------------------- Настройки (уменьшены для отладки) -------------------
CONCURRENT_REQUESTS = 50        # снижаем для стабильности
TOTAL_REQUESTS = 100            # для теста сделаем 100 запросов (потом можно увеличить)
REQUEST_TIMEOUT = 30            # таймаут побольше
LOG_FIRST_ERRORS = 10
# -------------------------------------------------------------------------

# Ваши данные (токен вставлен)
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOjMyMzk0OCwidGVsZWdyYW1JZCI6NjMyNTAzNzMyLCJyb2xlIjoidXNlciIsImlhdCI6MTc3NjYzNjM1MiwiZXhwIjoxNzc2NzIyNzUyfQ.oWhXOZxi-WhES7gAtMEeXrAHkl2AnJUyoHER89TZw1o"
TASK_ID = 342
BASE_URL = "https://maloycser.com"
URL = f"{BASE_URL}/api/tasks/{TASK_ID}/submit"

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "ru,en;q=0.9",
    "Authorization": f"Bearer {TOKEN}",
    "Origin": BASE_URL,
    "Referer": f"{BASE_URL}/tasks",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
}

PAYLOAD = {}  # пустой JSON

# Статистика
success_count = 0
error_count = 0
status_counter = defaultdict(int)
error_details = []
lock = asyncio.Lock()
error_logged = 0
start_time = None
total_requests_sent = 0  # счётчик отправленных

async def post_request(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> None:
    global success_count, error_count, error_logged, total_requests_sent
    async with semaphore:
        try:
            # Отладочное сообщение (редкое, чтобы не забивать логи)
            if total_requests_sent < 3:
                print(f"[DEBUG] Отправка запроса...")
            async with session.post(URL, json=PAYLOAD, headers=HEADERS, timeout=REQUEST_TIMEOUT) as resp:
                status = resp.status
                text = await resp.text()
                async with lock:
                    total_requests_sent += 1
                    status_counter[status] += 1
                    if status == 200:
                        success_count += 1
                    else:
                        error_count += 1
                        if error_logged < LOG_FIRST_ERRORS:
                            error_logged += 1
                            error_details.append({
                                "status": status,
                                "body": text[:500],
                            })
                    # Если это один из первых запросов, выводим статус сразу
                    if total_requests_sent <= 3:
                        print(f"[DEBUG] Ответ: статус {status}, длина тела {len(text)}")
        except asyncio.TimeoutError:
            async with lock:
                error_count += 1
                total_requests_sent += 1
                status_counter["timeout"] += 1
                if error_logged < LOG_FIRST_ERRORS:
                    error_logged += 1
                    error_details.append({"status": "timeout", "body": f"Таймаут {REQUEST_TIMEOUT}с"})
        except Exception as e:
            async with lock:
                error_count += 1
                total_requests_sent += 1
                status_counter["exception"] += 1
                if error_logged < LOG_FIRST_ERRORS:
                    error_logged += 1
                    error_details.append({"status": "exception", "body": str(e)[:500]})

async def progress_reporter(total: int, interval: float = 2.0):
    while True:
        await asyncio.sleep(interval)
        current = success_count + error_count
        if current >= total:
            break
        elapsed = asyncio.get_event_loop().time() - start_time
        rate = current / elapsed if elapsed > 0 else 0
        print(f"📊 {current}/{total} | ✅ {success_count} | ❌ {error_count} | {rate:.1f} з/с")
        sys.stdout.flush()  # принудительный сброс буфера

async def main():
    global start_time
    start_time = asyncio.get_event_loop().time()
    
    print(f"🚀 Отправка {TOTAL_REQUESTS} запросов к {URL}")
    print(f"⚙️  CONCURRENT: {CONCURRENT_REQUESTS} | Таймаут: {REQUEST_TIMEOUT}с")
    print(f"🔑 Токен: {TOKEN[:20]}...{TOKEN[-10:]}")
    sys.stdout.flush()

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS + 10, force_close=True)

    print("[DEBUG] Создание сессии...")
    sys.stdout.flush()

    async with aiohttp.ClientSession(connector=connector) as session:
        print("[DEBUG] Сессия создана, запуск задач...")
        sys.stdout.flush()
        
        tasks = [asyncio.create_task(post_request(session, semaphore)) for _ in range(TOTAL_REQUESTS)]
        reporter = asyncio.create_task(progress_reporter(TOTAL_REQUESTS))
        
        # Ждём все запросы
        await asyncio.gather(*tasks)
        reporter.cancel()

    # Финальный отчёт
    print("\n" + "=" * 60)
    print(f"✅ Успешно (200):        {success_count}")
    print(f"❌ Всего ошибок:         {error_count}")
    print("\n📋 Распределение по статусам:")
    for code, cnt in sorted(status_counter.items(), key=lambda x: -x[1]):
        print(f"   {code}: {cnt}")
    if error_details:
        print(f"\n🧾 Примеры ошибок (первые {len(error_details)}):")
        for i, err in enumerate(error_details, 1):
            print(f"\n--- Ошибка #{i} ---")
            print(f"Статус: {err['status']}")
            if err['body']:
                print(f"Тело ответа:\n{err['body']}")
    print("=" * 60)
    sys.stdout.flush()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  Прервано пользователем")
