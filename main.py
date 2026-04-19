import asyncio
import aiohttp
from collections import defaultdict

# ------------------- Настройки -------------------
CONCURRENT_REQUESTS = 200       # количество одновременных запросов
TOTAL_REQUESTS = 10001110          # общее число запросов
REQUEST_TIMEOUT = 1            # таймаут на один запрос (сек)
LOG_FIRST_ERRORS = 5            # показать первые N ошибок подробно
# -------------------------------------------------

# Ваш токен (вставлен напрямую)
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
    "Content-Type": "application/json",          # обязательно указываем
}

# Тело запроса (пустой JSON объект)
PAYLOAD = {}

# Статистика
success_count = 0
error_count = 0
status_counter = defaultdict(int)
error_details = []
lock = asyncio.Lock()
error_logged = 0
start_time = None

async def post_request(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> None:
    global success_count, error_count, error_logged
    async with semaphore:
        try:
            async with session.post(URL, json=PAYLOAD, headers=HEADERS, timeout=REQUEST_TIMEOUT) as resp:
                status = resp.status
                text = await resp.text()
                
                async with lock:
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
        except Exception as e:
            async with lock:
                error_count += 1
                status_counter["exception"] += 1
                if error_logged < LOG_FIRST_ERRORS:
                    error_logged += 1
                    error_details.append({
                        "status": "exception",
                        "body": str(e)[:500],
                    })

async def progress_reporter(total: int, interval: float = 2.0):
    while True:
        await asyncio.sleep(interval)
        current_total = success_count + error_count
        if current_total >= total:
            break
        elapsed = asyncio.get_event_loop().time() - start_time
        rate = current_total / elapsed if elapsed > 0 else 0
        print(f"📊 {current_total}/{total} | ✅ {success_count} | ❌ {error_count} | Скорость: {rate:.1f} з/с")

async def main():
    global start_time
    start_time = asyncio.get_event_loop().time()
    
    print(f"🚀 Отправка {TOTAL_REQUESTS} запросов к {URL}")
    print(f"⚙️  CONCURRENT: {CONCURRENT_REQUESTS} | Таймаут: {REQUEST_TIMEOUT}с")
    print(f"🔑 Токен: {TOKEN[:20]}...{TOKEN[-10:]}")

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS + 10, force_close=True)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [post_request(session, semaphore) for _ in range(TOTAL_REQUESTS)]
        reporter = asyncio.create_task(progress_reporter(TOTAL_REQUESTS))
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

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  Прервано пользователем")
