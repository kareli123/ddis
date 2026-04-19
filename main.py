import asyncio
import aiohttp
import os
from typing import Optional

# ------------------- Настройки (можно менять) -------------------
CONCURRENT_REQUESTS = 150       # количество одновременных запросов
TOTAL_REQUESTS = 10000          # общее число запросов
REQUEST_TIMEOUT = 10            # таймаут на один запрос (сек)
BATCH_SIZE = 50                 # размер пачки для вывода прогресса
# -----------------------------------------------------------------

# Данные берутся из переменных окружения (безопасно для Railway)
TOKEN = os.environ.get("API_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")  # замените на реальный
TASK_ID = int(os.environ.get("TASK_ID", "342"))
BASE_URL = os.environ.get("API_URL", "https://maloycser.com")
URL = f"{BASE_URL}/api/tasks/{TASK_ID}/submit"

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "ru,en;q=0.9",
    "Authorization": f"Bearer {TOKEN}",
    "Origin": BASE_URL,
    "Referer": f"{BASE_URL}/tasks",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# Статистика
success_count = 0
error_count = 0
lock = asyncio.Lock()

async def post_request(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> None:
    """Отправляет один POST-запрос, обновляет статистику."""
    global success_count, error_count
    async with semaphore:
        try:
            async with session.post(URL, headers=HEADERS, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status == 200:
                    async with lock:
                        success_count += 1
                else:
                    async with lock:
                        error_count += 1
                    # Текст ошибки можно логировать при необходимости
        except Exception:
            async with lock:
                error_count += 1

async def progress_reporter(total: int, interval: float = 2.0) -> None:
    """Периодически выводит прогресс выполнения."""
    while True:
        await asyncio.sleep(interval)
        current_total = success_count + error_count
        if current_total >= total:
            break
        print(f"📊 Прогресс: {current_total}/{total} "
              f"(✅ {success_count} | ❌ {error_count})")

async def main():
    print(f"🚀 Запуск отправки {TOTAL_REQUESTS} запросов к {URL}")
    print(f"⚙️  Одновременных соединений: {CONCURRENT_REQUESTS}")

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS + 10, force_close=True)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [post_request(session, semaphore) for _ in range(TOTAL_REQUESTS)]

        # Запускаем фоновую задачу для вывода прогресса
        reporter = asyncio.create_task(progress_reporter(TOTAL_REQUESTS))

        # Ждём выполнения всех запросов
        await asyncio.gather(*tasks)

        # Останавливаем reporter
        reporter.cancel()

    # Финальный отчёт
    print("\n" + "=" * 50)
    print(f"✅ Успешно:   {success_count}")
    print(f"❌ Ошибок:    {error_count}")
    print(f"📈 Всего:     {success_count + error_count}")
    print("=" * 50)

if __name__ == "__main__":
    # Для Railway важно использовать asyncio.run() (Python 3.7+)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  Прервано пользователем")
