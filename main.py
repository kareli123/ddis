import asyncio
import aiohttp
from aiohttp import web
import json
import sys
import time
from collections import defaultdict
import random

# --- Конфигурация ---
URL = "https://web.infotelegram.org/api/auth"
PAYLOAD_DATA = "tgWebAuthToken=K3lAmoEkgzGN9GxufePihbJHqpc2jY9U1leGcNQiJwOZELqyizWS-AR5LBo8du5A32f_UwtvZIyGxFwUfugFC67u3mVor3F6d1XewaEGN8_oj0HxfytRh7qrIw7j23eJz9bVVtDvNYQJ5DRlEWRdpSxW8uZuSnlECIMQr_EFwPc&tgWebAuthUserId=1337&tgWebAuthDcId=1"

# Настройки нагрузки
CONCURRENT_REQUESTS = 200        # Уменьшено, чтобы сервер не резал соединения
REQUEST_TIMEOUT = 5              # Таймаут 5 секунд (достаточно для ответа)
RATE_LIMIT_PER_SEC = 0           # 0 = без ограничения, иначе пауза между добавлением задач в очередь

# Логирование
LOG_SUCCESS_BODY_EVERY = 50      # Выводить тело ответа каждые N успешных запросов
LOG_ERROR_BODY = True            # Выводить тело при ошибках
LOG_HEADERS_ON_ERROR = True      # Выводить заголовки при ошибках
LOG_BODY_TRUNCATE = 500          # Обрезать длинное тело в логах

# Список User-Agent для ротации
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

# --- Глобальная статистика и блокировка для потокобезопасного вывода ---
stats = {
    "start_time": time.time(),
    "total_sent": 0,
    "total_success": 0,
    "total_failed": 0,
    "status_codes": defaultdict(int),
    "exceptions": defaultdict(int),
    "last_response": None,
}
print_lock = asyncio.Lock()

async def safe_print(*args, **kwargs):
    """Потокобезопасный вывод в консоль."""
    async with print_lock:
        print(*args, **kwargs)

def truncate(text, limit=LOG_BODY_TRUNCATE):
    if text is None:
        return "<empty>"
    if len(text) > limit:
        return text[:limit] + "... [truncated]"
    return text

async def send_request(session, payload_json):
    global stats
    headers = HEADERS.copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)
    # Можно добавить Referer, Origin для реализма
    headers["Referer"] = "https://web.infotelegram.org/"
    headers["Origin"] = "https://web.infotelegram.org"

    try:
        async with session.post(URL, json=payload_json, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            stats["total_sent"] += 1
            status = resp.status
            stats["status_codes"][status] += 1

            # Читаем тело
            try:
                response_text = await resp.text()
            except Exception:
                response_text = "<binary/undecodable>"

            if status == 200:
                stats["total_success"] += 1
                if stats["total_success"] % LOG_SUCCESS_BODY_EVERY == 0:
                    truncated = truncate(response_text)
                    await safe_print(f"[✓] Успех #{stats['total_success']} | Статус: {status} | Тело: {truncated}")
                elif stats["total_sent"] % 100 == 0:
                    await safe_print(f"Прогресс: отправлено {stats['total_sent']}, успешно {stats['total_success']}, ошибок {stats['total_failed']}")
                # Сохраняем последний ответ
                stats["last_response"] = {
                    "status": status,
                    "body": truncate(response_text, 200)
                }
            else:
                # Любой не-200 код
                error_msg = f"[!] Ошибка HTTP {status} | Запрос #{stats['total_sent']}"
                if LOG_ERROR_BODY:
                    error_msg += f" | Тело: {truncate(response_text)}"
                if LOG_HEADERS_ON_ERROR:
                    error_msg += f" | Заголовки: {dict(resp.headers)}"
                await safe_print(error_msg)
                stats["last_response"] = {
                    "status": status,
                    "headers": dict(resp.headers),
                    "body": truncate(response_text)
                }

    except asyncio.TimeoutError:
        stats["total_failed"] += 1
        stats["exceptions"]["TimeoutError"] += 1
        if stats["total_failed"] % 10 == 0:
            await safe_print(f"[X] Таймаут (всего ошибок: {stats['total_failed']})")
    except Exception as e:
        stats["total_failed"] += 1
        exc_name = type(e).__name__
        stats["exceptions"][exc_name] += 1
        if stats["total_failed"] % 10 == 0:
            await safe_print(f"[X] Ошибка: {exc_name} | {e} (всего ошибок: {stats['total_failed']})")

async def worker(worker_id, session, payload_json, queue):
    while True:
        await queue.get()
        await send_request(session, payload_json)
        queue.task_done()

async def burst_loop():
    payload_json = {"data": PAYLOAD_DATA}
    queue = asyncio.Queue(maxsize=CONCURRENT_REQUESTS * 2)

    async with aiohttp.ClientSession() as session:
        workers = []
        for i in range(CONCURRENT_REQUESTS):
            w = asyncio.create_task(worker(i, session, payload_json, queue))
            workers.append(w)

        while True:
            await queue.put(1)
            if RATE_LIMIT_PER_SEC > 0:
                await asyncio.sleep(1 / RATE_LIMIT_PER_SEC)
            else:
                await asyncio.sleep(0)  # отдаём управление

# --- Веб-сервер для мониторинга ---
async def health_check(request):
    uptime = int(time.time() - stats["start_time"])
    return web.Response(text=f"OK. Uptime: {uptime}s, sent: {stats['total_sent']}, success: {stats['total_success']}, failed: {stats['total_failed']}")

async def stats_json(request):
    uptime = int(time.time() - stats["start_time"])
    response_data = {
        "uptime_seconds": uptime,
        "total_sent": stats["total_sent"],
        "total_success": stats["total_success"],
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
    await safe_print("🌐 Health-сервер запущен на порту 8080")
    await asyncio.Event().wait()

async def main():
    await safe_print(f"🚀 Запуск нагрузочного теста: {CONCURRENT_REQUESTS} одновременных соединений")
    await safe_print(f"🎯 Цель: {URL}")
    await safe_print(f"⏱️ Таймаут: {REQUEST_TIMEOUT} сек")
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
        print(f"Успешно (200): {stats['total_success']}")
        print(f"Ошибок соединения: {stats['total_failed']}")
        print("Статусы HTTP:")
        for code, count in sorted(stats["status_codes"].items()):
            print(f"  {code}: {count}")
        if stats["exceptions"]:
            print("Исключения:")
            for exc, count in stats["exceptions"].items():
                print(f"  {exc}: {count}")
        if stats["last_response"]:
            print(f"Последний ответ: {stats['last_response']}")
        sys.exit(0)
