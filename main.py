import asyncio
import aiohttp
import time
import json
from aiohttp import TCPConnector

# Твои данные
FINGERPRINT = "ab91564731da6d678942178a4d31f4ba"
INIT_DATA = "user=%7B%22id%22%3A632503732%2C%22first_name%22%3A%22%40rekrut%22%2C%22last_name%22%3A%22%22%2C%22username%22%3A%22rekrut%22%2C%22language_code%22%3A%22ru%22%2C%22is_premium%22%3Atrue%2C%22allows_write_to_pm%22%3Atrue%2C%22photo_url%22%3A%22https%3A%5C%2F%5C%2Ft.me%5C%2Fi%5C%2Fuserpic%5C%2F320%5C%2F3Rh7rfuUzLDv9psEiz8liMd9OP75rDao7HhypSIsBzY.svg%22%7D&chat_instance=-1500624000961944755&chat_type=sender&auth_date=1777329916&signature=z29Zfu7xR5E0adU7_momtLqkeER8Gw8I8nPXi-H-FoJYJ7fOHC4_1VnEVAwJvCoo8IJT7M5KbENe1TU92RnYBQ&hash=d09e3f415fe69d50b88a3201e970bd5db05815d4087bc69f6281cb2c97b8613a"

URL = "https://api.subo-kick.com/auth/telegram"

PAYLOAD = {
    "fingerprint": FINGERPRINT,
    "initData": INIT_DATA
}

# Улучшенные заголовки — ближе к реальному Chrome 130+
HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://subo-kick.com",
    "Referer": "https://subo-kick.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Site": "same-site",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Sec-Ch-Ua": '"Chromium";v="130", "Microsoft Edge";v="130", "Not?A_Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
}

async def send_request(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, request_id: int):
    async with semaphore:
        try:
            start = time.time()
            async with session.post(URL, json=PAYLOAD, headers=HEADERS, timeout=15) as response:
                status = response.status
                text = await response.text()
                elapsed = time.time() - start

                # ВСЕГДА показываем ответ (но обрезаем слишком длинный body, чтобы консоль не взорвалась)
                print(f"\n[{request_id}] Status: {status} | Time: {elapsed:.3f}s")
                print(f"Headers: {dict(response.headers)}")
                print(f"Body: {text[:1500]}{'...' if len(text) > 1500 else ''}")

                # Если Cloudflare блокирует — часто возвращает 403/503 + HTML с challenge
                if status in (403, 503, 429) or "cloudflare" in text.lower() or "challenge" in text.lower():
                    print(f"[{request_id}] !!! Вероятный Cloudflare блок (403/503/challenge) !!!")

                return status, text
        except asyncio.TimeoutError:
            print(f"[{request_id}] Timeout")
            return "timeout", ""
        except Exception as e:
            print(f"[{request_id}] Error: {type(e).__name__} - {e}")
            return f"error: {type(e).__name__}", ""

async def main():
    concurrent = 80          # Снижаем до 80–120. При 250+ чаще всего сыплются таймауты и блоки от CF
    total_requests = 5000    # Сколько всего отправить. Можно поставить большой или сделать бесконечный цикл

    connector = TCPConnector(
        limit=concurrent * 2,
        limit_per_host=concurrent,
        ssl=False,
        ttl_dns_cache=300,
        keepalive_timeout=30,
        force_close=False
    )

    timeout = aiohttp.ClientTimeout(total=20, sock_connect=8, sock_read=12)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        semaphore = asyncio.Semaphore(concurrent)
        
        start_time = time.time()
        
        tasks = [send_request(session, semaphore, i) for i in range(total_requests)]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        elapsed = time.time() - start_time
        successful = sum(1 for r in results if isinstance(r, tuple) and isinstance(r[0], int) and 200 <= r[0] < 300)
        
        print(f"\n=== ИТОГО ===")
        print(f"Завершено за {elapsed:.2f} секунд")
        print(f"Отправлено: {total_requests}")
        print(f"Успешных (2xx): {successful}")
        print(f"Средняя скорость: {total_requests / elapsed:.1f} req/s")

if __name__ == "__main__":
    asyncio.run(main())
