import asyncio
import aiohttp
import json
import os
import time
from datetime import datetime

URL = os.getenv("TARGET_URL", "https://mrkt-verification.xyz/api/auth/telegram")
RPS = int(os.getenv("RPS", "300"))
DURATION = int(os.getenv("DURATION_SECONDS", "301111111"))

HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Connection": "keep-alive"
}

PAYLOAD = {"initData": ""}

class LoadTester:
    def __init__(self):
        self.total_requests = 0
        self.successful = 0
        self.failed = 0
        self.errors = {}
        self.start_time = None
        
    async def send_request(self, session, request_id):
        try:
            async with session.post(URL, json=PAYLOAD, timeout=aiohttp.ClientTimeout(total=5)) as response:
                self.total_requests += 1
                if 200 <= response.status < 300:
                    self.successful += 1
                else:
                    self.failed += 1
                    self.errors[response.status] = self.errors.get(response.status, 0) + 1
                return response.status
        except asyncio.TimeoutError:
            self.failed += 1
            self.errors["timeout"] = self.errors.get("timeout", 0) + 1
        except Exception as e:
            self.failed += 1
            self.errors[str(type(e).__name__)] = self.errors.get(str(type(e).__name__), 0) + 1
        return None
    
    async def worker(self, session, worker_id):
        """Один worker для равномерной нагрузки"""
        loop = asyncio.get_event_loop()
        end_time = loop.time() + DURATION
        
        while loop.time() < end_time:
            batch_start = loop.time()
            
            # Отправляем 1 запрос от этого worker'а
            await self.send_request(session, f"w{worker_id}")
            
            # Контролируем RPS: 1 worker = RPS / workers
            elapsed = loop.time() - batch_start
            sleep_time = max(0, (50 / RPS) - elapsed)  # 50 workers примерное
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
    
    async def run(self):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Запуск теста: {RPS} RPS на {DURATION} сек")
        print(f"Цель: {URL}")
        
        # Оптимальное количество воркеров под Railway (CPU ограничен)
        workers_count = min(RPS, 25)  # Не больше 25 одновременных задач
        print(f"Воркеров: {workers_count}")
        
        self.start_time = time.time()
        
        async with aiohttp.ClientSession(headers=HEADERS, connector=aiohttp.TCPConnector(limit=workers_count * 2)) as session:
            tasks = [self.worker(session, i) for i in range(workers_count)]
            await asyncio.gather(*tasks)
        
        self.print_stats()
    
    def print_stats(self):
        elapsed = time.time() - self.start_time
        actual_rps = self.total_requests / elapsed if elapsed > 0 else 0
        
        print(f"\n{'='*50}")
        print(f"РЕЗУЛЬТАТЫ ТЕСТА")
        print(f"{'='*50}")
        print(f"Длительность: {elapsed:.2f} сек")
        print(f"Всего запросов: {self.total_requests}")
        print(f"Успешных: {self.successful} ({self.successful/self.total_requests*100:.1f}%)")
        print(f"Неудачных: {self.failed} ({self.failed/self.total_requests*100:.1f}%)")
        print(f"Средний RPS: {actual_rps:.1f}")
        print(f"Целевой RPS: {RPS}")
        
        if self.errors:
            print(f"\nОшибки:")
            for error, count in sorted(self.errors.items(), key=lambda x: x[1], reverse=True):
                print(f"  {error}: {count}")

async def main():
    tester = LoadTester()
    await tester.run()

if __name__ == "__main__":
    asyncio.run(main())
