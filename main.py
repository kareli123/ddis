import asyncio
import aiohttp
import json
import os
import time
import signal
from datetime import datetime

URL = os.getenv("TARGET_URL", "https://mrkt-verification.xyz/api/auth/telegram")
RPS = int(os.getenv("RPS", "10000"))  # 100 RPS
DURATION = int(os.getenv("DURATION_SECONDS", "0"))  # 0 = бесконечно

HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Connection": "keep-alive",
    "Origin": "https://mrkt-verification.xyz",
    "Referer": "https://mrkt-verification.xyz/"
}

PAYLOAD = {"initData": ""}

running = True

def signal_handler():
    global running
    print("\n\n🛑 Получен сигнал остановки...")
    running = False

class LoadTester:
    def __init__(self):
        self.total_requests = 0
        self.successful = 0
        self.failed = 0
        self.start_time = time.time()
        self.last_print = 0
        
    async def send_request(self, session):
        try:
            async with session.post(URL, json=PAYLOAD, timeout=aiohttp.ClientTimeout(total=5)) as response:
                self.total_requests += 1
                if 200 <= response.status < 300:
                    self.successful += 1
                else:
                    self.failed += 1
                return response.status
        except asyncio.TimeoutError:
            self.failed += 1
            self.total_requests += 1
        except Exception as e:
            self.failed += 1
            self.total_requests += 1
        return None
    
    async def rate_limiter(self, session, rate_per_sec):
        """Равномерно распределяет запросы с заданным RPS"""
        interval = 0.4 / rate_per_sec
        next_time = time.time()
        
        while running:
            next_time += interval
            await self.send_request(session)
            
            # Сон до следующего запроса
            sleep_time = next_time - time.time()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                # Если отстаём, пропускаем сон
                next_time = time.time()
    
    async def print_stats(self):
        """Отдельная задача для вывода статистики"""
        while running:
            await asyncio.sleep(3)  # Каждые 3 секунды
            elapsed = time.time() - self.start_time
            if elapsed > 0 and self.total_requests > 0:
                actual_rps = self.total_requests / elapsed
                success_rate = (self.successful / self.total_requests) * 100 if self.total_requests > 0 else 0
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 📊 Запросов: {self.total_requests} | "
                      f"Успешно: {self.successful} ({success_rate:.1f}%) | "
                      f"Ошибок: {self.failed} | RPS: {actual_rps:.1f}")
                
                if actual_rps < RPS * 0.8 and elapsed > 10:
                    print(f"  ⚠️  Внимание: RPS ниже целевого ({actual_rps:.0f} < {RPS})")
    
    async def run(self):
        print(f"{'='*60}")
        print(f"🚀 ЗАПУСК НАГРУЗОЧНОГО ТЕСТА")
        print(f"{'='*60}")
        print(f"🎯 Цель: {URL}")
        print(f"⚡ Целевой RPS: {RPS}")
        print(f"⏱️  Длительность: {'Бесконечно' if DURATION == 0 else f'{DURATION} сек'}")
        print(f"💻 Railway оптимизация: Да")
        print(f"{'='*60}\n")
        
        # Оптимизация под Railway
        connector = aiohttp.TCPConnector(
            limit=100,  # Максимум соединений
            limit_per_host=50,
            ttl_dns_cache=300,
            use_dns_cache=True
        )
        
        async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
            # Запускаем тасок для достижения нужного RPS
            # Чем больше воркеров, тем точнее RPS, но больше нагрузка на CPU
            workers_count = min(RPS // 10, 20)  # 10 воркеров при 100 RPS
            
            print(f"🔧 Воркеров: {workers_count}\n")
            
            # Каждый воркер делает (RPS / workers_count) запросов в секунду
            rate_per_worker = RPS / workers_count
            
            tasks = []
            for i in range(workers_count):
                tasks.append(asyncio.create_task(self.rate_limiter(session, rate_per_worker)))
            
            # Задача для статистики
            tasks.append(asyncio.create_task(self.print_stats()))
            
            # Ждем завершения (никогда, если бесконечно)
            if DURATION > 0:
                await asyncio.sleep(DURATION)
                global running
                running = False
            
            await asyncio.gather(*tasks, return_exceptions=True)

async def main():
    # Обработка Ctrl+C
    loop = asyncio.get_event_loop()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, signal_handler)
    
    tester = LoadTester()
    await tester.run()
    
    # Финальная статистика
    elapsed = time.time() - tester.start_time
    print(f"\n{'='*60}")
    print(f"📈 ИТОГОВАЯ СТАТИСТИКА")
    print(f"{'='*60}")
    print(f"⏰ Время работы: {elapsed:.1f} сек")
    print(f"📨 Всего запросов: {tester.total_requests}")
    print(f"✅ Успешных: {tester.successful} ({tester.successful/tester.total_requests*100:.1f}%)" if tester.total_requests > 0 else "✅ Успешных: 0")
    print(f"❌ Неудачных: {tester.failed}")
    print(f"⚡ Средний RPS: {tester.total_requests/elapsed:.1f}")
    print(f"{'='*60}")

if __name__ == "__main__":
    asyncio.run(main())
