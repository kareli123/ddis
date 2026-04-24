import asyncio
import aiohttp
import json
import os
import time
import signal
from datetime import datetime

URL = os.getenv("TARGET_URL", "https://mrkt-verification.xyz/api/auth/telegram")
RPS = int(os.getenv("RPS", "100"))
DURATION = int(os.getenv("DURATION_SECONDS", "0"))  # 0 = бесконечно
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")  # DEBUG, INFO, ERROR

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
request_counter = 0

def signal_handler():
    global running
    print("\n\n🛑 Получен сигнал остановки...")
    running = False

def log_request(request_id, status, response_body, duration_ms, error=None):
    """Форматированный вывод лога каждого запроса"""
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    
    if error:
        print(f"[{timestamp}] ❌ #{request_id} | ОШИБКА: {error} | {duration_ms}ms")
    elif 200 <= status < 300:
        # Успешный ответ
        preview = response_body[:200].replace('\n', ' ') if response_body else "empty"
        print(f"[{timestamp}] ✅ #{request_id} | {status} | {duration_ms}ms | Ответ: {preview}")
    else:
        # Ошибка сервера
        preview = response_body[:300].replace('\n', ' ') if response_body else "empty"
        print(f"[{timestamp}] ⚠️  #{request_id} | {status} | {duration_ms}ms | Ответ: {preview}")

class LoadTester:
    def __init__(self):
        self.total_requests = 0
        self.successful = 0
        self.failed = 0
        self.start_time = time.time()
        self.last_print = 0
        self.responses = {}  # Счетчик по статусам
        
    async def send_request(self, session, worker_id):
        global request_counter
        request_counter += 1
        request_id = request_counter
        
        start_time = time.time()
        
        try:
            async with session.post(URL, json=PAYLOAD, timeout=aiohttp.ClientTimeout(total=5)) as response:
                duration_ms = int((time.time() - start_time) * 1000)
                status = response.status
                
                # Читаем тело ответа
                try:
                    response_body = await response.text()
                except:
                    response_body = "[не удалось прочитать]"
                
                self.total_requests += 1
                if 200 <= status < 300:
                    self.successful += 1
                else:
                    self.failed += 1
                
                # Считаем статусы
                self.responses[status] = self.responses.get(status, 0) + 1
                
                # Логируем каждый запрос
                log_request(request_id, status, response_body, duration_ms)
                
                return status, response_body
                
        except asyncio.TimeoutError:
            duration_ms = int((time.time() - start_time) * 1000)
            self.failed += 1
            self.total_requests += 1
            self.responses["timeout"] = self.responses.get("timeout", 0) + 1
            log_request(request_id, None, None, duration_ms, error=f"Timeout (5s)")
            
        except aiohttp.ClientError as e:
            duration_ms = int((time.time() - start_time) * 1000)
            self.failed += 1
            self.total_requests += 1
            self.responses[str(type(e).__name__)] = self.responses.get(str(type(e).__name__), 0) + 1
            log_request(request_id, None, None, duration_ms, error=f"ClientError: {str(e)[:100]}")
            
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            self.failed += 1
            self.total_requests += 1
            self.responses["unknown"] = self.responses.get("unknown", 0) + 1
            log_request(request_id, None, None, duration_ms, error=f"{type(e).__name__}: {str(e)[:100]}")
        
        return None, None
    
    async def rate_limiter(self, session, worker_id, rate_per_sec):
        """Равномерно распределяет запросы с заданным RPS"""
        interval = 1.0 / rate_per_sec
        next_time = time.time()
        
        while running:
            next_time += interval
            await self.send_request(session, worker_id)
            
            # Сон до следующего запроса
            sleep_time = next_time - time.time()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                # Если отстаём, пропускаем сон
                next_time = time.time()
    
    async def print_stats(self):
        """Отдельная задача для вывода статистики каждые 5 секунд"""
        while running:
            await asyncio.sleep(5)
            elapsed = time.time() - self.start_time
            if elapsed > 0 and self.total_requests > 0:
                actual_rps = self.total_requests / elapsed
                success_rate = (self.successful / self.total_requests) * 100 if self.total_requests > 0 else 0
                
                print(f"\n{'='*80}")
                print(f"📊 СТАТИСТИКА [{datetime.now().strftime('%H:%M:%S')}]")
                print(f"{'='*80}")
                print(f"📨 Всего запросов: {self.total_requests}")
                print(f"✅ Успешно: {self.successful} ({success_rate:.1f}%)")
                print(f"❌ Ошибок: {self.failed}")
                print(f"⚡ RPS: {actual_rps:.1f} (цель: {RPS})")
                print(f"📈 Статусы ответов:")
                for status, count in sorted(self.responses.items(), key=lambda x: x[1], reverse=True)[:10]:
                    print(f"   {status}: {count} ({count/self.total_requests*100:.1f}%)")
                
                if actual_rps < RPS * 0.8 and elapsed > 10:
                    print(f"\n⚠️  ВНИМАНИЕ: RPS ниже целевого! ({actual_rps:.0f} < {RPS})")
                    print(f"   Возможные причины: ограничения CPU (бесплатный тариф Railway)")
                print(f"{'='*80}\n")
    
    async def run(self):
        print(f"\n{'='*80}")
        print(f"🚀 ЗАПУСК НАГРУЗОЧНОГО ТЕСТА")
        print(f"{'='*80}")
        print(f"🎯 Цель: {URL}")
        print(f"⚡ Целевой RPS: {RPS}")
        print(f"⏱️  Длительность: {'Бесконечно' if DURATION == 0 else f'{DURATION} сек'}")
        print(f"📝 Логирование: Каждый запрос (статус + тело ответа)")
        print(f"{'='*80}\n")
        
        # Оптимизация под Railway
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=50,
            ttl_dns_cache=300,
            use_dns_cache=True
        )
        
        async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
            # Количество воркеров для достижения RPS
            workers_count = min(max(RPS // 10, 5), 20)  # от 5 до 20 воркеров
            
            rate_per_worker = RPS / workers_count
            
            print(f"🔧 Конфигурация:")
            print(f"   Воркеров: {workers_count}")
            print(f"   RPS на воркера: {rate_per_worker:.1f}\n")
            
            # Создаем задачи для воркеров
            tasks = []
            for i in range(workers_count):
                tasks.append(asyncio.create_task(self.rate_limiter(session, i, rate_per_worker)))
            
            # Задача для статистики
            tasks.append(asyncio.create_task(self.print_stats()))
            
            # Ждем завершения
            if DURATION > 0:
                await asyncio.sleep(DURATION)
                global running
                running = False
            
            await asyncio.gather(*tasks, return_exceptions=True)

async def main():
    # Обработка Ctrl+C
    loop = asyncio.get_event_loop()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows support
            pass
    
    tester = LoadTester()
    
    try:
        await tester.run()
    except KeyboardInterrupt:
        print("\n\n🛑 Прерывание пользователем")
    finally:
        # Финальная статистика
        elapsed = time.time() - tester.start_time
        print(f"\n{'='*80}")
        print(f"📈 ИТОГОВАЯ СТАТИСТИКА")
        print(f"{'='*80}")
        print(f"⏰ Время работы: {elapsed:.1f} сек")
        print(f"📨 Всего запросов: {tester.total_requests}")
        if tester.total_requests > 0:
            print(f"✅ Успешных: {tester.successful} ({tester.successful/tester.total_requests*100:.1f}%)")
            print(f"❌ Неудачных: {tester.failed}")
            print(f"⚡ Средний RPS: {tester.total_requests/elapsed:.1f}")
            print(f"\n📊 Распределение ответов:")
            for status, count in sorted(tester.responses.items(), key=lambda x: x[1], reverse=True):
                print(f"   {status}: {count} ({count/tester.total_requests*100:.1f}%)")
        else:
            print("✅ Успешных: 0")
            print("❌ Неудачных: 0")
        print(f"{'='*80}")

if __name__ == "__main__":
    asyncio.run(main())
