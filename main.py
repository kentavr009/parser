"""
Высокопроизводительный асинхронный парсер для Booking.com
Использует Playwright для полной эмуляции браузера с JavaScript

Запуск:
    python main.py
    
Перед первым запуском:
    playwright install chromium

Требования:
    - Файл urls.txt с URL-адресами отелей (один на строку)
    - Файл proxies.txt с прокси (форматы: user:pass@ip:port, ip:port:user:pass или ip:port)
    - Результаты сохраняются в results.csv
    - Ошибки записываются в errors.txt
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import random
import re
import time
import urllib.request
import urllib.error
from typing import Optional, Tuple, List, Dict, Any

from bs4 import BeautifulSoup
import aiofiles
# Playwright и aiohttp не импортируем здесь — могут зависать при загрузке. Импорт Playwright в run(); проверка прокси через urllib.

# Статический список UA (без загрузки из сети при старте — иначе fake_useragent может зависать)
STATIC_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Настройка логирования (FileHandler может зависнуть, если parser.log заблокирован)
try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('parser.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
except Exception as e:
    print(f"  Логирование в файл отключено: {e}", flush=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Константы
INITIAL_CONCURRENT = 5  # Начальная конкурентность (5 потоков)
MIN_CONCURRENT = 1  # Минимальная конкурентность
MAX_CONCURRENT = 10  # Максимальная конкурентность (фиксируем 5 потоков)
WORKER_0_DIRECT = True  # Поток 0 — основной адрес (без прокси); потоки 1–4 — прокси по кругу
MAX_RETRIES = 3
PAGE_TIMEOUT = 60000  # 60 секунд
INITIAL_DELAY = (2, 4)  # Начальная задержка
MIN_DELAY = (1, 2)  # Минимальная задержка
MAX_DELAY = (5, 8)  # Максимальная задержка

# Параметры адаптивной системы
ERROR_THRESHOLD = 0.3  # Если >30% ошибок - снижаем скорость
SUCCESS_THRESHOLD = 0.95  # Если >95% успеха - увеличиваем скорость
ADAPTATION_WINDOW = 20  # Окно для анализа (последние N запросов)

# Параметры валидации оценок
MIN_RATING = 2.5  # Минимальная реалистичная оценка отеля
MAX_RATING = 10.0  # Максимальная оценка

# Параметры валидации прокси
PROXY_CHECK_TIMEOUT = 10  # Таймаут проверки одного прокси (сек)
PROXY_VALIDATE_TOTAL_TIMEOUT = 120  # Макс. время на валидацию всех прокси (сек); иначе пропускаем
MAX_PROXY_LATENCY_MS = 15000  # Макс. латентность для выбора прокси (мс)
PROXY_CHECK_URL = 'https://api.ipify.org'  # Эндпоинт для проверки доступности


def _proxy_str_to_aiohttp_url(proxy: str) -> Optional[str]:
    """Преобразует строку прокси в URL для aiohttp (http://user:pass@host:port или http://host:port)."""
    if not proxy or not proxy.strip():
        return None
    proxy = proxy.strip()
    if '@' in proxy:
        try:
            auth_part, host_part = proxy.split('@', 1)
            user, password = auth_part.split(':', 1)
            ip, port = host_part.rsplit(':', 1)
            return f"http://{user}:{password}@{ip}:{port}"
        except (ValueError, IndexError):
            return None
    parts = proxy.split(':')
    if len(parts) == 4:
        ip, port, user, password = parts
        return f"http://{user}:{password}@{ip}:{port}"
    if len(parts) == 2:
        ip, port = parts
        return f"http://{ip}:{port}"
    return None


def _check_proxy_sync(proxy_str: str, timeout_sec: float) -> Dict[str, Any]:
    """Синхронная проверка прокси через urllib (без aiohttp — не блокирует старт скрипта)."""
    proxy_url = _proxy_str_to_aiohttp_url(proxy_str)
    if not proxy_url:
        return {"ok": False, "latency_ms": None, "error": "invalid proxy format"}
    try:
        t0 = time.perf_counter()
        proxy_handler = urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        opener = urllib.request.build_opener(proxy_handler)
        req = urllib.request.Request(PROXY_CHECK_URL, headers={"User-Agent": "Mozilla/5.0"})
        with opener.open(req, timeout=timeout_sec) as resp:
            code = getattr(resp, "status", None) or resp.getcode()
            if code != 200:
                return {"ok": False, "latency_ms": None, "error": f"status {code}"}
            resp.read()
        latency_ms = (time.perf_counter() - t0) * 1000
        return {"ok": True, "latency_ms": latency_ms, "error": None}
    except urllib.error.URLError as e:
        err = "timeout" if "timed out" in str(e).lower() else str(e.reason or e)[:200]
        return {"ok": False, "latency_ms": None, "error": err}
    except Exception as e:
        return {"ok": False, "latency_ms": None, "error": str(e)[:200]}


async def check_proxy(proxy_str: str, timeout_sec: float = 10) -> Dict[str, Any]:
    """
    Проверяет прокси: доступность и латентность через GET к PROXY_CHECK_URL.
    Использует urllib в executor, чтобы не блокировать event loop и не тянуть aiohttp при старте.
    """
    loop = asyncio.get_event_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, _check_proxy_sync, proxy_str, timeout_sec),
            timeout=timeout_sec + 5,
        )
    except asyncio.TimeoutError:
        return {"ok": False, "latency_ms": None, "error": "timeout"}


def is_valid_rating(rating: float) -> bool:
    """
    Проверяет, является ли оценка валидной для отеля.
    
    Args:
        rating: Оценка для проверки
        
    Returns:
        True если оценка валидна, False иначе
    """
    if not isinstance(rating, (int, float)):
        return False
    
    # Проверка диапазона
    if not (MIN_RATING <= rating <= MAX_RATING):
        return False
    
    # Отклоняем нереалистичные низкие оценки (меньше 2.5)
    # Оценки отелей на Booking.com обычно от 4.0 до 10.0
    # Но оставляем небольшой запас для редких случаев
    if rating < MIN_RATING:
        return False
    
    return True


class ProxyManager:
    """Менеджер прокси-серверов с метриками, валидацией и уникальным UA на прокси."""
    
    def __init__(self, proxies_file: str = 'proxies.txt'):
        self.proxies_file = proxies_file
        self.proxies: List[str] = []
        self.metrics: Dict[str, Dict[str, Any]] = {}  # proxy_str -> { latency_ms, success_count, fail_count, last_check_ok, last_check_at }
        self._proxy_user_agents: Dict[str, str] = {}  # proxy_str -> уникальный User-Agent (из статического списка)
        self.load_proxies()
    
    def _ensure_metrics(self, proxy_str: str) -> Dict[str, Any]:
        """Возвращает (и при необходимости создаёт) запись метрик для прокси."""
        if proxy_str not in self.metrics:
            self.metrics[proxy_str] = {
                "latency_ms": None,
                "success_count": 0,
                "fail_count": 0,
                "last_check_ok": False,
                "last_check_at": None,
            }
        return self.metrics[proxy_str]
    
    def load_proxies(self):
        """Загружает прокси из файла"""
        try:
            with open(self.proxies_file, 'r', encoding='utf-8') as f:
                self.proxies = [line.strip() for line in f if line.strip()]
            logger.info(f"Загружено {len(self.proxies)} прокси")
        except FileNotFoundError:
            logger.warning(f"Файл {self.proxies_file} не найден. Работа без прокси.")
            self.proxies = []
    
    def get_random_proxy(self) -> Optional[str]:
        """Возвращает случайный прокси или None"""
        if not self.proxies:
            return None
        return random.choice(self.proxies)
    
    def format_proxy(self, proxy: str) -> Optional[dict]:
        """Форматирует прокси для Playwright"""
        if not proxy:
            return None
        
        # Поддержка форматов: user:pass@ip:port, ip:port:user:pass, ip:port
        if '@' in proxy:
            try:
                auth_part, host_part = proxy.split('@', 1)
                user, password = auth_part.split(':', 1)
                ip, port = host_part.rsplit(':', 1)
                return {
                    "server": f"http://{ip}:{port}",
                    "username": user,
                    "password": password
                }
            except (ValueError, IndexError):
                return None
        
        parts = proxy.split(':')
        if len(parts) == 4:
            ip, port, user, password = parts
            return {
                "server": f"http://{ip}:{port}",
                "username": user,
                "password": password
            }
        elif len(parts) == 2:
            ip, port = parts
            return {
                "server": f"http://{ip}:{port}"
            }
        return None
    
    async def validate_proxies(self, timeout: float = PROXY_CHECK_TIMEOUT, max_concurrent: int = 10) -> None:
        """Параллельно проверяет все прокси, обновляет метрики. Логирует итог."""
        if not self.proxies:
            return
        sem = asyncio.Semaphore(max_concurrent)
        
        async def check_one(proxy_str: str) -> None:
            async with sem:
                result = await check_proxy(proxy_str, timeout_sec=timeout)
                m = self._ensure_metrics(proxy_str)
                m["last_check_ok"] = result["ok"]
                m["last_check_at"] = time.time()
                m["latency_ms"] = result.get("latency_ms")
                if result["ok"]:
                    pass  # уже обновлено
                else:
                    logger.debug(f"Прокси не прошел проверку: {proxy_str[:50]}... — {result.get('error', '')}")
        
        await asyncio.gather(*[check_one(p) for p in self.proxies], return_exceptions=True)
        
        valid = [p for p in self.proxies if self._ensure_metrics(p)["last_check_ok"]]
        latencies = [self.metrics[p]["latency_ms"] for p in valid if self.metrics[p].get("latency_ms") is not None]
        avg_lat = sum(latencies) / len(latencies) if latencies else 0
        logger.info(
            f"Валидация прокси: {len(valid)}/{len(self.proxies)} рабочих, "
            f"средняя латентность {avg_lat:.0f} мс"
        )
        if len(valid) < len(self.proxies):
            dead = len(self.proxies) - len(valid)
            logger.warning(f"Прокси не прошли проверку: {dead} шт.")
    
    def get_best_proxy(
        self,
        max_latency_ms: float = MAX_PROXY_LATENCY_MS,
        require_validated: bool = True,
    ) -> Optional[str]:
        """Возвращает прокси с лучшими метриками (валидный и с латентностью не выше порога). Fallback — случайный."""
        if not self.proxies:
            return None
        if require_validated:
            candidates = [
                p for p in self.proxies
                if self._ensure_metrics(p)["last_check_ok"]
                and (self.metrics[p].get("latency_ms") is None or self.metrics[p]["latency_ms"] <= max_latency_ms)
            ]
        else:
            candidates = self.proxies
        if not candidates:
            logger.warning("Нет подходящих прокси по метрикам, используем случайный из всех.")
            return self.get_random_proxy()
        # Взвешенный случайный: предпочитаем меньшую латентность и больший success_count
        weights = []
        for p in candidates:
            m = self._ensure_metrics(p)
            lat = m.get("latency_ms") or 5000
            success = m.get("success_count", 0) + 1
            fail = m.get("fail_count", 0) + 1
            weight = success / fail / (1 + lat / 1000)
            weights.append(max(weight, 0.01))
        return random.choices(candidates, weights=weights, k=1)[0]
    
    def record_success(self, proxy_str: Optional[str]) -> None:
        """Учитывает успешный запрос через прокси."""
        if proxy_str and proxy_str in self.metrics:
            self.metrics[proxy_str]["success_count"] = self.metrics[proxy_str].get("success_count", 0) + 1
    
    def record_failure(self, proxy_str: Optional[str]) -> None:
        """Учитывает неудачный запрос через прокси."""
        if proxy_str and proxy_str in self.metrics:
            self.metrics[proxy_str]["fail_count"] = self.metrics[proxy_str].get("fail_count", 0) + 1
    
    def get_user_agent_for_proxy(self, proxy_str: Optional[str]) -> str:
        """Возвращает уникальный User-Agent для данного прокси (стабильный между вызовами). Без загрузки из сети."""
        if not proxy_str:
            return random.choice(STATIC_USER_AGENTS)
        if proxy_str not in self._proxy_user_agents:
            self._proxy_user_agents[proxy_str] = random.choice(STATIC_USER_AGENTS)
        return self._proxy_user_agents[proxy_str]
    
    def _is_proxy_alive(self, proxy_str: str) -> bool:
        """Прокси считаем мёртвым, если много провалов и провалов больше, чем успехов."""
        m = self._ensure_metrics(proxy_str)
        fail_count = m.get("fail_count", 0)
        success_count = m.get("success_count", 0)
        if fail_count > 3 and fail_count > success_count:
            return False
        return True
    
    def get_alive_proxies(self) -> List[str]:
        """Список прокси, исключая мёртвые (по метрикам fail/success)."""
        return [p for p in self.proxies if self._is_proxy_alive(p)]
    
    def get_proxy_for_worker(self, worker_index: int, total_workers: int = MAX_CONCURRENT) -> Optional[str]:
        """
        Прокси для потока: 0 — без прокси (основной адрес), 1..N-1 — по кругу из живых.
        Исключаются мёртвые прокси.
        """
        if WORKER_0_DIRECT and worker_index == 0:
            return None
        alive = self.get_alive_proxies()
        if not alive:
            return None
        return alive[(worker_index - 1) % len(alive)]


class BookingParser:
    """Парсер для извлечения данных с Booking.com"""
    
    def __init__(self, proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager
        self.current_concurrent = INITIAL_CONCURRENT
        self.current_delay = INITIAL_DELAY
        self.semaphore = asyncio.Semaphore(self.current_concurrent)
        self.results_file = 'results.csv'
        self.errors_file = 'errors.txt'
        self.timing_file = 'timing.csv'  # url, duration_sec, success, connection, worker_slot — для анализа скорости
        self.csv_lock = asyncio.Lock()
        self._init_csv()
        self._init_timing_csv()
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.recent_results = []  # Последние результаты для адаптации
        self.adaptation_lock = asyncio.Lock()
    
    def _init_csv(self):
        """Инициализирует CSV файл"""
        try:
            with open(self.results_file, 'x', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['url', 'latitude', 'longitude', 'rating'])
        except FileExistsError:
            pass
    
    def _init_timing_csv(self):
        """Инициализирует CSV для метрик скорости (когда парсер быстрее/медленнее)."""
        try:
            with open(self.timing_file, 'x', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['url', 'duration_sec', 'success', 'connection', 'worker_slot'])
        except FileExistsError:
            pass
    
    async def save_timing(self, url: str, duration_sec: float, success: bool, connection: str, worker_slot: int):
        """Пишет строку в timing.csv для анализа скорости (прямой/прокси, слот)."""
        async with self.csv_lock:
            try:
                output = io.StringIO()
                w = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
                w.writerow([url, f"{duration_sec:.2f}", success, connection, worker_slot])
                async with aiofiles.open(self.timing_file, 'a', encoding='utf-8', newline='') as f:
                    await f.write(output.getvalue())
            except Exception:
                pass
    
    def parse_coordinates(self, html: str, soup: BeautifulSoup, url: str) -> Optional[Tuple[float, float]]:
        """Парсит координаты отеля из HTML"""
        
        # Метод 1: data-atlas-latlng
        try:
            elements = soup.find_all(attrs={'data-atlas-latlng': True})
            for elem in elements:
                latlng_str = elem.get('data-atlas-latlng', '').strip()
                if latlng_str:
                    try:
                        parts = latlng_str.split(',')
                        if len(parts) == 2:
                            lat, lng = float(parts[0].strip()), float(parts[1].strip())
                            if -90 <= lat <= 90 and -180 <= lng <= 180:
                                logger.info(f"✓ Координаты (data-atlas-latlng): {lat}, {lng}")
                                return (lat, lng)
                    except (ValueError, IndexError):
                        continue
        except Exception:
            pass
        
        # Метод 2: JSON-LD
        try:
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                if script.string:
                    try:
                        data = json.loads(script.string)
                        def find_coords(obj):
                            if isinstance(obj, dict):
                                if 'geo' in obj:
                                    geo = obj['geo']
                                    if isinstance(geo, dict):
                                        if 'latitude' in geo and 'longitude' in geo:
                                            try:
                                                lat = float(geo['latitude'])
                                                lng = float(geo['longitude'])
                                                if -90 <= lat <= 90 and -180 <= lng <= 180:
                                                    return (lat, lng)
                                            except (ValueError, TypeError):
                                                pass
                                for value in obj.values():
                                    result = find_coords(value)
                                    if result:
                                        return result
                            elif isinstance(obj, list):
                                for item in obj:
                                    result = find_coords(item)
                                    if result:
                                        return result
                            return None
                        
                        coords = find_coords(data)
                        if coords:
                            logger.info(f"✓ Координаты (JSON-LD): {coords[0]}, {coords[1]}")
                            return coords
                    except (json.JSONDecodeError, Exception):
                        continue
        except Exception:
            pass
        
        # Метод 3: JavaScript паттерны
        try:
            patterns = [
                r'["\']?latitude["\']?\s*[:=]\s*([-\d.]+).*?["\']?longitude["\']?\s*[:=]\s*([-\d.]+)',
                r'["\']?longitude["\']?\s*[:=]\s*([-\d.]+).*?["\']?latitude["\']?\s*[:=]\s*([-\d.]+)',
                r'\[([-\d.]+)\s*,\s*([-\d.]+)\].*?(?:lat|lng|coord)',
                r'data-atlas-latlng["\']?\s*[:=]\s*["\']?([-\d.]+),([-\d.]+)',
            ]
            
            for pattern in patterns:
                matches = re.finditer(pattern, html, re.I | re.DOTALL)
                for match in matches:
                    try:
                        if len(match.groups()) >= 2:
                            lat = float(match.group(1))
                            lng = float(match.group(2))
                            if -90 <= lat <= 90 and -180 <= lng <= 180:
                                logger.info(f"✓ Координаты (JS): {lat}, {lng}")
                                return (lat, lng)
                    except (ValueError, IndexError):
                        continue
        except Exception:
            pass
        
        # Метод 4: Ссылки на карты
        try:
            map_links = soup.find_all(['a', 'img'], href=re.compile(r'maps|google|openstreetmap'), limit=10)
            map_links.extend(soup.find_all('img', src=re.compile(r'maps|google'), limit=10))
            
            for link in map_links:
                href = link.get('href', '') or link.get('src', '')
                coords_match = re.search(r'[?&](?:q|ll|center|markers)=([-\d.]+),([-\d.]+)', href)
                if coords_match:
                    try:
                        lat = float(coords_match.group(1))
                        lng = float(coords_match.group(2))
                        if -90 <= lat <= 90 and -180 <= lng <= 180:
                            logger.info(f"✓ Координаты (map): {lat}, {lng}")
                            return (lat, lng)
                    except (ValueError, IndexError):
                        continue
        except Exception:
            pass
        
        logger.warning(f"✗ Координаты не найдены для {url}")
        return None
    
    def parse_rating(self, soup: BeautifulSoup, html: str) -> Optional[str]:
        """Парсит оценку отеля - ищем основную оценку, а не промежуточные"""
        
        logger.debug("Начинаем парсинг оценки из HTML")
        
        # Метод 0 (ВЫСШИЙ ПРИОРИТЕТ): Ищем атрибут data-review-score - самый надежный способ
        # Это прямой атрибут с оценкой, например: data-review-score="9.7"
        try:
            # Ищем все элементы с атрибутом data-review-score
            rating_elements = soup.find_all(attrs={'data-review-score': True})
            if rating_elements:
                ratings = []
                for elem in rating_elements:
                    try:
                        score_attr = elem.get('data-review-score')
                        if score_attr:
                            try:
                                r = float(score_attr.replace(',', '.'))
                                if 0 <= r <= 10:
                                    ratings.append(r)
                            except ValueError:
                                continue
                    except Exception:
                        continue
                
                if ratings:
                    # Фильтруем только валидные оценки
                    valid_ratings = [r for r in ratings if is_valid_rating(r)]
                    if valid_ratings:
                        # Берем максимальное значение (основная оценка обычно самая высокая)
                        rating = max(valid_ratings)
                        logger.info(f"✓ Оценка (data-review-score): {rating} из {valid_ratings} (всего найдено: {ratings})")
                        return str(rating)
        except Exception:
            pass
        
        # Метод 1: Основная оценка через data-testid="review-score-component"
        # Это ОСНОВНОЙ блок с общей оценкой отеля (например, "Scored 9.6")
        try:
            # Ищем именно основной блок с оценкой
            main_rating = soup.find(attrs={'data-testid': 'review-score-component'})
            
            if main_rating:
                # Получаем весь текст блока
                text = main_rating.get_text(strip=True)
                # Ищем паттерн "Scored X.X" или просто число
                matches = re.findall(r'(?:Scored\s+)?([1-9]|10)(?:[.,]\d+)?', text, re.I)
                
                # Если не нашли, ищем в элементе с aria-hidden="true" внутри блока
                if not matches:
                    hidden_elem = main_rating.find(attrs={'aria-hidden': 'true'})
                    if hidden_elem:
                        hidden_text = hidden_elem.get_text(strip=True)
                        matches = re.findall(r'\b([1-9]|10)(?:[.,]\d+)?\b', hidden_text)
                
                if matches:
                    ratings = []
                    for match in matches:
                        try:
                            rating = float(match.replace(',', '.'))
                            if 0 <= rating <= 10:
                                ratings.append(rating)
                        except ValueError:
                            continue
                    if ratings:
                        # Фильтруем только валидные оценки
                        valid_ratings = [r for r in ratings if is_valid_rating(r)]
                        if valid_ratings:
                            # Берем максимальное значение (основная оценка)
                            rating = max(valid_ratings)
                            logger.info(f"✓ Оценка (review-score-component): {rating} из {valid_ratings} (всего найдено: {ratings})")
                            return str(rating)
        except Exception:
            pass
        
        # Метод 1.1: Альтернативный основной блок
        try:
            main_rating = soup.find(attrs={'data-testid': 'review-score-right-component'})
            if main_rating:
                text = main_rating.get_text(strip=True)
                matches = re.findall(r'\b([1-9]|10)(?:[.,]\d+)?\b', text)
                if matches:
                    ratings = []
                    for match in matches:
                        try:
                            rating = float(match.replace(',', '.'))
                            if 0 <= rating <= 10:
                                ratings.append(rating)
                        except ValueError:
                            continue
                    if ratings:
                        # Фильтруем только валидные оценки
                        valid_ratings = [r for r in ratings if is_valid_rating(r)]
                        if valid_ratings:
                            rating = max(valid_ratings)
                            logger.info(f"✓ Оценка (review-score-right-component): {rating} из {valid_ratings} (всего найдено: {ratings})")
                            return str(rating)
        except Exception:
            pass
        
        # Метод 2: Ищем в основном блоке оценки (hp_review_score - это основной класс)
        try:
            # Ищем класс hp_review_score - это ОСНОВНАЯ оценка отеля
            main_score = soup.find(class_=re.compile(r'hp_review_score|review-score-right', re.I))
            if main_score:
                text = main_score.get_text(strip=True)
                matches = re.findall(r'\b([1-9]|10)(?:[.,]\d+)?\b', text)
                if matches:
                    ratings = [float(m.replace(',', '.')) for m in matches if 0 <= float(m.replace(',', '.')) <= 10]
                    if ratings:
                        # Фильтруем только валидные оценки
                        valid_ratings = [r for r in ratings if is_valid_rating(r)]
                        if valid_ratings:
                            rating = max(valid_ratings)  # Берем максимальное
                            logger.info(f"✓ Оценка (hp_review_score): {rating} из {valid_ratings} (всего найдено: {ratings})")
                            return str(rating)
        except Exception:
            pass
        
        # Метод 2.1: Ищем в других элементах с оценками, но исключаем категории
        try:
            # Ищем элементы с классом, содержащим "score" и "badge", но НЕ категории
            score_elements = soup.find_all(class_=re.compile(r'(?:review-)?score.*badge|badge.*score', re.I))
            # Исключаем элементы с категориями (cleanliness, comfort и т.д.)
            score_elements = [e for e in score_elements if not re.search(r'category|cleanliness|comfort|staff|facilities|value', str(e.get('class', [])), re.I)]
            
            scored_elements = []
            for elem in score_elements:
                text = elem.get_text(strip=True)
                matches = re.findall(r'\b([1-9]|10)(?:[.,]\d+)?\b', text)
                for match in matches:
                    try:
                        rating = float(match.replace(',', '.'))
                        if 0 <= rating <= 10:
                            # Приоритет большим значениям и большим элементам
                            scored_elements.append((rating, len(text), rating))  # Третий параметр для сортировки по значению
                    except ValueError:
                        continue
            
            if scored_elements:
                # Фильтруем только валидные оценки
                valid_scored = [(r, text_len, r_val) for r, text_len, r_val in scored_elements if is_valid_rating(r)]
                if valid_scored:
                    # Сортируем: сначала по значению оценки (максимальное), потом по размеру
                    valid_scored.sort(key=lambda x: (x[2], x[1]), reverse=True)
                    rating = valid_scored[0][0]
                    logger.info(f"✓ Оценка (scored element): {rating} из {[r[0] for r in valid_scored]} (всего найдено: {[r[0] for r in scored_elements]})")
                    return str(rating)
        except Exception:
            pass
        
        # Метод 3: Поиск в JavaScript - ищем основную оценку отеля (УЖЕСТОЧЕННЫЙ)
        # Ищем только в специфических JS объектах с контекстной проверкой
        try:
            # Ищем только в объектах, связанных с отелем и отзывами
            # Более строгие паттерны с контекстной проверкой
            patterns = [
                # Ищем в объектах hotelData, hotelInfo, reviewData
                r'(?:hotelData|hotelInfo|reviewData|bootstrapHotelData)[^}]*?(?:overallScore|reviewScore|hotelScore|score)\s*[:=]\s*([2-9]|10)(?:[.,]\d+)?',
                # Ищем в JSON структурах с контекстом "rating" или "score"
                r'["\'](?:overall|total|main|general)?(?:rating|score)["\']\s*:\s*([2-9]|10)(?:[.,]\d+)?',
                # Ищем в структурах типа {score: X.X} в контексте hotel/review
                r'(?:hotel|review|property)[^}]*?score\s*[:=]\s*([2-9]|10)(?:[.,]\d+)?',
            ]
            found_ratings = []
            for pattern in patterns:
                matches = re.finditer(pattern, html, re.I)
                for match in matches:
                    try:
                        rating_str = match.group(1).replace(',', '.')
                        rating = float(rating_str)
                        # Используем валидацию
                        if is_valid_rating(rating):
                            found_ratings.append(rating)
                    except (ValueError, IndexError):
                        continue
            
            if found_ratings:
                # Берем максимальную оценку (обычно это основная)
                rating = max(found_ratings)
                logger.info(f"✓ Оценка (JS строгий): {rating} из {found_ratings}")
                return str(rating)
        except Exception:
            pass
        
        # Метод 4: Fallback - ищем в контексте "overall", "total", "rating" (УЛУЧШЕННЫЙ)
        try:
            # Более строгие паттерны с проверкой контекста
            # Ищем только в контексте, явно указывающем на оценку отеля
            context_patterns = [
                # "Overall rating: X.X" или "Overall score: X.X"
                r'(?:overall|total|general|main)\s+(?:rating|score)\s*[:=]\s*([2-9]|10)(?:[.,]\d+)?',
                # "X.X out of 10" или "X.X/10" - только если есть явное указание на 10
                r'([2-9]|10)(?:[.,]\d+)?\s*(?:out\s+of|/)\s*10\b',
                # "Rating: X.X" в контексте review/hotel
                r'(?:review|hotel|property)[^:]*rating\s*[:=]\s*([2-9]|10)(?:[.,]\d+)?',
                # "Scored X.X" - явный индикатор оценки
                r'scored\s+([2-9]|10)(?:[.,]\d+)?',
            ]
            
            found_ratings = []
            for pattern in context_patterns:
                matches = re.finditer(pattern, html, re.I)
                for match in matches:
                    try:
                        rating = float(match.group(1).replace(',', '.'))
                        # Используем валидацию
                        if is_valid_rating(rating):
                            found_ratings.append(rating)
                    except (ValueError, IndexError):
                        continue
            
            if found_ratings:
                rating = max(found_ratings)
                logger.info(f"✓ Оценка (context строгий): {rating} из {found_ratings}")
                return str(rating)
        except Exception:
            pass
        
        # Метод 5 удален - слишком ненадежен, может захватывать неправильные значения
        # Лучше вернуть None, если надежные методы не нашли оценку
        
        logger.debug("Оценка не найдена надежными методами парсинга HTML")
        return None
    
    async def update_adaptation(self, success: bool):
        """Обновляет статистику и адаптирует скорость"""
        async with self.adaptation_lock:
            self.recent_results.append(success)
            if success:
                self.success_count += 1
            else:
                self.error_count += 1
            
            # Ограничиваем размер окна
            if len(self.recent_results) > ADAPTATION_WINDOW:
                old_result = self.recent_results.pop(0)
                if old_result:
                    self.success_count -= 1
                else:
                    self.error_count -= 1
            
            # Адаптируем каждые N запросов
            if len(self.recent_results) >= ADAPTATION_WINDOW:
                success_rate = self.success_count / len(self.recent_results)
                error_rate = self.error_count / len(self.recent_results)
                
                old_concurrent = self.current_concurrent
                old_delay = self.current_delay
                
                # Если много ошибок - снижаем скорость
                if error_rate > ERROR_THRESHOLD:
                    self.current_concurrent = max(MIN_CONCURRENT, self.current_concurrent - 1)
                    self.current_delay = (
                        min(MAX_DELAY[0], self.current_delay[0] + 0.5),
                        min(MAX_DELAY[1], self.current_delay[1] + 0.5)
                    )
                    logger.info(f"📉 Снижаем скорость: ошибок {error_rate:.1%} | concurrent: {old_concurrent}→{self.current_concurrent}, delay: {old_delay}→{self.current_delay}")
                # Если все хорошо - увеличиваем скорость
                elif success_rate > SUCCESS_THRESHOLD and self.current_concurrent < MAX_CONCURRENT:
                    self.current_concurrent = min(MAX_CONCURRENT, self.current_concurrent + 1)
                    self.current_delay = (
                        max(MIN_DELAY[0], self.current_delay[0] - 0.2),
                        max(MIN_DELAY[1], self.current_delay[1] - 0.2)
                    )
                    logger.info(f"📈 Увеличиваем скорость: успех {success_rate:.1%} | concurrent: {old_concurrent}→{self.current_concurrent}, delay: {old_delay}→{self.current_delay}")
                
                # Обновляем семафор
                if self.current_concurrent != old_concurrent:
                    self.semaphore = asyncio.Semaphore(self.current_concurrent)
    
    async def save_result(self, url: str, latitude: Optional[float], 
                         longitude: Optional[float], rating: Optional[str]):
        """Сохраняет результат в CSV"""
        async with self.csv_lock:
            output = io.StringIO()
            writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
            lat_str = str(latitude) if latitude is not None else ''
            lng_str = str(longitude) if longitude is not None else ''
            rating_str = rating if rating else 'No rating'
            writer.writerow([url, lat_str, lng_str, rating_str])
            
            try:
                async with aiofiles.open(self.results_file, 'a', encoding='utf-8', newline='') as f:
                    await f.write(output.getvalue())
                    await f.flush()
                self.processed_count += 1
                if self.processed_count % 5 == 0:
                    logger.info(f"Обработано: {self.processed_count} URL")
            except Exception as e:
                logger.error(f"Ошибка записи в CSV: {e}")
    
    async def save_error(self, url: str):
        """Сохраняет URL с ошибкой"""
        async with aiofiles.open(self.errors_file, 'a', encoding='utf-8') as f:
            await f.write(f"{url}\n")
            await f.flush()
    
    async def fetch_and_parse(self, context: BrowserContext, url: str) -> Tuple[bool, Optional[float], Optional[float], Optional[str]]:
        """Выполняет запрос через Playwright и парсит данные"""
        await asyncio.sleep(random.uniform(*self.current_delay))
        
        page = None
        for attempt in range(MAX_RETRIES):
            try:
                # Создаем НОВУЮ страницу для каждого URL
                try:
                    page = await context.new_page()
                except Exception as e:
                    # Контекст/браузер закрыт (краш Chromium или закрытие) — повторные попытки бесполезны
                    if 'Target' in type(e).__name__ and 'closed' in str(e).lower():
                        logger.warning(f"Контекст/браузер закрыт для {url.split('/')[-1]}: {type(e).__name__}")
                        return (False, None, None, None)
                    raise
                
                # Заголовки (User-Agent задаётся контекстом — уникальный на прокси)
                await page.set_extra_http_headers({
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': 'https://www.booking.com/',
                })
                
                # Переходим на страницу
                try:
                    response = await page.goto(url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
                except Exception as goto_error:
                    error_msg = str(goto_error)
                    # Игнорируем ERR_ABORTED, если это просто редирект или отмена
                    if 'ERR_ABORTED' in error_msg and attempt < MAX_RETRIES - 1:
                        logger.debug(f"ERR_ABORTED для {url}, попытка {attempt + 1}, продолжаем...")
                        if page:
                            await page.close()
                        await asyncio.sleep(random.uniform(2, 4))
                        continue
                    logger.warning(f"Ошибка goto для {url}, попытка {attempt + 1}: {type(goto_error).__name__}: {error_msg[:100]}")
                    if page:
                        await page.close()
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(random.uniform(3, 6))
                        continue
                    return (False, None, None, None)
                
                if not response or response.status not in (200, 202):
                    logger.warning(f"Статус {response.status if response else 'None'} для {url}, попытка {attempt + 1}")
                    if page:
                        await page.close()
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(random.uniform(3, 6))
                        continue
                    return (False, None, None, None)
                
                # Проверяем, что мы на правильном URL (после загрузки)
                await asyncio.sleep(1)  # Даем время на редиректы
                current_url = page.url
                # Нормализуем URL для сравнения (убираем параметры и якоря)
                url_base = url.split('?')[0].split('#')[0].lower()
                current_url_base = current_url.split('?')[0].split('#')[0].lower()
                
                # Проверяем совпадение базового URL
                if url_base not in current_url_base and current_url_base not in url_base:
                    # Иногда Booking.com делает редиректы, проверяем, что это все еще страница отеля
                    if 'booking.com/hotel' not in current_url_base:
                        logger.warning(f"Редирект на не-отель! Ожидали: {url}, получили: {current_url}")
                        if page:
                            await page.close()
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(random.uniform(3, 6))
                            continue
                        return (False, None, None, None)
                    # Если это все еще страница отеля, продолжаем
                    logger.debug(f"URL изменился, но это все еще отель: {current_url}")
                
                # Ждем загрузки контента
                await asyncio.sleep(3)
                
                # Ждем появления ключевых элементов
                try:
                    await page.wait_for_selector('body', timeout=10000)
                    # Ждем появления элементов с координатами или оценкой
                    try:
                        await page.wait_for_selector('[data-atlas-latlng], script[type="application/ld+json"]', timeout=5000)
                    except:
                        pass  # Не критично, если не найдено
                except Exception as e:
                    logger.debug(f"Ошибка ожидания селекторов: {e}")
                
                # Пытаемся получить оценку напрямую через Playwright (более точно)
                rating_from_dom = None
                try:
                    logger.debug("Начинаем парсинг оценки из DOM")
                    # ПРИОРИТЕТ 0 (ВЫСШИЙ): Ищем атрибут data-review-score - самый надежный способ
                    # Это прямой атрибут с оценкой, например: data-review-score="9.7"
                    try:
                        # Ищем все элементы с атрибутом data-review-score
                        rating_elements = await page.query_selector_all('[data-review-score]')
                        if rating_elements:
                            ratings = []
                            for elem in rating_elements:
                                try:
                                    # Получаем значение атрибута data-review-score
                                    score_attr = await elem.get_attribute('data-review-score')
                                    if score_attr:
                                        try:
                                            r = float(score_attr.replace(',', '.'))
                                            if 0 <= r <= 10:
                                                ratings.append(r)
                                        except ValueError:
                                            continue
                                except Exception:
                                    continue
                            
                            if ratings:
                                # Фильтруем только валидные оценки
                                valid_ratings = [r for r in ratings if is_valid_rating(r)]
                                if valid_ratings:
                                    # Берем максимальное значение (основная оценка обычно самая высокая)
                                    rating_from_dom = str(max(valid_ratings))
                                    logger.info(f"✓ Оценка из DOM (data-review-score): {rating_from_dom} из {valid_ratings} (всего найдено: {ratings})")
                    except Exception as e:
                        logger.debug(f"Ошибка поиска data-review-score: {e}")
                    
                    # ПРИОРИТЕТ 1: Ищем основной блок с оценкой - data-testid="review-score-component"
                    # Это основной блок с общей оценкой отеля (например, "Scored 9.6")
                    if not rating_from_dom:
                        try:
                            main_rating_element = await page.query_selector('[data-testid="review-score-component"]')
                            if main_rating_element:
                                # Получаем весь текст блока
                                rating_text = await main_rating_element.inner_text()
                                # Ищем паттерн "Scored X.X" или просто число
                                # Также ищем в элементе с aria-hidden="true" внутри этого блока
                                matches = re.findall(r'(?:Scored\s+)?([1-9]|10)(?:[.,]\d+)?', rating_text, re.I)
                                if not matches:
                                    # Пробуем найти элемент с aria-hidden="true" внутри
                                    hidden_element = await main_rating_element.query_selector('[aria-hidden="true"]')
                                    if hidden_element:
                                        hidden_text = await hidden_element.inner_text()
                                        matches = re.findall(r'\b([1-9]|10)(?:[.,]\d+)?\b', hidden_text)
                                
                                if matches:
                                    ratings = []
                                    for m in matches:
                                        try:
                                            r = float(m.replace(',', '.'))
                                            if 0 <= r <= 10:
                                                ratings.append(r)
                                        except ValueError:
                                            continue
                                    if ratings:
                                        # Фильтруем только валидные оценки
                                        valid_ratings = [r for r in ratings if is_valid_rating(r)]
                                        if valid_ratings:
                                            # Берем максимальное значение (основная оценка)
                                            rating_from_dom = str(max(valid_ratings))
                                            logger.info(f"✓ Оценка из DOM (review-score-component): {rating_from_dom} из {valid_ratings} (всего найдено: {ratings})")
                        except Exception as e:
                            logger.debug(f"Ошибка поиска review-score-component: {e}")
                    
                    # ПРИОРИТЕТ 2: Если не нашли в основном блоке, ищем в других местах
                    if not rating_from_dom:
                        main_selectors = [
                            '[data-testid="review-score-right-component"]',  # Альтернативный основной блок
                            '.hp_review_score',  # Класс основной оценки
                        ]
                        
                        for selector in main_selectors:
                            try:
                                rating_element = await page.query_selector(selector)
                                if rating_element:
                                    rating_text = await rating_element.inner_text()
                                    matches = re.findall(r'\b([1-9]|10)(?:[.,]\d+)?\b', rating_text)
                                    if matches:
                                        ratings = []
                                        for m in matches:
                                            try:
                                                r = float(m.replace(',', '.'))
                                                if 0 <= r <= 10:
                                                    ratings.append(r)
                                            except ValueError:
                                                continue
                                        if ratings:
                                            # Фильтруем только валидные оценки
                                            valid_ratings = [r for r in ratings if is_valid_rating(r)]
                                            if valid_ratings:
                                                rating_from_dom = str(max(valid_ratings))
                                                logger.info(f"✓ Оценка из DOM ({selector}): {rating_from_dom} из {valid_ratings} (всего найдено: {ratings})")
                                                break
                            except Exception:
                                continue
                    
                    # ПРИОРИТЕТ 3: Fallback - ищем во всех элементах с review-score (УЛУЧШЕННЫЙ)
                    # Исключаем категории оценок, ищем только основную оценку
                    if not rating_from_dom:
                        try:
                            all_score_elements = await page.query_selector_all('[data-testid*="review-score"]')
                            all_ratings = []
                            for elem in all_score_elements:
                                try:
                                    # Получаем data-testid для проверки категории
                                    test_id = await elem.get_attribute('data-testid') or ''
                                    
                                    # Исключаем элементы с категориями (cleanliness, comfort, staff, facilities, value)
                                    if any(cat in test_id.lower() for cat in ['cleanliness', 'comfort', 'staff', 'facilities', 'value', 'location', 'wifi']):
                                        continue
                                    
                                    # Проверяем наличие текста "Overall" или "Scored" для подтверждения основной оценки
                                    text = await elem.inner_text()
                                    text_lower = text.lower()
                                    
                                    # Пропускаем элементы, которые явно не являются основной оценкой
                                    if any(indicator in text_lower for indicator in ['cleanliness', 'comfort', 'staff', 'facilities', 'value', 'location', 'wifi']):
                                        continue
                                    
                                    matches = re.findall(r'\b([1-9]|10)(?:[.,]\d+)?\b', text)
                                    for m in matches:
                                        try:
                                            r = float(m.replace(',', '.'))
                                            # Используем валидацию
                                            if is_valid_rating(r):
                                                # Приоритет элементам с текстом "Overall", "Scored", "Rating"
                                                priority = 1 if any(indicator in text_lower for indicator in ['overall', 'scored', 'rating', 'score']) else 0
                                                all_ratings.append((r, priority))
                                        except ValueError:
                                            continue
                                except Exception:
                                    continue
                            
                            if all_ratings:
                                # Сортируем: сначала по приоритету (элементы с "Overall"/"Scored"), потом по значению
                                all_ratings.sort(key=lambda x: (x[1], x[0]), reverse=True)
                                rating_from_dom = str(all_ratings[0][0])
                                logger.info(f"✓ Оценка из DOM (fallback улучшенный): {rating_from_dom} из {[r[0] for r in all_ratings[:5]]} (всего найдено: {len(all_ratings)})")
                        except Exception:
                            pass
                            
                except Exception as e:
                    logger.debug(f"Не удалось получить оценку из DOM: {e}")
                
                # Получаем HTML после выполнения JavaScript
                html = await page.content()
                
                # Закрываем страницу сразу после получения HTML
                await page.close()
                page = None
                
                if not html or len(html) < 500:
                    logger.warning(f"Слишком короткий HTML ({len(html)} символов) для {url}")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(random.uniform(3, 6))
                        continue
                    return (False, None, None, None)
                
                soup = BeautifulSoup(html, 'lxml')
                
                # Парсим данные
                coords = self.parse_coordinates(html, soup, url)
                latitude, longitude = coords if coords else (None, None)
                
                # Используем оценку из DOM, если получили, иначе парсим из HTML
                if rating_from_dom:
                    rating = rating_from_dom
                else:
                    rating = self.parse_rating(soup, html)
                
                success = latitude is not None and longitude is not None
                
                # Дополнительная проверка: убеждаемся, что координаты разумные для Италии
                if latitude and longitude:
                    # Италия примерно: lat 36-47, lng 6-19
                    if not (35 <= latitude <= 48 and 5 <= longitude <= 20):
                        logger.warning(f"Координаты выходят за пределы Италии: lat={latitude}, lng={longitude} для {url}")
                        # Все равно используем, но логируем
                
                if success:
                    logger.info(f"✓ Успешно: {url.split('/')[-1]} | lat={latitude}, lng={longitude}, rating={rating}")
                else:
                    logger.warning(f"⚠ Частично: {url.split('/')[-1]} | lat={latitude}, lng={longitude}, rating={rating}")
                
                return (True, latitude, longitude, rating)
            
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                logger.warning(f"Ошибка для {url.split('/')[-1]}, попытка {attempt + 1}: {error_type}: {error_msg[:150]}")
                
                if page:
                    try:
                        await page.close()
                    except Exception:
                        pass
                    page = None
                
                # Контекст/браузер закрыт — повторные попытки бесполезны
                if 'Target' in error_type and 'closed' in error_msg.lower():
                    logger.warning(f"Контекст/браузер закрыт, пропуск {url.split('/')[-1]}")
                    return (False, None, None, None)
                # Для TimeoutError увеличиваем задержку
                if 'Timeout' in error_type and attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(random.uniform(5, 8))
                    continue
                elif attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(random.uniform(3, 6))
                    continue
        
        # Закрываем страницу, если она еще открыта
        if page:
            try:
                await page.close()
            except:
                pass
        
        return (False, None, None, None)
    
    async def process_url(
        self,
        context: BrowserContext,
        url: str,
        proxy_str: Optional[str] = None,
        worker_slot: int = 0,
    ):
        """Обрабатывает один URL. proxy_str — учёт успехов/провалов; worker_slot — для timing и round-robin."""
        async with self.semaphore:
            t0 = time.perf_counter()
            success, latitude, longitude, rating = await self.fetch_and_parse(context, url)
            duration_sec = time.perf_counter() - t0
            
            connection = "direct" if proxy_str is None else "proxy"
            await self.save_timing(url, duration_sec, success, connection, worker_slot)
            
            await self.update_adaptation(success)
            
            if success:
                await self.save_result(url, latitude, longitude, rating)
                self.proxy_manager.record_success(proxy_str)
            else:
                await self.save_error(url)
                self.proxy_manager.record_failure(proxy_str)
    
    async def run(self, urls_file: str = 'urls.txt'):
        """Основной метод запуска"""
        logger.info("Старт парсера...")
        try:
            with open(urls_file, 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f if line.strip()]
            logger.info(f"Загружено {len(urls)} URL-адресов")
        except FileNotFoundError:
            logger.error(f"Файл {urls_file} не найден!")
            return
        
        logger.info("Запуск Playwright...")
        from playwright.async_api import async_playwright, Browser, BrowserContext, Page
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            logger.info("Браузер запущен.")
            
            try:
                # Валидация прокси перед запуском (если есть прокси); с общим таймаутом, чтобы не висеть
                if self.proxy_manager.proxies:
                    logger.info(f"Валидация {len(self.proxy_manager.proxies)} прокси (макс. {PROXY_VALIDATE_TOTAL_TIMEOUT} сек)...")
                    try:
                        await asyncio.wait_for(
                            self.proxy_manager.validate_proxies(
                                timeout=PROXY_CHECK_TIMEOUT,
                                max_concurrent=10,
                            ),
                            timeout=PROXY_VALIDATE_TOTAL_TIMEOUT,
                        )
                        logger.info("Валидация прокси завершена.")
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"Валидация прокси не завершилась за {PROXY_VALIDATE_TOTAL_TIMEOUT} сек, "
                            "продолжаем без обновления метрик (будут использованы старые или случайный прокси)."
                        )
                
                # 5 потоков: слот 0 — основной адрес (без прокси), слоты 1–4 — прокси по кругу (исключая мёртвые)
                batch_size = MAX_CONCURRENT
                num_workers = batch_size
                for batch_start in range(0, len(urls), batch_size):
                    batch = urls[batch_start:batch_start + batch_size]
                    batch_num = batch_start // batch_size + 1
                    total_batches = (len(urls) - 1) // batch_size + 1
                    logger.info(f"Обработка батча {batch_num}/{total_batches} ({len(batch)} URL)")
                    
                    # Для каждого слота — свой контекст (прокси или прямой)
                    contexts = []
                    try:
                        for slot in range(num_workers):
                            proxy_str = self.proxy_manager.get_proxy_for_worker(slot, num_workers)
                            proxy_dict = self.proxy_manager.format_proxy(proxy_str) if proxy_str else None
                            opts = {
                                'viewport': {'width': 1920, 'height': 1080},
                                'user_agent': self.proxy_manager.get_user_agent_for_proxy(proxy_str),
                            }
                            if proxy_dict:
                                opts['proxy'] = proxy_dict
                            ctx = await browser.new_context(**opts)
                            contexts.append((ctx, proxy_str))
                        
                        tasks = [
                            self.process_url(contexts[j][0], batch[j], contexts[j][1], j)
                            for j in range(len(batch))
                        ]
                        await asyncio.gather(*tasks, return_exceptions=True)
                    finally:
                        for ctx, _ in contexts:
                            try:
                                await ctx.close()
                            except Exception:
                                pass
                    
                    if batch_start + batch_size < len(urls):
                        delay = random.uniform(5, 10)
                        logger.info(f"Пауза {delay:.1f} сек перед следующим батчем...")
                        await asyncio.sleep(delay)
            
            finally:
                await browser.close()
        
        logger.info(f"✓ Парсинг завершен! Обработано: {self.processed_count} URL")
        logger.info(
            f"Метрики скорости: {self.timing_file} — колонки url, duration_sec, success, connection (direct|proxy), worker_slot. "
            "По ним можно сравнить, когда быстрее (прямой vs прокси, по слотам)."
        )


async def main():
    """Точка входа"""
    logger.info("Старт парсера...")
    proxy_manager = ProxyManager('proxies.txt')
    parser = BookingParser(proxy_manager)
    await parser.run('urls.txt')


if __name__ == '__main__':
    asyncio.run(main())
