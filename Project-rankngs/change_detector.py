"""
change_detector.py
==================
Мониторинг порталов-источников на появление новых приказов.

Задачи:
  1. Обход страниц-списков документов по каждому источнику
  2. Быстрая проверка: изменилась ли страница? (хеш DOM / ETag)
  3. Извлечение ссылок на документы
  4. Сравнение с уже известными приказами в БД
  5. Создание записей новых приказов (статус 'new') для оркестратора
  6. Детектирование аномалий: изменение структуры DOM (алерт)

Типы источников:
  - pdf_portal:   HTML-страница со ссылками на PDF
                  (mos.ru, mst.mosreg.ru, kfis.gov.spb.ru, minsport.krasnodar.ru)
  - json_embed:   JSON-данные встроены в JS-переменную на странице
                  (msrfinfo.ru)
  - html_table:   HTML-таблица с данными (будущий тип)

Стратегия по risk_class:
  - green:  httpx, 1–3с между запросами, ETag/If-Modified-Since
  - amber:  Playwright, 3–8с, увеличенные задержки, change detection
  - red:    только ручной импорт, алерты при обнаружении нового

Использование:
    detector = ChangeDetector(db_url="postgresql+asyncpg://...", pdf_output_dir="./pdfs")
    await detector.initialize()

    # Проверить один источник
    result = await detector.check_source("spb_kfkis")

    # Проверить все активные источники
    results = await detector.check_all()

    # Запуск периодической проверки
    await detector.run_loop(interval_minutes=60)

Зависимости:
    pip install httpx sqlalchemy[asyncio] asyncpg
    (опционально) pip install playwright  — для amber-источников
"""

import asyncio
import hashlib
import json
import logging
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urljoin, urlparse, parse_qs

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Модели данных
# ---------------------------------------------------------------------------

class CheckStatus(str, Enum):
    UNCHANGED   = "unchanged"      # Страница не изменилась (хеш совпал)
    NEW_DOCS    = "new_docs"       # Найдены новые документы
    CHANGED     = "changed"        # Страница изменилась, но новых документов нет
    ERROR       = "error"          # Ошибка при проверке
    SKIPPED     = "skipped"        # Пропущен (red/inactive/rate limit)


@dataclass
class DiscoveredDocument:
    """Один обнаруженный документ на странице источника."""
    url: str                       # URL страницы или PDF
    file_url: Optional[str] = None # Прямая ссылка на PDF (если известна)
    title: Optional[str] = None    # Заголовок ссылки/карточки
    order_number: str = ""         # Номер приказа (если удалось извлечь)
    order_date: str = ""           # Дата приказа (если удалось извлечь)
    order_type: str = "приказ"     # приказ/распоряжение


@dataclass
class CheckResult:
    """Результат проверки одного источника."""
    source_code: str
    status: CheckStatus
    duration_ms: int = 0

    # Что нашли
    page_hash: Optional[str] = None
    page_hash_changed: bool = False
    links_total: int = 0
    links_new: int = 0
    orders_created: int = 0

    # Документы
    discovered: list[DiscoveredDocument] = field(default_factory=list)
    new_documents: list[DiscoveredDocument] = field(default_factory=list)

    # Ошибка
    error: Optional[str] = None
    etag: Optional[str] = None

    def summary(self) -> str:
        if self.status == CheckStatus.NEW_DOCS:
            return (
                f"🆕 {self.source_code}: {self.links_new} новых из {self.links_total} "
                f"({self.orders_created} создано в БД), {self.duration_ms}ms"
            )
        if self.status == CheckStatus.UNCHANGED:
            return f"✅ {self.source_code}: без изменений, {self.duration_ms}ms"
        if self.status == CheckStatus.CHANGED:
            return f"🔄 {self.source_code}: страница изменилась, но новых документов нет"
        if self.status == CheckStatus.ERROR:
            return f"❌ {self.source_code}: {self.error}"
        return f"⏭️ {self.source_code}: пропущен"


# ---------------------------------------------------------------------------
# Конфигурация проверки по типам источников
# ---------------------------------------------------------------------------

# Паттерны URL для извлечения ссылок на документы
# Загружаются из source_registry (единый реестр)
try:
    from source_registry import as_detect_patterns as _load_patterns
    SOURCE_PATTERNS: dict[str, dict] = _load_patterns()
except ImportError:
    SOURCE_PATTERNS: dict[str, dict] = {
        "moskva_tstisk": {
            "list_urls": [
                "https://www.mos.ru/moskomsport/documents/prisvoenie-sportivnykh-razryadov-po-vidam-sporta/",
                "https://www.mos.ru/moskomsport/documents/prisvoenie-kvalifikatsionnykh-kategoriy-sportivnykh-sudey/",
            ],
            "link_regex": r'href=["\']([^"\']*view/\d+[^"\']*)["\']',
            "title_regex": r'>([^<]*(?:Приказ|Распоряжение)[^<]*)<',
            "order_date_regex": r'от\s+(\d{1,2}[\.\s]\d{2}[\.\s]\d{4})',
            "order_number_regex": r'[№N]\s*(\S+)',
            "method": "playwright",
            "pagination": None,
        },
        "moskva_moskumsport": {
            "list_urls": [
                "https://www.mos.ru/moskomsport/documents/prisvoenie-sportivnykh-razryadov-po-vidam-sporta/",
            ],
            "link_regex": r'href=["\']([^"\']*view/\d+[^"\']*)["\']',
            "title_regex": r'>([^<]*Распоряжение[^<]*)<',
            "method": "playwright",
            "pagination": None,
        },
        "mo_mособлспорт": {
            "list_urls": [
                "https://mst.mosreg.ru/dokumenty/prisvoenie-sportivnykh-razryadov-kandidat-v-mastera-sporta-i-pervyi-sportivnyi-razryad",
            ],
            "link_regex": r'href=["\']([^"\']*(?:rasporiaz|prikaz)[^"\']*)["\']',
            "method": "playwright",
            "pagination": "?page={n}",
            "max_pages": 3,
        },
        "spb_kfkis": {
            "list_urls": [
                "https://kfis.gov.spb.ru/docs/?type=54",
            ],
            "link_regex": r'href=["\']([^"\']*?(?:/docs/\d+|/documents/\d+)[^"\']*)["\']',
            "title_regex": r'class=["\']doc-title["\'][^>]*>([^<]+)<',
            "method": "httpx",
            "pagination": "&page={n}",
            "max_pages": 3,
        },
        "krasnodar_minsport": {
            "list_urls": [
                "https://minsport.krasnodar.ru/activities/sport/prisvoenie-sportivnyx-razryadov/",
            ],
            "link_regex": r'href=["\']([^"\']*\.pdf)["\']',
            "method": "httpx",
            "pagination": None,
        },
        "rf_minsport": {
            "list_urls": [
                "https://msrfinfo.ru/awards/",
            ],
            "method": "httpx",
            "source_type": "json_embed",
            "js_var": "$obj",
        },
    }

# User-agents (совпадают с pdf_downloader)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]


# ---------------------------------------------------------------------------
# Change Detector
# ---------------------------------------------------------------------------

class ChangeDetector:
    """
    Проверяет порталы-источники на появление новых приказов.

    Алгоритм для каждого источника:
    1. Загрузить страницу-список документов
    2. Вычислить хеш контента → сравнить с last_page_hash
       - Если не изменился и last_checked_at < 1 ч назад → UNCHANGED
    3. Извлечь ссылки на документы
    4. Сравнить с уже известными URL в orders
    5. Для новых → создать записи в orders (status='new')
    6. Обновить last_page_hash, last_etag, last_checked_at
    """

    def __init__(
        self,
        db_url: Optional[str] = None,
        pdf_output_dir: str = "./pdfs",
        playwright_headless: bool = True,
        request_timeout: float = 30.0,
    ):
        self._db_url = db_url
        self._pdf_dir = pdf_output_dir
        self._headless = playwright_headless
        self._timeout = request_timeout

        # Компоненты
        self._engine = None
        self._browser = None
        self._playwright = None
        self._initialized = False
        self._browser_sem = asyncio.Semaphore(2)  # макс. 2 Playwright сессии

    async def initialize(self):
        """Инициализирует БД и HTTP клиент."""
        # DB
        if self._db_url:
            try:
                from sqlalchemy.ext.asyncio import create_async_engine
                self._engine = create_async_engine(
                    self._db_url, pool_size=3, echo=False,
                )
                async with self._engine.begin() as conn:
                    from sqlalchemy import text
                    await conn.execute(text("SELECT 1"))
                logger.info("ChangeDetector: БД подключена")
            except Exception as e:
                logger.error(f"ChangeDetector: ошибка БД — {e}")
                self._engine = None
        else:
            logger.info("ChangeDetector: работа без БД (dry-run)")

        self._initialized = True

    async def shutdown(self):
        """Освобождает ресурсы."""
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        if self._engine:
            await self._engine.dispose()

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, *args):
        await self.shutdown()

    # ------------------------------------------------------------------
    # Публичный API
    # ------------------------------------------------------------------

    async def check_source(self, source_code: str) -> CheckResult:
        """
        Проверяет один источник на новые документы.
        """
        if not self._initialized:
            raise RuntimeError("Не инициализирован. Вызовите await initialize()")

        t0 = time.monotonic()

        # Получаем конфиг источника
        source_info = await self._get_source_info(source_code)
        pattern = SOURCE_PATTERNS.get(source_code, {})

        if not pattern:
            return CheckResult(
                source_code=source_code,
                status=CheckStatus.SKIPPED,
                error=f"Нет паттерна для источника '{source_code}'",
            )

        # Проверяем risk_class
        risk = source_info.get("risk_class", "green") if source_info else "green"
        if risk == "red":
            logger.info(f"[{source_code}] risk=red, пропускаем автоматическую проверку")
            return CheckResult(
                source_code=source_code,
                status=CheckStatus.SKIPPED,
                error="risk_class=red, только ручной импорт",
            )

        # Выполняем проверку
        try:
            result = await self._check_source_impl(source_code, pattern, source_info)
            result.duration_ms = int((time.monotonic() - t0) * 1000)
            logger.info(result.summary())
            return result
        except Exception as e:
            logger.error(f"[{source_code}] ошибка: {e}")
            return CheckResult(
                source_code=source_code,
                status=CheckStatus.ERROR,
                error=str(e),
                duration_ms=int((time.monotonic() - t0) * 1000),
            )

    async def check_all(
        self,
        source_codes: Optional[list[str]] = None,
        skip_red: bool = True,
    ) -> list[CheckResult]:
        """
        Проверяет все (или указанные) активные источники.
        Последовательно, с паузами между источниками.
        """
        if source_codes is None:
            source_codes = await self._get_active_source_codes()

        if not source_codes:
            # Fallback: все из SOURCE_PATTERNS
            source_codes = list(SOURCE_PATTERNS.keys())

        results = []
        for i, code in enumerate(source_codes):
            if i > 0:
                delay = random.uniform(2, 5)
                logger.debug(f"Пауза {delay:.1f}с перед {code}")
                await asyncio.sleep(delay)

            result = await self.check_source(code)
            results.append(result)

        # Сводка
        new = sum(r.links_new for r in results)
        created = sum(r.orders_created for r in results)
        errors = sum(1 for r in results if r.status == CheckStatus.ERROR)
        logger.info(
            f"ChangeDetector: проверено {len(results)} источников, "
            f"{new} новых ссылок, {created} создано, {errors} ошибок"
        )

        return results

    async def run_loop(
        self,
        interval_minutes: int = 60,
        source_codes: Optional[list[str]] = None,
    ):
        """
        Бесконечный цикл проверки с указанным интервалом.
        Для продакшена: запускать через supervisor/systemd.
        """
        logger.info(
            f"ChangeDetector: запуск периодической проверки "
            f"(интервал={interval_minutes}мин)"
        )
        while True:
            try:
                await self.check_all(source_codes)
            except Exception as e:
                logger.error(f"Ошибка в цикле проверки: {e}")

            logger.info(f"Следующая проверка через {interval_minutes} мин")
            await asyncio.sleep(interval_minutes * 60)

    # ------------------------------------------------------------------
    # Внутренняя логика
    # ------------------------------------------------------------------

    async def _check_source_impl(
        self,
        source_code: str,
        pattern: dict,
        source_info: Optional[dict],
    ) -> CheckResult:
        """Основная логика проверки одного источника."""
        result = CheckResult(source_code=source_code, status=CheckStatus.UNCHANGED)

        list_urls = pattern.get("list_urls", [])
        method = pattern.get("method", "httpx")
        source_type = pattern.get("source_type", "pdf_portal")

        all_discovered: list[DiscoveredDocument] = []

        for list_url in list_urls:
            # Загружаем страницу(ы) — с учётом пагинации
            pages_html = await self._fetch_list_pages(
                list_url, method, pattern
            )

            for page_url, html, etag in pages_html:
                # Вычисляем хеш
                content_hash = self._hash_content(html)

                # Извлекаем документы в зависимости от типа
                if source_type == "json_embed":
                    docs = self._extract_json_embed(html, page_url, pattern)
                else:
                    docs = self._extract_pdf_links(html, page_url, pattern)

                all_discovered.extend(docs)

                # Сохраняем хеш первой страницы
                if result.page_hash is None:
                    result.page_hash = content_hash
                    result.etag = etag

        # Дедупликация по URL
        seen_urls = set()
        unique_docs = []
        for doc in all_discovered:
            url_key = doc.file_url or doc.url
            if url_key not in seen_urls:
                seen_urls.add(url_key)
                unique_docs.append(doc)

        result.discovered = unique_docs
        result.links_total = len(unique_docs)

        # Сравниваем с БД — какие документы уже известны?
        known_urls = await self._get_known_urls(source_code)
        new_docs = []
        for doc in unique_docs:
            url_key = doc.file_url or doc.url
            if url_key not in known_urls and doc.url not in known_urls:
                new_docs.append(doc)

        result.new_documents = new_docs
        result.links_new = len(new_docs)

        # Проверяем, изменился ли хеш страницы
        old_hash = source_info.get("last_page_hash") if source_info else None
        if old_hash and result.page_hash:
            result.page_hash_changed = (old_hash != result.page_hash)

        # Определяем статус
        if new_docs:
            result.status = CheckStatus.NEW_DOCS
            # Создаём записи в БД
            result.orders_created = await self._create_orders(
                source_code, new_docs
            )
        elif result.page_hash_changed:
            result.status = CheckStatus.CHANGED
        else:
            result.status = CheckStatus.UNCHANGED

        # Обновляем last_page_hash, last_checked_at
        await self._update_source_check(
            source_code, result.page_hash, result.etag
        )

        return result

    # ------------------------------------------------------------------
    # Загрузка страниц
    # ------------------------------------------------------------------

    async def _fetch_list_pages(
        self,
        base_url: str,
        method: str,
        pattern: dict,
    ) -> list[tuple[str, str, Optional[str]]]:
        """
        Загружает одну или несколько страниц списка (с пагинацией).
        Возвращает: [(url, html, etag), ...]
        """
        pages = []

        # Первая страница
        html, etag = await self._fetch_page(base_url, method)
        pages.append((base_url, html, etag))

        # Пагинация
        pagination = pattern.get("pagination")
        max_pages = pattern.get("max_pages", 1)

        if pagination and max_pages > 1:
            for page_num in range(2, max_pages + 1):
                page_url = base_url + pagination.format(n=page_num)
                try:
                    delay = random.uniform(1.5, 3.0)
                    await asyncio.sleep(delay)
                    html_page, _ = await self._fetch_page(page_url, method)
                    pages.append((page_url, html_page, None))
                except Exception as e:
                    logger.debug(
                        f"Пагинация: страница {page_num} недоступна ({e}), "
                        f"остановка"
                    )
                    break

        return pages

    async def _fetch_page(
        self, url: str, method: str
    ) -> tuple[str, Optional[str]]:
        """
        Загружает одну страницу. Возвращает (html, etag).
        """
        if method == "playwright":
            return await self._fetch_playwright(url), None
        else:
            return await self._fetch_httpx(url)

    async def _fetch_httpx(self, url: str) -> tuple[str, Optional[str]]:
        """Загрузка через httpx с поддержкой ETag."""
        try:
            import httpx
        except ImportError:
            raise ImportError("pip install httpx")

        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "ru-RU,ru;q=0.9",
        }

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=self._timeout,
            headers=headers,
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            etag = resp.headers.get("etag")
            return resp.text, etag

    async def _fetch_playwright(self, url: str) -> str:
        """Загрузка через Playwright (для JS-рендеринга и антиботов)."""
        async with self._browser_sem:  # макс. 2 параллельных сессии
            browser = await self._ensure_browser()
            context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                locale="ru-RU",
            )
            page = await context.new_page()

            try:
                await page.goto(url, wait_until="networkidle", timeout=60_000)
                # Ждём дополнительно для JS-контента
                await asyncio.sleep(2)
                html = await page.content()
                return html
            finally:
                await page.close()
                await context.close()

    async def _ensure_browser(self):
        """Lazy-инициализация Playwright."""
        if self._browser is None:
            try:
                from playwright.async_api import async_playwright
                self._playwright = await async_playwright().start()
                self._browser = await self._playwright.chromium.launch(
                    headless=self._headless,
                )
                logger.info("ChangeDetector: Playwright browser запущен")
            except ImportError:
                raise ImportError(
                    "pip install playwright && playwright install chromium"
                )
        return self._browser

    # ------------------------------------------------------------------
    # Извлечение ссылок на документы
    # ------------------------------------------------------------------

    def _extract_pdf_links(
        self,
        html: str,
        page_url: str,
        pattern: dict,
    ) -> list[DiscoveredDocument]:
        """Извлекает ссылки на документы из HTML (pdf_portal)."""
        link_regex = pattern.get("link_regex", r'href=["\']([^"\']*\.pdf)["\']')

        # Находим все ссылки
        raw_links = re.findall(link_regex, html, re.IGNORECASE)
        links = list(dict.fromkeys(  # дедупликация с сохранением порядка
            urljoin(page_url, href) for href in raw_links
        ))

        docs = []
        for link in links:
            doc = DiscoveredDocument(url=link)

            # Если ссылка ведёт напрямую на PDF
            if link.lower().endswith(".pdf"):
                doc.file_url = link

            # Попытка извлечь метаданные из контекста ссылки
            self._enrich_from_html(doc, html, link, pattern)
            docs.append(doc)

        return docs

    def _extract_json_embed(
        self,
        html: str,
        page_url: str,
        pattern: dict,
    ) -> list[DiscoveredDocument]:
        """Извлекает данные из JSON, встроенного в JS-переменную (json_embed)."""
        js_var = pattern.get("js_var", "$obj")

        # Ищем: var $obj = {...}; или $obj = {...};
        regex = rf'{re.escape(js_var)}\s*=\s*(\{{.*?\}});'
        match = re.search(regex, html, re.DOTALL)
        if not match:
            # Альтернативный паттерн: массив
            regex = rf'{re.escape(js_var)}\s*=\s*(\[.*?\]);'
            match = re.search(regex, html, re.DOTALL)

        if not match:
            logger.warning(f"json_embed: переменная '{js_var}' не найдена")
            return []

        try:
            data = json.loads(match.group(1))
        except json.JSONDecodeError as e:
            logger.warning(f"json_embed: ошибка парсинга JSON — {e}")
            return []

        # Преобразуем JSON в DiscoveredDocument
        docs = []
        items = data if isinstance(data, list) else data.get("data", data.get("items", []))
        if isinstance(items, dict):
            items = list(items.values()) if all(isinstance(v, dict) for v in items.values()) else [items]

        for item in items:
            if not isinstance(item, dict):
                continue
            doc = DiscoveredDocument(
                url=urljoin(page_url, str(item.get("url", item.get("link", "")))),
                title=item.get("title", item.get("name", "")),
                order_number=str(item.get("number", item.get("order_number", ""))),
                order_date=str(item.get("date", item.get("order_date", ""))),
            )
            if doc.url and doc.url != page_url:
                docs.append(doc)

        return docs

    @staticmethod
    def _enrich_from_html(
        doc: DiscoveredDocument,
        html: str,
        link: str,
        pattern: dict,
    ):
        """Обогащает документ метаданными из HTML-контекста ссылки."""
        # Ищем контекст вокруг ссылки (±500 символов)
        idx = html.find(link)
        if idx < 0:
            # Ссылка может быть без домена в HTML
            parsed = urlparse(link)
            short = parsed.path
            idx = html.find(short)

        if idx >= 0:
            ctx = html[max(0, idx - 500):idx + 500]

            # Номер приказа
            num_regex = pattern.get(
                "order_number_regex",
                r'[№N]\s*([А-Яа-яA-Za-z0-9\-/]+)'
            )
            m = re.search(num_regex, ctx)
            if m and not doc.order_number:
                doc.order_number = m.group(1).strip()

            # Дата
            date_regex = pattern.get(
                "order_date_regex",
                r'(\d{2}[\.\s]\d{2}[\.\s]\d{4})'
            )
            m = re.search(date_regex, ctx)
            if m and not doc.order_date:
                doc.order_date = m.group(1).replace(" ", ".")

            # Заголовок
            title_regex = pattern.get("title_regex")
            if title_regex:
                m = re.search(title_regex, ctx)
                if m and not doc.title:
                    doc.title = m.group(1).strip()

            # Тип: приказ или распоряжение
            ctx_lower = ctx.lower()
            if "распоряжение" in ctx_lower:
                doc.order_type = "распоряжение"
            elif "приказ" in ctx_lower:
                doc.order_type = "приказ"

    # ------------------------------------------------------------------
    # Работа с БД
    # ------------------------------------------------------------------

    async def _get_source_info(self, source_code: str) -> Optional[dict]:
        """Получает информацию об источнике из БД."""
        if not self._engine:
            return None

        from sqlalchemy import text

        async with self._engine.begin() as conn:
            row = await conn.execute(
                text("""
                    SELECT id, code, source_type, risk_class,
                           last_page_hash, last_etag, last_checked_at,
                           discovery_config, active
                    FROM registry_sources
                    WHERE code = :code
                """),
                {"code": source_code},
            )
            r = row.fetchone()
            if r:
                return dict(r._mapping)
        return None

    async def _get_active_source_codes(self) -> list[str]:
        """Возвращает коды активных источников из БД."""
        if not self._engine:
            return list(SOURCE_PATTERNS.keys())

        from sqlalchemy import text

        async with self._engine.begin() as conn:
            rows = await conn.execute(
                text("SELECT code FROM registry_sources WHERE active = TRUE ORDER BY code")
            )
            return [r[0] for r in rows.fetchall()]

    async def _get_known_urls(self, source_code: str) -> set[str]:
        """Возвращает множество уже известных URL для источника."""
        if not self._engine:
            return set()

        from sqlalchemy import text

        async with self._engine.begin() as conn:
            # Находим source_id
            row = await conn.execute(
                text("SELECT id FROM registry_sources WHERE code = :code"),
                {"code": source_code},
            )
            r = row.fetchone()
            if not r:
                return set()
            source_id = str(r[0])

            # Все URL из orders для этого источника
            rows = await conn.execute(
                text("""
                    SELECT source_url, file_url
                    FROM orders
                    WHERE source_id = :sid
                """),
                {"sid": source_id},
            )
            urls = set()
            for r in rows.fetchall():
                if r[0]:
                    urls.add(r[0])
                if r[1]:
                    urls.add(r[1])
            return urls

    async def _create_orders(
        self,
        source_code: str,
        docs: list[DiscoveredDocument],
    ) -> int:
        """Создаёт записи приказов для новых документов."""
        if not self._engine:
            logger.info(
                f"[{source_code}] dry-run: создано бы {len(docs)} приказов"
            )
            return len(docs)

        from sqlalchemy import text
        from uuid import uuid4

        async with self._engine.begin() as conn:
            # source_id
            row = await conn.execute(
                text("SELECT id FROM registry_sources WHERE code = :code"),
                {"code": source_code},
            )
            r = row.fetchone()
            if not r:
                logger.error(f"Источник '{source_code}' не найден")
                return 0
            source_id = str(r[0])

            created = 0
            for doc in docs:
                try:
                    # Проверяем дубликат по URL
                    existing = await conn.execute(
                        text("""
                            SELECT id FROM orders
                            WHERE source_id = :sid AND (
                                source_url = :url OR file_url = :furl
                            )
                            LIMIT 1
                        """),
                        {"sid": source_id, "url": doc.url, "furl": doc.file_url},
                    )
                    if existing.fetchone():
                        continue

                    order_id = str(uuid4())
                    await conn.execute(
                        text("""
                            INSERT INTO orders (
                                id, source_id, order_number, order_date,
                                order_type, title, source_url, file_url, status
                            ) VALUES (
                                :id, :sid, :num, :dt,
                                :type, :title, :surl, :furl, 'new'
                            )
                        """),
                        {
                            "id": order_id,
                            "sid": source_id,
                            "num": doc.order_number or "",
                            "dt": doc.order_date or "",
                            "type": doc.order_type,
                            "title": doc.title,
                            "surl": doc.url,
                            "furl": doc.file_url,
                        },
                    )
                    created += 1
                    logger.info(
                        f"[{source_code}] 📄 Новый: "
                        f"{doc.order_number or doc.url[:60]}"
                    )

                except Exception as e:
                    logger.warning(
                        f"[{source_code}] ошибка создания записи: {e}"
                    )

            return created

    async def _update_source_check(
        self,
        source_code: str,
        page_hash: Optional[str],
        etag: Optional[str],
    ):
        """Обновляет last_page_hash, last_etag, last_checked_at."""
        if not self._engine:
            return

        from sqlalchemy import text

        fields = ["last_checked_at = NOW()"]
        params: dict = {"code": source_code}

        if page_hash:
            fields.append("last_page_hash = :hash")
            params["hash"] = page_hash
        if etag:
            fields.append("last_etag = :etag")
            params["etag"] = etag

        sql = f"UPDATE registry_sources SET {', '.join(fields)} WHERE code = :code"

        async with self._engine.begin() as conn:
            await conn.execute(text(sql), params)

    async def _log_to_db(
        self,
        source_code: str,
        level: str,
        message: str,
        details: Optional[dict] = None,
    ):
        """Записывает событие в processing_log."""
        if not self._engine:
            return

        from sqlalchemy import text
        from uuid import uuid4

        async with self._engine.begin() as conn:
            row = await conn.execute(
                text("SELECT id FROM registry_sources WHERE code = :code"),
                {"code": source_code},
            )
            r = row.fetchone()
            source_id = str(r[0]) if r else None

            await conn.execute(
                text("""
                    INSERT INTO processing_log
                    (id, source_id, level, stage, message, details)
                    VALUES (:id, :sid, :level, 'change_detection', :msg, :det::jsonb)
                """),
                {
                    "id": str(uuid4()),
                    "sid": source_id,
                    "level": level,
                    "msg": message[:2000],
                    "det": json.dumps(details or {}, ensure_ascii=False),
                },
            )

    # ------------------------------------------------------------------
    # Утилиты
    # ------------------------------------------------------------------

    @staticmethod
    def _hash_content(html: str) -> str:
        """
        Хеш «значимой» части HTML.
        Убирает: пробелы, timestamps, nonce, csrf, session tokens —
        чтобы хеш менялся только при реальном изменении контента.
        """
        # Удаляем скрипты и стили (часто содержат nonce/timestamps)
        cleaned = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        cleaned = re.sub(r'<style[^>]*>.*?</style>', '', cleaned, flags=re.DOTALL | re.IGNORECASE)

        # Удаляем HTML-комментарии
        cleaned = re.sub(r'<!--.*?-->', '', cleaned, flags=re.DOTALL)

        # Нормализуем пробелы
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()

        # Удаляем типичные динамические атрибуты
        cleaned = re.sub(r'(csrf|nonce|token|session|timestamp)=["\'][^"\']*["\']',
                         '', cleaned, flags=re.IGNORECASE)

        return hashlib.sha256(cleaned.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

async def _main():
    import argparse

    parser = argparse.ArgumentParser(
        description="SportRank Change Detector — мониторинг новых приказов"
    )
    sub = parser.add_subparsers(dest="command")

    # check
    p_check = sub.add_parser("check", help="Проверить один источник")
    p_check.add_argument("source", help="Код источника (spb_kfkis, ...)")
    p_check.add_argument("--db", help="PostgreSQL URL")

    # check-all
    p_all = sub.add_parser("check-all", help="Проверить все источники")
    p_all.add_argument("--db", help="PostgreSQL URL")

    # loop
    p_loop = sub.add_parser("loop", help="Периодическая проверка")
    p_loop.add_argument("--db", required=True, help="PostgreSQL URL")
    p_loop.add_argument("--interval", type=int, default=60, help="Интервал в минутах")

    # test-extract
    p_test = sub.add_parser("test-extract", help="Тест извлечения ссылок из HTML")
    p_test.add_argument("source", help="Код источника")
    p_test.add_argument("html_file", help="HTML-файл для анализа")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if args.command == "check":
        async with ChangeDetector(db_url=getattr(args, "db", None)) as det:
            result = await det.check_source(args.source)
            _print_check_result(result)

    elif args.command == "check-all":
        async with ChangeDetector(db_url=getattr(args, "db", None)) as det:
            results = await det.check_all()
            for r in results:
                _print_check_result(r)

    elif args.command == "loop":
        async with ChangeDetector(db_url=args.db) as det:
            await det.run_loop(interval_minutes=args.interval)

    elif args.command == "test-extract":
        det = ChangeDetector()
        html = Path(args.html_file).read_text(encoding="utf-8")
        pattern = SOURCE_PATTERNS.get(args.source, {})
        docs = det._extract_pdf_links(html, "http://example.com", pattern)
        print(f"Найдено документов: {len(docs)}")
        for doc in docs[:20]:
            print(f"  📄 {doc.order_number or '?'} от {doc.order_date or '?'}")
            print(f"     URL: {doc.url[:80]}")
            if doc.title:
                print(f"     Заголовок: {doc.title[:60]}")
            print()

    else:
        parser.print_help()


def _print_check_result(result: CheckResult):
    """Красивый вывод результата проверки."""
    print()
    print(result.summary())
    print(f"  Хеш страницы:   {(result.page_hash or '')[:16]}...")
    print(f"  Хеш изменился:  {'да' if result.page_hash_changed else 'нет'}")
    print(f"  Ссылок всего:   {result.links_total}")
    print(f"  Новых:          {result.links_new}")
    print(f"  Создано в БД:   {result.orders_created}")
    print(f"  Время:          {result.duration_ms}ms")

    if result.new_documents:
        print(f"\n  Новые документы:")
        for doc in result.new_documents[:10]:
            title = doc.title or doc.order_number or doc.url[:60]
            print(f"    📄 {title}")
            if doc.order_date:
                print(f"       Дата: {doc.order_date}")
            print(f"       URL: {(doc.file_url or doc.url)[:80]}")

    if result.error:
        print(f"\n  ⚠️ Ошибка: {result.error}")


if __name__ == "__main__":
    asyncio.run(_main())
