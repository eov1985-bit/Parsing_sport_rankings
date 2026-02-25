"""
pdf_downloader.py
=================
Скачивание PDF-приказов с порталов, защищённых Servicepipe и аналогами.

Стратегия обхода:
  1. Playwright (headless Chromium) — для Servicepipe/Antibot (mst.mosreg.ru и др.)
  2. requests + retry — для открытых порталов (kfis.gov.spb.ru, minsport.krasnodar.ru)
  3. Автоопределение метода по source_type и наличию antibot в discovery_config

Использование:
    downloader = PdfDownloader(output_dir="./pdfs")

    # Один файл
    path = await downloader.download(url="https://...", source_code="spb_kfkis")

    # Пакетно из БД
    await downloader.download_pending(db_conn, limit=50)

Зависимости:
    pip install playwright httpx tenacity
    playwright install chromium
"""

import asyncio
import hashlib
import logging
import random
import re
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Конфигурация по источникам
# ---------------------------------------------------------------------------

SOURCE_CONFIG: dict[str, dict] = {}
try:
    from source_registry import as_download_configs as _load_dl_configs
    SOURCE_CONFIG = _load_dl_configs()
except ImportError:
    SOURCE_CONFIG = {
        "mo_mособлспорт": {"method": "playwright", "antibot": "servicepipe", "base_url": "https://mst.mosreg.ru", "delay_range": (3, 8), "wait_selector": "a.document-link, a[href$='.pdf']"},
        "moskva_tstisk": {"method": "playwright", "antibot": "servicepipe", "base_url": "https://www.mos.ru", "delay_range": (2, 6), "wait_selector": "a[href$='.pdf']"},
        "moskva_moskumsport": {"method": "playwright", "antibot": "servicepipe", "base_url": "https://www.mos.ru", "delay_range": (2, 6), "wait_selector": "a[href$='.pdf']"},
        "spb_kfkis": {"method": "httpx", "base_url": "https://kfis.gov.spb.ru", "delay_range": (1, 3)},
        "krasnodar_minsport": {"method": "httpx", "base_url": "https://minsport.krasnodar.ru", "delay_range": (1, 4)},
        "rf_minsport": {"method": "httpx", "base_url": "https://msrfinfo.ru", "delay_range": (1, 2)},
    }

# User-agents для ротации (актуальные, 2025–2026)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]


# ---------------------------------------------------------------------------
# Исключения
# ---------------------------------------------------------------------------

class DownloadError(Exception):
    pass

class AntibotDetected(DownloadError):
    """Портал вернул страницу Servicepipe/антибот-проверки."""
    pass

class PdfNotFound(DownloadError):
    pass


# ---------------------------------------------------------------------------
# Основной класс
# ---------------------------------------------------------------------------

class PdfDownloader:
    """
    Скачивает PDF-файлы с региональных спортивных порталов.

    Автоматически выбирает метод (Playwright vs httpx) по source_code.
    Сохраняет файлы в output_dir с именем по SHA256.
    """

    def __init__(
        self,
        output_dir: str = "./pdfs",
        playwright_headless: bool = True,
        max_retries: int = 3,
        request_timeout: int = 60,
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.playwright_headless = playwright_headless
        self.max_retries = max_retries
        self.request_timeout = request_timeout

        self._playwright = None
        self._browser = None

    # ------------------------------------------------------------------
    # Публичный API
    # ------------------------------------------------------------------

    async def download(
        self,
        url: str,
        source_code: str,
        filename_hint: Optional[str] = None,
    ) -> Path:
        """
        Скачивает один PDF.
        Возвращает Path к сохранённому файлу.
        Файл именуется по SHA256 содержимого (идемпотентно).
        """
        config = SOURCE_CONFIG.get(source_code, {})
        method = config.get("method", "httpx")

        logger.info(f"[{source_code}] download via {method}: {url}")

        if method == "playwright":
            content = await self._download_playwright(url, config)
        else:
            content = await self._download_httpx(url, config)

        if not self._is_pdf(content):
            # Возможно, вернули HTML-страницу антибота
            if b"servicepipe" in content.lower() or b"<html" in content[:200].lower():
                raise AntibotDetected(
                    f"Получен HTML вместо PDF: {url}\n"
                    f"Первые 200 байт: {content[:200]}"
                )
            raise DownloadError(f"Ответ не является PDF: {url}")

        return self._save(content, url, filename_hint)

    async def discover_and_download(
        self,
        source_code: str,
        list_url: str,
        link_pattern: Optional[str] = None,
    ) -> list[Path]:
        """
        Обходит страницу списка документов, находит ссылки на PDF и скачивает их.
        link_pattern — regex для фильтрации ссылок (опционально).
        """
        config = SOURCE_CONFIG.get(source_code, {})
        method = config.get("method", "httpx")

        logger.info(f"[{source_code}] discovering links on: {list_url}")

        if method == "playwright":
            links = await self._discover_playwright(list_url, config, link_pattern)
        else:
            links = await self._discover_httpx(list_url, config, link_pattern)

        logger.info(f"[{source_code}] найдено ссылок: {len(links)}")

        paths = []
        for i, link in enumerate(links):
            try:
                delay = random.uniform(*config.get("delay_range", (1, 3)))
                if i > 0:
                    await asyncio.sleep(delay)
                path = await self.download(link, source_code)
                paths.append(path)
                logger.info(f"  [{i+1}/{len(links)}] ✓ {path.name}")
            except (DownloadError, Exception) as e:
                logger.error(f"  [{i+1}/{len(links)}] ✗ {link}: {e}")

        return paths

    async def close(self):
        """Закрывает Playwright браузер если был открыт."""
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    # ------------------------------------------------------------------
    # Playwright методы (для Servicepipe-защищённых порталов)
    # ------------------------------------------------------------------

    async def _ensure_browser(self):
        """Лениво инициализирует Playwright браузер."""
        if self._browser is not None:
            return

        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise ImportError(
                "Установи playwright: pip install playwright && playwright install chromium"
            )

        self._playwright = await async_playwright().__aenter__()
        self._browser = await self._playwright.chromium.launch(
            headless=self.playwright_headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

    async def _new_context(self, config: dict):
        """Создаёт новый браузерный контекст с реалистичными заголовками."""
        await self._ensure_browser()
        ua = random.choice(USER_AGENTS)
        context = await self._browser.new_context(
            user_agent=ua,
            viewport={"width": 1366, "height": 768},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            accept_downloads=True,
            extra_http_headers={
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "DNT": "1",
            },
        )

        # Скрываем WebDriver-признаки
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
            Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru']});
        """)

        return context

    async def _download_playwright(self, url: str, config: dict) -> bytes:
        """Скачивает PDF через Playwright с обходом антибота."""
        context = await self._new_context(config)
        page = await context.new_page()

        try:
            # Шаг 1: Переходим на главную страницу сайта (устанавливаем куки/сессию)
            base_url = config.get("base_url", "")
            if base_url:
                parsed = urlparse(url)
                home = f"{parsed.scheme}://{parsed.netloc}"
                await page.goto(home, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(random.uniform(1, 3))

            # Шаг 2: Переходим на целевую страницу
            response = await page.goto(
                url,
                wait_until="networkidle",
                timeout=self.request_timeout * 1000,
            )

            if response is None:
                raise DownloadError(f"Playwright: нет ответа для {url}")

            # Шаг 3: Проверяем что не попали на страницу антибота
            content_type = response.headers.get("content-type", "")
            if "application/pdf" in content_type:
                # Прямой PDF-ответ
                return await response.body()

            # Шаг 4: Если HTML — ищем прямую ссылку на PDF или ждём редиректа
            html = await page.content()
            if self._is_antibot_page(html):
                # Ждём прохождения проверки (Servicepipe иногда требует JS-рендеринг)
                logger.debug(f"Антибот-страница, ждём 5с...")
                await asyncio.sleep(5)
                await page.wait_for_load_state("networkidle", timeout=15_000)
                content_type = (await page.evaluate(
                    "() => document.contentType"
                ) or "")

            # Если страница содержит embed/iframe с PDF
            pdf_url = await self._find_pdf_in_page(page, url)
            if pdf_url:
                return await self._download_playwright(pdf_url, config)

            # Последняя попытка: скачать как файл через download event
            body = await response.body()
            return body

        finally:
            await page.close()
            await context.close()

    async def _discover_playwright(
        self,
        list_url: str,
        config: dict,
        link_pattern: Optional[str],
    ) -> list[str]:
        """Извлекает ссылки на PDF со страницы через Playwright."""
        context = await self._new_context(config)
        page = await context.new_page()

        try:
            await page.goto(list_url, wait_until="networkidle", timeout=60_000)

            # Ждём нужный селектор если указан
            selector = config.get("wait_selector")
            if selector:
                try:
                    await page.wait_for_selector(selector, timeout=10_000)
                except Exception:
                    pass

            links = await page.evaluate("""
                () => Array.from(document.querySelectorAll('a[href]'))
                    .map(a => a.href)
                    .filter(h => h)
            """)

            return self._filter_pdf_links(links, list_url, link_pattern)

        finally:
            await page.close()
            await context.close()

    # ------------------------------------------------------------------
    # httpx методы (для открытых порталов)
    # ------------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def _download_httpx(self, url: str, config: dict) -> bytes:
        """Скачивает файл через httpx с retry."""
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/pdf,*/*",
            "Accept-Language": "ru-RU,ru;q=0.9",
            "Referer": config.get("base_url", ""),
        }

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=self.request_timeout,
            headers=headers,
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.content

    async def _discover_httpx(
        self,
        list_url: str,
        config: dict,
        link_pattern: Optional[str],
    ) -> list[str]:
        """Извлекает ссылки на PDF через httpx + простой парсинг."""
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "ru-RU,ru;q=0.9",
        }

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=self.request_timeout,
            headers=headers,
        ) as client:
            response = await client.get(list_url)
            response.raise_for_status()
            html = response.text

        # Простой regex-парсинг ссылок (без BeautifulSoup)
        hrefs = re.findall(r'href=["\']([^"\']+)["\']', html)
        links = [urljoin(list_url, h) for h in hrefs]
        return self._filter_pdf_links(links, list_url, link_pattern)

    # ------------------------------------------------------------------
    # Вспомогательные методы
    # ------------------------------------------------------------------

    @staticmethod
    def _is_pdf(content: bytes) -> bool:
        """PDF начинается с магических байт %PDF."""
        return content[:4] == b"%PDF"

    @staticmethod
    def _is_antibot_page(html: str) -> bool:
        """Эвристика: определяем страницу Servicepipe/антибота."""
        lower = html.lower()
        markers = [
            "servicepipe", "ddos-guard", "cloudflare",
            "checking your browser", "проверка браузера",
            "just a moment", "enable javascript",
        ]
        return any(m in lower for m in markers)

    @staticmethod
    async def _find_pdf_in_page(page, base_url: str) -> Optional[str]:
        """Ищет прямую ссылку на PDF на HTML-странице."""
        try:
            urls = await page.evaluate("""
                () => {
                    const links = [];
                    document.querySelectorAll('a[href], iframe[src], embed[src]').forEach(el => {
                        const src = el.href || el.src || '';
                        if (src.toLowerCase().includes('.pdf')) links.push(src);
                    });
                    return links;
                }
            """)
            if urls:
                return urljoin(base_url, urls[0])
        except Exception:
            pass
        return None

    @staticmethod
    def _filter_pdf_links(
        links: list[str],
        base_url: str,
        pattern: Optional[str],
    ) -> list[str]:
        """
        Фильтрует список ссылок: оставляет только PDF.
        Если задан pattern — дополнительно фильтрует по regex.
        Дедуплицирует.
        """
        seen = set()
        result = []
        for link in links:
            if not link:
                continue
            lower = link.lower()
            # Принимаем ссылки на .pdf или с /media/docs/
            if ".pdf" not in lower and "/media/docs/" not in lower:
                continue
            if pattern and not re.search(pattern, link, re.IGNORECASE):
                continue
            if link not in seen:
                seen.add(link)
                result.append(link)
        return result

    def _save(
        self,
        content: bytes,
        source_url: str,
        filename_hint: Optional[str],
    ) -> Path:
        """
        Сохраняет PDF на диск.
        Имя файла = SHA256[:16].pdf (идемпотентно — повторная загрузка не создаёт дубль).
        """
        sha = hashlib.sha256(content).hexdigest()
        filename = f"{sha[:16]}.pdf"
        path = self.output_dir / filename

        if path.exists():
            logger.debug(f"Файл уже существует (skip): {path}")
            return path

        path.write_bytes(content)
        logger.info(f"Сохранён: {path} ({len(content):,} байт) ← {source_url}")
        return path


# ---------------------------------------------------------------------------
# Специализированный дискавери для КФКиС СПб
# (приказы + все отдельные PDF-приложения к каждому приказу)
# ---------------------------------------------------------------------------

class SpbKfkisDiscovery:
    """
    Обходит kfis.gov.spb.ru и скачивает:
    - Главный PDF приказа
    - Все PDF приложений к нему (Приложение 1, 2, 3...)

    Возвращает: {main_pdf: Path, attachments: [Path, ...]}
    """

    BASE_URL = "https://kfis.gov.spb.ru"
    LIST_PATH = "/deyatelnost/prisvoenie-sportivnykh-razryadov/"

    def __init__(self, downloader: PdfDownloader):
        self.dl = downloader

    async def discover_order_group(
        self, order_page_url: str
    ) -> dict[str, Path | list[Path]]:
        """
        Со страницы одного приказа скачивает главный файл и все приложения.
        """
        config = SOURCE_CONFIG["spb_kfkis"]

        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
            r = await client.get(
                order_page_url,
                headers={"User-Agent": random.choice(USER_AGENTS)},
            )
            r.raise_for_status()
            html = r.text

        hrefs = re.findall(r'href=["\']([^"\']*\.pdf)["\']', html, re.IGNORECASE)
        links = list(dict.fromkeys(  # дедупликация с сохранением порядка
            urljoin(self.BASE_URL, h) for h in hrefs
        ))

        if not links:
            raise PdfNotFound(f"Нет PDF на странице: {order_page_url}")

        # Первая ссылка — главный документ, остальные — приложения
        # (по соглашению kfis.gov.spb.ru: Распоряжение идёт первым)
        main_link = links[0]
        attachment_links = links[1:]

        main_path = await self.dl.download(main_link, "spb_kfkis")
        attachment_paths = []
        for i, link in enumerate(attachment_links):
            await asyncio.sleep(random.uniform(0.5, 1.5))
            path = await self.dl.download(link, "spb_kfkis")
            attachment_paths.append(path)
            logger.info(f"  Приложение {i+1}: {path.name}")

        return {"main": main_path, "attachments": attachment_paths}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

async def _main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Скачать PDF приказа о спортивных разрядах"
    )
    parser.add_argument("url",  help="URL страницы или прямая ссылка на PDF")
    parser.add_argument("source", help="Код источника (mo_mособлспорт, spb_kfkis, ...)")
    parser.add_argument("--out", default="./pdfs", help="Папка для сохранения")
    parser.add_argument("--visible", action="store_true", help="Видимый браузер (для отладки)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    async with PdfDownloader(
        output_dir=args.out,
        playwright_headless=not args.visible,
    ) as dl:
        try:
            if args.source == "spb_kfkis" and "kfis.gov.spb.ru" in args.url and not args.url.endswith(".pdf"):
                # Страница приказа СПб — скачиваем всю группу файлов
                spb = SpbKfkisDiscovery(dl)
                result = await spb.discover_order_group(args.url)
                print(f"Главный файл:    {result['main']}")
                for i, p in enumerate(result["attachments"], 1):
                    print(f"Приложение {i}:  {p}")
            else:
                path = await dl.download(args.url, args.source)
                print(f"Скачан: {path}")
        except AntibotDetected as e:
            print(f"[!] Антибот: {e}")
        except DownloadError as e:
            print(f"[!] Ошибка загрузки: {e}")


if __name__ == "__main__":
    asyncio.run(_main())
