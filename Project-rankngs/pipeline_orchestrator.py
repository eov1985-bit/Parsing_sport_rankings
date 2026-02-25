"""
pipeline_orchestrator.py
========================
Оркестратор конвейера SportRank: связывает все компоненты в единый
end-to-end pipeline обработки приказов.

Конвейер:
  1. Получение задания (URL/файл/запись из БД)
  2. Скачивание PDF (pdf_downloader.PdfDownloader)
  3. Извлечение текста (ocr_pipeline.OcrPipeline)
  4. Структурированное извлечение данных (llm_extractor.LLMExtractor)
  5. Нормализация видов спорта (sport_normalizer.SportNormalizer)
  6. Сохранение в PostgreSQL (встроенный DB Adapter)
  7. Логирование каждого шага (processing_log)

Режимы работы:
  - Одиночный файл: process_file(path, source_code, order_number, order_date)
  - По URL: process_url(url, source_code)
  - Пакетно из БД: process_pending(limit=50)
  - Повторная обработка: reprocess(order_id)

Ключевые принципы:
  - Идемпотентность: повторный запуск не создаёт дубликатов (file_hash + UNIQUE)
  - Атомарность: транзакция на весь приказ (все записи или ничего)
  - Наблюдаемость: каждый шаг → processing_log + метрики
  - Устойчивость: ошибка одного приказа не ломает пакетную обработку

Использование:
    # Минимальный (без БД, с файлом)
    orch = PipelineOrchestrator(anthropic_api_key="sk-ant-...")
    result = await orch.process_file(
        "order.pdf",
        source_code="moskva_tstisk",
        order_number="С-2/26",
        order_date="17.02.2026",
    )
    print(f"Извлечено: {result.records_extracted} записей")

    # Полный (с БД и всеми компонентами)
    orch = PipelineOrchestrator(
        db_url="postgresql+asyncpg://user:pass@localhost/sportrank",
        anthropic_api_key="sk-ant-...",
        sport_registry_xls="Reestr.xls",
    )
    await orch.initialize()
    await orch.process_pending(limit=20)
    await orch.shutdown()

Зависимости:
    pip install sqlalchemy[asyncio] asyncpg anthropic pypdf
    (опционально) pip install pdf2image pytesseract openpyxl rapidfuzz
"""

import asyncio
import hashlib
import json
import logging
import os
import time
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from enum import Enum
from pathlib import Path
from typing import Optional, Union
from uuid import UUID, uuid4

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Безопасность: SSRF-защита и лимиты PDF
# ---------------------------------------------------------------------------

MAX_PDF_SIZE = int(os.environ.get("MAX_PDF_SIZE", 50 * 1024 * 1024))  # 50 MB
MAX_PDF_PAGES = int(os.environ.get("MAX_PDF_PAGES", 200))

# Разрешённые домены (извлекаются из SOURCE_METADATA)
_ALLOWED_DOMAINS = {
    "www.mos.ru", "mos.ru",
    "mst.mosreg.ru",
    "kfis.gov.spb.ru",
    "minsport.krasnodar.ru",
    "msrfinfo.ru",
}

# Динамический allowlist: домены, добавленные через admin UI (→ DB)
# Обновляется через register_domain() при создании нового источника
_dynamic_domains: set[str] = set()


def register_domain(domain: str) -> None:
    """Регистрирует домен в динамическом SSRF-allowlist.

    Вызывается при создании/обновлении источника через Admin API,
    чтобы новые домены работали без перезапуска сервера.
    """
    d = domain.strip().lower().strip(".")
    if d:
        _dynamic_domains.add(d)
        logger.info(f"SSRF: домен '{d}' добавлен в динамический allowlist")


def get_allowed_domains() -> set[str]:
    """Возвращает полное множество разрешённых доменов (static + registry + dynamic)."""
    result = set(_ALLOWED_DOMAINS) | _dynamic_domains
    try:
        result.update(ALLOWED_DOMAINS)  # type: ignore[name-defined]
    except NameError:
        pass
    return result


def validate_url(url: str, source_code: str = "") -> bool:
    """
    Проверяет URL на SSRF.

    Политика:
      - только http/https
      - блокируем userinfo (user:pass@host)
      - разрешаем домены из ALLOWED_DOMAINS (source_registry) + fallback _ALLOWED_DOMAINS
      - блокируем loopback/private/link-local/reserved/multicast IP (все DNS A/AAAA)
    """
    from urllib.parse import urlsplit
    import ipaddress
    import socket

    try:
        parsed = urlsplit(url.strip())
    except Exception:
        logger.warning(f"SSRF: не удалось распарсить URL: {url}")
        return False

    # Только HTTP/HTTPS
    scheme = (parsed.scheme or "").lower()
    if scheme not in ("http", "https"):
        logger.warning(f"SSRF: заблокирована схема '{scheme}' в URL: {url}")
        return False

    if not parsed.netloc:
        logger.warning(f"SSRF: отсутствует netloc в URL: {url}")
        return False

    # Блокируем userinfo (user:pass@host)
    if "@" in parsed.netloc:
        logger.warning(f"SSRF: заблокирован userinfo в URL: {url}")
        return False

    hostname = (parsed.hostname or "").strip(".").lower()
    if not hostname:
        logger.warning(f"SSRF: отсутствует hostname в URL: {url}")
        return False

    # Домены: static + registry + динамические (добавленные через UI)
    allowlist = get_allowed_domains()

    if hostname not in allowlist:
        logger.warning(f"SSRF: домен '{hostname}' не в whitelist. URL: {url}")
        return False

    # Блокируем private/localhost/link-local/reserved/multicast IP для всех DNS-записей
    port = parsed.port or (443 if scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(hostname, port, proto=socket.IPPROTO_TCP)
    except Exception as e:
        logger.warning(f"SSRF: DNS lookup failed for {hostname}: {e}")
        return False

    for info in infos:
        ip_str = info[4][0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            logger.warning(f"SSRF: некорректный IP '{ip_str}' для {hostname}")
            return False
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            logger.warning(f"SSRF: заблокирован IP {ip} для {hostname}")
            return False

    return True


def normalize_rank(value: Optional[str]) -> str:
    """Нормализует разряд/звание (ЕВСК) в единое каноническое написание.

    Возвращает пустую строку, если значение не распознано.
    Используется в _step_normalize для очистки rank_category перед записью в БД.
    """
    import re as _re

    if not value:
        return ""
    s = str(value).strip().lower()
    if not s:
        return ""

    # Унификация символов
    s = s.replace("ё", "е").replace("№", " ")
    s = " ".join(s.split())

    # Быстрые алиасы (точное совпадение после lower + strip)
    aliases = {
        "кмс": "кандидат в мастера спорта",
        "кандидат в мастера спорта": "кандидат в мастера спорта",
        "мс": "мастер спорта россии",
        "мастер спорта": "мастер спорта россии",
        "мастер спорта россии": "мастер спорта россии",
        "мсмк": "мастер спорта россии международного класса",
        "мастер спорта международного класса": "мастер спорта россии международного класса",
        "змс": "заслуженный мастер спорта россии",
        "заслуженный мастер спорта": "заслуженный мастер спорта россии",
        "заслуженный мастер спорта россии": "заслуженный мастер спорта россии",
        "гроссмейстер россии": "гроссмейстер россии",
        "гм": "гроссмейстер россии",
        "гмр": "гроссмейстер россии",
        "зтр": "заслуженный тренер россии",
        "заслуженный тренер россии": "заслуженный тренер россии",
    }
    if s in aliases:
        return aliases[s]

    # Юношеские разряды (проверяем ДО обычных — III > II > I)
    if "юнош" in s:
        if _re.search(r"\b(3|iii)\b", s) or "трет" in s:
            return "третий юношеский спортивный разряд"
        if _re.search(r"\b(2|ii)\b", s) or "втор" in s:
            return "второй юношеский спортивный разряд"
        if _re.search(r"\b(1|i)\b", s) or "перв" in s:
            return "первый юношеский спортивный разряд"

    # Спортивные разряды (III > II > I — порядок важен!)
    if "разряд" in s:
        if _re.search(r"\b(3|iii)\b", s) or "трет" in s:
            return "третий спортивный разряд"
        if _re.search(r"\b(2|ii)\b", s) or "втор" in s:
            return "второй спортивный разряд"
        if _re.search(r"\b(1|i)\b", s) or "перв" in s:
            return "первый спортивный разряд"

    # Звания (содержательные проверки)
    if "кандидат" in s and "мастер" in s:
        return "кандидат в мастера спорта"
    if "международ" in s and "мастер" in s:
        return "мастер спорта россии международного класса"
    if "заслуж" in s and "мастер" in s:
        return "заслуженный мастер спорта россии"
    if "мастер" in s and "спорта" in s:
        return "мастер спорта россии"
    if "заслуж" in s and "тренер" in s:
        return "заслуженный тренер россии"
    if "гроссмейстер" in s:
        return "гроссмейстер россии"

    # Почётные звания (Приказ №856)
    if "почетн" in s and "судь" in s:
        return "почетный спортивный судья россии"
    if "почетн" in s and "мастер" in s:
        return "почетный мастер спорта россии"
    if "почетн" in s and "тренер" in s:
        return "почетный тренер россии"

    return ""


def validate_pdf_size(pdf_bytes: bytes) -> bool:
    """Проверяет размер PDF."""
    if len(pdf_bytes) > MAX_PDF_SIZE:
        logger.warning(
            f"PDF слишком большой: {len(pdf_bytes) / 1024 / 1024:.1f} MB "
            f"(лимит {MAX_PDF_SIZE / 1024 / 1024:.0f} MB)"
        )
        return False
    return True


# ---------------------------------------------------------------------------
# Модели результата
# ---------------------------------------------------------------------------

class StepStatus(str, Enum):
    """Статус отдельного шага конвейера."""
    PENDING   = "pending"
    RUNNING   = "running"
    SUCCESS   = "success"
    FAILED    = "failed"
    SKIPPED   = "skipped"


@dataclass
class StepResult:
    """Результат одного шага."""
    step: str                   # 'download', 'ocr', 'extract', 'normalize', 'save'
    status: StepStatus = StepStatus.PENDING
    duration_ms: int = 0
    message: str = ""
    details: dict = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Итоговый результат обработки одного приказа."""
    order_id: Optional[str] = None        # UUID приказа в БД
    source_code: str = ""
    order_number: str = ""
    order_date: str = ""

    # Статус
    success: bool = False
    status: str = "new"                   # 'new' → 'downloaded' → 'extracted' → 'failed'

    # Метрики
    file_hash: Optional[str] = None
    page_count: int = 0
    text_length: int = 0
    ocr_method: str = ""
    ocr_confidence: float = 0.0
    records_extracted: int = 0
    records_saved: int = 0
    sports_normalized: int = 0
    sports_unmatched: int = 0

    # Шаги
    steps: list[StepResult] = field(default_factory=list)

    # Ошибка (если была)
    error: Optional[str] = None

    # Время
    total_duration_ms: int = 0

    def summary(self) -> str:
        """Краткая сводка для логирования."""
        if self.success:
            return (
                f"✅ {self.source_code} {self.order_number} от {self.order_date}: "
                f"{self.records_saved} записей, {self.page_count} стр., "
                f"OCR={self.ocr_method} (conf={self.ocr_confidence:.2f}), "
                f"{self.total_duration_ms}ms"
            )
        return (
            f"❌ {self.source_code} {self.order_number}: {self.error or 'unknown'}"
        )


# ---------------------------------------------------------------------------
# Конфигурация источников → метаданные для LLM
# ---------------------------------------------------------------------------

# SOURCE_METADATA: загружается из source_registry (единый реестр)
try:
    from source_registry import as_meta as _load_meta, get_all_domains as _load_domains
    SOURCE_METADATA: dict[str, dict] = _load_meta()
    ALLOWED_DOMAINS = _load_domains()
    logger.info(f"Метаданные загружены из source_registry: {len(SOURCE_METADATA)} источников")
except ImportError:
    logger.warning("source_registry не найден, используем встроенный fallback")
    SOURCE_METADATA: dict[str, dict] = {
        "moskva_tstisk": {"issuing_body": "ГКУ «ЦСТиСК» Москомспорта", "order_type": "приказ"},
        "moskva_moskumsport": {"issuing_body": "Департамент спорта города Москвы (Москомспорт)", "order_type": "распоряжение"},
        "mo_mособлспорт": {"issuing_body": "Министерство физической культуры и спорта Московской области", "order_type": "распоряжение"},
        "krasnodar_minsport": {"issuing_body": "Министерство физической культуры и спорта Краснодарского края", "order_type": "приказ"},
        "spb_kfkis": {"issuing_body": "Комитет по физической культуре и спорту Санкт-Петербурга", "order_type": "распоряжение"},
        "rf_minsport": {"issuing_body": "Министерство спорта Российской Федерации", "order_type": "приказ"},
    }


# ---------------------------------------------------------------------------
# DB Adapter (SQLAlchemy async)
# ---------------------------------------------------------------------------

class DbAdapter:
    """
    Адаптер для записи результатов конвейера в PostgreSQL.

    Работает через raw SQL поверх asyncpg (без тяжёлого ORM),
    но с полной транзакционной семантикой.

    При отсутствии подключения к БД — молча пропускает запись
    (режим dry-run для отладки).
    """

    def __init__(self, db_url: Optional[str] = None):
        self._engine = None
        self._db_url = db_url

    async def connect(self):
        """Инициализирует подключение к БД."""
        if not self._db_url:
            logger.info("DB: нет db_url, работаем в режиме dry-run")
            return

        try:
            from sqlalchemy.ext.asyncio import create_async_engine
            self._engine = create_async_engine(
                self._db_url,
                pool_size=5,
                max_overflow=10,
                echo=False,
            )
            # Проверяем подключение
            async with self._engine.begin() as conn:
                await conn.execute(
                    __import__("sqlalchemy").text("SELECT 1")
                )
            logger.info("DB: подключение установлено")
        except Exception as e:
            logger.error(f"DB: ошибка подключения — {e}")
            self._engine = None

    async def close(self):
        """Закрывает подключение."""
        if self._engine:
            await self._engine.dispose()

    @property
    def connected(self) -> bool:
        return self._engine is not None

    # ------------------------------------------------------------------
    # Операции с приказами
    # ------------------------------------------------------------------

    async def get_or_create_order(
        self,
        source_code: str,
        order_number: str,
        order_date: str,
        order_type: str = "приказ",
        title: Optional[str] = None,
        source_url: Optional[str] = None,
        file_url: Optional[str] = None,
    ) -> Optional[str]:
        """
        Находит или создаёт запись приказа. Возвращает UUID.
        Идемпотентно: если приказ уже есть (source_id + order_number + order_date) —
        возвращает существующий UUID.
        """
        if not self._engine:
            return str(uuid4())

        from sqlalchemy import text

        async with self._engine.begin() as conn:
            # Находим source_id
            row = await conn.execute(
                text("SELECT id FROM registry_sources WHERE code = :code"),
                {"code": source_code},
            )
            source_row = row.fetchone()
            if not source_row:
                logger.error(f"DB: источник '{source_code}' не найден в registry_sources")
                return None
            source_id = str(source_row[0])

            # Ищем существующий приказ
            row = await conn.execute(
                text("""
                    SELECT id FROM orders
                    WHERE source_id = :sid AND order_number = :num AND order_date = :dt
                """),
                {"sid": source_id, "num": order_number, "dt": order_date},
            )
            existing = row.fetchone()
            if existing:
                logger.debug(f"DB: приказ уже существует: {existing[0]}")
                return str(existing[0])

            # Создаём новый
            order_id = str(uuid4())
            await conn.execute(
                text("""
                    INSERT INTO orders (id, source_id, order_number, order_date,
                                        order_type, title, source_url, file_url, status)
                    VALUES (:id, :sid, :num, :dt, :type, :title, :surl, :furl, 'new')
                """),
                {
                    "id": order_id, "sid": source_id,
                    "num": order_number, "dt": order_date,
                    "type": order_type, "title": title,
                    "surl": source_url, "furl": file_url,
                },
            )
            logger.debug(f"DB: создан приказ {order_id}")
            return order_id

    async def update_order_status(
        self,
        order_id: str,
        status: str,
        file_hash: Optional[str] = None,
        ocr_method: Optional[str] = None,
        ocr_confidence: Optional[float] = None,
        page_count: Optional[int] = None,
        error_message: Optional[str] = None,
    ):
        """Обновляет статус и метаданные приказа."""
        if not self._engine:
            return

        from sqlalchemy import text

        fields = ["status = :status"]
        params: dict = {"id": order_id, "status": status}

        if file_hash is not None:
            fields.append("file_hash = :hash")
            params["hash"] = file_hash
        if ocr_method is not None:
            fields.append("ocr_method = :ocr")
            params["ocr"] = ocr_method
        if ocr_confidence is not None:
            fields.append("ocr_confidence = :conf")
            params["conf"] = ocr_confidence
        if page_count is not None:
            fields.append("page_count = :pages")
            params["pages"] = page_count
        if error_message is not None:
            fields.append("error_message = :err")
            params["err"] = error_message[:1000]
        if status == "extracted":
            fields.append("extracted_at = NOW()")

        sql = f"UPDATE orders SET {', '.join(fields)} WHERE id = :id"

        async with self._engine.begin() as conn:
            await conn.execute(text(sql), params)

    async def save_assignments(
        self,
        order_id: str,
        rows: list[dict],
    ) -> int:
        """
        Сохраняет записи присвоений для приказа.
        Атомарно: удаляет старые записи и вставляет новые.
        Возвращает количество вставленных записей.
        """
        if not self._engine:
            return len(rows)

        from sqlalchemy import text

        async with self._engine.begin() as conn:
            # Удаляем старые записи (для reprocess)
            await conn.execute(
                text("DELETE FROM assignments WHERE order_id = :oid"),
                {"oid": order_id},
            )

            count = 0
            for row in rows:
                try:
                    await conn.execute(
                        text("""
                            INSERT INTO assignments (
                                id, order_id, fio, fio_normalized,
                                birth_date, birth_date_raw,
                                ias_id, submission_number,
                                assignment_type, rank_category,
                                sport, sport_original, sport_id,
                                action, extra_fields,
                                llm_model, confidence
                            ) VALUES (
                                :id, :oid, :fio, NULL,
                                :bdate, :bdate_raw,
                                :ias_id, :sub_num,
                                :atype, :rank,
                                :sport, :sport_orig, :sport_id,
                                :action, :extra::jsonb,
                                :model, :conf
                            )
                        """),
                        {
                            "id": str(uuid4()),
                            "oid": order_id,
                            "fio": row.get("fio", ""),
                            "bdate": self._parse_date(row.get("birth_date")),
                            "bdate_raw": row.get("birth_date"),
                            "ias_id": row.get("ias_id"),
                            "sub_num": row.get("submission_number"),
                            "atype": row.get("assignment_type", "sport_rank"),
                            "rank": row.get("rank_category", ""),
                            "sport": row.get("sport"),
                            "sport_orig": row.get("sport_original"),
                            "sport_id": row.get("sport_id"),
                            "action": row.get("action", "assignment"),
                            "extra": json.dumps(
                                row.get("extra_fields", {}), ensure_ascii=False
                            ),
                            "model": row.get("llm_model"),
                            "conf": row.get("confidence"),
                        },
                    )
                    count += 1
                except Exception as e:
                    logger.warning(f"DB: ошибка записи: {e} | fio={row.get('fio')}")

            return count

    async def log_processing(
        self,
        order_id: Optional[str],
        source_code: str,
        level: str,
        stage: str,
        message: str,
        details: Optional[dict] = None,
    ):
        """Записывает лог обработки."""
        if not self._engine:
            return

        from sqlalchemy import text

        async with self._engine.begin() as conn:
            # Находим source_id по коду
            source_id = None
            if source_code:
                row = await conn.execute(
                    text("SELECT id FROM registry_sources WHERE code = :code"),
                    {"code": source_code},
                )
                r = row.fetchone()
                if r:
                    source_id = str(r[0])

            await conn.execute(
                text("""
                    INSERT INTO processing_log (id, order_id, source_id, level, stage, message, details)
                    VALUES (:id, :oid, :sid, :level, :stage, :msg, :det::jsonb)
                """),
                {
                    "id": str(uuid4()),
                    "oid": order_id,
                    "sid": source_id,
                    "level": level,
                    "stage": stage,
                    "msg": message[:2000],
                    "det": json.dumps(details or {}, ensure_ascii=False),
                },
            )

    async def get_pending_orders(self, limit: int = 50) -> list[dict]:
        """Возвращает приказы со статусом 'new' или 'downloaded'."""
        if not self._engine:
            return []

        from sqlalchemy import text

        async with self._engine.begin() as conn:
            rows = await conn.execute(
                text("""
                    SELECT o.id, o.order_number, o.order_date, o.order_type,
                           o.source_url, o.file_url, o.file_hash, o.status,
                           rs.code as source_code
                    FROM orders o
                    JOIN registry_sources rs ON o.source_id = rs.id
                    WHERE o.status IN ('new', 'downloaded')
                    ORDER BY o.created_at ASC
                    LIMIT :limit
                """),
                {"limit": limit},
            )
            return [dict(r._mapping) for r in rows.fetchall()]

    async def check_file_exists(self, file_hash: str) -> Optional[str]:
        """Проверяет, существует ли приказ с таким file_hash. Возвращает order_id."""
        if not self._engine:
            return None

        from sqlalchemy import text

        async with self._engine.begin() as conn:
            row = await conn.execute(
                text("SELECT id FROM orders WHERE file_hash = :hash LIMIT 1"),
                {"hash": file_hash},
            )
            r = row.fetchone()
            return str(r[0]) if r else None

    @staticmethod
    def _parse_date(date_str: Optional[str]) -> Optional[str]:
        """Парсит дату из формата ДД.ММ.ГГГГ в YYYY-MM-DD для PostgreSQL."""
        if not date_str:
            return None
        import re
        m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", date_str)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        return None


# ---------------------------------------------------------------------------
# Оркестратор
# ---------------------------------------------------------------------------

class PipelineOrchestrator:
    """
    Центральный оркестратор конвейера SportRank.

    Связывает: PdfDownloader → OcrPipeline → LLMExtractor →
               SportNormalizer → DbAdapter → processing_log
    """

    def __init__(
        self,
        # БД (опционально — без неё работает в dry-run)
        db_url: Optional[str] = None,

        # LLM (обязательно для извлечения данных)
        anthropic_api_key: Optional[str] = None,
        llm_model: str = "claude-haiku-4-5-20251001",

        # OCR
        enable_vision: bool = False,

        # Справочник видов спорта (путь к XLS)
        sport_registry_xls: Optional[str] = None,

        # PDF downloader
        pdf_output_dir: str = "./pdfs",
        playwright_headless: bool = True,
    ):
        self._db_url = db_url
        self._api_key = anthropic_api_key
        self._llm_model = llm_model
        self._enable_vision = enable_vision
        self._sport_xls = sport_registry_xls
        self._pdf_dir = pdf_output_dir
        self._headless = playwright_headless

        # Компоненты (инициализируются в initialize())
        self.db: Optional[DbAdapter] = None
        self.downloader = None  # PdfDownloader
        self.ocr = None         # OcrPipeline
        self.extractor = None   # LLMExtractor
        self.rule_extractor = None  # RuleExtractor (fallback без LLM)
        self.normalizer = None  # SportNormalizer

        self._initialized = False

    async def initialize(self):
        """
        Инициализирует все компоненты.
        Вызывать перед первым process_*().
        """
        logger.info("Pipeline: инициализация компонентов...")

        # DB
        self.db = DbAdapter(self._db_url)
        await self.db.connect()

        # PDF Downloader
        try:
            from pdf_downloader import PdfDownloader
            self.downloader = PdfDownloader(
                output_dir=self._pdf_dir,
                playwright_headless=self._headless,
            )
        except ImportError as e:
            logger.warning(f"Pipeline: PdfDownloader недоступен ({e}). process_url() отключён.")
            self.downloader = None

        # OCR Pipeline
        try:
            from ocr_pipeline import OcrPipeline
            self.ocr = OcrPipeline(
                enable_vision=self._enable_vision and bool(self._api_key),
                anthropic_api_key=self._api_key if self._enable_vision else None,
            )
        except ImportError as e:
            logger.error(f"Pipeline: OcrPipeline недоступен ({e}).")
            self.ocr = None

        # LLM Extractor
        if self._api_key:
            try:
                from llm_extractor import LLMExtractor
                self.extractor = LLMExtractor(
                    api_key=self._api_key,
                    model=self._llm_model,
                )
            except ImportError as e:
                logger.warning(f"Pipeline: LLMExtractor недоступен ({e}).")
                self.extractor = None
        else:
            logger.warning("Pipeline: нет API key — LLM Extractor отключён")

        # Rule-based Extractor (fallback при отсутствии LLM)
        try:
            from rule_extractor import RuleExtractor
            # normalizer будет установлен ниже, пока None
            self.rule_extractor = RuleExtractor(sport_normalizer=None)
            logger.info("Pipeline: RuleExtractor загружен (fallback)")
        except ImportError as e:
            logger.warning(f"Pipeline: RuleExtractor недоступен ({e})")
            self.rule_extractor = None

        # Sport Normalizer
        from sport_normalizer import SportNormalizer
        self.normalizer = SportNormalizer()
        if self._sport_xls and Path(self._sport_xls).exists():
            self.normalizer.load_xls(self._sport_xls)
            logger.info(f"Pipeline: справочник ВРВС загружен из {self._sport_xls}")
        else:
            logger.info("Pipeline: справочник ВРВС не указан, нормализация ограничена")

        # Привязываем normalizer к rule_extractor
        if self.rule_extractor and self.normalizer:
            self.rule_extractor.sport_normalizer = self.normalizer

        self._initialized = True
        logger.info("Pipeline: все компоненты инициализированы ✓")

    async def shutdown(self):
        """Корректно завершает все компоненты."""
        if self.downloader:
            await self.downloader.close()
        if self.db:
            await self.db.close()
        logger.info("Pipeline: shutdown ✓")

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, *args):
        await self.shutdown()

    # ------------------------------------------------------------------
    # Режим 1: Обработка локального файла
    # ------------------------------------------------------------------

    async def process_file(
        self,
        pdf_path: Union[str, Path],
        source_code: str,
        order_number: str = "",
        order_date: str = "",
        title: Optional[str] = None,
    ) -> PipelineResult:
        """
        Обрабатывает локальный PDF-файл.
        Проходит шаги: OCR → LLM Extract → Normalize → Save.
        (Шаг Download пропускается.)
        """
        self._ensure_initialized()
        t0 = time.monotonic()
        result = PipelineResult(
            source_code=source_code,
            order_number=order_number,
            order_date=order_date,
        )

        path = Path(pdf_path)
        if not path.exists():
            result.error = f"Файл не найден: {path}"
            result.status = "failed"
            return result

        pdf_bytes = path.read_bytes()

        # Проверка лимитов PDF
        if not validate_pdf_size(pdf_bytes):
            result.error = f"PDF превышает лимит ({len(pdf_bytes) / 1024 / 1024:.1f} MB)"
            result.status = "failed"
            result.total_duration_ms = int((time.monotonic() - t0) * 1000)
            return result

        result.file_hash = hashlib.sha256(pdf_bytes).hexdigest()

        # Идемпотентность: проверяем file_hash
        if self.db and self.db.connected:
            existing = await self.db.check_file_exists(result.file_hash)
            if existing:
                logger.info(
                    f"Pipeline: файл уже обработан (hash={result.file_hash[:12]}), "
                    f"order_id={existing}"
                )
                result.order_id = existing
                result.status = "extracted"
                result.success = True
                result.steps.append(StepResult(
                    step="dedup", status=StepStatus.SKIPPED,
                    message=f"Файл уже обработан: {existing}",
                ))
                return result

        # Создаём запись приказа в БД
        meta = SOURCE_METADATA.get(source_code, {})
        order_type = meta.get("order_type", "приказ")
        issuing_body = meta.get("issuing_body", source_code)

        if self.db and self.db.connected:
            result.order_id = await self.db.get_or_create_order(
                source_code=source_code,
                order_number=order_number,
                order_date=order_date,
                order_type=order_type,
                title=title,
            )

        # Шаг 1: OCR
        ocr_result = await self._step_ocr(pdf_bytes, result)
        if not ocr_result:
            result.total_duration_ms = int((time.monotonic() - t0) * 1000)
            return result

        text = ocr_result

        # Шаг 2: LLM Extract (с rule-based fallback)
        rows = await self._step_extract(
            text, issuing_body, order_date, order_number, result,
            source_code=source_code,
        )
        if rows is None:
            result.total_duration_ms = int((time.monotonic() - t0) * 1000)
            return result

        # Шаг 3: Normalize sports
        rows = self._step_normalize(rows, result)

        # Шаг 4: Save to DB
        await self._step_save(rows, result)

        result.total_duration_ms = int((time.monotonic() - t0) * 1000)
        logger.info(result.summary())
        return result

    # ------------------------------------------------------------------
    # Режим 2: Обработка по URL
    # ------------------------------------------------------------------

    async def process_url(
        self,
        url: str,
        source_code: str,
        order_number: str = "",
        order_date: str = "",
        title: Optional[str] = None,
    ) -> PipelineResult:
        """
        Скачивает PDF по URL и обрабатывает.
        Полный конвейер: Download → OCR → Extract → Normalize → Save.
        """
        self._ensure_initialized()
        t0 = time.monotonic()
        result = PipelineResult(
            source_code=source_code,
            order_number=order_number,
            order_date=order_date,
        )

        # SSRF-защита: проверка URL
        if not validate_url(url, source_code):
            result.error = f"URL заблокирован (SSRF): {url}"
            result.status = "failed"
            result.total_duration_ms = int((time.monotonic() - t0) * 1000)
            return result

        # Шаг 0: Download
        pdf_path = await self._step_download(url, source_code, result)
        if not pdf_path:
            result.total_duration_ms = int((time.monotonic() - t0) * 1000)
            return result

        # Дальше как process_file
        pdf_bytes = pdf_path.read_bytes()

        # Проверка лимитов PDF
        if not validate_pdf_size(pdf_bytes):
            result.error = f"PDF превышает лимит ({len(pdf_bytes) / 1024 / 1024:.1f} MB)"
            result.status = "failed"
            result.total_duration_ms = int((time.monotonic() - t0) * 1000)
            return result

        result.file_hash = hashlib.sha256(pdf_bytes).hexdigest()

        # Идемпотентность
        if self.db and self.db.connected:
            existing = await self.db.check_file_exists(result.file_hash)
            if existing:
                result.order_id = existing
                result.status = "extracted"
                result.success = True
                result.steps.append(StepResult(
                    step="dedup", status=StepStatus.SKIPPED,
                    message=f"Файл уже обработан: {existing}",
                ))
                result.total_duration_ms = int((time.monotonic() - t0) * 1000)
                return result

        meta = SOURCE_METADATA.get(source_code, {})
        order_type = meta.get("order_type", "приказ")
        issuing_body = meta.get("issuing_body", source_code)

        if self.db and self.db.connected:
            result.order_id = await self.db.get_or_create_order(
                source_code=source_code,
                order_number=order_number,
                order_date=order_date,
                order_type=order_type,
                title=title,
                source_url=url,
                file_url=url,
            )
            await self.db.update_order_status(
                result.order_id, "downloaded",
                file_hash=result.file_hash,
            )

        # OCR → Extract → Normalize → Save
        text = await self._step_ocr(pdf_bytes, result)
        if not text:
            result.total_duration_ms = int((time.monotonic() - t0) * 1000)
            return result

        rows = await self._step_extract(
            text, issuing_body, order_date, order_number, result,
            source_code=source_code,
        )
        if rows is None:
            result.total_duration_ms = int((time.monotonic() - t0) * 1000)
            return result

        rows = self._step_normalize(rows, result)
        await self._step_save(rows, result)

        result.total_duration_ms = int((time.monotonic() - t0) * 1000)
        logger.info(result.summary())
        return result

    # ------------------------------------------------------------------
    # Режим 3: Пакетная обработка из БД
    # ------------------------------------------------------------------

    async def process_pending(self, limit: int = 50) -> list[PipelineResult]:
        """
        Обрабатывает приказы со статусом 'new'/'downloaded' из БД.
        Возвращает список результатов.
        """
        self._ensure_initialized()
        if not self.db or not self.db.connected:
            logger.error("Pipeline: process_pending требует подключения к БД")
            return []

        pending = await self.db.get_pending_orders(limit=limit)
        logger.info(f"Pipeline: {len(pending)} приказов в очереди")

        results = []
        for order in pending:
            try:
                if order.get("file_url"):
                    r = await self.process_url(
                        url=order["file_url"],
                        source_code=order["source_code"],
                        order_number=order["order_number"],
                        order_date=str(order["order_date"]),
                    )
                elif order.get("source_url"):
                    r = await self.process_url(
                        url=order["source_url"],
                        source_code=order["source_code"],
                        order_number=order["order_number"],
                        order_date=str(order["order_date"]),
                    )
                else:
                    r = PipelineResult(
                        source_code=order["source_code"],
                        order_number=order["order_number"],
                        error="Нет URL для скачивания",
                        status="failed",
                    )
                results.append(r)
            except Exception as e:
                logger.error(
                    f"Pipeline: необработанная ошибка для {order['order_number']}: {e}"
                )
                results.append(PipelineResult(
                    source_code=order.get("source_code", ""),
                    order_number=order.get("order_number", ""),
                    error=str(e),
                    status="failed",
                ))

        # Сводка
        ok = sum(1 for r in results if r.success)
        fail = len(results) - ok
        total_records = sum(r.records_saved for r in results)
        logger.info(
            f"Pipeline: пакет завершён — {ok} успешных, {fail} ошибок, "
            f"{total_records} записей всего"
        )

        return results

    # ------------------------------------------------------------------
    # Режим 4: Повторная обработка
    # ------------------------------------------------------------------

    async def reprocess(self, order_id: str) -> PipelineResult:
        """
        Повторно обрабатывает приказ по его UUID.
        Удаляет старые записи, перезапускает extract + normalize + save.
        """
        self._ensure_initialized()
        if not self.db or not self.db.connected:
            raise RuntimeError("reprocess требует подключения к БД")

        from sqlalchemy import text

        # Получаем данные приказа
        async with self.db._engine.begin() as conn:
            row = await conn.execute(
                text("""
                    SELECT o.*, rs.code as source_code
                    FROM orders o
                    JOIN registry_sources rs ON o.source_id = rs.id
                    WHERE o.id = :id
                """),
                {"id": order_id},
            )
            order = row.fetchone()
            if not order:
                raise ValueError(f"Приказ не найден: {order_id}")

        order_dict = dict(order._mapping)

        # Сбрасываем статус
        await self.db.update_order_status(order_id, "downloaded")

        # Если есть file_url — перекачиваем и перепроцессим
        if order_dict.get("file_url"):
            return await self.process_url(
                url=order_dict["file_url"],
                source_code=order_dict["source_code"],
                order_number=order_dict["order_number"],
                order_date=str(order_dict["order_date"]),
            )

        raise ValueError(
            f"Приказ {order_id} не имеет file_url для повторной обработки"
        )

    # ------------------------------------------------------------------
    # Шаги конвейера (internal)
    # ------------------------------------------------------------------

    async def _step_download(
        self,
        url: str,
        source_code: str,
        result: PipelineResult,
    ) -> Optional[Path]:
        """Шаг 0: Скачивание PDF."""
        step = StepResult(step="download", status=StepStatus.RUNNING)
        t0 = time.monotonic()

        try:
            from pdf_downloader import DownloadError, AntibotDetected

            if not self.downloader:
                step.status = StepStatus.FAILED
                step.message = "PdfDownloader не инициализирован (зависимости не установлены)"
                result.steps.append(step)
                result.error = step.message
                result.status = "failed"
                return None

            path = await self.downloader.download(url, source_code)
            step.status = StepStatus.SUCCESS
            step.message = f"Скачан: {path.name}"
            step.details = {"path": str(path), "url": url}
            result.steps.append(step)

            await self._log(result, "info", "download", f"Скачан: {path.name}")
            return path

        except AntibotDetected as e:
            step.status = StepStatus.FAILED
            step.message = f"Антибот: {e}"
            result.steps.append(step)
            result.error = f"Антибот-защита: {e}"
            result.status = "failed"
            await self._log(result, "error", "download", str(e))
            return None

        except (DownloadError, Exception) as e:
            step.status = StepStatus.FAILED
            step.message = str(e)
            result.steps.append(step)
            result.error = f"Ошибка загрузки: {e}"
            result.status = "failed"
            await self._log(result, "error", "download", str(e))
            return None

        finally:
            step.duration_ms = int((time.monotonic() - t0) * 1000)

    async def _step_ocr(
        self,
        pdf_bytes: bytes,
        result: PipelineResult,
    ) -> Optional[str]:
        """Шаг 1: Извлечение текста (OCR Pipeline)."""
        step = StepResult(step="ocr", status=StepStatus.RUNNING)
        t0 = time.monotonic()

        try:
            from ocr_pipeline import OcrError

            ocr_result = await self.ocr.process_bytes(pdf_bytes)

            result.page_count = ocr_result.page_count
            result.text_length = len(ocr_result.text)
            result.ocr_method = ocr_result.method.value
            result.ocr_confidence = ocr_result.confidence

            step.status = StepStatus.SUCCESS
            step.message = (
                f"{ocr_result.page_count} стр., "
                f"{len(ocr_result.text):,} символов, "
                f"метод={ocr_result.method.value}"
            )
            step.details = {
                "page_count": ocr_result.page_count,
                "text_length": len(ocr_result.text),
                "method": ocr_result.method.value,
                "confidence": ocr_result.confidence,
                "methods_used": ocr_result.methods_used,
            }
            result.steps.append(step)

            # Обновляем статус в БД
            if result.order_id and self.db and self.db.connected:
                await self.db.update_order_status(
                    result.order_id, "downloaded",
                    file_hash=result.file_hash,
                    ocr_method=ocr_result.method.value,
                    ocr_confidence=ocr_result.confidence,
                    page_count=ocr_result.page_count,
                )

            await self._log(result, "info", "ocr", step.message, step.details)
            return ocr_result.text

        except OcrError as e:
            step.status = StepStatus.FAILED
            step.message = str(e)
            result.steps.append(step)
            result.error = f"OCR ошибка: {e}"
            result.status = "failed"
            await self._log(result, "error", "ocr", str(e))
            await self._update_failed(result)
            return None

        except Exception as e:
            step.status = StepStatus.FAILED
            step.message = f"Unexpected: {e}"
            result.steps.append(step)
            result.error = str(e)
            result.status = "failed"
            await self._log(result, "error", "ocr", traceback.format_exc())
            await self._update_failed(result)
            return None

        finally:
            step.duration_ms = int((time.monotonic() - t0) * 1000)

    async def _step_extract(
        self,
        text: str,
        issuing_body: str,
        order_date: str,
        order_number: str,
        result: PipelineResult,
        source_code: str = "",
    ) -> Optional[list[dict]]:
        """Шаг 2: извлечение структурированных данных (LLM → rule fallback)."""
        step = StepResult(step="extract", status=StepStatus.RUNNING)
        t0 = time.monotonic()

        rows = None
        extract_method = None

        # Стратегия: LLM primary → rule_extractor fallback
        if self.extractor:
            try:
                # LLM extract — I/O-bound (API call), выносим из event loop
                rows = await asyncio.to_thread(
                    self.extractor.extract,
                    text=text,
                    issuing_body=issuing_body,
                    order_date=order_date,
                    order_number=order_number,
                )
                extract_method = self._llm_model
            except Exception as e:
                logger.warning(
                    f"LLM Extractor ошибка ({type(e).__name__}: {e}), "
                    f"переключение на RuleExtractor"
                )
                rows = None

        # Fallback на rule-based
        if rows is None and self.rule_extractor:
            try:
                # Rule-based extract — CPU-bound, выносим из event loop
                rows = await asyncio.to_thread(
                    self.rule_extractor.extract,
                    text=text,
                    issuing_body=issuing_body,
                    order_date=order_date,
                    order_number=order_number,
                    source_code=source_code,
                )
                extract_method = "rule_extractor"
            except Exception as e:
                logger.error(f"RuleExtractor ошибка: {e}")
                rows = None

        # Ни один экстрактор не сработал
        if rows is None or len(rows) == 0:
            step.status = StepStatus.FAILED
            step.message = "Ни один экстрактор не извлёк данных"
            result.steps.append(step)
            result.error = "Извлечение не удалось (LLM и rule_extractor)"
            result.status = "failed"
            step.duration_ms = int((time.monotonic() - t0) * 1000)
            await self._log(result, "error", "extract", step.message)
            await self._update_failed(result)
            return None

        try:
            # Конвертируем AssignmentRow → dict
            row_dicts = []
            for row in rows:
                d = row.to_dict()
                d["llm_model"] = extract_method
                row_dicts.append(d)

            result.records_extracted = len(row_dicts)

            step.status = StepStatus.SUCCESS
            step.message = f"Извлечено {len(row_dicts)} записей ({extract_method})"
            step.details = {
                "records": len(row_dicts),
                "text_length": len(text),
                "method": extract_method,
            }
            result.steps.append(step)

            await self._log(result, "info", "extract", step.message, step.details)
            return row_dicts

        except Exception as e:
            step.status = StepStatus.FAILED
            step.message = str(e)
            result.steps.append(step)
            result.error = f"Ошибка конвертации: {e}"
            result.status = "failed"
            await self._log(result, "error", "extract", traceback.format_exc())
            await self._update_failed(result)
            return None

        finally:
            step.duration_ms = int((time.monotonic() - t0) * 1000)

    def _step_normalize(
        self,
        rows: list[dict],
        result: PipelineResult,
    ) -> list[dict]:
        """Шаг 3: Нормализация разрядов (ЕВСК) и видов спорта (ВРВС)."""
        step = StepResult(step="normalize", status=StepStatus.RUNNING)
        t0 = time.monotonic()

        # 3a) Нормализация разрядов/званий (ЕВСК)
        ranks_normalized = 0
        for row in rows:
            rank_raw = row.get("rank_category") or row.get("rank") or ""
            if rank_raw:
                rank_norm = normalize_rank(rank_raw)
                if rank_norm and rank_norm != rank_raw.strip().lower():
                    row["rank_category_original"] = row.get("rank_category_original") or rank_raw
                    row["rank_category"] = rank_norm
                    ranks_normalized += 1

        # 3b) Нормализация видов спорта (ВРВС)
        if not self.normalizer:
            step.status = StepStatus.SKIPPED
            step.message = f"Нормализатор ВРВС не загружен; разрядов нормализовано: {ranks_normalized}"
            result.steps.append(step)
            return rows

        matched = 0
        unmatched = 0

        for row in rows:
            sport = row.get("sport")
            if not sport:
                continue

            norm_result = self.normalizer.normalize(sport)

            if norm_result.canonical_name:
                row["sport"] = norm_result.canonical_name
                if norm_result.sport_id:
                    row["sport_id"] = norm_result.sport_id
                # Сохраняем оригинал если изменился
                if sport != norm_result.canonical_name:
                    row["sport_original"] = row.get("sport_original") or sport
                matched += 1
            else:
                unmatched += 1

        result.sports_normalized = matched
        result.sports_unmatched = unmatched

        step.status = StepStatus.SUCCESS
        step.message = (
            f"Спорт: {matched} норм. / {unmatched} не найд.; разряды: {ranks_normalized} норм."
        )
        step.details = {"matched": matched, "unmatched": unmatched}
        step.duration_ms = int((time.monotonic() - t0) * 1000)
        result.steps.append(step)

        return rows

    async def _step_save(
        self,
        rows: list[dict],
        result: PipelineResult,
    ):
        """Шаг 4: Сохранение в БД."""
        step = StepResult(step="save", status=StepStatus.RUNNING)
        t0 = time.monotonic()

        try:
            if result.order_id and self.db and self.db.connected:
                saved = await self.db.save_assignments(result.order_id, rows)
                result.records_saved = saved

                await self.db.update_order_status(
                    result.order_id, "extracted",
                    ocr_method=result.ocr_method,
                    ocr_confidence=result.ocr_confidence,
                    page_count=result.page_count,
                )
            else:
                # Dry-run: считаем всё успешным
                result.records_saved = len(rows)

            result.success = True
            result.status = "extracted"

            step.status = StepStatus.SUCCESS
            step.message = f"Сохранено {result.records_saved} записей"
            step.details = {"saved": result.records_saved}
            result.steps.append(step)

            await self._log(
                result, "info", "save",
                f"Сохранено {result.records_saved} записей из {result.records_extracted}",
            )

        except Exception as e:
            step.status = StepStatus.FAILED
            step.message = str(e)
            result.steps.append(step)
            result.error = f"DB ошибка: {e}"
            result.status = "failed"
            await self._log(result, "error", "save", traceback.format_exc())
            await self._update_failed(result)

        finally:
            step.duration_ms = int((time.monotonic() - t0) * 1000)

    # ------------------------------------------------------------------
    # Утилиты
    # ------------------------------------------------------------------

    def _ensure_initialized(self):
        if not self._initialized:
            raise RuntimeError(
                "Pipeline не инициализирован. Вызовите await initialize() "
                "или используйте async with PipelineOrchestrator(...) as orch:"
            )

    async def _log(
        self,
        result: PipelineResult,
        level: str,
        stage: str,
        message: str,
        details: Optional[dict] = None,
    ):
        """Записывает лог в БД и в logger."""
        log_msg = f"[{result.source_code}] [{stage}] {message}"
        if level == "error":
            logger.error(log_msg)
        elif level == "warn":
            logger.warning(log_msg)
        else:
            logger.debug(log_msg)

        if self.db and self.db.connected:
            try:
                await self.db.log_processing(
                    order_id=result.order_id,
                    source_code=result.source_code,
                    level=level,
                    stage=stage,
                    message=message,
                    details=details,
                )
            except Exception as e:
                logger.warning(f"Ошибка записи лога в БД: {e}")

    async def _update_failed(self, result: PipelineResult):
        """Обновляет статус приказа в БД на 'failed'."""
        if result.order_id and self.db and self.db.connected:
            await self.db.update_order_status(
                result.order_id, "failed",
                error_message=result.error,
            )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

async def _main():
    import argparse
    import os

    parser = argparse.ArgumentParser(
        description="SportRank Pipeline Orchestrator"
    )
    sub = parser.add_subparsers(dest="command")

    # process-file
    p_file = sub.add_parser("process-file", help="Обработать локальный PDF")
    p_file.add_argument("pdf", help="Путь к PDF-файлу")
    p_file.add_argument("--source", required=True, help="Код источника")
    p_file.add_argument("--number", default="", help="Номер приказа")
    p_file.add_argument("--date", default="", help="Дата приказа (ДД.ММ.ГГГГ)")
    p_file.add_argument("--db", default=None, help="PostgreSQL URL")
    p_file.add_argument("--sport-xls", default=None, help="Путь к XLS ВРВС")

    # process-url
    p_url = sub.add_parser("process-url", help="Скачать и обработать по URL")
    p_url.add_argument("url", help="URL страницы или PDF")
    p_url.add_argument("--source", required=True, help="Код источника")
    p_url.add_argument("--number", default="", help="Номер приказа")
    p_url.add_argument("--date", default="", help="Дата приказа (ДД.ММ.ГГГГ)")
    p_url.add_argument("--db", default=None, help="PostgreSQL URL")
    p_url.add_argument("--sport-xls", default=None, help="Путь к XLS ВРВС")

    # process-pending
    p_pending = sub.add_parser("process-pending", help="Обработать очередь из БД")
    p_pending.add_argument("--db", required=True, help="PostgreSQL URL")
    p_pending.add_argument("--limit", type=int, default=50)
    p_pending.add_argument("--sport-xls", default=None, help="Путь к XLS ВРВС")

    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if getattr(args, "verbose", False) else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    api_key = os.environ.get("ANTHROPIC_API_KEY")

    if args.command == "process-file":
        async with PipelineOrchestrator(
            db_url=args.db,
            anthropic_api_key=api_key,
            sport_registry_xls=args.sport_xls,
        ) as orch:
            result = await orch.process_file(
                args.pdf,
                source_code=args.source,
                order_number=args.number,
                order_date=args.date,
            )
            _print_result(result)

    elif args.command == "process-url":
        async with PipelineOrchestrator(
            db_url=args.db,
            anthropic_api_key=api_key,
            sport_registry_xls=args.sport_xls,
        ) as orch:
            result = await orch.process_url(
                args.url,
                source_code=args.source,
                order_number=args.number,
                order_date=args.date,
            )
            _print_result(result)

    elif args.command == "process-pending":
        async with PipelineOrchestrator(
            db_url=args.db,
            anthropic_api_key=api_key,
            sport_registry_xls=getattr(args, "sport_xls", None),
        ) as orch:
            results = await orch.process_pending(limit=args.limit)
            print(f"\nОбработано: {len(results)}")
            ok = sum(1 for r in results if r.success)
            print(f"  Успешно:  {ok}")
            print(f"  Ошибки:   {len(results) - ok}")
            print(f"  Записей:  {sum(r.records_saved for r in results)}")

    else:
        parser.print_help()


def _print_result(result: PipelineResult):
    """Красивый вывод результата в CLI."""
    print()
    print("=" * 60)
    print(result.summary())
    print("=" * 60)
    print(f"  Order ID:       {result.order_id or '(dry-run)'}")
    print(f"  File hash:      {(result.file_hash or '')[:16]}...")
    print(f"  Страниц:        {result.page_count}")
    print(f"  OCR метод:      {result.ocr_method}")
    print(f"  OCR уверенн.:   {result.ocr_confidence:.2f}")
    print(f"  Текст:          {result.text_length:,} символов")
    print(f"  Извлечено:      {result.records_extracted} записей")
    print(f"  Сохранено:      {result.records_saved} записей")
    print(f"  Спорт нормализ: {result.sports_normalized}")
    print(f"  Спорт не найд:  {result.sports_unmatched}")
    print(f"  Время:          {result.total_duration_ms}ms")
    print()
    print("Шаги:")
    for s in result.steps:
        icon = {"success": "✅", "failed": "❌", "skipped": "⏭️"}.get(
            s.status.value, "⏳"
        )
        print(f"  {icon} {s.step:12s} {s.duration_ms:>5d}ms  {s.message}")
    if result.error:
        print(f"\n  ⚠️  Ошибка: {result.error}")


if __name__ == "__main__":
    asyncio.run(_main())
