"""
source_registry.py
==================
Единый реестр источников SportRank.

Консолидирует три ранее раздельных конфигурации:
  - pdf_downloader.SOURCE_CONFIG  (параметры загрузки)
  - change_detector.SOURCE_PATTERNS (паттерны обнаружения)
  - pipeline_orchestrator.SOURCE_METADATA (метаданные извлечения)

Каждый источник описывается одним dataclass SourceConfig, содержащим
ВСЕ параметры для всех модулей.  Модули получают конфиг через:

    from source_registry import SOURCES, get_source
    cfg = get_source("spb_kfkis")
    cfg.download.method   # "httpx"
    cfg.detect.link_regex # r'...'
    cfg.meta.issuing_body # "Комитет по ФКиС..."

В продакшене параметры могут загружаться из таблицы registry_sources
(столбец discovery_config JSONB) — этот модуль служит fallback'ом
и единственным местом для ручного редактирования конфигурации.

Зависимости: только стандартная библиотека Python.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Конфигурация: загрузка (pdf_downloader)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DownloadConfig:
    """Параметры загрузки PDF для pdf_downloader."""
    method: str = "httpx"                   # "httpx" | "playwright"
    base_url: str = ""                      # корневой URL для urljoin
    antibot: Optional[str] = None           # "servicepipe" | None
    delay_min: float = 1.0                  # мин. задержка между запросами (сек)
    delay_max: float = 3.0                  # макс. задержка
    wait_selector: Optional[str] = None     # CSS-селектор для Playwright
    max_retries: int = 3                    # попытки скачивания
    timeout: int = 30                       # таймаут одного запроса (сек)


# ---------------------------------------------------------------------------
# Конфигурация: обнаружение (change_detector)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DetectConfig:
    """Параметры обнаружения новых документов для change_detector."""
    list_urls: tuple[str, ...] = ()         # страницы со списками документов
    link_regex: str = r'href=["\']([^"\']*\.pdf)["\']'
    title_regex: Optional[str] = None       # regex для заголовка документа
    order_date_regex: Optional[str] = None  # regex для даты приказа
    order_number_regex: Optional[str] = None
    source_type: str = "pdf_portal"         # "pdf_portal" | "json_embed"
    js_var: Optional[str] = None            # имя JS-переменной (json_embed)
    pagination: Optional[str] = None        # шаблон пагинации (?page={n})
    max_pages: int = 1                      # макс. страниц для обхода


# ---------------------------------------------------------------------------
# Конфигурация: метаданные (pipeline_orchestrator)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MetaConfig:
    """Метаданные источника для pipeline/extractor."""
    issuing_body: str = ""                  # кто издаёт приказ
    order_type: str = "приказ"              # "приказ" | "распоряжение"
    region: str = ""                        # регион
    official_basis: str = ""                # нормативная основа (8-ФЗ, 152-ФЗ)


# ---------------------------------------------------------------------------
# Единый конфиг источника
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceConfig:
    """Полная конфигурация одного источника."""
    code: str                               # уникальный код (moskva_tstisk)
    name: str                               # человекочитаемое название
    risk_class: str = "green"               # green | amber | red
    active: bool = True
    download: DownloadConfig = field(default_factory=DownloadConfig)
    detect: DetectConfig = field(default_factory=DetectConfig)
    meta: MetaConfig = field(default_factory=MetaConfig)

    # --- Совместимость со старыми модулями ---

    def to_download_config(self) -> dict:
        """Формат SOURCE_CONFIG для pdf_downloader."""
        return {
            "method": self.download.method,
            "base_url": self.download.base_url,
            "antibot": self.download.antibot,
            "delay_range": (self.download.delay_min, self.download.delay_max),
            "wait_selector": self.download.wait_selector,
        }

    def to_detect_pattern(self) -> dict:
        """Формат SOURCE_PATTERNS для change_detector."""
        d = {
            "list_urls": list(self.detect.list_urls),
            "link_regex": self.detect.link_regex,
            "method": self.download.method,  # метод общий
        }
        if self.detect.title_regex:
            d["title_regex"] = self.detect.title_regex
        if self.detect.order_date_regex:
            d["order_date_regex"] = self.detect.order_date_regex
        if self.detect.order_number_regex:
            d["order_number_regex"] = self.detect.order_number_regex
        if self.detect.source_type != "pdf_portal":
            d["source_type"] = self.detect.source_type
        if self.detect.js_var:
            d["js_var"] = self.detect.js_var
        if self.detect.pagination:
            d["pagination"] = self.detect.pagination
            d["max_pages"] = self.detect.max_pages
        return d

    def to_meta(self) -> dict:
        """Формат SOURCE_METADATA для pipeline_orchestrator."""
        return {
            "issuing_body": self.meta.issuing_body,
            "order_type": self.meta.order_type,
        }


# ============================================================================
# РЕЕСТР ИСТОЧНИКОВ
# ============================================================================

SOURCES: dict[str, SourceConfig] = {

    # --- Москва: ЦСТиСК Москомспорта ---
    "moskva_tstisk": SourceConfig(
        code="moskva_tstisk",
        name="ГКУ «ЦСТиСК» Москомспорта",
        risk_class="amber",
        active=True,
        download=DownloadConfig(
            method="playwright",
            base_url="https://www.mos.ru",
            antibot="servicepipe",
            delay_min=2, delay_max=6,
            wait_selector="a[href$='.pdf']",
        ),
        detect=DetectConfig(
            list_urls=(
                "https://www.mos.ru/moskomsport/documents/prisvoenie-sportivnykh-razryadov-po-vidam-sporta/",
                "https://www.mos.ru/moskomsport/documents/prisvoenie-kvalifikatsionnykh-kategoriy-sportivnykh-sudey/",
            ),
            link_regex=r'href=["\']([^"\']*view/\d+[^"\']*)["\']',
            title_regex=r'>([^<]*(?:Приказ|Распоряжение)[^<]*)<',
            order_date_regex=r'от\s+(\d{1,2}[\.\s]\d{2}[\.\s]\d{4})',
            order_number_regex=r'[№N]\s*(\S+)',
        ),
        meta=MetaConfig(
            issuing_body="ГКУ «ЦСТиСК» Москомспорта",
            order_type="приказ",
            region="г. Москва",
            official_basis="8-ФЗ",
        ),
    ),

    # --- Москва: Москомспорт ---
    "moskva_moskumsport": SourceConfig(
        code="moskva_moskumsport",
        name="Департамент спорта города Москвы (Москомспорт)",
        risk_class="amber",
        active=True,
        download=DownloadConfig(
            method="playwright",
            base_url="https://www.mos.ru",
            antibot="servicepipe",
            delay_min=2, delay_max=6,
            wait_selector="a[href$='.pdf']",
        ),
        detect=DetectConfig(
            list_urls=(
                "https://www.mos.ru/moskomsport/documents/prisvoenie-sportivnykh-razryadov-po-vidam-sporta/",
            ),
            link_regex=r'href=["\']([^"\']*view/\d+[^"\']*)["\']',
            title_regex=r'>([^<]*Распоряжение[^<]*)<',
        ),
        meta=MetaConfig(
            issuing_body="Департамент спорта города Москвы (Москомспорт)",
            order_type="распоряжение",
            region="г. Москва",
            official_basis="8-ФЗ",
        ),
    ),

    # --- Московская область: МОСОБЛСПОРТ ---
    "mo_mособлспорт": SourceConfig(
        code="mo_mособлспорт",
        name="Министерство физической культуры и спорта Московской области",
        risk_class="red",
        active=True,
        download=DownloadConfig(
            method="playwright",
            base_url="https://mst.mosreg.ru",
            antibot="servicepipe",
            delay_min=3, delay_max=8,
            wait_selector="a.document-link, a[href$='.pdf']",
        ),
        detect=DetectConfig(
            list_urls=(
                "https://mst.mosreg.ru/dokumenty/prisvoenie-sportivnykh-razryadov-"
                "kandidat-v-mastera-sporta-i-pervyi-sportivnyi-razryad",
            ),
            link_regex=r'href=["\']([^"\']*(?:rasporiaz|prikaz)[^"\']*)["\']',
            pagination="?page={n}",
            max_pages=3,
        ),
        meta=MetaConfig(
            issuing_body="Министерство физической культуры и спорта Московской области",
            order_type="распоряжение",
            region="Московская область",
            official_basis="8-ФЗ",
        ),
    ),

    # --- Санкт-Петербург: КФКиС ---
    "spb_kfkis": SourceConfig(
        code="spb_kfkis",
        name="Комитет по физической культуре и спорту Санкт-Петербурга",
        risk_class="green",
        active=True,
        download=DownloadConfig(
            method="httpx",
            base_url="https://kfis.gov.spb.ru",
            delay_min=1, delay_max=3,
        ),
        detect=DetectConfig(
            list_urls=(
                "https://kfis.gov.spb.ru/docs/?type=54",
            ),
            link_regex=r'href=["\']([^"\']*?(?:/docs/\d+|/documents/\d+)[^"\']*)["\']',
            title_regex=r'class=["\']doc-title["\'][^>]*>([^<]+)<',
            pagination="&page={n}",
            max_pages=3,
        ),
        meta=MetaConfig(
            issuing_body="Комитет по физической культуре и спорту Санкт-Петербурга",
            order_type="распоряжение",
            region="г. Санкт-Петербург",
            official_basis="8-ФЗ",
        ),
    ),

    # --- Краснодарский край ---
    "krasnodar_minsport": SourceConfig(
        code="krasnodar_minsport",
        name="Министерство физической культуры и спорта Краснодарского края",
        risk_class="green",
        active=True,
        download=DownloadConfig(
            method="httpx",
            base_url="https://minsport.krasnodar.ru",
            delay_min=1, delay_max=4,
        ),
        detect=DetectConfig(
            list_urls=(
                "https://minsport.krasnodar.ru/activities/sport/prisvoenie-sportivnyx-razryadov/",
            ),
            link_regex=r'href=["\']([^"\']*\.pdf)["\']',
        ),
        meta=MetaConfig(
            issuing_body="Министерство физической культуры и спорта Краснодарского края",
            order_type="приказ",
            region="Краснодарский край",
            official_basis="8-ФЗ",
        ),
    ),

    # --- Россия: Минспорт РФ (msrfinfo.ru) ---
    "rf_minsport": SourceConfig(
        code="rf_minsport",
        name="Министерство спорта Российской Федерации",
        risk_class="green",
        active=False,
        download=DownloadConfig(
            method="httpx",
            base_url="https://msrfinfo.ru",
            delay_min=1, delay_max=2,
        ),
        detect=DetectConfig(
            list_urls=(
                "https://msrfinfo.ru/awards/",
            ),
            source_type="json_embed",
            js_var="$obj",
        ),
        meta=MetaConfig(
            issuing_body="Министерство спорта Российской Федерации",
            order_type="приказ",
            region="Россия",
            official_basis="329-ФЗ",
        ),
    ),
}


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def get_source(code: str) -> Optional[SourceConfig]:
    """Получить конфигурацию источника по коду. None если не найден."""
    return SOURCES.get(code)


def get_active_sources() -> list[SourceConfig]:
    """Список активных источников."""
    return [s for s in SOURCES.values() if s.active]


def get_sources_by_risk(risk_class: str) -> list[SourceConfig]:
    """Источники с заданным risk_class."""
    return [s for s in SOURCES.values() if s.risk_class == risk_class]


def get_all_domains() -> set[str]:
    """Все разрешённые домены (для SSRF whitelist)."""
    from urllib.parse import urlparse
    domains = set()
    for s in SOURCES.values():
        if s.download.base_url:
            host = urlparse(s.download.base_url).hostname
            if host:
                domains.add(host)
        for url in s.detect.list_urls:
            host = urlparse(url).hostname
            if host:
                domains.add(host)
    return domains


# ---------------------------------------------------------------------------
# Compat: генерация старых словарей для обратной совместимости
# ---------------------------------------------------------------------------

def as_download_configs() -> dict[str, dict]:
    """Генерирует SOURCE_CONFIG в формате pdf_downloader."""
    return {code: src.to_download_config() for code, src in SOURCES.items()}


def as_detect_patterns() -> dict[str, dict]:
    """Генерирует SOURCE_PATTERNS в формате change_detector."""
    return {code: src.to_detect_pattern() for code, src in SOURCES.items()}


def as_meta() -> dict[str, dict]:
    """Генерирует SOURCE_METADATA в формате pipeline_orchestrator."""
    return {code: src.to_meta() for code, src in SOURCES.items()}


# ---------------------------------------------------------------------------
# CLI: информация о реестре
# ---------------------------------------------------------------------------

def main():
    """Вывод реестра в консоль."""
    import sys

    print(f"SportRank Source Registry: {len(SOURCES)} источников\n")
    print(f"{'Код':<24} {'Риск':<7} {'Акт.':<5} {'Метод':<12} {'Регион'}")
    print("-" * 80)
    for code, s in SOURCES.items():
        active = "✓" if s.active else "✗"
        print(f"{code:<24} {s.risk_class:<7} {active:<5} {s.download.method:<12} {s.meta.region}")

    print(f"\nРазрешённые домены: {', '.join(sorted(get_all_domains()))}")

    if "--compat" in sys.argv:
        import json
        print("\n--- SOURCE_CONFIG (pdf_downloader) ---")
        print(json.dumps(as_download_configs(), indent=2, ensure_ascii=False, default=str))
        print("\n--- SOURCE_PATTERNS (change_detector) ---")
        print(json.dumps(as_detect_patterns(), indent=2, ensure_ascii=False))
        print("\n--- SOURCE_METADATA (pipeline_orchestrator) ---")
        print(json.dumps(as_meta(), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
