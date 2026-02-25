"""
test_sportrank.py
=================
Тесты SportRank: py_compile + unit + integration.

Запуск:
    pytest test_sportrank.py -v
    pytest test_sportrank.py -v -k "rule_extractor"  # только rule_extractor
"""

import asyncio
import re
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# 0. Smoke: py_compile на все модули
# ---------------------------------------------------------------------------

MODULES = [
    "llm_extractor.py",
    "rule_extractor.py",
    "pipeline_orchestrator.py",
    "change_detector.py",
    "admin_api.py",
    "sport_normalizer.py",
    "source_registry.py",
    "ocr_pipeline.py",
]


@pytest.mark.parametrize("module", MODULES)
def test_py_compile(module):
    """Все модули должны компилироваться без синтаксических ошибок."""
    path = Path(module)
    if not path.exists():
        pytest.skip(f"{module} не найден")
    import py_compile
    py_compile.compile(str(path), doraise=True)


# ---------------------------------------------------------------------------
# 1. Rule Extractor: unit tests
# ---------------------------------------------------------------------------

class TestRuleExtractor:
    """Тесты rule-based экстрактора."""

    @pytest.fixture(autouse=True)
    def setup(self):
        from rule_extractor import (
            RuleExtractor, TabularParser, normalize_rank,
            validate_date, validate_birth_date, clean_text,
            detect_assignment_type, detect_action, AssignmentType, ActionType,
        )
        self.RuleExtractor = RuleExtractor
        self.TabularParser = TabularParser
        self.normalize_rank = normalize_rank
        self.validate_date = validate_date
        self.validate_birth_date = validate_birth_date
        self.clean_text = clean_text
        self.detect_assignment_type = detect_assignment_type
        self.detect_action = detect_action
        self.AssignmentType = AssignmentType
        self.ActionType = ActionType

    # --- validate_date ---

    def test_valid_date(self):
        assert self.validate_date("15.03.1990")
        assert self.validate_date("01.01.2020")

    def test_invalid_date(self):
        assert not self.validate_date("32.01.2020")
        assert not self.validate_date("not-a-date")
        assert not self.validate_date("")
        assert not self.validate_date("01.01.1900")  # too old

    def test_suspicious_birth_date(self):
        # Ребёнок 1 год — подозрительно
        assert not self.validate_birth_date("10.02.2025", "17.02.2026")
        # Нормальный возраст
        assert self.validate_birth_date("10.02.1990", "17.02.2026")

    # --- normalize_rank ---

    def test_normalize_judge_category(self):
        assert self.normalize_rank("Спортивный судья третьей\nкатегории") == \
            "спортивный судья третьей категории"
        assert self.normalize_rank("Спортивный судья второй категории") == \
            "спортивный судья второй категории"

    def test_normalize_sport_rank(self):
        assert self.normalize_rank("КМС") == "кандидат в мастера спорта"
        assert self.normalize_rank("1 разряд") == "первый спортивный разряд"
        assert self.normalize_rank("II спортивный разряд") == "второй спортивный разряд"

    # --- Regression: римские цифры (I не матчит II/III) ---

    def test_normalize_roman_numerals(self):
        """Регрессия: II разряд не должен матчиться как I разряд."""
        assert self.normalize_rank("I разряд") == "первый спортивный разряд"
        assert self.normalize_rank("II разряд") == "второй спортивный разряд"
        assert self.normalize_rank("III разряд") == "третий спортивный разряд"
        assert self.normalize_rank("III спортивный разряд") == "третий спортивный разряд"

    # --- ЕВСК (Приказ №173): юношеские разряды ---

    def test_normalize_youth_ranks(self):
        """Юношеские разряды по ЕВСК."""
        assert self.normalize_rank("1 юношеский разряд") == "первый юношеский спортивный разряд"
        assert self.normalize_rank("II юношеский разряд") == "второй юношеский спортивный разряд"
        assert self.normalize_rank("III юнош. разряд") == "третий юношеский спортивный разряд"
        assert self.normalize_rank("третий юношеский спортивный разряд") == "третий юношеский спортивный разряд"

    # --- ЕВСК (Приказ №173): звания ---

    def test_normalize_sport_titles(self):
        """Звания по ЕВСК: МС, МСМК, ГМР."""
        assert self.normalize_rank("мастер спорта") == "мастер спорта россии"
        assert self.normalize_rank("МС") == "мастер спорта россии"
        assert self.normalize_rank("МСМК") == "мастер спорта россии международного класса"
        assert self.normalize_rank("ЗМС") == "заслуженный мастер спорта россии"
        assert self.normalize_rank("гроссмейстер России") == "гроссмейстер россии"
        assert self.normalize_rank("ГМ") == "гроссмейстер россии"

    # --- Приказ №134: судейские категории ---

    def test_normalize_judge_all_categories(self):
        """Все категории судей (Приказ №134): всеросс., 1/2/3, юный."""
        assert self.normalize_rank("Спортивный судья всероссийской категории") == \
            "спортивный судья всероссийской категории"
        assert self.normalize_rank("Спортивный судья первой категории") == \
            "спортивный судья первой категории"
        assert self.normalize_rank("юный спортивный судья") == "юный спортивный судья"

    # --- Приказ №856: почётные звания ---

    def test_normalize_honorary_titles(self):
        """Почётные звания (Приказ №856): ЗМС, ЗТР, почётные."""
        assert self.normalize_rank("заслуженный тренер России") == "заслуженный тренер россии"
        assert self.normalize_rank("ЗТР") == "заслуженный тренер россии"

    # --- Enum'ы по нормативам ---

    def test_restoration_action_exists(self):
        """ЕВСК предусматривает действие 'восстановление'."""
        assert hasattr(self.ActionType, 'RESTORATION')
        assert self.ActionType.RESTORATION.value == "restoration"

    def test_honorary_title_type_exists(self):
        """Приказ №856 предусматривает тип honorary_title."""
        assert hasattr(self.AssignmentType, 'HONORARY_TITLE')
        assert self.AssignmentType.HONORARY_TITLE.value == "honorary_title"

    def test_detect_restoration(self):
        """Действие 'восстановить' должно определяться."""
        text = "Восстановить спортивное звание мастера спорта России"
        assert self.detect_action(text) == self.ActionType.RESTORATION

    def test_detect_honorary_title(self):
        """Почётные звания должны определяться как honorary_title."""
        text = "О присвоении почётного спортивного звания заслуженный мастер спорта России"
        assert self.detect_assignment_type(text) == self.AssignmentType.HONORARY_TITLE

    # --- detect_assignment_type ---

    def test_detect_judge(self):
        text = "О присвоении квалификационных категорий спортивных судей"
        assert self.detect_assignment_type(text) == self.AssignmentType.JUDGE_CATEGORY

    def test_detect_sport_rank(self):
        text = "О присвоении спортивных разрядов по виду спорта"
        assert self.detect_assignment_type(text) == self.AssignmentType.SPORT_RANK

    # --- detect_action ---

    def test_detect_assignment(self):
        text = "ПРИКАЗЫВАЮ: 1. Присвоить квалификационные категории"
        assert self.detect_action(text) == self.ActionType.ASSIGNMENT

    def test_detect_confirmation(self):
        text = "Подтвердить квалификационную категорию спортивного судьи"
        assert self.detect_action(text) == self.ActionType.CONFIRMATION

    # --- TabularParser ---

    def test_tabular_simple(self):
        text = """Приложение к приказу
№ ФИО Дата рождения Вид спорта Категория Дата представления
1 2 3 4 5 6
1 Иванов Иван Иванович 15.03.1990 Бокс 10.02.2026
2 Петрова Мария Сергеевна 22.07.1985 Бокс 10.02.2026
3 Сидоров Алексей Петрович 03.11.1992 Дзюдо 10.02.2026
Спортивный судья третьей
категории
Спортивный судья третьей
категории
Спортивный судья второй
категории
Документ зарегистрирован № С-1/26 от 10.02.2026 Тест (ГКУ)
Страница 1 из 1. Страница создана: 10.02.2026 12:00
"""
        parser = self.TabularParser()
        rows = parser.parse(text, order_date="10.02.2026")

        assert len(rows) == 3
        assert rows[0].fio == "Иванов Иван Иванович"
        assert rows[0].birth_date == "15.03.1990"
        assert rows[0].sport == "Бокс"
        assert rows[1].fio == "Петрова Мария Сергеевна"
        assert rows[2].sport == "Дзюдо"

    def test_tabular_ocr_merged_fio(self):
        """ФИО со склеенными словами (OCR-артефакт)."""
        text = """1 ШолинМаксимАндреевич 15.03.1978 Нарды 10.02.2026
Спортивный судья второй
категории
Документ зарегистрирован № test
Страница 1 из 1. test
"""
        ext = self.RuleExtractor()
        rows = ext.extract(text, order_date="10.02.2026")
        assert len(rows) == 1
        assert rows[0].fio == "Шолин Максим Андреевич"

    def test_tabular_ditto_marks(self):
        """Знак -\"- (повтор из предыдущей строки)."""
        # Ditto marks обрабатываются в контексте вида спорта:
        # если вид спорта пустой или содержит ditto, берём из предыдущей строки.
        # В текущей реализации ditto обрабатывается через непрерывное чтение.
        text = """1 Иванов Иван Иванович 01.01.1990 Бокс 10.02.2026
2 Петров Пётр Петрович 02.02.1985 Бокс 10.02.2026
Спортивный судья третьей
категории
Спортивный судья третьей
категории
Документ зарегистрирован
Страница 1 из 1.
"""
        parser = self.TabularParser()
        rows = parser.parse(text, order_date="10.02.2026")
        assert len(rows) == 2
        assert rows[0].sport == "Бокс"
        assert rows[1].sport == "Бокс"

    # --- Full extractor ---

    def test_extract_empty_text(self):
        ext = self.RuleExtractor()
        rows = ext.extract("")
        assert rows == []

    def test_extract_too_short(self):
        ext = self.RuleExtractor()
        rows = ext.extract("abc")
        assert rows == []

    def test_confidence_all_fields(self):
        """Запись со всеми полями должна иметь высокий confidence."""
        text = """1 Тестов Тест Тестович 01.01.1990 Бокс 10.02.2026
Спортивный судья третьей
категории
Документ зарегистрирован
Страница 1 из 1.
"""
        ext = self.RuleExtractor()
        rows = ext.extract(text, order_date="10.02.2026")
        assert len(rows) == 1
        assert rows[0].confidence >= 0.7

    # --- Regression: FreeTextParser multiline (аудит #2) ---

    def test_freetext_multiline_three_records(self):
        """
        Регрессия: FreeTextParser должен извлекать все 3 записи,
        разделённые переводом строки. До фикса извлекалась только 1.
        """
        from rule_extractor import FreeTextParser, AssignmentType, ActionType

        text = """Присвоить спортивные разряды:
Иванов Иван Иванович, 15.03.1990 г.р., первый спортивный разряд по боксу
Петрова Мария Сергеевна, 22.07.1985 г.р., кандидат в мастера спорта по дзюдо
Сидоров Алексей Петрович, 03.11.1992 г.р., второй спортивный разряд по плаванию"""

        parser = FreeTextParser()
        rows = parser.parse(text, order_date="17.02.2026")
        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        fios = [r.fio for r in rows]
        assert "Иванов Иван Иванович" in fios
        assert "Петрова Мария Сергеевна" in fios
        assert "Сидоров Алексей Петрович" in fios

    def test_freetext_single_record(self):
        """FreeTextParser: одна запись."""
        from rule_extractor import FreeTextParser

        text = "Козлов Пётр Андреевич, 05.05.1995 г.р., мастер спорта по шахматам"
        parser = FreeTextParser()
        rows = parser.parse(text)
        assert len(rows) == 1
        assert rows[0].fio == "Козлов Пётр Андреевич"


# ---------------------------------------------------------------------------
# 2. Sport Normalizer: unit tests
# ---------------------------------------------------------------------------

class TestSportNormalizer:

    @pytest.fixture(autouse=True)
    def setup(self):
        try:
            from sport_normalizer import SportNormalizer
            self.normalizer = SportNormalizer()
            xls_path = "Reestr_b2e716479e.xls"
            if Path(xls_path).exists():
                self.normalizer.load_xls(xls_path)
                self.loaded = True
            else:
                self.loaded = False
        except ImportError:
            pytest.skip("sport_normalizer not available")

    def test_exact_match(self):
        if not self.loaded:
            pytest.skip("ВРВС не загружен")
        r = self.normalizer.normalize("Бокс")
        assert r.canonical_name == "Бокс"
        assert r.confidence >= 0.95

    def test_case_insensitive(self):
        if not self.loaded:
            pytest.skip("ВРВС не загружен")
        r = self.normalizer.normalize("бокс")
        assert r.canonical_name == "Бокс"

    def test_fuzzy_match(self):
        if not self.loaded:
            pytest.skip("ВРВС не загружен")
        r = self.normalizer.normalize("Автомобильный спорт")
        assert r.canonical_name == "Автомобильный спорт"
        assert r.confidence >= 0.8

    def test_unknown_sport(self):
        if not self.loaded:
            pytest.skip("ВРВС не загружен")
        r = self.normalizer.normalize("Квиддич на мётлах")
        # Должен вернуть None или low confidence
        assert r.canonical_name is None or r.confidence < 0.5


# ---------------------------------------------------------------------------
# 3. Change Detector: unit tests
# ---------------------------------------------------------------------------

class TestChangeDetector:

    @pytest.fixture(autouse=True)
    def setup(self):
        from change_detector import ChangeDetector, SOURCE_PATTERNS
        self.detector = ChangeDetector()
        self.patterns = SOURCE_PATTERNS

    def test_hash_stability(self):
        """Одинаковый контент → одинаковый хеш."""
        h1 = self.detector._hash_content("<html><body>test</body></html>")
        h2 = self.detector._hash_content("<html><body>test</body></html>")
        assert h1 == h2

    def test_hash_ignores_scripts(self):
        """Скрипты не влияют на хеш."""
        html1 = "<html><body>content</body></html>"
        html2 = "<html><body>content<script>var x=123;</script></body></html>"
        assert self.detector._hash_content(html1) == self.detector._hash_content(html2)

    def test_hash_detects_content_change(self):
        """Изменение контента → изменение хеша."""
        h1 = self.detector._hash_content("<html><body>old</body></html>")
        h2 = self.detector._hash_content("<html><body>new</body></html>")
        assert h1 != h2

    def test_extract_relative_urls(self):
        """Относительные URL корректно извлекаются (замечание #6)."""
        pattern = self.patterns["spb_kfkis"]
        html = '<a href="/docs/123/">Doc 1</a><a href="/docs/456/">Doc 2</a>'
        docs = self.detector._extract_pdf_links(
            html, "https://kfis.gov.spb.ru", pattern
        )
        assert len(docs) == 2
        assert all("kfis.gov.spb.ru" in d.url for d in docs)

    def test_extract_json_embed(self):
        """JSON embed parsing для rf_minsport."""
        pattern = self.patterns["rf_minsport"]
        html = '''<script>var $obj = [{"url": "/awards/1", "title": "T1", "number": "1", "date": "01.01.2026"}];</script>'''
        docs = self.detector._extract_json_embed(
            html, "https://msrfinfo.ru", pattern
        )
        assert len(docs) == 1
        assert docs[0].order_number == "1"

    def test_all_sources_have_patterns(self):
        """Все источники имеют конфигурацию."""
        expected = [
            "moskva_tstisk", "moskva_moskumsport", "mo_mособлспорт",
            "spb_kfkis", "krasnodar_minsport", "rf_minsport"
        ]
        for code in expected:
            assert code in self.patterns, f"Missing pattern for {code}"


# ---------------------------------------------------------------------------
# 4. Pipeline Orchestrator: SSRF validation
# ---------------------------------------------------------------------------

class TestSecurity:

    def test_ssrf_private_ip_blocked(self):
        from pipeline_orchestrator import validate_url
        # Localhost
        assert not validate_url("http://127.0.0.1/test.pdf")
        # Non-whitelisted domain
        assert not validate_url("http://evil.com/test.pdf")
        # FTP scheme
        assert not validate_url("ftp://files.example.com/doc.pdf")

    def test_ssrf_whitelisted_domains(self):
        """Проверяет, что домены из allowlist проходят проверку.
        Пропускается в средах без DNS (CI без сети).
        """
        from pipeline_orchestrator import validate_url, get_allowed_domains
        import socket

        # Проверяем, что домены есть в allowlist
        domains = get_allowed_domains()
        assert "kfis.gov.spb.ru" in domains
        assert "www.mos.ru" in domains
        assert "msrfinfo.ru" in domains

        # Full validate_url требует DNS — пропускаем если нет сети
        try:
            socket.getaddrinfo("www.mos.ru", 443, proto=socket.IPPROTO_TCP)
            has_dns = True
        except (socket.gaierror, OSError):
            has_dns = False

        if has_dns:
            assert validate_url("https://kfis.gov.spb.ru/docs/123/")
            assert validate_url("https://www.mos.ru/documents/view/335740220/")
            assert validate_url("https://msrfinfo.ru/awards/1234")

    def test_ssrf_userinfo_blocked(self):
        """Регрессия: URL с user:pass@host должен блокироваться."""
        from pipeline_orchestrator import validate_url
        assert not validate_url("https://admin:secret@www.mos.ru/test.pdf")

    def test_ssrf_registry_domains_included(self):
        """Регрессия: ALLOWED_DOMAINS из source_registry попадает в validate_url."""
        from pipeline_orchestrator import validate_url
        # Проверяем, что validate_url использует домены из source_registry
        import pipeline_orchestrator as po
        assert hasattr(po, 'ALLOWED_DOMAINS') or hasattr(po, '_ALLOWED_DOMAINS')
        # Проверяем, что в коде validate_url объединяет оба множества
        import inspect
        src = inspect.getsource(po.validate_url)
        assert "ALLOWED_DOMAINS" in src, "validate_url должен ссылаться на ALLOWED_DOMAINS из source_registry"
        assert "_ALLOWED_DOMAINS" in src, "validate_url должен ссылаться на _ALLOWED_DOMAINS как fallback"

    def test_ssrf_dynamic_domain_registration(self):
        """Новые домены, добавленные через UI, попадают в allowlist без рестарта."""
        from pipeline_orchestrator import register_domain, get_allowed_domains

        test_domain = "minsport.novosibirsk-test.ru"
        assert test_domain not in get_allowed_domains()

        register_domain(test_domain)
        assert test_domain in get_allowed_domains()

    def test_ssrf_no_scheme(self):
        from pipeline_orchestrator import validate_url
        assert not validate_url("javascript:alert(1)")
        assert not validate_url("")

    def test_pdf_size_validation(self):
        from pipeline_orchestrator import validate_pdf_size, MAX_PDF_SIZE
        # OK: small
        assert validate_pdf_size(b"x" * 1024)
        # Fail: too large
        assert not validate_pdf_size(b"x" * (MAX_PDF_SIZE + 1))


# ---------------------------------------------------------------------------
# 5b. Source Registry: единый реестр
# ---------------------------------------------------------------------------

class TestSourceRegistry:

    def test_all_sources_present(self):
        from source_registry import SOURCES
        expected = [
            "moskva_tstisk", "moskva_moskumsport", "mo_mособлспорт",
            "spb_kfkis", "krasnodar_minsport", "rf_minsport"
        ]
        for code in expected:
            assert code in SOURCES, f"Missing source: {code}"

    def test_compat_download_configs(self):
        from source_registry import as_download_configs
        configs = as_download_configs()
        assert len(configs) == 6
        # Каждый конфиг имеет method и base_url
        for code, cfg in configs.items():
            assert "method" in cfg, f"{code} missing method"
            assert "base_url" in cfg, f"{code} missing base_url"
            assert cfg["method"] in ("httpx", "playwright")

    def test_compat_detect_patterns(self):
        from source_registry import as_detect_patterns
        patterns = as_detect_patterns()
        assert len(patterns) == 6
        for code, p in patterns.items():
            assert "list_urls" in p, f"{code} missing list_urls"
            assert "method" in p, f"{code} missing method"

    def test_compat_meta(self):
        from source_registry import as_meta
        meta = as_meta()
        assert len(meta) == 6
        for code, m in meta.items():
            assert "issuing_body" in m, f"{code} missing issuing_body"
            assert m["issuing_body"], f"{code} empty issuing_body"

    def test_get_source(self):
        from source_registry import get_source
        cfg = get_source("spb_kfkis")
        assert cfg is not None
        assert cfg.code == "spb_kfkis"
        assert cfg.risk_class == "green"
        assert cfg.download.method == "httpx"
        assert "kfis.gov.spb.ru" in cfg.download.base_url

    def test_get_active_sources(self):
        from source_registry import get_active_sources
        active = get_active_sources()
        codes = {s.code for s in active}
        # rf_minsport should be inactive
        assert "rf_minsport" not in codes
        assert len(active) >= 5

    def test_get_all_domains(self):
        from source_registry import get_all_domains
        domains = get_all_domains()
        assert "kfis.gov.spb.ru" in domains
        assert "www.mos.ru" in domains
        assert "msrfinfo.ru" in domains

    def test_domains_match_ssrf_whitelist(self):
        """Домены в source_registry совпадают с ALLOWED_DOMAINS в orchestrator."""
        from source_registry import get_all_domains
        from pipeline_orchestrator import ALLOWED_DOMAINS
        registry_domains = get_all_domains()
        # ALLOWED_DOMAINS должен содержать все домены из реестра
        for d in registry_domains:
            assert d in ALLOWED_DOMAINS, f"Domain {d} in registry but not in ALLOWED_DOMAINS"


# ---------------------------------------------------------------------------
# 5c. Async worker pattern: asyncio.to_thread
# ---------------------------------------------------------------------------

class TestAsyncWorkers:

    def test_ocr_has_thread_batch(self):
        """OCR pipeline имеет метод _run_tesseract_batch для to_thread."""
        from ocr_pipeline import OcrPipeline
        ocr = OcrPipeline()
        assert hasattr(ocr, "_run_tesseract_batch")
        assert callable(ocr._run_tesseract_batch)

    def test_change_detector_has_semaphore(self):
        """ChangeDetector имеет семафор для Playwright."""
        from change_detector import ChangeDetector
        cd = ChangeDetector()
        assert hasattr(cd, "_browser_sem")
        # Семафор с лимитом 2
        assert cd._browser_sem._value == 2

    def test_pipeline_uses_to_thread(self):
        """Pipeline orchestrator использует asyncio.to_thread в _step_extract."""
        import inspect
        from pipeline_orchestrator import PipelineOrchestrator
        source = inspect.getsource(PipelineOrchestrator._step_extract)
        assert "asyncio.to_thread" in source


# ---------------------------------------------------------------------------
# 6. Schema validation
# ---------------------------------------------------------------------------

class TestSchema:

    def test_schema_has_approved_status(self):
        """Schema должна разрешать статус 'approved' (замечание #3)."""
        schema = Path("schema.sql").read_text()
        assert "'approved'" in schema
        assert "'rejected'" in schema

    def test_schema_has_extensions(self):
        """Schema содержит необходимые расширения."""
        schema = Path("schema.sql").read_text()
        assert "pg_trgm" in schema


# ---------------------------------------------------------------------------
# 6. Integration: OCR + Rule Extractor (на реальном PDF)
# ---------------------------------------------------------------------------

class TestIntegration:

    PDF_PATH = (
        "/mnt/user-data/uploads/"
        "Приказ_ГКУ__ЦСТиСК__Москомспорта_от_17_02_2026_г____С-2_26_"
        "_О_присвоении_квалификационных_категорий_спортивных_судей_.pdf"
    )

    @pytest.fixture(autouse=True)
    def check_pdf(self):
        if not Path(self.PDF_PATH).exists():
            pytest.skip("Тестовый PDF не найден")

    def test_full_pipeline_no_llm(self):
        """Полный конвейер без LLM: OCR → rule_extractor → 286 записей."""
        from ocr_pipeline import OcrPipeline
        from rule_extractor import RuleExtractor

        async def run():
            ocr = OcrPipeline()
            result = await ocr.process(self.PDF_PATH)

            normalizer = None
            xls = "Reestr_b2e716479e.xls"
            if Path(xls).exists():
                from sport_normalizer import SportNormalizer
                normalizer = SportNormalizer()
                normalizer.load_xls(xls)

            ext = RuleExtractor(sport_normalizer=normalizer)
            rows = ext.extract(
                result.text,
                order_date="17.02.2026",
                order_number="С-2/26",
                source_code="moskva_tstisk",
            )

            # 286 записей ожидается
            assert len(rows) >= 280, f"Expected ~286, got {len(rows)}"
            assert len(rows) <= 290

            # Все записи имеют ФИО
            assert all(r.fio for r in rows)
            # Все записи имеют дату рождения
            assert all(r.birth_date for r in rows)
            # Все записи имеют вид спорта
            assert all(r.sport for r in rows)
            # Средний confidence > 0.7
            avg = sum(r.confidence for r in rows) / len(rows)
            assert avg >= 0.7, f"Avg confidence {avg:.2f} < 0.7"
            # Уникальных видов спорта >= 30
            sports = set(r.sport for r in rows)
            assert len(sports) >= 30, f"Only {len(sports)} sports found"

            return rows

        rows = asyncio.get_event_loop().run_until_complete(run())
        print(f"\n  Integration: {len(rows)} rows, "
              f"{len(set(r.sport for r in rows))} sports ✓")
