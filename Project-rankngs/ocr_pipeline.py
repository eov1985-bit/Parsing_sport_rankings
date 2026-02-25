"""
ocr_pipeline.py
================
Трёхуровневый pipeline извлечения текста из PDF-приказов.

Уровни обработки (по возрастанию стоимости):
  1. pypdf — прямое извлечение текста из текстового PDF (бесплатно)
  2. Tesseract OCR — для скан-PDF, с предобработкой изображения (бесплатно)
  3. Claude Vision — для плохих сканов, где Tesseract даёт < порога (платно)

Большинство приказов региональных ведомств — текстовые PDF (Word → PDF).
Сканы встречаются реже, обычно у старых документов.

Использование:
    pipeline = OcrPipeline()

    # Один файл
    result = await pipeline.process("path/to/order.pdf")
    print(result.text, result.method, result.confidence, result.page_count)

    # С файлом-байтами
    result = await pipeline.process_bytes(pdf_bytes)

Зависимости:
    pip install pypdf pdf2image Pillow pytesseract anthropic
    apt install tesseract-ocr tesseract-ocr-rus poppler-utils
"""

import asyncio
import hashlib
import io
import logging
import re
import statistics
import tempfile
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional, Union

import pypdf

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

# Минимум символов на странице, чтобы считать pypdf-извлечение успешным
MIN_CHARS_PER_PAGE = 80

# Минимум читаемых символов (кириллица + цифры + пунктуация) в тексте Tesseract
# чтобы не уходить на уровень 3 (Claude Vision)
MIN_READABLE_RATIO = 0.70

# DPI для конвертации PDF → изображение (320 — эмпирически оптимально)
SCAN_DPI = 320

# Контрастность при предобработке (PIL ImageEnhance)
CONTRAST_FACTOR = 1.6

# Tesseract: языки и режим сегментации
TESSERACT_LANG = "rus+eng"
TESSERACT_PSM = 6  # блок текста

# Claude Vision: лимит на страницу (символов в ответе)
VISION_MAX_TOKENS = 4096


# ---------------------------------------------------------------------------
# Модели данных
# ---------------------------------------------------------------------------

class OcrMethod(str, Enum):
    """Метод, которым был получен текст."""
    PYPDF     = "pypdf"       # уровень 1 — текстовый слой PDF
    TESSERACT = "tesseract"   # уровень 2 — Tesseract OCR по скану
    VISION    = "vision"      # уровень 3 — Claude Vision API


@dataclass
class PageResult:
    """Результат обработки одной страницы."""
    page_num: int               # 1-based
    text: str
    method: OcrMethod
    confidence: float           # 0.0–1.0
    char_count: int = 0

    def __post_init__(self):
        self.char_count = len(self.text)


@dataclass
class PipelineResult:
    """Результат обработки всего PDF."""
    text: str                   # склеенный текст всех страниц
    method: OcrMethod           # доминирующий метод (тот, что использовался чаще)
    confidence: float           # средняя уверенность по страницам
    page_count: int
    pages: list[PageResult] = field(default_factory=list)
    file_hash: Optional[str] = None  # SHA256 исходного PDF

    # Статистика по методам
    methods_used: dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Исключения
# ---------------------------------------------------------------------------

class OcrError(Exception):
    """Базовое исключение OCR-пайплайна."""
    pass


class EmptyPdfError(OcrError):
    """PDF не содержит страниц."""
    pass


class AllPagesFailedError(OcrError):
    """Ни одна страница не была успешно обработана."""
    pass


# ---------------------------------------------------------------------------
# Основной класс
# ---------------------------------------------------------------------------

class OcrPipeline:
    """
    Трёхуровневый pipeline: pypdf → Tesseract → Claude Vision.

    Каждая страница обрабатывается автоматически на минимально
    достаточном уровне. Если текстовый слой есть и достаточно
    длинный — уровень 1. Если нет или текст скудный — уровень 2.
    Если Tesseract выдал мусор — уровень 3 (если включён).
    """

    def __init__(
        self,
        enable_vision: bool = False,
        anthropic_api_key: Optional[str] = None,
        vision_model: str = "claude-haiku-4-5-20251001",
        min_chars_per_page: int = MIN_CHARS_PER_PAGE,
        min_readable_ratio: float = MIN_READABLE_RATIO,
        scan_dpi: int = SCAN_DPI,
        contrast_factor: float = CONTRAST_FACTOR,
    ):
        """
        Args:
            enable_vision: включить уровень 3 (Claude Vision).
                           Требует anthropic_api_key.
            anthropic_api_key: ключ API Anthropic (для Vision).
            vision_model: модель Claude для Vision.
            min_chars_per_page: минимум символов для pypdf-уровня.
            min_readable_ratio: минимальная доля читаемых символов
                                для Tesseract.
            scan_dpi: DPI для конвертации скана в изображение.
            contrast_factor: коэффициент усиления контрастности.
        """
        self.enable_vision = enable_vision
        self.min_chars_per_page = min_chars_per_page
        self.min_readable_ratio = min_readable_ratio
        self.scan_dpi = scan_dpi
        self.contrast_factor = contrast_factor

        self._vision_client = None
        self._vision_model = vision_model

        if enable_vision:
            if not anthropic_api_key:
                raise ValueError(
                    "enable_vision=True требует anthropic_api_key"
                )
            import anthropic
            self._vision_client = anthropic.Anthropic(api_key=anthropic_api_key)

    # ------------------------------------------------------------------
    # Публичный API
    # ------------------------------------------------------------------

    async def process(self, pdf_path: Union[str, Path]) -> PipelineResult:
        """
        Обрабатывает PDF-файл по пути.
        Возвращает PipelineResult с текстом и метаданными.
        """
        path = Path(pdf_path)
        if not path.exists():
            raise FileNotFoundError(f"PDF не найден: {path}")

        pdf_bytes = path.read_bytes()
        return await self.process_bytes(pdf_bytes)

    async def process_bytes(self, pdf_bytes: bytes) -> PipelineResult:
        """
        Обрабатывает PDF из байтов.
        Возвращает PipelineResult с текстом и метаданными.
        """
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            raise OcrError("Данные не являются валидным PDF")

        file_hash = hashlib.sha256(pdf_bytes).hexdigest()
        page_results: list[PageResult] = []

        # ----------------------------------------------------------
        # Уровень 1: pypdf — извлечение текстового слоя
        # ----------------------------------------------------------
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        page_count = len(reader.pages)

        if page_count == 0:
            raise EmptyPdfError("PDF не содержит страниц")

        logger.info(
            f"PDF: {page_count} стр., {len(pdf_bytes):,} байт, "
            f"hash={file_hash[:12]}..."
        )

        pages_needing_ocr: list[int] = []  # 0-based индексы

        for i, page in enumerate(reader.pages):
            try:
                text = page.extract_text() or ""
            except Exception as e:
                logger.warning(f"  Стр.{i+1}: pypdf ошибка — {e}")
                text = ""

            text = text.strip()
            clean_len = self._count_readable_chars(text)

            if clean_len >= self.min_chars_per_page:
                # Уровень 1 — достаточно текста
                confidence = min(1.0, clean_len / (self.min_chars_per_page * 3))
                page_results.append(PageResult(
                    page_num=i + 1,
                    text=text,
                    method=OcrMethod.PYPDF,
                    confidence=round(confidence, 3),
                ))
                logger.debug(
                    f"  Стр.{i+1}: pypdf OK ({clean_len} символов, "
                    f"conf={confidence:.2f})"
                )
            else:
                # Недостаточно текста — нужен OCR
                pages_needing_ocr.append(i)
                logger.debug(
                    f"  Стр.{i+1}: pypdf недостаточно ({clean_len} символов) "
                    f"→ OCR"
                )

        # ----------------------------------------------------------
        # Уровень 2: Tesseract OCR (только для страниц без текста)
        # ----------------------------------------------------------
        pages_needing_vision: list[int] = []

        if pages_needing_ocr:
            logger.info(
                f"  OCR нужен для {len(pages_needing_ocr)} стр.: "
                f"{[p+1 for p in pages_needing_ocr]}"
            )

            # Tesseract — CPU-bound, выносим из event loop
            tesseract_results = await asyncio.to_thread(
                self._run_tesseract_batch, pdf_bytes, pages_needing_ocr
            )

            for page_idx, text, readable_ratio in tesseract_results:

                if readable_ratio >= self.min_readable_ratio:
                    confidence = round(readable_ratio * 0.9, 3)  # OCR чуть ниже
                    page_results.append(PageResult(
                        page_num=page_idx + 1,
                        text=text,
                        method=OcrMethod.TESSERACT,
                        confidence=confidence,
                    ))
                    logger.debug(
                        f"  Стр.{page_idx+1}: Tesseract OK "
                        f"(readable={readable_ratio:.2f}, "
                        f"conf={confidence:.2f})"
                    )
                else:
                    # Tesseract не справился — нужен Vision
                    pages_needing_vision.append(page_idx)
                    logger.debug(
                        f"  Стр.{page_idx+1}: Tesseract плохо "
                        f"(readable={readable_ratio:.2f}) → Vision"
                    )

                    # Всё равно сохраняем Tesseract-результат как fallback
                    if text.strip():
                        page_results.append(PageResult(
                            page_num=page_idx + 1,
                            text=text,
                            method=OcrMethod.TESSERACT,
                            confidence=round(readable_ratio * 0.5, 3),
                        ))

        # ----------------------------------------------------------
        # Уровень 3: Claude Vision (если Tesseract не справился)
        # ----------------------------------------------------------
        if pages_needing_vision and self.enable_vision and self._vision_client:
            logger.info(
                f"  Vision нужен для {len(pages_needing_vision)} стр.: "
                f"{[p+1 for p in pages_needing_vision]}"
            )
            images = self._pdf_to_images(pdf_bytes, pages_needing_vision)

            for page_idx, img in zip(pages_needing_vision, images):
                try:
                    text = self._vision_ocr(img)
                    if text.strip():
                        # Заменяем Tesseract-fallback если он был
                        page_results = [
                            p for p in page_results
                            if p.page_num != page_idx + 1
                        ]
                        page_results.append(PageResult(
                            page_num=page_idx + 1,
                            text=text,
                            method=OcrMethod.VISION,
                            confidence=0.85,  # Vision обычно хорош
                        ))
                        logger.debug(
                            f"  Стр.{page_idx+1}: Vision OK "
                            f"({len(text)} символов)"
                        )
                except Exception as e:
                    logger.error(
                        f"  Стр.{page_idx+1}: Vision ошибка — {e}"
                    )
        elif pages_needing_vision:
            logger.warning(
                f"  {len(pages_needing_vision)} стр. нуждаются в Vision, "
                f"но он отключён (enable_vision=False)"
            )

        # ----------------------------------------------------------
        # Сборка результата
        # ----------------------------------------------------------
        if not page_results:
            raise AllPagesFailedError(
                f"Ни одна из {page_count} страниц не обработана успешно"
            )

        # Сортируем по номеру страницы
        page_results.sort(key=lambda p: p.page_num)

        # Склеиваем текст
        full_text = "\n\n".join(p.text for p in page_results if p.text.strip())

        # Статистика по методам
        methods_used: dict[str, int] = {}
        for p in page_results:
            methods_used[p.method.value] = methods_used.get(p.method.value, 0) + 1

        # Доминирующий метод — который использовался чаще всего
        dominant_method = max(methods_used, key=methods_used.get)

        # Средняя уверенность
        avg_confidence = round(
            statistics.mean(p.confidence for p in page_results), 3
        )

        result = PipelineResult(
            text=full_text,
            method=OcrMethod(dominant_method),
            confidence=avg_confidence,
            page_count=page_count,
            pages=page_results,
            file_hash=file_hash,
            methods_used=methods_used,
        )

        logger.info(
            f"  Итого: {len(full_text):,} символов, "
            f"метод={dominant_method}, "
            f"уверенность={avg_confidence:.2f}, "
            f"методы={methods_used}"
        )

        return result

    # ------------------------------------------------------------------
    # Уровень 2: работа с изображениями и Tesseract
    # ------------------------------------------------------------------

    def _run_tesseract_batch(
        self,
        pdf_bytes: bytes,
        pages_needing_ocr: list[int],
    ) -> list[tuple[int, str, float]]:
        """
        Синхронный batch Tesseract OCR. Вызывается через asyncio.to_thread().
        Возвращает list[(page_idx, text, readable_ratio)].
        """
        results = []
        images = self._pdf_to_images(pdf_bytes, pages_needing_ocr)

        for page_idx, img in zip(pages_needing_ocr, images):
            preprocessed = self._preprocess_image(img)
            text = self._tesseract_ocr(preprocessed)
            readable_ratio = self._readable_ratio(text)
            results.append((page_idx, text, readable_ratio))

        return results

    def _pdf_to_images(
        self, pdf_bytes: bytes, page_indices: list[int]
    ) -> list:
        """
        Конвертирует указанные страницы PDF в PIL-изображения.
        Использует pdf2image (poppler-utils).

        Args:
            pdf_bytes: содержимое PDF
            page_indices: 0-based индексы страниц

        Returns:
            Список PIL.Image в том же порядке, что и page_indices.
        """
        try:
            from pdf2image import convert_from_bytes
        except ImportError:
            raise ImportError(
                "Установи pdf2image: pip install pdf2image\n"
                "И poppler-utils: apt install poppler-utils"
            )

        # pdf2image принимает first_page/last_page (1-based)
        # Для эффективности конвертируем только нужные страницы
        # Если страницы разрозненные — конвертируем каждую отдельно
        images = []
        for idx in page_indices:
            page_images = convert_from_bytes(
                pdf_bytes,
                dpi=self.scan_dpi,
                first_page=idx + 1,
                last_page=idx + 1,
                fmt="png",
                thread_count=2,
            )
            if page_images:
                images.append(page_images[0])
            else:
                # Пустая страница — создаём заглушку
                from PIL import Image
                images.append(Image.new("RGB", (100, 100), "white"))

        return images

    def _preprocess_image(self, image) -> "PIL.Image":
        """
        Предобработка скана для улучшения качества Tesseract OCR.

        Pipeline:
          1. Конвертация в grayscale
          2. Autocontrast (нормализация яркости)
          3. Усиление контрастности (×1.6)
          4. Слабый медианный фильтр (убрать точечный шум)
          5. Адаптивная бинаризация (Otsu) — если доступен OpenCV

        Без OpenCV: шаги 1–3 через PIL (достаточно для большинства случаев).
        """
        from PIL import Image, ImageOps, ImageEnhance, ImageFilter

        # 1. Grayscale
        if image.mode != "L":
            gray = image.convert("L")
        else:
            gray = image.copy()

        # 2. Autocontrast — нормализация яркости
        gray = ImageOps.autocontrast(gray, cutoff=0.5)

        # 3. Усиление контрастности
        enhancer = ImageEnhance.Contrast(gray)
        gray = enhancer.enhance(self.contrast_factor)

        # 4. Медианный фильтр (убирает точечный шум)
        gray = gray.filter(ImageFilter.MedianFilter(size=3))

        # 5. Попытка адаптивной бинаризации через OpenCV (Otsu)
        try:
            import numpy as np
            import cv2

            arr = np.array(gray)

            # Deskew: коррекция наклона
            arr = self._deskew(arr)

            # Otsu-бинаризация
            _, binary = cv2.threshold(
                arr, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
            )

            return Image.fromarray(binary)

        except ImportError:
            # OpenCV недоступен — возвращаем PIL-результат
            logger.debug("OpenCV недоступен, пропускаем бинаризацию/deskew")
            return gray

    @staticmethod
    def _deskew(image_array) -> "np.ndarray":
        """
        Коррекция наклона скана.
        Определяет угол по линиям Hough и поворачивает.
        """
        import numpy as np
        import cv2

        # Находим края
        edges = cv2.Canny(image_array, 50, 150, apertureSize=3)

        # Линии Хафа
        lines = cv2.HoughLinesP(
            edges, 1, np.pi / 180,
            threshold=100,
            minLineLength=image_array.shape[1] // 4,
            maxLineGap=10,
        )

        if lines is None or len(lines) < 3:
            return image_array

        # Средний угол наклона горизонтальных линий
        angles = []
        for line in lines:
            x1, y1, x2, y2 = line[0]
            angle = np.degrees(np.arctan2(y2 - y1, x2 - x1))
            # Берём только почти горизонтальные (±15°)
            if abs(angle) < 15:
                angles.append(angle)

        if not angles:
            return image_array

        median_angle = np.median(angles)

        if abs(median_angle) < 0.3:
            # Слишком малый наклон — не трогаем
            return image_array

        logger.debug(f"  Deskew: поворот на {median_angle:.2f}°")

        # Поворот
        h, w = image_array.shape[:2]
        center = (w // 2, h // 2)
        M = cv2.getRotationMatrix2D(center, median_angle, 1.0)
        rotated = cv2.warpAffine(
            image_array, M, (w, h),
            flags=cv2.INTER_CUBIC,
            borderMode=cv2.BORDER_REPLICATE,
        )

        return rotated

    @staticmethod
    def _tesseract_ocr(image) -> str:
        """
        Запускает Tesseract OCR на предобработанном изображении.

        Returns:
            Извлечённый текст.
        """
        try:
            import pytesseract
        except ImportError:
            raise ImportError(
                "Установи pytesseract: pip install pytesseract\n"
                "И Tesseract: apt install tesseract-ocr tesseract-ocr-rus"
            )

        config = (
            f"--psm {TESSERACT_PSM} "
            f"-l {TESSERACT_LANG} "
            "--oem 3"  # LSTM + legacy
        )

        text = pytesseract.image_to_string(image, config=config)
        return text.strip()

    # ------------------------------------------------------------------
    # Уровень 3: Claude Vision
    # ------------------------------------------------------------------

    def _vision_ocr(self, image) -> str:
        """
        Отправляет изображение страницы в Claude Vision для OCR.
        Используется для плохих сканов, где Tesseract не справляется.

        Returns:
            Извлечённый текст.
        """
        if not self._vision_client:
            raise OcrError("Vision-клиент не инициализирован")

        import base64

        # Конвертируем PIL Image в PNG bytes
        buf = io.BytesIO()
        image.save(buf, format="PNG", optimize=True)
        img_bytes = buf.getvalue()
        img_b64 = base64.standard_b64encode(img_bytes).decode("utf-8")

        message = self._vision_client.messages.create(
            model=self._vision_model,
            max_tokens=VISION_MAX_TOKENS,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": img_b64,
                        },
                    },
                    {
                        "type": "text",
                        "text": (
                            "Это скан страницы из российского официального "
                            "приказа о присвоении спортивных разрядов или "
                            "квалификационных категорий.\n\n"
                            "Извлеки ВЕСЬ текст со страницы. "
                            "Сохраняй структуру таблиц, нумерацию строк, "
                            "ФИО, даты, названия видов спорта.\n\n"
                            "Верни ТОЛЬКО текст документа, без комментариев."
                        ),
                    },
                ],
            }],
        )

        return message.content[0].text.strip()

    # ------------------------------------------------------------------
    # Утилиты оценки качества
    # ------------------------------------------------------------------

    @staticmethod
    def _count_readable_chars(text: str) -> int:
        """
        Подсчитывает количество «читаемых» символов:
        кириллица, латиница, цифры, базовая пунктуация, пробелы.
        """
        if not text:
            return 0
        readable = re.findall(
            r"[А-ЯЁа-яёA-Za-z0-9\s\.\,\;\:\-\(\)\"\'«»№/]", text
        )
        return len(readable)

    @staticmethod
    def _readable_ratio(text: str) -> float:
        """
        Доля читаемых символов в тексте.
        Используется для оценки качества Tesseract.
        0.0 — всё нечитаемо, 1.0 — всё чисто.
        """
        if not text or len(text) < 10:
            return 0.0
        readable = len(re.findall(
            r"[А-ЯЁа-яёA-Za-z0-9\s\.\,\;\:\-\(\)\"\'«»№/]", text
        ))
        return readable / len(text)

    # ------------------------------------------------------------------
    # Удобные методы для диагностики
    # ------------------------------------------------------------------

    async def analyze(self, pdf_path: Union[str, Path]) -> dict:
        """
        Анализирует PDF без полной обработки.
        Возвращает словарь с метаданными и рекомендациями.
        """
        path = Path(pdf_path)
        pdf_bytes = path.read_bytes()
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))

        page_count = len(reader.pages)
        file_hash = hashlib.sha256(pdf_bytes).hexdigest()

        analysis = {
            "file": str(path),
            "file_hash": file_hash,
            "file_size": len(pdf_bytes),
            "page_count": page_count,
            "pages": [],
            "recommendation": "",
        }

        text_pages = 0
        scan_pages = 0

        for i, page in enumerate(reader.pages):
            try:
                text = page.extract_text() or ""
            except Exception:
                text = ""

            clean_len = self._count_readable_chars(text.strip())
            is_text = clean_len >= self.min_chars_per_page

            page_info = {
                "page": i + 1,
                "has_text_layer": is_text,
                "readable_chars": clean_len,
                "preview": text.strip()[:100] if text.strip() else "(пусто)",
            }
            analysis["pages"].append(page_info)

            if is_text:
                text_pages += 1
            else:
                scan_pages += 1

        if scan_pages == 0:
            analysis["recommendation"] = "pypdf (все страницы текстовые)"
        elif text_pages == 0:
            analysis["recommendation"] = "Tesseract (все страницы — сканы)"
        else:
            analysis["recommendation"] = (
                f"Смешанный: {text_pages} текстовых + {scan_pages} сканов"
            )

        return analysis


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

async def _main():
    import argparse
    import json as json_mod

    parser = argparse.ArgumentParser(
        description="OCR Pipeline для PDF-приказов SportRank"
    )
    parser.add_argument("pdf", help="Путь к PDF-файлу")
    parser.add_argument(
        "--mode", choices=["process", "analyze"], default="process",
        help="Режим: process (извлечь текст) или analyze (только анализ)"
    )
    parser.add_argument(
        "--vision", action="store_true",
        help="Включить Claude Vision (уровень 3)"
    )
    parser.add_argument(
        "--api-key", default=None,
        help="Anthropic API key (для Vision)"
    )
    parser.add_argument(
        "--out", default=None,
        help="Файл для сохранения результата (.txt или .json)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Подробный вывод"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    import os
    api_key = args.api_key or os.environ.get("ANTHROPIC_API_KEY")

    pipeline = OcrPipeline(
        enable_vision=args.vision and bool(api_key),
        anthropic_api_key=api_key if args.vision else None,
    )

    if args.mode == "analyze":
        result = await pipeline.analyze(args.pdf)
        print(json_mod.dumps(result, ensure_ascii=False, indent=2))
        return

    result = await pipeline.process(args.pdf)

    print(f"Файл:        {args.pdf}")
    print(f"Страниц:     {result.page_count}")
    print(f"Символов:    {len(result.text):,}")
    print(f"Метод:       {result.method.value}")
    print(f"Уверенность: {result.confidence:.2f}")
    print(f"Методы:      {result.methods_used}")
    print(f"SHA256:      {result.file_hash[:16]}...")
    print("-" * 60)

    if args.out:
        out_path = Path(args.out)
        if out_path.suffix == ".json":
            data = {
                "file_hash": result.file_hash,
                "page_count": result.page_count,
                "method": result.method.value,
                "confidence": result.confidence,
                "methods_used": result.methods_used,
                "text": result.text,
                "pages": [
                    {
                        "page_num": p.page_num,
                        "method": p.method.value,
                        "confidence": p.confidence,
                        "char_count": p.char_count,
                    }
                    for p in result.pages
                ],
            }
            out_path.write_text(
                json_mod.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        else:
            out_path.write_text(result.text, encoding="utf-8")
        print(f"Сохранено: {out_path}")
    else:
        # Выводим первые 2000 символов текста
        preview = result.text[:2000]
        print(preview)
        if len(result.text) > 2000:
            print(f"\n... ещё {len(result.text) - 2000:,} символов")


if __name__ == "__main__":
    import asyncio
    asyncio.run(_main())
