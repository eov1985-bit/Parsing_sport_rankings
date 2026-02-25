"""
llm_extractor.py
================
Извлечение данных о присвоении спортивных разрядов и категорий
из текста PDF-приказов через Claude API.

Поддерживаемые органы и форматы:
  - ГКУ «ЦСТиСК» Москомспорта (Москва, 2-й/3-й разряды, судьи, специалисты)
  - Департамент спорта г. Москвы / Москомспорт (КМС, 1-й разряд, судьи 1-й кат.)
  - МОСОБЛСПОРТ / Минспорт МО (КМС, 1-й, судьи, специалисты; таблица и free-text)
  - Минспорт Краснодарского края (КМС, 1-й разряд; секционная таблица)
  - КФКиС Санкт-Петербурга (2-й, 3-й разряды, судьи; с отказами)
  - Минспорт РФ / msrfinfo.ru (МС, МСМК, ЗМС — обрабатывается отдельно через JSON)

Структурные типы документов:
  flat_table      — одна таблица, вид спорта в строке (ЦСТиСК С-2/26)
  appendix_table  — таблицы по приложениям, вид спорта в заголовке (Москомспорт, КФКиС)
  section_table   — секционная таблица с жирными заголовками видов спорта (Краснодар)
  free_text       — присвоение в произвольном тексте, 1–несколько человек (МОСОБЛСПОРТ)
"""

import json
import re
import anthropic
from dataclasses import dataclass, field, asdict
from typing import Optional
from enum import Enum


# ---------------------------------------------------------------------------
# Модели данных
# ---------------------------------------------------------------------------

class AssignmentType(str, Enum):
    SPORT_RANK         = "sport_rank"          # КМС, 1-й, 2-й, 3-й разряд, МС, МСМК, ГМР
    JUDGE_CATEGORY     = "judge_category"      # Судья всеросс./1/2/3 кат., юный судья
    SPECIALIST_CATEGORY = "specialist_category" # Специалист ФКиС высшей/первой кат.
    COACH_CATEGORY     = "coach_category"      # Квалификационная категория тренера
    HONORARY_TITLE     = "honorary_title"      # Почётные: ЗМС, ЗТР, почётный судья и др.


class ActionType(str, Enum):
    ASSIGNMENT   = "assignment"    # Присвоить
    CONFIRMATION = "confirmation"  # Подтвердить
    REFUSAL      = "refusal"       # Отказать
    REVOCATION   = "revocation"    # Лишить
    RESTORATION  = "restoration"   # Восстановить (ЕВСК, Приказ №173)


@dataclass
class AssignmentRow:
    """Одна запись о присвоении/подтверждении/отказе из приказа."""

    # Обязательные поля
    fio: str                                # ФИО как в документе (может быть 2–4 слова)
    rank_category: str                      # «II спортивный разряд», «КМС», «судья 1 кат.» и т.д.
    assignment_type: AssignmentType
    action: ActionType = ActionType.ASSIGNMENT

    # Часто присутствуют
    sport: Optional[str] = None             # Нормализованный вид спорта
    sport_original: Optional[str] = None   # Как в документе (может содержать опечатки)
    birth_date: Optional[str] = None        # ДД.ММ.ГГГГ или None
    ias_id: Optional[int] = None           # ID лица в ИАС «Спорт» (Москва)
    submission_number: Optional[str] = None # Номер представления (рег. номер пакета документов)

    # Опциональные поля (зависят от органа)
    extra_fields: dict = field(default_factory=dict)
    # Примеры extra_fields:
    #   submission_date: str          — дата представления (ЦСТиСК судьи, ДД.ММ.ГГГГ)
    #   coach_fio: str | list[str]    — тренер(ы) (Краснодар, СПб)
    #   municipality: str             — муниципальное образование (Краснодар)
    #   department: str               — ведомство (Краснодар)
    #   organization: str             — место работы (специалисты)
    #   position: str                 — должность (специалисты)
    #   rank_start_date: str          — начало срока действия (подтверждения)
    #   refusal_reason: str           — причина отказа (КФКиС СПб)
    #   birth_date_suspicious: bool   — дата рождения выглядит как опечатка

    def to_dict(self) -> dict:
        d = asdict(self)
        d["assignment_type"] = self.assignment_type.value
        d["action"] = self.action.value
        return d


# ---------------------------------------------------------------------------
# Промпты
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Ты — парсер официальных российских приказов о присвоении \
спортивных разрядов и квалификационных категорий.

Твоя задача: извлечь ВСЕ записи из документа и вернуть JSON-массив.
Каждый элемент массива — одно присвоение, подтверждение, отказ или лишение.

ВАЖНО:
- Верни ТОЛЬКО валидный JSON-массив. Без markdown-блоков, без пояснений.
- Первый символ ответа должен быть '[', последний — ']'.
"""

EXTRACTION_PROMPT = """Документ (текст из PDF):
---
{text}
---

Метаданные документа:
  Орган: {issuing_body}
  Дата: {order_date}
  Номер: {order_number}

Извлеки все записи и верни JSON-массив. Каждый элемент:
{{
  "fio": "ФИО точно как в документе (может быть 2 или 4 слова)",
  "birth_date": "ДД.ММ.ГГГГ (убери 'г.' если есть) или null",
  "ias_id": число или null,
  "submission_number": "строка или null",
  "assignment_type": "sport_rank | judge_category | specialist_category | coach_category",
  "rank_category": "точное название категории/разряда",
  "sport": "нормализованное название вида спорта или null",
  "sport_original": "как в документе (если отличается от sport) или null",
  "action": "assignment | confirmation | refusal | revocation",
  "extra_fields": {{
    // включай только те поля, которые реально присутствуют:
    "submission_date": "ДД.ММ.ГГГГ",   // дата представления
    "coach_fio": ["Иванов И.И."],       // тренер(ы), всегда массив
    "municipality": "г. Краснодар",     // муниципальное образование
    "department": "ОУ ФК и С МО",      // ведомство
    "organization": "МБОУ СШОР №1",    // место работы (специалисты)
    "position": "заместитель директора",// должность (специалисты)
    "rank_start_date": "ДД.ММ.ГГГГ",   // начало срока действия (при подтверждении)
    "refusal_reason": "текст причины"  // только при action=refusal
  }}
}}

ПРАВИЛА:

1. ФИО:
   - Сохраняй точно, даже если нестандартное: «Крутая Ирина» (2 слова),
     «Муганлинский Руфат Тагир оглы» (4 слова)
   - Зачищай leading/trailing punctuation: «-Суликова» → «Суликова»
   - Если ФИО в дательном падеже (free-text документы) — переводи в именительный:
     «Лядащеву Роману Владимировичу» → «Лядащев Роман Владимирович»
   - Если ФИО явно разбито на 2-3 строки (OCR) — склей в одну строку

2. Вид спорта:
   - Вид спорта может быть в заголовке секции/приложения — применяй ко всем
     строкам этой секции
   - Нормализуй опечатки: «Спортиваня акробатика» → sport: «Спортивная акробатика»,
     sport_original: «Спортиваня акробатика»
   - «Киокушин» и «Киокусинкай» — один вид спорта, нормализуй к «Киокусинкай»
   - Если вид спорта не указан (специалисты, некоторые судьи) — sport: null

3. Разряд/категория:
   - Используй нормализованные названия:
     «II спортивный разряд», «III спортивный разряд»,
     «КМС», «1 разряд» / «первый спортивный разряд»,
     «судья первой категории», «судья второй категории», «судья третьей категории»,
     «специалист высшей квалификационной категории»,
     «специалист первой квалификационной категории»

4. assignment_type определяй по контексту:
   - «КМС», «МС», «МСМК», «1 разряд», «2 разряд», «3 разряд» → sport_rank
   - «спортивный судья» → judge_category
   - «специалист в области физической культуры и спорта» → specialist_category
   - «ЗТР», «Заслуженный тренер» → coach_category

5. action:
   - «присвоить» → assignment
   - «подтвердить», «считать подтвердившим» → confirmation
   - «отказать в присвоении» → refusal
   - «лишить» → revocation
   - Если в заголовке приложения указано «присвоение (подтверждение)» —
     определяй action по контексту каждой строки если возможно,
     иначе используй «assignment»

6. Знак -«- (или -"-, «то же») означает значение из строки выше — подставь его

7. Дата рождения 10.02.2025 у спортсмена — подозрительно (человеку был бы 1 год),
   добавь в extra_fields: "birth_date_suspicious": true

8. «Самоподготовка» в поле тренера — не ФИО, записывай строкой: ["Самоподготовка"]

9. Несколько тренеров в одной ячейке — всегда массив:
   «Иванов И.И., Петров П.П.» → ["Иванов И.И.", "Петров П.П."]

10. Игнорируй служебный текст: реквизиты документа, подписи должностных лиц,
    технические пометки, колонтитулы страниц

11. ias_id — числовой идентификатор лица в ИАС «Спорт» (Информационно-аналитическая
    система спортивной отрасли города Москвы). Присутствует в приказах ЦСТиСК/Москомспорта
    в виде числовой колонки (обычно 5–7 цифр). Если нет — null.

12. submission_number — регистрационный номер пакета документов на присвоение.
    Может быть числом или строкой с префиксом (напр. «Р-1234»).
    Присутствует не во всех типах приказов. Если нет — null.
"""


# ---------------------------------------------------------------------------
# Экстрактор
# ---------------------------------------------------------------------------

class LLMExtractor:
    """
    Извлекает записи из текста PDF-приказа через Claude API.

    Использование:
        extractor = LLMExtractor(api_key="sk-ant-...", model="claude-haiku-4-5-20251001")
        rows = extractor.extract(
            text=pdf_text,
            issuing_body="ГКУ «ЦСТиСК» Москомспорта",
            order_date="17.02.2026",
            order_number="С-2/26"
        )
    """

    # Символьные лимиты для разбивки на чанки
    # claude-haiku context: ~200k токенов ≈ ~150k символов
    # Оставляем запас на промпт (~3k токенов)
    CHUNK_SIZE_CHARS = 120_000

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-haiku-4-5-20251001",
        max_tokens: int = 8192,
    ):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens

    # ------------------------------------------------------------------
    # Публичный метод
    # ------------------------------------------------------------------

    def extract(
        self,
        text: str,
        issuing_body: str = "",
        order_date: str = "",
        order_number: str = "",
    ) -> list[AssignmentRow]:
        """
        Основной метод: принимает текст PDF, возвращает список AssignmentRow.

        При тексте > CHUNK_SIZE_CHARS автоматически разбивает на чанки
        и объединяет результаты.
        """
        chunks = self._split_text(text)
        all_rows: list[AssignmentRow] = []

        for i, chunk in enumerate(chunks):
            raw = self._call_api(
                text=chunk,
                issuing_body=issuing_body,
                order_date=order_date,
                order_number=order_number,
            )
            rows = self._parse_response(raw)
            all_rows.extend(rows)

        return all_rows

    # ------------------------------------------------------------------
    # Внутренние методы
    # ------------------------------------------------------------------

    def _split_text(self, text: str) -> list[str]:
        """
        Разбивает текст на чанки по CHUNK_SIZE_CHARS символов.
        Старается резать по границам страниц или пустым строкам.
        """
        if len(text) <= self.CHUNK_SIZE_CHARS:
            return [text]

        chunks = []
        start = 0
        while start < len(text):
            end = start + self.CHUNK_SIZE_CHARS
            if end >= len(text):
                chunks.append(text[start:])
                break

            # Ищем ближайший разрыв страницы или двойной перевод строки назад
            cut = text.rfind("\n\n", start, end)
            if cut == -1 or cut <= start:
                cut = text.rfind("\n", start, end)
            if cut == -1 or cut <= start:
                cut = end  # крайний случай — режем жёстко

            chunks.append(text[start:cut])
            start = cut + 1  # skip the newline

        return chunks

    def _call_api(
        self,
        text: str,
        issuing_body: str,
        order_date: str,
        order_number: str,
    ) -> str:
        """Вызывает Claude API и возвращает сырой текст ответа."""
        user_content = EXTRACTION_PROMPT.format(
            text=text,
            issuing_body=issuing_body or "не указан",
            order_date=order_date or "не указана",
            order_number=order_number or "не указан",
        )

        message = self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_content}],
        )
        return message.content[0].text

    def _parse_response(self, raw: str) -> list[AssignmentRow]:
        """
        Парсит JSON-ответ от LLM в список AssignmentRow.
        Устойчив к markdown-обёрткам и мелким JSON-ошибкам.
        """
        # Зачищаем markdown если модель всё же добавила
        cleaned = raw.strip()
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        cleaned = cleaned.strip()

        # Находим первый '[' и последний ']'
        start = cleaned.find("[")
        end = cleaned.rfind("]")
        if start == -1 or end == -1:
            raise ValueError(
                f"LLM вернул не JSON-массив. Первые 200 символов:\n{raw[:200]}"
            )
        json_str = cleaned[start : end + 1]

        try:
            items = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON decode error: {e}\nСтрока: {json_str[:500]}") from e

        rows = []
        for i, item in enumerate(items):
            try:
                row = self._item_to_row(item)
                rows.append(row)
            except Exception as e:
                # Не падаем из-за одной плохой записи — логируем и пропускаем
                print(f"[WARN] Запись {i} пропущена: {e}\n  Данные: {item}")

        return rows

    def _item_to_row(self, item: dict) -> AssignmentRow:
        """Конвертирует один dict из JSON в AssignmentRow с валидацией."""
        fio = self._clean_fio(item.get("fio", ""))
        if not fio:
            raise ValueError("Пустое ФИО")

        rank_category = item.get("rank_category", "").strip()
        if not rank_category:
            raise ValueError("Пустой rank_category")

        # assignment_type с fallback
        try:
            assignment_type = AssignmentType(item.get("assignment_type", "sport_rank"))
        except ValueError:
            assignment_type = AssignmentType.SPORT_RANK

        # action с fallback
        try:
            action = ActionType(item.get("action", "assignment"))
        except ValueError:
            action = ActionType.ASSIGNMENT

        # birth_date: нормализуем и проверяем
        birth_date = self._normalize_date(item.get("birth_date"))

        # ias_id: только int или None
        ias_id = item.get("ias_id")
        if ias_id is not None:
            try:
                ias_id = int(ias_id)
            except (TypeError, ValueError):
                ias_id = None

        # submission_number: строка или None
        submission_number = item.get("submission_number")
        if submission_number is not None:
            submission_number = str(submission_number).strip()
            if not submission_number:
                submission_number = None

        # sport нормализация
        sport = item.get("sport") or None
        sport_original = item.get("sport_original") or None
        if sport:
            sport = sport.strip()
        if sport_original and sport_original == sport:
            sport_original = None  # не дублируем если одинаковые

        # extra_fields: очищаем None-значения
        extra = item.get("extra_fields") or {}
        extra = {k: v for k, v in extra.items() if v is not None and v != "" and v != []}

        # Проверка подозрительной даты рождения
        if birth_date and (order_date := item.get("_order_date")):
            # Если год рождения > год приказа - 5 → подозрительно
            match = re.search(r"\d{2}\.\d{2}\.(\d{4})", birth_date)
            if match:
                birth_year = int(match.group(1))
                order_year_match = re.search(r"(\d{4})", order_date)
                if order_year_match:
                    order_year = int(order_year_match.group(1))
                    if birth_year > order_year - 5:
                        extra["birth_date_suspicious"] = True

        return AssignmentRow(
            fio=fio,
            rank_category=rank_category,
            assignment_type=assignment_type,
            action=action,
            sport=sport,
            sport_original=sport_original,
            birth_date=birth_date,
            ias_id=ias_id,
            submission_number=submission_number,
            extra_fields=extra,
        )

    # ------------------------------------------------------------------
    # Утилиты нормализации
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_fio(fio: str) -> str:
        """
        Зачищает ФИО:
        - убирает leading/trailing знаки препинания (OCR-артефакты вроде «-Суликова»)
        - убирает лишние пробелы
        - схлопывает внутренние пробелы
        """
        if not fio:
            return ""
        cleaned = fio.strip()
        # Убираем leading punctuation (дефис, точку, запятую и т.д.)
        cleaned = re.sub(r"^[\-\.\,\;\:]+\s*", "", cleaned)
        # Убираем trailing punctuation
        cleaned = re.sub(r"\s*[\-\.\,\;\:]+$", "", cleaned)
        # Схлопываем двойные пробелы
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip()

    @staticmethod
    def _normalize_date(date_val) -> Optional[str]:
        """
        Нормализует дату к формату ДД.ММ.ГГГГ.
        Убирает 'г.' в конце, исправляет разделители.
        Возвращает None если не распознано.
        """
        if not date_val:
            return None
        s = str(date_val).strip()
        # Убираем 'г.' в конце
        s = re.sub(r"\s*г\.$", "", s).strip()
        # Заменяем дефисы/слэши на точки
        s = re.sub(r"[-/]", ".", s)
        # Проверяем формат ДД.ММ.ГГГГ
        if re.match(r"^\d{2}\.\d{2}\.\d{4}$", s):
            return s
        # Пробуем ГГГГ.ММ.ДД → ДД.ММ.ГГГГ
        m = re.match(r"^(\d{4})\.(\d{2})\.(\d{2})$", s)
        if m:
            return f"{m.group(3)}.{m.group(2)}.{m.group(1)}"
        return None  # не распознали — не храним мусор


# ---------------------------------------------------------------------------
# CLI / быстрый тест
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    import os

    if len(sys.argv) < 2:
        print("Использование: python llm_extractor.py <pdf_or_txt_file> [issuing_body] [date] [number]")
        sys.exit(1)

    filepath = sys.argv[1]
    issuing_body = sys.argv[2] if len(sys.argv) > 2 else ""
    order_date   = sys.argv[3] if len(sys.argv) > 3 else ""
    order_number = sys.argv[4] if len(sys.argv) > 4 else ""

    # Читаем текст
    if filepath.endswith(".pdf"):
        try:
            import pypdf
            reader = pypdf.PdfReader(filepath)
            text = "\n".join(p.extract_text() or "" for p in reader.pages)
        except ImportError:
            print("Установи pypdf: pip install pypdf")
            sys.exit(1)
    else:
        with open(filepath, encoding="utf-8") as f:
            text = f.read()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Установи переменную окружения ANTHROPIC_API_KEY")
        sys.exit(1)

    extractor = LLMExtractor(
        api_key=api_key,
        model=os.environ.get("EXTRACTOR_MODEL", "claude-haiku-4-5-20251001"),
    )

    print(f"Извлекаю из: {filepath}")
    print(f"Орган: {issuing_body or '(не указан)'}")
    print(f"Длина текста: {len(text):,} символов")
    print("-" * 60)

    rows = extractor.extract(
        text=text,
        issuing_body=issuing_body,
        order_date=order_date,
        order_number=order_number,
    )

    print(f"Извлечено записей: {len(rows)}")
    print()

    # Выводим первые 5 записей
    for i, row in enumerate(rows[:5]):
        d = row.to_dict()
        print(f"[{i+1}] {d['fio']}")
        print(f"     Разряд: {d['rank_category']}")
        print(f"     Спорт: {d['sport']}")
        print(f"     Дата рожд.: {d['birth_date']}")
        print(f"     IAS ID: {d['ias_id']}")
        print(f"     № представления: {d['submission_number']}")
        print(f"     Action: {d['action']}")
        if d["extra_fields"]:
            print(f"     Extra: {d['extra_fields']}")
        print()

    if len(rows) > 5:
        print(f"... и ещё {len(rows) - 5} записей")

    # Сохраняем полный JSON
    out_path = filepath.rsplit(".", 1)[0] + "_extracted.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump([r.to_dict() for r in rows], f, ensure_ascii=False, indent=2)
    print(f"\nПолный результат сохранён: {out_path}")
