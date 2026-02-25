"""
rule_extractor.py
=================
Rule-based экстрактор данных из приказов о присвоении спортивных разрядов
и квалификационных категорий. Работает без LLM — чистый Python + regex.

Заменяет llm_extractor.py при отсутствии API-ключа, исчерпании лимитов
или блокировке провайдера.

Поддерживаемые форматы:
  1. TabularParser — табличные приказы (ЦСТиСК, КФКиС, Краснодар)
     Формат: № | ФИО | Дата рождения | Вид спорта | Категория | Дата представления
  2. SectionParser — приказы с секциями по видам спорта
     Заголовок вида спорта → блок записей
  3. FreeTextParser — распоряжения свободной формы
     Паттерн «присвоить ФИО разряд»

Интерфейс полностью совместим с LLMExtractor:
    extractor = RuleExtractor(sport_normalizer=normalizer)
    rows = extractor.extract(text, issuing_body, order_date, order_number)
    # rows: list[AssignmentRow]

Зависимости: только стандартная библиотека + llm_extractor (для AssignmentRow).
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Union

logger = logging.getLogger(__name__)

# Импортируем модель данных из llm_extractor (dataclass + enums)
try:
    from llm_extractor import AssignmentRow, AssignmentType, ActionType
except ImportError:
    # Standalone fallback: определяем минимальные модели
    from dataclasses import asdict
    from enum import Enum

    class AssignmentType(str, Enum):
        SPORT_RANK = "sport_rank"
        JUDGE_CATEGORY = "judge_category"
        SPECIALIST_CATEGORY = "specialist_category"
        COACH_CATEGORY = "coach_category"
        HONORARY_TITLE = "honorary_title"

    class ActionType(str, Enum):
        ASSIGNMENT = "assignment"
        CONFIRMATION = "confirmation"
        REFUSAL = "refusal"
        REVOCATION = "revocation"
        RESTORATION = "restoration"

    @dataclass
    class AssignmentRow:
        fio: str = ""
        birth_date: Optional[str] = None
        ias_id: Optional[int] = None
        submission_number: Optional[str] = None
        assignment_type: AssignmentType = AssignmentType.SPORT_RANK
        rank_category: str = ""
        sport: Optional[str] = None
        sport_original: Optional[str] = None
        action: ActionType = ActionType.ASSIGNMENT
        extra_fields: dict = field(default_factory=dict)
        confidence: float = 0.0
        llm_model: Optional[str] = None

        def to_dict(self):
            d = {
                "fio": self.fio,
                "birth_date": self.birth_date,
                "ias_id": self.ias_id,
                "submission_number": self.submission_number,
                "assignment_type": self.assignment_type.value
                    if isinstance(self.assignment_type, Enum)
                    else self.assignment_type,
                "rank_category": self.rank_category,
                "sport": self.sport,
                "sport_original": self.sport_original,
                "action": self.action.value
                    if isinstance(self.action, Enum)
                    else self.action,
                "extra_fields": self.extra_fields,
                "confidence": self.confidence,
                "llm_model": self.llm_model,
            }
            return d


# ---------------------------------------------------------------------------
# Константы и паттерны
# ---------------------------------------------------------------------------

# ФИО: 2-5 слов с заглавной буквы (поддержка «оглы», «кызы», etc.)
RE_FIO = re.compile(
    r'([А-ЯЁA-Z][а-яёa-z]+(?:\s+[А-ЯЁA-Za-z][а-яёa-z]+){1,4})'
)

# Дата: ДД.ММ.ГГГГ
RE_DATE = re.compile(r'(\d{2}\.\d{2}\.\d{4})')

# IAS ID: 4-7 цифр (обычно 5-6)
RE_IAS_ID = re.compile(r'\b(\d{4,7})\b')

# Номер строки таблицы: число от 1 до 9999 в начале строки
RE_ROW_NUM = re.compile(r'^\s*(\d{1,4})\s+')

# Строка данных: номер + ФИО + дата рождения + ... (универсальный паттерн)
# ФИО допускает склеенные слова (OCR артефакт): «МаксимАндреевич»
RE_DATA_ROW = re.compile(
    r'^\s*(\d{1,4})\s+'                       # № п/п
    r'([А-ЯЁ][а-яё]+(?:\s*[А-ЯЁа-яё][а-яё]+){1,4})\s+'  # ФИО (с опц. пробелами)
    r'(\d{2}\.\d{2}\.\d{4})\s+'               # Дата рождения
    r'(.+?)\s+'                                # Вид спорта (жадный до даты)
    r'(\d{2}\.\d{2}\.\d{4})'                   # Дата представления
)

# Альтернативный паттерн: с IAS ID (ЦСТиСК специфичный)
RE_DATA_ROW_IAS = re.compile(
    r'^\s*(\d{1,4})\s+'                        # № п/п
    r'([А-ЯЁ][а-яё]+(?:\s*[А-ЯЁа-яё][а-яё]+){1,4})\s+'  # ФИО
    r'(\d{2}\.\d{2}\.\d{4})\s+'               # Дата рождения
    r'(\d{4,7})\s+'                            # IAS ID
    r'(.+?)\s+'                                # Вид спорта
    r'(\d{2}\.\d{2}\.\d{4})'                   # Дата представления
)

# Паттерн для категории судьи (многострочный вывод pypdf)
RE_JUDGE_CAT = re.compile(
    r'[Сс]портивный\s+судья\s+'
    r'(первой|второй|третьей|высшей)\s*\n?\s*категории',
    re.IGNORECASE
)

# Разряды спортсменов
RANK_PATTERNS = {
    # === Спортивные звания (ЕВСК, Приказ №173 от 03.03.2025) ===
    r'(?:заслуж\w*\s+мастер\s+спорта|ЗМС)\b': 'заслуженный мастер спорта россии',
    r'(?:мастер\s+спорта\s+(?:России\s+)?международного\s+класса|МСМК)\b': 'мастер спорта россии международного класса',
    r'(?:гроссмейстер(?:\s+России)?|ГМ|ГМР)\b': 'гроссмейстер россии',
    r'(?:кандидат\s+в\s+мастера\s+спорта|КМС)\b': 'кандидат в мастера спорта',
    r'(?:мастер\s+спорта(?:\s+России)?|МС)\b': 'мастер спорта россии',

    # === Почётные спортивные звания (Приказ №856 от 24.10.2022) ===
    r'(?:заслуж\w*\s+тренер\s+России|ЗТР)\b': 'заслуженный тренер россии',
    r'почетн\w*\s+спортивн\w*\s+судь\w*\s+России': 'почетный спортивный судья россии',
    r'почетн\w*\s+мастер\w*\s+спорта\s+России': 'почетный мастер спорта россии',
    r'почетн\w*\s+тренер\w*\s+России': 'почетный тренер россии',

    # === Юношеские спортивные разряды (ЕВСК: III до I — длинные перед короткими) ===
    r'(?:третий|3|III)\s*(?:-й)?\s*(?:юношеский\s+)?(?:юношеский\s+)?(?:спортивный\s+)?разряд\s*\(?\s*юнош': 'третий юношеский спортивный разряд',
    r'(?:второй|2|II)\s*(?:-й)?\s*(?:юношеский\s+)?(?:юношеский\s+)?(?:спортивный\s+)?разряд\s*\(?\s*юнош': 'второй юношеский спортивный разряд',
    r'(?:первый|1|I)\s*(?:-й)?\s*(?:юношеский\s+)?(?:юношеский\s+)?(?:спортивный\s+)?разряд\s*\(?\s*юнош': 'первый юношеский спортивный разряд',
    r'(?:третий|3)\s+юношеский\s+(?:спортивный\s+)?разряд': 'третий юношеский спортивный разряд',
    r'(?:второй|2)\s+юношеский\s+(?:спортивный\s+)?разряд': 'второй юношеский спортивный разряд',
    r'(?:первый|1)\s+юношеский\s+(?:спортивный\s+)?разряд': 'первый юношеский спортивный разряд',
    r'\bIII\s+юнош': 'третий юношеский спортивный разряд',
    r'\bII\s+юнош': 'второй юношеский спортивный разряд',
    r'\bI\s+юнош': 'первый юношеский спортивный разряд',

    # === Спортивные разряды (ВАЖНО: III/II до I — иначе I матчится как подстрока II) ===
    r'(?:третий|3)\s*(?:-й)?\s*(?:спортивный\s+)?разряд': 'третий спортивный разряд',
    r'(?:второй|2)\s*(?:-й)?\s*(?:спортивный\s+)?разряд': 'второй спортивный разряд',
    r'(?:первый|1)\s*(?:-й)?\s*(?:спортивный\s+)?разряд': 'первый спортивный разряд',
    r'\bIII\s*(?:-й)?\s*(?:спортивный\s+)?разряд': 'третий спортивный разряд',
    r'\bII\s*(?:-й)?\s*(?:спортивный\s+)?разряд': 'второй спортивный разряд',
    r'\bI\s*(?:-й)?\s*(?:спортивный\s+)?разряд': 'первый спортивный разряд',

    # === Квалификационные категории спортивных судей (Приказ №134 от 28.02.2017) ===
    r'[Сс]портивный\s+судья\s+всеросс\w*\s*\n?\s*категории': 'спортивный судья всероссийской категории',
    r'[Сс]портивный\s+судья\s+первой\s*\n?\s*категории': 'спортивный судья первой категории',
    r'[Сс]портивный\s+судья\s+второй\s*\n?\s*категории': 'спортивный судья второй категории',
    r'[Сс]портивный\s+судья\s+третьей\s*\n?\s*категории': 'спортивный судья третьей категории',
    r'[Юю]ный\s+спортивный\s+судья': 'юный спортивный судья',

    # === Квалификационные категории специалистов (Приказ №838) ===
    r'[Сс]пециалист\s+(?:высшей|первой|второй)\s*\n?\s*квалификационной\s*\n?\s*категории': None,  # dynamic
}

# Действия
ACTION_PATTERNS = {
    r'присвоить': ActionType.ASSIGNMENT,
    r'подтвердить|считать\s+подтвердив': ActionType.CONFIRMATION,
    r'отказать': ActionType.REFUSAL,
    r'лишить': ActionType.REVOCATION,
    r'восстановить': ActionType.RESTORATION,
}

# Ditto marks (знак повтора)
DITTO_PATTERNS = re.compile(
    r'^[\s]*(?:-\s*[«""\"]?\s*-|то\s+же|—\s*[«""\"]?\s*—|-\s*//\s*-)\s*$',
    re.IGNORECASE
)

# Страничный разделитель (нижний колонтитул pypdf)
RE_PAGE_FOOTER = re.compile(
    r'(?:Документ\s+зарегистрирован|Страница\s+\d+\s+из\s+\d+)',
    re.IGNORECASE
)

# Заголовок таблицы
RE_TABLE_HEADER = re.compile(
    r'(?:№\s+ФИО|№\s+п/?п\s+ФИО|Фамилия.*Имя.*Отчество)',
    re.IGNORECASE
)


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def clean_text(text: str) -> str:
    """Базовая очистка текста."""
    # Нормализация пробелов (но не переносов строк)
    text = re.sub(r'[^\S\n]+', ' ', text)
    # Удаление BOM, zero-width chars
    text = text.replace('\ufeff', '').replace('\u200b', '')
    return text


def validate_date(date_str: str) -> bool:
    """Проверяет, что дата валидна и в разумном диапазоне."""
    try:
        d = datetime.strptime(date_str, "%d.%m.%Y")
        return 1930 <= d.year <= 2030
    except (ValueError, TypeError):
        return False


def validate_birth_date(birth_str: str, order_date: str = "") -> bool:
    """Проверяет дату рождения на подозрительность."""
    if not validate_date(birth_str):
        return False
    try:
        bd = datetime.strptime(birth_str, "%d.%m.%Y")
        if order_date:
            od = datetime.strptime(order_date, "%d.%m.%Y")
            age = (od - bd).days / 365.25
            return 5 <= age <= 100
        return True
    except (ValueError, TypeError):
        return True


def detect_assignment_type(text: str) -> AssignmentType:
    """Определяет тип присвоения по тексту документа."""
    lower = text[:3000].lower()  # смотрим начало
    # Почётные звания (Приказ №856: ЗМС, ЗТР, почётный судья/мастер/тренер)
    if 'почетн' in lower or 'почётн' in lower:
        return AssignmentType.HONORARY_TITLE
    if 'заслуженн' in lower and ('мастер' in lower or 'тренер' in lower):
        return AssignmentType.HONORARY_TITLE
    if 'спортивный судья' in lower or 'судей' in lower or 'судьи' in lower:
        return AssignmentType.JUDGE_CATEGORY
    if 'специалист' in lower:
        return AssignmentType.SPECIALIST_CATEGORY
    if 'тренер' in lower or 'зтр' in lower:
        return AssignmentType.COACH_CATEGORY
    return AssignmentType.SPORT_RANK


def detect_action(text: str) -> ActionType:
    """Определяет действие по тексту документа."""
    lower = text[:3000].lower()
    for pattern, action in ACTION_PATTERNS.items():
        if re.search(pattern, lower):
            return action
    return ActionType.ASSIGNMENT


def normalize_rank(rank_text: str) -> str:
    """Нормализует название разряда/категории."""
    # Склеиваем многострочные категории
    rank_text = re.sub(r'\s*\n\s*', ' ', rank_text).strip()

    for pattern, normalized in RANK_PATTERNS.items():
        if re.search(pattern, rank_text, re.IGNORECASE):
            if normalized:
                return normalized
            # Для специалистов — извлекаем уровень
            m = re.search(
                r'(высшей|первой|второй)\s*квалификационной\s*категории',
                rank_text, re.IGNORECASE
            )
            if m:
                return f"специалист {m.group(1)} квалификационной категории"
    return rank_text.strip()


# ---------------------------------------------------------------------------
# Парсеры
# ---------------------------------------------------------------------------

class TabularParser:
    """
    Парсит табличные приказы: № | ФИО | ДР | Вид спорта | Категория | Дата.

    Особенность pypdf: категории извлекаются отдельным блоком после строк
    данных на каждой странице. Нужно сопоставлять по позиции.
    """

    def parse(
        self,
        text: str,
        order_date: str = "",
        default_type: AssignmentType = AssignmentType.SPORT_RANK,
        default_action: ActionType = ActionType.ASSIGNMENT,
        sport_normalizer=None,
    ) -> list[AssignmentRow]:
        """Парсит текст и возвращает список AssignmentRow."""
        text = clean_text(text)

        # Разбиваем на страницы по футеру
        pages = self._split_pages(text)
        all_rows = []

        for page_text in pages:
            rows = self._parse_page(
                page_text, order_date, default_type,
                default_action, sport_normalizer
            )
            all_rows.extend(rows)

        # Переиндексация — если на разных страницах нумерация продолжается,
        # проверяем уникальность
        logger.info(f"TabularParser: извлечено {len(all_rows)} записей")
        return all_rows

    def _split_pages(self, text: str) -> list[str]:
        """Разбивает текст на страницы по футерам."""
        parts = re.split(
            r'Документ\s+зарегистрирован[^\n]*\n'
            r'Страница\s+\d+\s+из\s+\d+[^\n]*\n?',
            text
        )
        # Фильтруем пустые
        return [p.strip() for p in parts if p.strip()]

    def _parse_page(
        self,
        page_text: str,
        order_date: str,
        default_type: AssignmentType,
        default_action: ActionType,
        sport_normalizer,
    ) -> list[AssignmentRow]:
        """Парсит одну страницу."""
        lines = page_text.split('\n')

        # Шаг 1: Извлекаем строки данных (с номером)
        data_rows = []
        # Шаг 2: Собираем категории (блок после строк данных)
        categories = []

        in_data = False
        in_categories = False
        current_category_lines = []

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            # Пропускаем заголовки таблицы
            if RE_TABLE_HEADER.search(stripped):
                in_data = True
                continue

            # Пропускаем строки типа "1 2 3 4 5 6" (нумерация столбцов)
            if re.match(r'^\d(\s+\d){2,}$', stripped):
                continue

            # Пропускаем служебные строки
            if RE_PAGE_FOOTER.search(stripped):
                continue
            if stripped.startswith('Приложение к') or stripped.startswith('Список лиц'):
                in_data = True
                continue

            # Попытка распарсить как строку данных
            m = RE_DATA_ROW_IAS.match(stripped)
            if m:
                data_rows.append({
                    'num': int(m.group(1)),
                    'fio': m.group(2).strip(),
                    'birth_date': m.group(3),
                    'ias_id': int(m.group(4)),
                    'sport': m.group(5).strip(),
                    'submission_date': m.group(6),
                })
                in_data = True
                in_categories = False
                continue

            m = RE_DATA_ROW.match(stripped)
            if m:
                data_rows.append({
                    'num': int(m.group(1)),
                    'fio': m.group(2).strip(),
                    'birth_date': m.group(3),
                    'ias_id': None,
                    'sport': m.group(4).strip(),
                    'submission_date': m.group(5),
                })
                in_data = True
                in_categories = False
                continue

            # Если мы после данных и видим текст категории
            if in_data and not in_categories:
                if re.search(r'(?:судья|разряд|категори|КМС|МС|мастер|специалист)',
                             stripped, re.IGNORECASE):
                    in_categories = True

            if in_categories:
                # Собираем строки категорий
                current_category_lines.append(stripped)

        # Шаг 3: Парсим категории из собранного блока
        if current_category_lines:
            categories = self._parse_category_block(
                '\n'.join(current_category_lines)
            )

        # Шаг 4: Сопоставляем данные с категориями
        rows = []
        for i, data in enumerate(data_rows):
            rank = categories[i] if i < len(categories) else ""

            # Нормализуем вид спорта
            sport = data['sport']
            sport_original = None
            if sport_normalizer:
                nr = sport_normalizer.normalize(sport)
                if nr.canonical_name:
                    if nr.canonical_name != sport:
                        sport_original = sport
                    sport = nr.canonical_name

            # Confidence
            conf = self._calc_confidence(data, rank)

            # Extra fields
            extra = {"parse_method": "rule_based"}
            if data.get('submission_date'):
                extra["submission_date"] = data['submission_date']
            if not validate_birth_date(data.get('birth_date', ''), order_date):
                extra["birth_date_suspicious"] = True
            if conf < 0.5:
                extra["needs_review"] = True

            row = AssignmentRow(
                fio=data['fio'],
                birth_date=data.get('birth_date'),
                ias_id=data.get('ias_id'),
                assignment_type=default_type,
                rank_category=normalize_rank(rank) if rank else "",
                sport=sport,
                sport_original=sport_original,
                action=default_action,
                extra_fields=extra,
                confidence=conf,
                llm_model="rule_extractor",
            )
            rows.append(row)

        return rows

    def _parse_category_block(self, block: str) -> list[str]:
        """
        Парсит блок категорий, где каждая категория может занимать 1-2 строки.
        Пример pypdf вывода:
            Спортивный судья третьей
            категории
            Спортивный судья второй
            категории
        """
        categories = []
        lines = block.split('\n')
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue

            # Проверяем: это начало категории?
            if re.search(r'(?:судья|разряд|КМС|МС|мастер|специалист|кандидат)',
                         line, re.IGNORECASE):
                # Skip garbage: header fragments, registration marks
                if re.search(r'(?:ГКУ|Москомспорт|___|Приложение|от\s+_|Список\s+лиц|зарегистрирован)',
                             line, re.IGNORECASE):
                    i += 1
                    continue
                # Может быть многострочная — склеиваем следующую строку
                full = line
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    # Если следующая строка — продолжение (например "категории")
                    if next_line and not re.match(r'^\d', next_line) and \
                       not re.search(r'^[Сс]портивный|^[Кк]андидат|^[Мм]астер',
                                     next_line):
                        full = full + ' ' + next_line
                        i += 1
                categories.append(full)
            i += 1

        return categories

    @staticmethod
    def _calc_confidence(data: dict, rank: str) -> float:
        """Вычисляет confidence для строки."""
        score = 0.0
        total = 5.0

        # ФИО валидно
        if data.get('fio') and len(data['fio'].split()) >= 2:
            score += 1.0
        # Дата рождения валидна
        if validate_date(data.get('birth_date', '')):
            score += 1.0
        # Вид спорта есть
        if data.get('sport') and len(data['sport']) > 2:
            score += 1.0
        # Категория есть
        if rank and len(rank) > 3:
            score += 1.0
        # IAS ID есть (необязательное)
        if data.get('ias_id'):
            score += 1.0
        else:
            total -= 0.5  # не штрафуем сильно

        return round(min(score / total, 1.0), 2)


class SectionParser:
    """
    Парсит приказы с секциями по видам спорта.

    Структура:
        Автомобильный спорт
        1 Иванов Иван Иванович 01.01.1990 ...
        2 Петров Пётр Петрович 02.02.1985 ...

        Бокс
        3 Сидоров Сергей 03.03.1992 ...
    """

    def parse(
        self,
        text: str,
        order_date: str = "",
        default_type: AssignmentType = AssignmentType.SPORT_RANK,
        default_action: ActionType = ActionType.ASSIGNMENT,
        sport_normalizer=None,
    ) -> list[AssignmentRow]:
        """Парсит секционный формат."""
        text = clean_text(text)

        # Удаляем футеры/хедеры
        text = re.sub(
            r'Документ\s+зарегистрирован[^\n]*\n'
            r'Страница\s+\d+\s+из\s+\d+[^\n]*',
            '', text
        )

        rows = []
        current_sport = None
        lines = text.split('\n')

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            # Пропускаем заголовки и служебные строки
            if RE_TABLE_HEADER.search(stripped):
                continue
            if re.match(r'^\d(\s+\d){2,}$', stripped):
                continue

            # Проверяем: это заголовок вида спорта?
            if sport_normalizer and not RE_ROW_NUM.match(stripped):
                nr = sport_normalizer.normalize(stripped.strip())
                if nr.canonical_name and nr.confidence >= 0.80:
                    current_sport = nr.canonical_name
                    continue

            # Пробуем как строку данных
            m = RE_DATA_ROW.match(stripped)
            if not m:
                m = RE_DATA_ROW_IAS.match(stripped)

            if m:
                groups = m.groups()
                if len(groups) == 5:
                    fio, bd, sport, subdate = groups[1], groups[2], groups[3], groups[4]
                    ias_id = None
                elif len(groups) == 6:
                    fio, bd, ias_id, sport, subdate = (
                        groups[1], groups[2], int(groups[3]),
                        groups[4], groups[5]
                    )
                else:
                    continue

                # Если вид спорта из секции, используем его
                use_sport = current_sport or sport.strip()

                extra = {"parse_method": "rule_based"}
                if subdate:
                    extra["submission_date"] = subdate

                row = AssignmentRow(
                    fio=fio.strip(),
                    birth_date=bd,
                    ias_id=ias_id if isinstance(ias_id, int) else None,
                    assignment_type=default_type,
                    rank_category="",  # в секционном формате категория в заголовке
                    sport=use_sport,
                    action=default_action,
                    extra_fields=extra,
                    confidence=0.75,
                    llm_model="rule_extractor",
                )
                rows.append(row)

        logger.info(f"SectionParser: извлечено {len(rows)} записей")
        return rows


class FreeTextParser:
    """
    Парсит приказы свободной формы (распоряжения).

    Ищет паттерны вида:
        «присвоить ФИО, ДД.ММ.ГГГГ г.р., разряд по виду спорта»
    """

    # Паттерн свободного текста
    RE_FREE = re.compile(
        r'([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁа-яё][а-яё]+){0,3})'
        r'[,\s]+(\d{2}\.\d{2}\.\d{4})\s*(?:г\.?\s*р\.?)?'
        r'[,\s—–-]+(.+?)'
        r'(?=\s*(?:[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁа-яё][а-яё]+){0,3}'
        r'[,\s]+\d{2}\.\d{2}\.\d{4}|\Z))',
        re.MULTILINE | re.DOTALL
    )

    def parse(
        self,
        text: str,
        order_date: str = "",
        default_type: AssignmentType = AssignmentType.SPORT_RANK,
        default_action: ActionType = ActionType.ASSIGNMENT,
        sport_normalizer=None,
    ) -> list[AssignmentRow]:
        """Парсит свободный текст."""
        text = clean_text(text)
        rows = []

        # Сначала определяем действие из контекста
        action = detect_action(text) or default_action
        atype = detect_assignment_type(text) or default_type

        # Находим все ФИО + дата рождения в тексте
        for m in self.RE_FREE.finditer(text):
            fio = m.group(1).strip()
            bd = m.group(2)
            context = m.group(3).strip()

            # Из контекста пытаемся извлечь разряд
            rank = ""
            for pattern, normalized in RANK_PATTERNS.items():
                rm = re.search(pattern, context, re.IGNORECASE)
                if rm:
                    rank = normalized or rm.group(0)
                    break

            # Вид спорта из контекста
            sport = None
            if sport_normalizer:
                # Ищем в контексте слова, похожие на виды спорта
                words = re.findall(r'[А-ЯЁа-яё]+(?:\s+[а-яё]+){0,3}', context)
                for w in words:
                    nr = sport_normalizer.normalize(w.strip())
                    if nr.canonical_name and nr.confidence >= 0.80:
                        sport = nr.canonical_name
                        break

            extra = {"parse_method": "rule_based_freetext"}
            if not rank:
                extra["needs_review"] = True

            row = AssignmentRow(
                fio=fio,
                birth_date=bd if validate_date(bd) else None,
                assignment_type=atype,
                rank_category=normalize_rank(rank) if rank else "",
                sport=sport,
                action=action,
                extra_fields=extra,
                confidence=0.5 if not rank else 0.7,
                llm_model="rule_extractor",
            )
            rows.append(row)

        logger.info(f"FreeTextParser: извлечено {len(rows)} записей")
        return rows


# ---------------------------------------------------------------------------
# Основной класс
# ---------------------------------------------------------------------------

class RuleExtractor:
    """
    Rule-based экстрактор. Drop-in замена LLMExtractor.

    Автоматически определяет формат приказа и выбирает парсер.
    Совместим с pipeline_orchestrator: возвращает list[AssignmentRow].
    """

    def __init__(self, sport_normalizer=None):
        """
        Args:
            sport_normalizer: экземпляр SportNormalizer (опционально,
                              повышает качество нормализации видов спорта).
        """
        self.sport_normalizer = sport_normalizer
        self.tabular = TabularParser()
        self.section = SectionParser()
        self.freetext = FreeTextParser()

    def extract(
        self,
        text: str,
        issuing_body: str = "",
        order_date: str = "",
        order_number: str = "",
        source_code: str = "",
    ) -> list[AssignmentRow]:
        """
        Извлекает записи из текста приказа.

        Интерфейс совместим с LLMExtractor.extract().
        Дополнительный параметр source_code помогает выбрать парсер.

        Returns:
            list[AssignmentRow] — извлечённые записи.
        """
        if not text or len(text.strip()) < 50:
            logger.warning("RuleExtractor: пустой или слишком короткий текст")
            return []

        # Определяем тип и действие из заголовка документа
        default_type = detect_assignment_type(text)
        default_action = detect_action(text)

        # Выбираем парсер
        parser_name, rows = self._auto_parse(
            text, order_date, default_type, default_action, source_code
        )

        # Пост-обработка
        rows = self._post_process(rows, order_date, order_number)

        logger.info(
            f"RuleExtractor: {parser_name} → {len(rows)} записей, "
            f"avg confidence={self._avg_confidence(rows):.2f}"
        )

        return rows

    def _auto_parse(
        self,
        text: str,
        order_date: str,
        default_type: AssignmentType,
        default_action: ActionType,
        source_code: str,
    ) -> tuple[str, list[AssignmentRow]]:
        """Автоопределение формата и выбор парсера."""

        kwargs = dict(
            order_date=order_date,
            default_type=default_type,
            default_action=default_action,
            sport_normalizer=self.sport_normalizer,
        )

        # Подсказка по source_code
        if source_code in ("moskva_tstisk", "moskva_moskumsport"):
            # Известный табличный формат с pypdf-спецификой
            rows = self.tabular.parse(text, **kwargs)
            if rows:
                return "TabularParser", rows

        # Эвристика 1: есть ли нумерованные строки с датами?
        data_row_count = len(RE_DATA_ROW.findall(text))
        data_row_ias_count = len(RE_DATA_ROW_IAS.findall(text))

        if data_row_count >= 3 or data_row_ias_count >= 3:
            # Табличный формат
            rows = self.tabular.parse(text, **kwargs)
            if rows:
                return "TabularParser", rows

        # Эвристика 2: есть ли заголовки видов спорта?
        if self.sport_normalizer:
            sport_headers = 0
            for line in text.split('\n'):
                line = line.strip()
                if line and not RE_ROW_NUM.match(line) and len(line) < 60:
                    nr = self.sport_normalizer.normalize(line)
                    if nr.canonical_name and nr.confidence >= 0.85:
                        sport_headers += 1
            if sport_headers >= 2:
                rows = self.section.parse(text, **kwargs)
                if rows:
                    return "SectionParser", rows

        # Эвристика 3: свободный текст с ФИО и датами
        fio_count = len(RE_FIO.findall(text))
        date_count = len(RE_DATE.findall(text))
        if fio_count >= 3 and date_count >= 3:
            rows = self.freetext.parse(text, **kwargs)
            if rows:
                return "FreeTextParser", rows

        # Fallback: пробуем все по порядку
        for name, parser in [
            ("TabularParser", self.tabular),
            ("SectionParser", self.section),
            ("FreeTextParser", self.freetext),
        ]:
            rows = parser.parse(text, **kwargs)
            if rows:
                return name, rows

        logger.warning("RuleExtractor: не удалось извлечь записи ни одним парсером")
        return "None", []

    def _post_process(
        self,
        rows: list[AssignmentRow],
        order_date: str,
        order_number: str,
    ) -> list[AssignmentRow]:
        """Пост-обработка: дедупликация, валидация, обогащение."""
        seen = set()
        result = []

        for row in rows:
            # Дедупликация по ФИО + дата рождения
            key = (row.fio, row.birth_date)
            if key in seen:
                continue
            seen.add(key)

            # Валидация ФИО
            if not row.fio or len(row.fio) < 3:
                continue

            # Расклеивание OCR-артефактов: «МаксимАндреевич» → «Максим Андреевич»
            row.fio = re.sub(r'([а-яё])([А-ЯЁ])', r'\1 \2', row.fio)

            # Убираем мусорные ФИО (заголовки, которые случайно распарсились)
            if re.search(
                r'(?:Приложение|Список|Приказ|категори|разряд)',
                row.fio, re.IGNORECASE
            ):
                continue

            # Валидация даты рождения
            if row.birth_date and not validate_date(row.birth_date):
                row.extra_fields["birth_date_suspicious"] = True
                row.confidence = min(row.confidence, 0.6)

            # Подозрительная дата рождения
            if row.birth_date and order_date:
                if not validate_birth_date(row.birth_date, order_date):
                    row.extra_fields["birth_date_suspicious"] = True

            result.append(row)

        return result

    @staticmethod
    def _avg_confidence(rows: list[AssignmentRow]) -> float:
        if not rows:
            return 0.0
        return sum(r.confidence for r in rows) / len(rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse
    import asyncio
    import json

    parser = argparse.ArgumentParser(
        description="Rule-based экстрактор SportRank"
    )
    sub = parser.add_subparsers(dest="command")

    p_file = sub.add_parser("extract", help="Извлечь записи из PDF")
    p_file.add_argument("pdf", help="Путь к PDF")
    p_file.add_argument("--source", default="", help="Код источника")
    p_file.add_argument("--date", default="", help="Дата приказа")
    p_file.add_argument("--number", default="", help="Номер приказа")
    p_file.add_argument("--sport-xls", default=None, help="Путь к ВРВС XLS")
    p_file.add_argument("--json", action="store_true", help="Вывод в JSON")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if args.command == "extract":
        # OCR
        from ocr_pipeline import OcrPipeline

        async def run():
            ocr = OcrPipeline()
            result = await ocr.process(args.pdf)
            text = result.text

            # Sport normalizer
            normalizer = None
            if args.sport_xls:
                from sport_normalizer import SportNormalizer
                normalizer = SportNormalizer()
                normalizer.load_xls(args.sport_xls)

            # Extract
            ext = RuleExtractor(sport_normalizer=normalizer)
            rows = ext.extract(
                text,
                order_date=args.date,
                order_number=args.number,
                source_code=args.source,
            )

            if args.json:
                import json
                data = [r.to_dict() for r in rows]
                print(json.dumps(data, ensure_ascii=False, indent=2))
            else:
                print(f"\nИзвлечено: {len(rows)} записей\n")
                for i, r in enumerate(rows, 1):
                    print(f"  {i:3d}. {r.fio}")
                    print(f"       ДР: {r.birth_date or '—'}")
                    print(f"       Спорт: {r.sport or '—'}")
                    print(f"       Категория: {r.rank_category or '—'}")
                    print(f"       Conf: {r.confidence}")
                    print()

        asyncio.run(run())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
