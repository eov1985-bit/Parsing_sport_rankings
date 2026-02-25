-- =============================================================================
-- SportRank — схема базы данных
-- PostgreSQL 15+
--
-- Покрывает источники:
--   Минспорт РФ (msrfinfo.ru), Москомспорт, ГКУ «ЦСТиСК», МОСОБЛСПОРТ,
--   Минспорт Краснодарского края, КФКиС Санкт-Петербурга
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Расширения
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";   -- для нечёткого поиска по ФИО

-- ---------------------------------------------------------------------------
-- Справочник источников (органы, выпускающие приказы)
-- ---------------------------------------------------------------------------
CREATE TABLE registry_sources (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code            TEXT UNIQUE NOT NULL,   -- краткий код: 'moskva_tstisk', 'spb_kfkis' и т.д.
    name            TEXT NOT NULL,          -- «ГКУ "ЦСТиСК" Москомспорта»
    region          TEXT NOT NULL,          -- «г. Москва», «Санкт-Петербург», «Краснодарский край»
    federal_subject TEXT,                   -- «77» (код субъекта РФ)

    -- Тип источника определяет стратегию парсинга
    source_type     TEXT NOT NULL           -- 'pdf_portal' | 'json_embed' | 'html_table'
                    CHECK (source_type IN ('pdf_portal', 'json_embed', 'html_table')),

    -- Конфигурация для Discovery Agent
    discovery_config JSONB NOT NULL DEFAULT '{}',
    -- Примеры discovery_config:
    -- pdf_portal:  {"base_url": "https://mst.mosreg.ru/...", "antibot": "servicepipe",
    --               "list_pattern": "/?page={n}", "link_selector": "a.document-link"}
    -- json_embed:  {"base_url": "https://msrfinfo.ru/awards/", "js_var": "$obj"}

    -- Основание официальности по 8-ФЗ: домен принадлежит органу/подведомственной
    -- организации. Хранится как атрибут для контроля качества источника.
    official_basis  TEXT,               -- «домен mos.ru принадлежит Правительству Москвы»

    -- Классификация риска антибот-защиты (green/amber/red)
    risk_class      TEXT DEFAULT 'green'
                    CHECK (risk_class IN ('green', 'amber', 'red')),

    -- Хеш последней известной версии страницы-источника (для change detection)
    last_page_hash  TEXT,
    last_etag       TEXT,               -- ETag от последнего запроса
    last_checked_at TIMESTAMPTZ,

    active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Приказы / распоряжения
-- ---------------------------------------------------------------------------
CREATE TABLE orders (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id       UUID NOT NULL REFERENCES registry_sources(id),

    -- Реквизиты документа
    order_number    TEXT NOT NULL,          -- «С-2/26», «150-р», «102»
    order_date      DATE NOT NULL,
    order_type      TEXT NOT NULL           -- 'приказ' | 'распоряжение'
                    CHECK (order_type IN ('приказ', 'распоряжение')),
    title           TEXT,                   -- «О присвоении спортивных разрядов»

    -- Источник файла
    source_url      TEXT,                   -- URL страницы/документа
    file_url        TEXT,                   -- прямая ссылка на PDF (если отдельный файл)
    file_hash       TEXT,                   -- SHA256 содержимого PDF

    -- Статус обработки
    status          TEXT NOT NULL DEFAULT 'new'
                    CHECK (status IN ('new', 'downloaded', 'extracted', 'approved', 'rejected', 'failed')),
    error_message   TEXT,

    -- Метаданные обработки
    ocr_method      TEXT                    -- 'pypdf' | 'pdfminer' | 'tesseract'
                    CHECK (ocr_method IN ('pypdf', 'pdfminer', 'tesseract', NULL)),
    ocr_confidence  REAL,                   -- 0.0–1.0 (если применялся OCR)
    extracted_at    TIMESTAMPTZ,
    page_count      INTEGER,

    -- Поле для хранения связанных файлов (СПб: приложения в отдельных PDF)
    -- Главный документ имеет parent_id = NULL
    -- Приложения ссылаются на parent_id главного документа
    parent_id       UUID REFERENCES orders(id),
    attachment_num  INTEGER,                -- номер приложения: 1, 2, 3...

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (source_id, order_number, order_date)
);

CREATE INDEX idx_orders_source     ON orders(source_id);
CREATE INDEX idx_orders_date       ON orders(order_date DESC);
CREATE INDEX idx_orders_status     ON orders(status);
CREATE INDEX idx_orders_parent     ON orders(parent_id);

-- ---------------------------------------------------------------------------
-- Записи о присвоении (основная таблица)
-- ---------------------------------------------------------------------------
CREATE TABLE assignments (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id        UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,

    -- ФИО
    fio             TEXT NOT NULL,          -- как в документе («Харуца Василе», «Тагир оглы»)
    fio_normalized  TEXT,                   -- для поиска: UPPER + убраны знаки

    -- Даты
    birth_date      DATE,                   -- NULL если не указана в документе
    birth_date_raw  TEXT,                   -- оригинал для аудита («01.01.2025 г.»)

    -- Идентификатор лица в ИАС «Спорт» (Информационно-аналитическая система
    -- спортивной отрасли города Москвы). Числовой ID, присваиваемый системой
    -- ИАС «Спорт» каждому зарегистрированному лицу (спортсмену, судье, тренеру).
    -- Присутствует в приказах ЦСТиСК/Москомспорта. Nullable: не все источники
    -- содержат этот идентификатор.
    ias_id          INTEGER,

    -- Номер представления — регистрационный номер пакета документов,
    -- поданных на присвоение спортивного разряда, звания или категории.
    -- Присваивается при регистрации пакета в органе. Может отсутствовать
    -- (не все органы/типы приказов содержат это поле). Текстовый, т.к.
    -- может содержать буквенные префиксы или дефисы (напр. «Р-1234»).
    submission_number TEXT,

    -- Тип записи
    assignment_type TEXT NOT NULL
                    CHECK (assignment_type IN (
                        'sport_rank',           -- КМС, МС, МСМК, ГМР, 1/2/3 разряд, юнош. разряды
                        'judge_category',       -- судья всерос./1/2/3 кат., юный спортивный судья
                        'specialist_category',  -- специалист высшей/первой кат.
                        'coach_category',       -- квалификационная категория тренера
                        'honorary_title'        -- ЗМС, ЗТР, почётный судья/мастер/тренер (Приказ №856)
                    )),

    -- Разряд/категория (нормализованное название)
    rank_category   TEXT NOT NULL,
    -- Примеры: «КМС», «II спортивный разряд», «судья первой категории»,
    --          «специалист высшей квалификационной категории»

    -- Вид спорта
    sport           TEXT,                   -- нормализованный («Киокусинкай»)
    sport_original  TEXT,                   -- как в документе (может содержать опечатки)
    sport_id        UUID REFERENCES sports(id), -- ссылка на справочник ВРВС (заполняется нормализатором)

    -- Действие
    action          TEXT NOT NULL DEFAULT 'assignment'
                    CHECK (action IN (
                        'assignment',     -- Присвоить
                        'confirmation',   -- Подтвердить
                        'refusal',        -- Отказать
                        'revocation',     -- Лишить
                        'restoration'     -- Восстановить (ЕВСК, Приказ №173)
                    )),

    -- Все опциональные поля в JSONB:
    -- Москва/ЦСТиСК:   submission_date (date str)
    -- Краснодар/СПб:   coach_fio (array), municipality (str), department (str)
    -- Специалисты:     position (str), organization (str)
    -- Подтверждения:   rank_start_date (date str)
    -- Отказы:          refusal_reason (str)
    -- Контроль:        birth_date_suspicious (bool)
    -- msrfinfo.ru:     person_id (int), deprivation (bool), is_secret (bool)
    extra_fields    JSONB NOT NULL DEFAULT '{}',

    -- Метаданные извлечения
    llm_model       TEXT,                   -- «claude-haiku-4-5-20251001»
    confidence      REAL,                   -- если LLM вернул уверенность

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Индексы для поиска
CREATE INDEX idx_assignments_order      ON assignments(order_id);
CREATE INDEX idx_assignments_fio        ON assignments USING gin(fio gin_trgm_ops);
CREATE INDEX idx_assignments_fio_norm   ON assignments(fio_normalized);
CREATE INDEX idx_assignments_sport      ON assignments(sport);
CREATE INDEX idx_assignments_sport_id   ON assignments(sport_id) WHERE sport_id IS NOT NULL;
CREATE INDEX idx_assignments_rank       ON assignments(rank_category);
CREATE INDEX idx_assignments_type       ON assignments(assignment_type);
CREATE INDEX idx_assignments_action     ON assignments(action);
CREATE INDEX idx_assignments_ias_id     ON assignments(ias_id) WHERE ias_id IS NOT NULL;
CREATE INDEX idx_assignments_sub_num   ON assignments(submission_number) WHERE submission_number IS NOT NULL;
CREATE INDEX idx_assignments_extra      ON assignments USING gin(extra_fields);

-- ---------------------------------------------------------------------------
-- Лог обработки (для отладки и мониторинга)
-- ---------------------------------------------------------------------------
CREATE TABLE processing_log (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id    UUID REFERENCES orders(id),
    source_id   UUID REFERENCES registry_sources(id),
    level       TEXT NOT NULL CHECK (level IN ('info', 'warn', 'error')),
    stage       TEXT NOT NULL,  -- 'download' | 'ocr' | 'extract' | 'save'
    message     TEXT NOT NULL,
    details     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_log_order   ON processing_log(order_id);
CREATE INDEX idx_log_level   ON processing_log(level);
CREATE INDEX idx_log_created ON processing_log(created_at DESC);

-- ---------------------------------------------------------------------------
-- Метрики качества по источникам (per-source, per-run)
-- Рекомендовано внешним аудитом: coverage, OCR rate, field completeness,
-- row yield, link rot
-- ---------------------------------------------------------------------------
CREATE TABLE quality_metrics (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id       UUID NOT NULL REFERENCES registry_sources(id),
    measured_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Coverage: доля обнаруженных документов, прошедших полный цикл
    coverage        REAL,               -- 0.0–1.0

    -- OCR rate: доля документов, где включился OCR (индикатор стоимости)
    ocr_rate        REAL,               -- 0.0–1.0

    -- Field completeness: заполненность ключевых полей
    field_completeness JSONB,           -- {"fio": 1.0, "sport": 0.95, "birth_date": 0.87, ...}

    -- Row yield: среднее число записей на документ
    avg_row_yield   REAL,
    min_row_yield   INTEGER,
    max_row_yield   INTEGER,

    -- Link rot: доля ссылок, которые перестали быть доступны
    link_rot_rate   REAL,               -- 0.0–1.0

    -- Средняя уверенность извлечения
    avg_confidence  REAL,

    -- Количество документов в выборке
    sample_size     INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_qm_source  ON quality_metrics(source_id);
CREATE INDEX idx_qm_date    ON quality_metrics(measured_at DESC);

-- ---------------------------------------------------------------------------
-- Golden sets: эталонные наборы для QA и CI/CD парсеров
-- ---------------------------------------------------------------------------
CREATE TABLE golden_sets (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id       UUID NOT NULL REFERENCES registry_sources(id),
    order_id        UUID REFERENCES orders(id),

    -- Эталонный JSON результат
    expected_json   JSONB NOT NULL,     -- массив ожидаемых AssignmentRow

    -- Метаданные
    description     TEXT,               -- «Приказ С-2/26 от 17.02.2026, 286 записей»
    record_count    INTEGER NOT NULL,
    verified_by     TEXT,               -- кто проверил (admin username)
    verified_at     TIMESTAMPTZ,

    active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_golden_source ON golden_sets(source_id);

-- ---------------------------------------------------------------------------
-- Справочник видов спорта (ВРВС — Всероссийский реестр видов спорта)
--
-- Версионная модель:
--   - sport_registry_versions: снимки реестра (загружаются из XLS)
--   - sports: канонические виды спорта (уникальные по code_base)
--   - sport_names: наименования с «сроком жизни» (valid_from/valid_to)
--     По умолчанию valid_to = NULL (бесконечный). При переименовании
--     старое имя получает valid_to, создаётся новое с valid_from.
--     Пример: «Тайский бокс» valid_to='2024-01-01' → «Муайтай» valid_from='2024-01-01'
--   - sport_disciplines: дисциплины внутри вида спорта
-- ---------------------------------------------------------------------------

-- Версии реестра (каждый импорт XLS — новая версия)
CREATE TABLE sport_registry_versions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    version_label   TEXT NOT NULL,         -- «ВРВС от 24.07.2025», «v2025-07»
    source_file     TEXT,                  -- имя файла XLS
    source_hash     TEXT,                  -- SHA256 файла
    imported_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sport_count     INTEGER,
    discipline_count INTEGER,
    notes           TEXT                   -- «Добавлен муайтай, исключён тайский бокс»
);

-- Канонические виды спорта
CREATE TABLE sports (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code_base       INTEGER NOT NULL,      -- базовый номер-код (152, 166, 70...)
    code_full       TEXT NOT NULL,          -- полный код «166-0-5-5-1-1-Я»
    section         INTEGER NOT NULL       -- раздел ВРВС: 1/2/3/4
                    CHECK (section IN (1, 2, 3, 4)),
    -- 1: Признанные (не нац./не воен./не общерос.)
    -- 2: Общероссийские
    -- 3: Национальные
    -- 4: Военно-прикладные и служебно-прикладные

    current_name    TEXT NOT NULL,          -- актуальное название
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (code_base, section)
);

CREATE INDEX idx_sports_name ON sports USING gin(current_name gin_trgm_ops);
CREATE INDEX idx_sports_code ON sports(code_base);

-- Наименования видов спорта с «сроком жизни»
-- Позволяет отслеживать переименования и сопоставлять
-- старые названия из приказов прошлых лет
CREATE TABLE sport_names (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sport_id        UUID NOT NULL REFERENCES sports(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,          -- «Тайский бокс», «Муайтай»
    name_normalized TEXT NOT NULL,          -- UPPER, без пунктуации, для поиска
    is_primary      BOOLEAN NOT NULL DEFAULT TRUE, -- основное имя в текущей версии ВРВС
    valid_from      DATE,                  -- с какой даты действует (NULL = всегда)
    valid_to        DATE,                  -- до какой даты (NULL = бессрочно)
    source_version_id UUID REFERENCES sport_registry_versions(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sport_names_norm ON sport_names(name_normalized);
CREATE INDEX idx_sport_names_gin  ON sport_names USING gin(name_normalized gin_trgm_ops);
CREATE INDEX idx_sport_names_sport ON sport_names(sport_id);

-- Спортивные дисциплины
CREATE TABLE sport_disciplines (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sport_id        UUID NOT NULL REFERENCES sports(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    code_full       TEXT,                  -- код дисциплины «152-13-1-8-1-1-Я»
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_disc_sport ON sport_disciplines(sport_id);
CREATE INDEX idx_disc_name  ON sport_disciplines USING gin(name gin_trgm_ops);

-- ---------------------------------------------------------------------------
-- Справочник должностей иных специалистов (Приказ Минспорта №838 от 19.10.2022)
-- ---------------------------------------------------------------------------
CREATE TABLE specialist_positions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code            TEXT UNIQUE NOT NULL,       -- краткий код: 'admin_trener', 'analitik' и т.д.
    name            TEXT NOT NULL,              -- «администратор тренировочного процесса»
    source_doc      TEXT NOT NULL DEFAULT 'Приказ №838 от 19.10.2022',
    effective_from  DATE NOT NULL DEFAULT '2022-10-19',
    effective_to    DATE,                       -- NULL = действует бессрочно
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Перечень иных специалистов (Приказ Минспорта №838 от 19.10.2022)
INSERT INTO specialist_positions (code, name) VALUES
    ('admin_tp',           'администратор тренировочного процесса'),
    ('analitik',           'аналитик'),
    ('vrach',              'врач по спортивной медицине'),
    ('instruktor_sport',   'инструктор по спорту'),
    ('instruktor_adapt',   'инструктор по адаптивной физической культуре'),
    ('massazhist',         'массажист'),
    ('mekhanic',           'механик'),
    ('nauchniy',           'научный сотрудник'),
    ('perevodchik_rzhya',  'переводчик русского жестового языка'),
    ('psiholog',           'психолог'),
    ('specialist_ad',      'специалист по антидопинговому обеспечению'),
    ('specialist_pitanie', 'специалист по спортивному питанию'),
    ('specialist_video',   'специалист по видеоанализу'),
    ('khoreograf',         'хореограф'),
    ('fizioterapevt',      'физиотерапевт');

-- ---------------------------------------------------------------------------
-- Справочник почётных спортивных званий (Приказ №856 от 24.10.2022, ред. 04.03.2025)
-- ---------------------------------------------------------------------------
CREATE TABLE honorary_titles (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code            TEXT UNIQUE NOT NULL,       -- 'zms', 'ztr', 'pochetniy_sudya', 'pochetniy_master', 'pochetniy_trener'
    name            TEXT NOT NULL,              -- «заслуженный мастер спорта России»
    source_doc      TEXT NOT NULL DEFAULT 'Приказ №856 от 24.10.2022',
    effective_from  DATE NOT NULL DEFAULT '2022-10-24',
    effective_to    DATE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO honorary_titles (code, name) VALUES
    ('zms',              'заслуженный мастер спорта России'),
    ('ztr',              'заслуженный тренер России'),
    ('pochetniy_sudya',  'почетный спортивный судья России'),
    ('pochetniy_master', 'почетный мастер спорта России'),
    ('pochetniy_trener', 'почетный тренер России');

-- ---------------------------------------------------------------------------
-- Справочник разрядов и званий ЕВСК (Приказ №173 от 03.03.2025)
-- ---------------------------------------------------------------------------
CREATE TABLE evsk_ranks (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code            TEXT UNIQUE NOT NULL,       -- 'kms', 'ms', 'msmk', 'gmr', '1sr', '2sr', '3sr', '1ysr', ...
    name            TEXT NOT NULL,              -- каноническое название: «кандидат в мастера спорта»
    category        TEXT NOT NULL               -- 'звание' | 'разряд' | 'юношеский_разряд'
                    CHECK (category IN ('звание', 'разряд', 'юношеский_разряд')),
    rank_order      INT NOT NULL,               -- порядок сортировки (МС=10, КМС=20, 1р=30...)
    source_doc      TEXT NOT NULL DEFAULT 'Приказ №173 от 03.03.2025',
    effective_from  DATE NOT NULL DEFAULT '2025-03-03',
    effective_to    DATE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO evsk_ranks (code, name, category, rank_order) VALUES
    ('msmk', 'мастер спорта россии международного класса', 'звание', 5),
    ('ms',   'мастер спорта россии',                       'звание', 10),
    ('gmr',  'гроссмейстер россии',                        'звание', 8),
    ('kms',  'кандидат в мастера спорта',                  'разряд', 20),
    ('1sr',  'первый спортивный разряд',                   'разряд', 30),
    ('2sr',  'второй спортивный разряд',                   'разряд', 40),
    ('3sr',  'третий спортивный разряд',                   'разряд', 50),
    ('1ysr', 'первый юношеский спортивный разряд',         'юношеский_разряд', 60),
    ('2ysr', 'второй юношеский спортивный разряд',         'юношеский_разряд', 70),
    ('3ysr', 'третий юношеский спортивный разряд',         'юношеский_разряд', 80);

-- ---------------------------------------------------------------------------
-- Триггер: автообновление updated_at на registry_sources
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_sources_updated_at
    BEFORE UPDATE ON registry_sources
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_sports_updated_at
    BEFORE UPDATE ON sports
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- Триггер: автозаполнение fio_normalized
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION normalize_fio()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.fio_normalized = UPPER(
        REGEXP_REPLACE(
            TRANSLATE(NEW.fio,
                'абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',
                'абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ'
            ),
            '[^А-ЯЁа-яё ]', '', 'g'
        )
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_assignments_fio_norm
    BEFORE INSERT OR UPDATE OF fio ON assignments
    FOR EACH ROW EXECUTE FUNCTION normalize_fio();

-- ---------------------------------------------------------------------------
-- Триггер: автозаполнение name_normalized в sport_names
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION normalize_sport_name()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.name_normalized = UPPER(
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(NEW.name, '[\s]+', ' ', 'g'),
                '[^А-ЯЁа-яёA-Za-z0-9 \-]', '', 'g'
            )
        )
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_sport_names_norm
    BEFORE INSERT OR UPDATE OF name ON sport_names
    FOR EACH ROW EXECUTE FUNCTION normalize_sport_name();

-- ---------------------------------------------------------------------------
-- Начальные данные: известные источники
-- ---------------------------------------------------------------------------
INSERT INTO registry_sources (code, name, region, federal_subject, source_type, discovery_config) VALUES

('moskva_tstisk', 'ГКУ «ЦСТиСК» Москомспорта', 'г. Москва', '77', 'pdf_portal',
 '{"base_url": "https://www.mos.ru/moskomsport/documents/", "doc_types": ["2_3_razryad", "sud_2_3", "specialist"]}'),

('moskva_moskumsport', 'Департамент спорта города Москвы (Москомспорт)', 'г. Москва', '77', 'pdf_portal',
 '{"base_url": "https://www.mos.ru/moskomsport/documents/", "doc_types": ["kms_1_razryad", "sud_1"]}'),

('mo_mособлспорт', 'Министерство физической культуры и спорта Московской области (МОСОБЛСПОРТ)', 'Московская область', '50', 'pdf_portal',
 '{"base_url": "https://mst.mosreg.ru/deyatelnost/dokumenty/prikazy-ob-osvоenii-sportivnykh/", "antibot": "servicepipe"}'),

('krasnodar_minsport', 'Министерство физической культуры и спорта Краснодарского края', 'Краснодарский край', '23', 'pdf_portal',
 '{"base_url": "https://minsport.krasnodar.ru/activities/sport/prisvoenie-sportivnyx-razryadov/"}'),

('spb_kfkis', 'Комитет по физической культуре и спорту Санкт-Петербурга (КФКиС)', 'г. Санкт-Петербург', '78', 'pdf_portal',
 '{"base_url": "https://kfis.gov.spb.ru/deyatelnost/prisvoenie-sportivnykh-razryadov/", "attachments_separate": true}'),

('rf_minsport', 'Министерство спорта Российской Федерации (msrfinfo.ru)', 'Российская Федерация', '00', 'json_embed',
 '{"base_url": "https://msrfinfo.ru/awards/", "js_var": "$obj", "covers": ["МС", "МСМК", "ЗМС", "ЗТР", "судья всерос"]}');

-- ---------------------------------------------------------------------------
-- Полезные представления
-- ---------------------------------------------------------------------------

-- Сводка по источникам: сколько приказов и записей обработано
CREATE VIEW v_source_stats AS
SELECT
    rs.code,
    rs.name,
    rs.region,
    COUNT(DISTINCT o.id)  AS orders_count,
    COUNT(a.id)           AS assignments_count,
    MAX(o.order_date)     AS last_order_date
FROM registry_sources rs
LEFT JOIN orders o ON o.source_id = rs.id AND o.status = 'extracted'
LEFT JOIN assignments a ON a.order_id = o.id
GROUP BY rs.id, rs.code, rs.name, rs.region
ORDER BY assignments_count DESC;

-- Поиск по ФИО с источником
CREATE VIEW v_assignments_full AS
SELECT
    a.id,
    a.fio,
    a.birth_date,
    a.ias_id,
    a.submission_number,
    a.assignment_type,
    a.rank_category,
    a.sport,
    a.sport_id,
    a.action,
    o.order_number,
    o.order_date,
    o.order_type,
    rs.name  AS issuing_body,
    rs.region,
    a.extra_fields,
    a.created_at
FROM assignments a
JOIN orders o     ON a.order_id  = o.id
JOIN registry_sources rs ON o.source_id = rs.id;
