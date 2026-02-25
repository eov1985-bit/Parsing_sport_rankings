"""
admin_api.py
============
FastAPI-бэкенд для админ-панели SportRank.

Эндпоинты:
  GET  /api/dashboard         — сводка: источники, приказы, метрики
  GET  /api/sources           — список источников + статистика
  PATCH /api/sources/{code}   — обновить источник (risk_class, active)
  GET  /api/orders            — список приказов (фильтры: status, source, date)
  GET  /api/orders/{id}       — детали приказа + записи
  POST /api/orders/{id}/reprocess  — повторная обработка
  POST /api/orders/{id}/approve    — утвердить (статус → approved)
  POST /api/orders/{id}/reject     — отклонить (статус → rejected)
  GET  /api/assignments       — поиск по ФИО/спорту
  GET  /api/logs              — processing_log (фильтры: source, level, stage)
  GET  /api/quality           — метрики качества по источникам
  POST /api/actions/check-all — запуск проверки всех источников
  POST /api/actions/process-pending — запуск обработки очереди

Запуск:
  uvicorn admin_api:app --host 0.0.0.0 --port 8000 --reload

Зависимости:
  pip install fastapi uvicorn sqlalchemy[asyncio] asyncpg
"""

import json
import os
from datetime import datetime, date, timedelta
from typing import Optional
from uuid import UUID

# Динамическая регистрация доменов в SSRF-allowlist при создании источников
try:
    from pipeline_orchestrator import register_domain as _register_ssrf_domain
except ImportError:
    def _register_ssrf_domain(domain: str) -> None:
        pass  # pipeline_orchestrator не загружен

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 24
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "sportrank")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SportRank Admin API",
    version="1.1.0",
    description="Управление конвейером парсинга спортивных приказов",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

security = HTTPBearer(auto_error=False)


def _create_token(username: str) -> str:
    """Создаёт JWT-токен."""
    import jwt
    payload = {
        "sub": username,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRE_HOURS),
        "iat": datetime.utcnow(),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _verify_token(token: str) -> Optional[str]:
    """Проверяет JWT-токен. Возвращает username или None."""
    import jwt
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload.get("sub")
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None


async def require_auth(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> str:
    """FastAPI dependency — обязательная аутентификация."""
    if not credentials:
        raise HTTPException(401, "Требуется авторизация", headers={"WWW-Authenticate": "Bearer"})
    user = _verify_token(credentials.credentials)
    if not user:
        raise HTTPException(401, "Невалидный или просроченный токен", headers={"WWW-Authenticate": "Bearer"})
    return user


class LoginRequest(BaseModel):
    username: str
    password: str


@app.post("/api/auth/login")
async def login(body: LoginRequest):
    """Аутентификация. Возвращает JWT-токен."""
    if body.username == ADMIN_USER and body.password == ADMIN_PASS:
        token = _create_token(body.username)
        return {"token": token, "expires_in": JWT_EXPIRE_HOURS * 3600}
    raise HTTPException(401, "Неверные учётные данные")


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Проверяет JWT на всех /api/* кроме /api/auth и /api/health."""
    path = request.url.path
    # Пропускаем публичные маршруты
    if path in ("/api/auth/login", "/api/health", "/docs", "/openapi.json", "/redoc"):
        return await call_next(request)
    if not path.startswith("/api/"):
        return await call_next(request)
    # Проверяем токен
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=401, content={"detail": "Требуется авторизация"})
    token = auth[7:]
    user = _verify_token(token)
    if not user:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=401, content={"detail": "Невалидный токен"})
    request.state.user = user
    return await call_next(request)


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+asyncpg://sportrank:sportrank@localhost:5432/sportrank",
)

_engine = None


async def get_engine():
    global _engine
    if _engine is None:
        from sqlalchemy.ext.asyncio import create_async_engine
        _engine = create_async_engine(DB_URL, pool_size=5, echo=False)
    return _engine


async def fetch_all(sql: str, params: dict = None):
    from sqlalchemy import text
    engine = await get_engine()
    async with engine.begin() as conn:
        rows = await conn.execute(text(sql), params or {})
        return [dict(r._mapping) for r in rows.fetchall()]


async def fetch_one(sql: str, params: dict = None):
    from sqlalchemy import text
    engine = await get_engine()
    async with engine.begin() as conn:
        row = await conn.execute(text(sql), params or {})
        r = row.fetchone()
        return dict(r._mapping) if r else None


async def execute(sql: str, params: dict = None):
    from sqlalchemy import text
    engine = await get_engine()
    async with engine.begin() as conn:
        await conn.execute(text(sql), params or {})


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class SourceUpdate(BaseModel):
    risk_class: Optional[str] = None
    active: Optional[bool] = None


class SourceCreate(BaseModel):
    """Полная модель для создания/обновления источника через UI."""
    code: str
    name: str
    region: str
    federal_subject: Optional[str] = None
    source_type: str = "pdf_portal"
    risk_class: str = "green"
    active: bool = False  # новые источники отключены по умолчанию
    official_basis: Optional[str] = None

    # Download config
    download_method: str = "httpx"          # httpx | playwright
    base_url: str = ""
    antibot: Optional[str] = None
    delay_min: float = 1.0
    delay_max: float = 3.0
    wait_selector: Optional[str] = None

    # Detection config
    list_urls: list[str] = []
    link_regex: str = r'href=["\']([^"\']*\.pdf)["\']'
    title_regex: Optional[str] = None
    order_date_regex: Optional[str] = None
    order_number_regex: Optional[str] = None
    pagination: Optional[str] = None
    max_pages: int = 1
    js_var: Optional[str] = None

    # Meta config
    issuing_body: str = ""
    order_type: str = "приказ"

    def to_discovery_config(self) -> dict:
        return {
            "download": {
                "method": self.download_method,
                "base_url": self.base_url,
                "antibot": self.antibot,
                "delay_min": self.delay_min,
                "delay_max": self.delay_max,
                "wait_selector": self.wait_selector,
            },
            "detect": {
                "list_urls": self.list_urls,
                "link_regex": self.link_regex,
                "title_regex": self.title_regex,
                "order_date_regex": self.order_date_regex,
                "order_number_regex": self.order_number_regex,
                "pagination": self.pagination,
                "max_pages": self.max_pages,
                "js_var": self.js_var,
            },
            "meta": {
                "issuing_body": self.issuing_body,
                "order_type": self.order_type,
            },
        }


class RegexTestRequest(BaseModel):
    """Тест regex-паттерна на HTML-фрагменте."""
    html: str
    regex: str
    base_url: str = ""


class OrderAction(BaseModel):
    reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@app.get("/api/dashboard")
async def dashboard():
    """Сводка для главного экрана."""
    sources = await fetch_all("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE active) as active,
            COUNT(*) FILTER (WHERE risk_class = 'green') as green,
            COUNT(*) FILTER (WHERE risk_class = 'amber') as amber,
            COUNT(*) FILTER (WHERE risk_class = 'red') as red
        FROM registry_sources
    """)

    orders = await fetch_all("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE status = 'new') as new,
            COUNT(*) FILTER (WHERE status = 'downloaded') as downloaded,
            COUNT(*) FILTER (WHERE status = 'extracted') as extracted,
            COUNT(*) FILTER (WHERE status = 'approved') as approved,
            COUNT(*) FILTER (WHERE status = 'failed') as failed,
            COUNT(*) FILTER (WHERE created_at > NOW() - interval '24 hours') as last_24h,
            COUNT(*) FILTER (WHERE created_at > NOW() - interval '7 days') as last_7d
        FROM orders
    """)

    assignments = await fetch_all("""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT sport) as sports_count,
            COUNT(DISTINCT fio_normalized) as unique_people
        FROM assignments
    """)

    recent_errors = await fetch_all("""
        SELECT source_id, stage, message, created_at
        FROM processing_log
        WHERE level = 'error'
        ORDER BY created_at DESC
        LIMIT 5
    """)

    return {
        "sources": sources[0] if sources else {},
        "orders": orders[0] if orders else {},
        "assignments": assignments[0] if assignments else {},
        "recent_errors": recent_errors,
        "timestamp": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

@app.get("/api/sources")
async def list_sources():
    """Список источников со статистикой."""
    return await fetch_all("""
        SELECT
            rs.id, rs.code, rs.name, rs.region, rs.federal_subject,
            rs.source_type, rs.risk_class, rs.active,
            rs.last_checked_at, rs.last_page_hash,
            rs.official_basis, rs.discovery_config,
            rs.created_at, rs.updated_at,
            COUNT(DISTINCT o.id) FILTER (WHERE o.status = 'extracted') as orders_ok,
            COUNT(DISTINCT o.id) FILTER (WHERE o.status = 'new') as orders_pending,
            COUNT(DISTINCT o.id) FILTER (WHERE o.status = 'failed') as orders_failed,
            COUNT(a.id) as total_assignments,
            MAX(o.order_date) as last_order_date
        FROM registry_sources rs
        LEFT JOIN orders o ON o.source_id = rs.id
        LEFT JOIN assignments a ON a.order_id = o.id
        GROUP BY rs.id
        ORDER BY rs.code
    """)


@app.get("/api/sources/{code}")
async def get_source(code: str):
    """Детали одного источника с полной конфигурацией."""
    source = await fetch_one("""
        SELECT
            rs.*, 
            COUNT(DISTINCT o.id) FILTER (WHERE o.status = 'extracted') as orders_ok,
            COUNT(DISTINCT o.id) FILTER (WHERE o.status = 'new') as orders_pending,
            COUNT(DISTINCT o.id) FILTER (WHERE o.status = 'failed') as orders_failed,
            COUNT(a.id) as total_assignments
        FROM registry_sources rs
        LEFT JOIN orders o ON o.source_id = rs.id
        LEFT JOIN assignments a ON a.order_id = o.id
        WHERE rs.code = :code
        GROUP BY rs.id
    """, {"code": code})
    if not source:
        raise HTTPException(404, f"Source '{code}' not found")
    return source


@app.post("/api/sources")
async def create_source(body: SourceCreate):
    """Создать новый источник через UI."""
    # Валидация
    if body.source_type not in ("pdf_portal", "json_embed", "html_table"):
        raise HTTPException(400, "source_type must be pdf_portal/json_embed/html_table")
    if body.risk_class not in ("green", "amber", "red"):
        raise HTTPException(400, "risk_class must be green/amber/red")
    if not body.code or not body.name:
        raise HTTPException(400, "code and name are required")
    if not body.list_urls:
        raise HTTPException(400, "At least one list_url is required")

    # Проверка уникальности
    existing = await fetch_one(
        "SELECT code FROM registry_sources WHERE code = :code",
        {"code": body.code},
    )
    if existing:
        raise HTTPException(409, f"Source '{body.code}' already exists")

    config = body.to_discovery_config()

    await execute("""
        INSERT INTO registry_sources
            (code, name, region, federal_subject, source_type,
             discovery_config, official_basis, risk_class, active)
        VALUES
            (:code, :name, :region, :fs, :st,
             :config::jsonb, :ob, :rc, :active)
    """, {
        "code": body.code,
        "name": body.name,
        "region": body.region,
        "fs": body.federal_subject,
        "st": body.source_type,
        "config": json.dumps(config, ensure_ascii=False),
        "ob": body.official_basis,
        "rc": body.risk_class,
        "active": body.active,
    })

    # Регистрируем домен в SSRF-allowlist (без перезапуска)
    if body.base_url:
        from urllib.parse import urlparse as _up
        _host = _up(body.base_url).hostname
        if _host:
            _register_ssrf_domain(_host)

    return {"ok": True, "code": body.code, "message": "Source created"}


@app.put("/api/sources/{code}")
async def update_source_full(code: str, body: SourceCreate):
    """Полное обновление источника через UI."""
    existing = await fetch_one(
        "SELECT id FROM registry_sources WHERE code = :code",
        {"code": code},
    )
    if not existing:
        raise HTTPException(404, f"Source '{code}' not found")

    config = body.to_discovery_config()

    await execute("""
        UPDATE registry_sources SET
            name = :name, region = :region, federal_subject = :fs,
            source_type = :st, discovery_config = :config::jsonb,
            official_basis = :ob, risk_class = :rc, active = :active,
            updated_at = NOW()
        WHERE code = :code
    """, {
        "code": code,
        "name": body.name,
        "region": body.region,
        "fs": body.federal_subject,
        "st": body.source_type,
        "config": json.dumps(config, ensure_ascii=False),
        "ob": body.official_basis,
        "rc": body.risk_class,
        "active": body.active,
    })

    # Регистрируем домен в SSRF-allowlist (без перезапуска)
    if body.base_url:
        from urllib.parse import urlparse as _up
        _host = _up(body.base_url).hostname
        if _host:
            _register_ssrf_domain(_host)

    return {"ok": True, "code": code, "message": "Source updated"}


@app.patch("/api/sources/{code}")
async def update_source(code: str, body: SourceUpdate):
    """Быстрое обновление источника (risk_class, active)."""
    fields, params = [], {"code": code}
    if body.risk_class is not None:
        if body.risk_class not in ("green", "amber", "red"):
            raise HTTPException(400, "risk_class must be green/amber/red")
        fields.append("risk_class = :rc")
        params["rc"] = body.risk_class
    if body.active is not None:
        fields.append("active = :act")
        params["act"] = body.active

    if not fields:
        raise HTTPException(400, "No fields to update")

    await execute(
        f"UPDATE registry_sources SET {', '.join(fields)}, updated_at = NOW() WHERE code = :code",
        params,
    )
    return {"ok": True}


@app.delete("/api/sources/{code}")
async def delete_source(code: str):
    """Удалить источник (только если нет приказов)."""
    orders = await fetch_one("""
        SELECT COUNT(*) as cnt FROM orders o
        JOIN registry_sources rs ON o.source_id = rs.id
        WHERE rs.code = :code
    """, {"code": code})

    if orders and orders["cnt"] > 0:
        raise HTTPException(
            400,
            f"Cannot delete: source has {orders['cnt']} orders. "
            f"Deactivate instead (PATCH active=false)."
        )

    await execute(
        "DELETE FROM registry_sources WHERE code = :code",
        {"code": code},
    )
    return {"ok": True, "message": f"Source '{code}' deleted"}


@app.post("/api/sources/test-regex")
async def test_regex(body: RegexTestRequest):
    """Тестирование regex-паттерна на HTML-фрагменте (для визуального конструктора)."""
    import re as _re
    from urllib.parse import urljoin

    try:
        compiled = _re.compile(body.regex, _re.IGNORECASE)
    except _re.error as e:
        return {"ok": False, "error": f"Invalid regex: {e}", "matches": []}

    raw_matches = compiled.findall(body.html)
    results = []
    for m in raw_matches[:50]:  # лимит на 50
        url = urljoin(body.base_url, m) if body.base_url else m
        results.append({"raw": m, "resolved": url})

    return {
        "ok": True,
        "total": len(raw_matches),
        "shown": len(results),
        "matches": results,
    }


@app.post("/api/sources/{code}/check")
async def trigger_check_source(code: str, background_tasks: BackgroundTasks):
    """Запустить проверку конкретного источника."""
    existing = await fetch_one(
        "SELECT code, active FROM registry_sources WHERE code = :code",
        {"code": code},
    )
    if not existing:
        raise HTTPException(404, f"Source '{code}' not found")

    # В продакшене: background_tasks.add_task(run_check_source, code)
    return {"ok": True, "message": f"Check triggered for {code}"}


# ---------------------------------------------------------------------------
# Source Testing: Live Page Fetch + Regex (шаг ④ аудита)
# ---------------------------------------------------------------------------

class LiveTestRequest(BaseModel):
    """Загрузить реальную страницу и применить regex."""
    url: str
    link_regex: str
    title_regex: Optional[str] = None
    base_url: str = ""


@app.post("/api/sources/test-live")
async def test_live_page(body: LiveTestRequest):
    """
    Загружает реальную страницу источника и применяет regex-паттерны.
    Позволяет оператору проверить, что конфигурация находит документы
    на живом сайте, без необходимости запускать change_detector.
    """
    import re as _re
    from urllib.parse import urljoin, urlparse

    # Валидация URL
    parsed = urlparse(body.url)
    if parsed.scheme not in ("http", "https"):
        return {"ok": False, "error": "URL должен начинаться с http:// или https://"}

    # Fetch страницы
    try:
        import httpx
        async with httpx.AsyncClient(
            timeout=15, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 SportRank/1.0"}
        ) as client:
            resp = await client.get(body.url)
            resp.raise_for_status()
            html = resp.text
    except ImportError:
        return {"ok": False, "error": "httpx не установлен"}
    except Exception as e:
        return {"ok": False, "error": f"Ошибка загрузки: {type(e).__name__}: {e}"}

    base = body.base_url or f"{parsed.scheme}://{parsed.netloc}"

    # Применяем link_regex
    try:
        link_re = _re.compile(body.link_regex, _re.IGNORECASE)
    except _re.error as e:
        return {"ok": False, "error": f"Невалидный link_regex: {e}"}

    raw_links = link_re.findall(html)
    links = []
    for m in raw_links[:30]:
        resolved = urljoin(base, m)
        links.append({"raw": m, "resolved": resolved})

    # Применяем title_regex (опционально)
    titles = []
    if body.title_regex:
        try:
            title_re = _re.compile(body.title_regex, _re.IGNORECASE)
            titles = title_re.findall(html)[:30]
        except _re.error:
            pass

    return {
        "ok": True,
        "url": body.url,
        "html_length": len(html),
        "links_found": len(raw_links),
        "links": links,
        "titles": titles[:30],
        "page_title": (lambda m: m.group(1) if m else None)(_re.search(r"<title>([^<]+)</title>", html, _re.I)),
    }


# ---------------------------------------------------------------------------
# Source Testing: Pipeline Smoke Test (шаг ⑤ аудита)
# ---------------------------------------------------------------------------

@app.post("/api/sources/{code}/test-pipeline")
async def test_pipeline_on_source(code: str, request: Request):
    """
    Тестовый прогон pipeline на одном PDF.
    Принимает PDF как binary body. Запускает OCR → rule_extractor
    и возвращает извлечённые записи без записи в БД.
    Позволяет оператору убедиться, что источник обрабатывается корректно.
    """
    source = await fetch_one(
        "SELECT code, discovery_config FROM registry_sources WHERE code = :code",
        {"code": code},
    )
    if not source:
        raise HTTPException(404, f"Source '{code}' not found")

    # Получаем PDF
    body = await request.body()
    if not body or body[:4] != b"%PDF":
        raise HTTPException(400, "Body должен быть валидным PDF-файлом")
    if len(body) > 50 * 1024 * 1024:
        raise HTTPException(400, "PDF слишком большой (макс. 50 MB)")

    dc = source.get("discovery_config", {}) if isinstance(source, dict) else {}
    meta = dc.get("meta", {})

    results = {
        "source": code,
        "pdf_size": len(body),
        "ocr": None,
        "extraction": None,
        "records": [],
        "errors": [],
    }

    # Шаг 1: OCR
    try:
        import asyncio
        from ocr_pipeline import OcrPipeline
        ocr = OcrPipeline()
        ocr_result = await ocr.process_bytes(body)
        results["ocr"] = {
            "page_count": ocr_result.page_count,
            "text_length": len(ocr_result.text),
            "method": ocr_result.method.value,
            "confidence": ocr_result.confidence,
            "text_preview": ocr_result.text[:500],
        }
    except Exception as e:
        results["errors"].append(f"OCR ошибка: {type(e).__name__}: {e}")
        return results

    # Шаг 2: Extraction (rule-based)
    try:
        from rule_extractor import RuleExtractor
        ext = RuleExtractor()
        rows = ext.extract(
            ocr_result.text,
            issuing_body=meta.get("issuing_body", ""),
            order_date="",
            order_number="",
            source_code=code,
        )
        results["extraction"] = {
            "method": "rule_extractor",
            "records_count": len(rows),
            "avg_confidence": round(sum(r.confidence for r in rows) / max(len(rows), 1), 3),
        }
        results["records"] = [
            {
                "fio": r.fio,
                "birth_date": r.birth_date,
                "sport": r.sport,
                "rank_category": r.rank_category,
                "assignment_type": r.assignment_type.value if hasattr(r.assignment_type, 'value') else str(r.assignment_type),
                "action": r.action.value if hasattr(r.action, 'value') else str(r.action),
                "confidence": r.confidence,
            }
            for r in rows[:100]  # показываем первые 100
        ]
    except Exception as e:
        results["errors"].append(f"Extraction ошибка: {type(e).__name__}: {e}")

    return results


# ---------------------------------------------------------------------------
# Golden Set Management (шаги ②⑦ аудита)
# ---------------------------------------------------------------------------

@app.post("/api/sources/{code}/golden-set")
async def upload_golden_set(code: str, request: Request):
    """
    Загрузить PDF в golden set источника.
    Файл сохраняется в ./golden/{code}/. При загрузке автоматически
    прогоняется OCR+extraction и сохраняется как expected baseline.
    """
    import os, hashlib

    source = await fetch_one(
        "SELECT id FROM registry_sources WHERE code = :code",
        {"code": code},
    )
    if not source:
        raise HTTPException(404, f"Source '{code}' not found")

    body = await request.body()
    if not body or body[:4] != b"%PDF":
        raise HTTPException(400, "Body должен быть валидным PDF")

    # Сохраняем файл
    golden_dir = os.path.join(
        os.environ.get("GOLDEN_SET_DIR", "./golden"), code
    )
    os.makedirs(golden_dir, exist_ok=True)

    file_hash = hashlib.sha256(body).hexdigest()[:12]
    filename = f"golden_{file_hash}.pdf"
    filepath = os.path.join(golden_dir, filename)

    with open(filepath, "wb") as f:
        f.write(body)

    # Автоматический прогон extraction
    extraction_result = None
    try:
        from ocr_pipeline import OcrPipeline
        from rule_extractor import RuleExtractor

        ocr = OcrPipeline()
        ocr_result = await ocr.process_bytes(body)
        ext = RuleExtractor()
        rows = ext.extract(ocr_result.text, source_code=code)

        extraction_result = {
            "records_count": len(rows),
            "records": [r.to_dict() for r in rows],
        }

        # Сохраняем expected JSON
        import json as _json
        json_path = filepath.replace(".pdf", "_expected.json")
        with open(json_path, "w", encoding="utf-8") as jf:
            _json.dump(extraction_result, jf, ensure_ascii=False, indent=2)

    except Exception as e:
        extraction_result = {"error": str(e)}

    return {
        "ok": True,
        "filename": filename,
        "hash": file_hash,
        "size": len(body),
        "extraction": extraction_result,
    }


@app.get("/api/sources/{code}/golden-set")
async def list_golden_set(code: str):
    """Список golden set файлов для источника."""
    import os, glob

    golden_dir = os.path.join(
        os.environ.get("GOLDEN_SET_DIR", "./golden"), code
    )

    if not os.path.isdir(golden_dir):
        return {"files": [], "count": 0}

    files = []
    for pdf in sorted(glob.glob(os.path.join(golden_dir, "*.pdf"))):
        name = os.path.basename(pdf)
        json_path = pdf.replace(".pdf", "_expected.json")
        has_expected = os.path.exists(json_path)

        expected_count = 0
        if has_expected:
            try:
                import json as _json
                with open(json_path) as jf:
                    data = _json.load(jf)
                    expected_count = data.get("records_count", 0)
            except Exception:
                pass

        files.append({
            "filename": name,
            "size": os.path.getsize(pdf),
            "has_expected": has_expected,
            "expected_count": expected_count,
        })

    return {"files": files, "count": len(files)}


@app.post("/api/sources/{code}/golden-set/{filename}/validate")
async def validate_golden_set(code: str, filename: str):
    """
    Повторно прогоняет pipeline на golden set файле и сравнивает
    с сохранённым expected результатом. Показывает дельту.
    Позволяет оператору проверить, что изменения не сломали извлечение.
    """
    import os

    golden_dir = os.path.join(
        os.environ.get("GOLDEN_SET_DIR", "./golden"), code
    )
    pdf_path = os.path.join(golden_dir, filename)
    json_path = pdf_path.replace(".pdf", "_expected.json")

    if not os.path.exists(pdf_path):
        raise HTTPException(404, "Golden set file not found")

    # Загружаем expected
    expected = None
    if os.path.exists(json_path):
        try:
            import json as _json
            with open(json_path) as jf:
                expected = _json.load(jf)
        except Exception:
            pass

    # Прогоняем extraction
    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    actual = None
    try:
        from ocr_pipeline import OcrPipeline
        from rule_extractor import RuleExtractor

        ocr = OcrPipeline()
        ocr_result = await ocr.process_bytes(pdf_bytes)
        ext = RuleExtractor()
        rows = ext.extract(ocr_result.text, source_code=code)
        actual = {
            "records_count": len(rows),
            "records": [r.to_dict() for r in rows],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

    # Сравнение
    expected_count = expected.get("records_count", 0) if expected else 0
    actual_count = actual["records_count"]
    delta = actual_count - expected_count

    return {
        "ok": True,
        "filename": filename,
        "expected_count": expected_count,
        "actual_count": actual_count,
        "delta": delta,
        "match": delta == 0,
        "status": "pass" if abs(delta) <= 2 else "warn" if abs(delta) <= 5 else "fail",
    }


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

@app.get("/api/orders")
async def list_orders(
    status: Optional[str] = None,
    source: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(50, le=200),
    offset: int = 0,
):
    """Список приказов с фильтрами."""
    where, params = ["1=1"], {}

    if status:
        where.append("o.status = :status")
        params["status"] = status
    if source:
        where.append("rs.code = :source")
        params["source"] = source
    if search:
        where.append("(o.order_number ILIKE :q OR o.title ILIKE :q)")
        params["q"] = f"%{search}%"

    params["limit"] = limit
    params["offset"] = offset

    return await fetch_all(f"""
        SELECT
            o.id, o.order_number, o.order_date, o.order_type,
            o.title, o.status, o.page_count, o.ocr_method,
            o.ocr_confidence, o.error_message,
            o.source_url, o.file_url, o.file_hash,
            o.created_at, o.extracted_at,
            rs.code as source_code, rs.name as source_name,
            (SELECT COUNT(*) FROM assignments a WHERE a.order_id = o.id) as record_count
        FROM orders o
        JOIN registry_sources rs ON o.source_id = rs.id
        WHERE {' AND '.join(where)}
        ORDER BY o.created_at DESC
        LIMIT :limit OFFSET :offset
    """, params)


@app.get("/api/orders/{order_id}")
async def get_order(order_id: str):
    """Детали приказа + все записи."""
    order = await fetch_one("""
        SELECT
            o.*, rs.code as source_code, rs.name as source_name
        FROM orders o
        JOIN registry_sources rs ON o.source_id = rs.id
        WHERE o.id = :id
    """, {"id": order_id})

    if not order:
        raise HTTPException(404, "Order not found")

    assignments = await fetch_all("""
        SELECT
            a.id, a.fio, a.birth_date, a.ias_id,
            a.submission_number, a.assignment_type,
            a.rank_category, a.sport, a.sport_original,
            a.action, a.extra_fields, a.confidence, a.llm_model,
            a.created_at
        FROM assignments a
        WHERE a.order_id = :oid
        ORDER BY a.created_at
    """, {"oid": order_id})

    logs = await fetch_all("""
        SELECT level, stage, message, details, created_at
        FROM processing_log
        WHERE order_id = :oid
        ORDER BY created_at
    """, {"oid": order_id})

    return {
        "order": order,
        "assignments": assignments,
        "logs": logs,
    }


@app.post("/api/orders/{order_id}/approve")
async def approve_order(order_id: str, body: OrderAction = OrderAction()):
    """Утвердить приказ (human-in-loop QA)."""
    order = await fetch_one("SELECT status FROM orders WHERE id = :id", {"id": order_id})
    if not order:
        raise HTTPException(404, "Order not found")
    if order["status"] != "extracted":
        raise HTTPException(400, f"Cannot approve order in status '{order['status']}'")

    await execute(
        "UPDATE orders SET status = 'approved' WHERE id = :id",
        {"id": order_id},
    )
    return {"ok": True, "status": "approved"}


@app.post("/api/orders/{order_id}/reject")
async def reject_order(order_id: str, body: OrderAction = OrderAction()):
    """Отклонить приказ."""
    await execute(
        "UPDATE orders SET status = 'rejected', error_message = :reason WHERE id = :id",
        {"id": order_id, "reason": body.reason or "Rejected by admin"},
    )
    return {"ok": True, "status": "rejected"}


@app.post("/api/orders/{order_id}/reprocess")
async def reprocess_order(order_id: str, background_tasks: BackgroundTasks):
    """Повторная обработка приказа (в фоне)."""
    order = await fetch_one("SELECT id, status FROM orders WHERE id = :id", {"id": order_id})
    if not order:
        raise HTTPException(404, "Order not found")

    await execute(
        "UPDATE orders SET status = 'new', error_message = NULL WHERE id = :id",
        {"id": order_id},
    )

    # В продакшене: запуск в фоне через background_tasks
    # background_tasks.add_task(run_reprocess, order_id)

    return {"ok": True, "status": "new", "message": "Queued for reprocessing"}


# ---------------------------------------------------------------------------
# Assignments
# ---------------------------------------------------------------------------

@app.get("/api/assignments")
async def search_assignments(
    q: Optional[str] = None,
    sport: Optional[str] = None,
    source: Optional[str] = None,
    limit: int = Query(50, le=200),
    offset: int = 0,
):
    """Поиск записей по ФИО/спорту."""
    where, params = ["1=1"], {}

    if q:
        where.append("(a.fio ILIKE :q OR a.fio_normalized ILIKE :q)")
        params["q"] = f"%{q}%"
    if sport:
        where.append("a.sport ILIKE :sport")
        params["sport"] = f"%{sport}%"
    if source:
        where.append("rs.code = :source")
        params["source"] = source

    params["limit"] = limit
    params["offset"] = offset

    return await fetch_all(f"""
        SELECT
            a.id, a.fio, a.birth_date, a.ias_id,
            a.assignment_type, a.rank_category, a.sport,
            a.action, a.confidence,
            o.order_number, o.order_date,
            rs.code as source_code
        FROM assignments a
        JOIN orders o ON a.order_id = o.id
        JOIN registry_sources rs ON o.source_id = rs.id
        WHERE {' AND '.join(where)}
        ORDER BY a.created_at DESC
        LIMIT :limit OFFSET :offset
    """, params)


# ---------------------------------------------------------------------------
# Logs & Quality
# ---------------------------------------------------------------------------

@app.get("/api/logs")
async def get_logs(
    source: Optional[str] = None,
    level: Optional[str] = None,
    stage: Optional[str] = None,
    limit: int = Query(100, le=500),
):
    where, params = ["1=1"], {"limit": limit}
    if source:
        where.append("rs.code = :source")
        params["source"] = source
    if level:
        where.append("pl.level = :level")
        params["level"] = level
    if stage:
        where.append("pl.stage = :stage")
        params["stage"] = stage

    return await fetch_all(f"""
        SELECT
            pl.id, pl.level, pl.stage, pl.message,
            pl.details, pl.created_at,
            rs.code as source_code, o.order_number
        FROM processing_log pl
        LEFT JOIN registry_sources rs ON pl.source_id = rs.id
        LEFT JOIN orders o ON pl.order_id = o.id
        WHERE {' AND '.join(where)}
        ORDER BY pl.created_at DESC
        LIMIT :limit
    """, params)


@app.get("/api/quality")
async def get_quality():
    """Метрики качества по источникам (последние)."""
    return await fetch_all("""
        SELECT DISTINCT ON (qm.source_id)
            qm.*,
            rs.code as source_code, rs.name as source_name
        FROM quality_metrics qm
        JOIN registry_sources rs ON qm.source_id = rs.id
        ORDER BY qm.source_id, qm.measured_at DESC
    """)


# ---------------------------------------------------------------------------
# Actions (pipeline triggers)
# ---------------------------------------------------------------------------

@app.post("/api/actions/check-all")
async def trigger_check_all(background_tasks: BackgroundTasks):
    """Запустить проверку всех источников."""
    # В продакшене: background_tasks.add_task(run_check_all)
    return {"ok": True, "message": "Change detection triggered"}


@app.post("/api/actions/process-pending")
async def trigger_process_pending(
    limit: int = Query(20, le=100),
    background_tasks: BackgroundTasks = None,
):
    """Запустить обработку очереди."""
    pending = await fetch_all(
        "SELECT COUNT(*) as cnt FROM orders WHERE status IN ('new', 'downloaded')"
    )
    count = pending[0]["cnt"] if pending else 0
    # В продакшене: background_tasks.add_task(run_process_pending, limit)
    return {"ok": True, "pending": count, "will_process": min(count, limit)}


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/api/health")
async def health():
    try:
        await fetch_one("SELECT 1 as ok")
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return {"status": "degraded", "db": str(e)}
