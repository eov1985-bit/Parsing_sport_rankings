import { useState, useEffect, useReducer } from "react";

/* ═══════════════════════════════════════════════════════════════════════
   SPORTRANK Admin v3 — Production-ready Dashboard
   - No sessionStorage/localStorage (Claude artifact compatible)
   - No confirm() — state-based confirmation dialogs
   - In-memory state with simulated API calls
   - Full source management wizard (5 steps)
   - Inline regex tester
   - Pipeline test & golden set management
   ═══════════════════════════════════════════════════════════════════════ */

// ═══════════════════════ THEME ═══════════════════════
const C = {
  bg: "#080A0E", bg1: "#0E1117", bg2: "#161B25", bg3: "#1C2333", bgHov: "#222B3A",
  bdr: "#2A3345", bdrFocus: "#4A7ADB",
  tx: "#D8DCE4", txDim: "#8893A6", txMute: "#4E5A6E",
  acc: "#4B8DF8", accDim: "#3565B5", accGlow: "rgba(75,141,248,0.12)",
  ok: "#34D399", okBg: "rgba(52,211,153,0.08)",
  warn: "#F59E0B", warnBg: "rgba(245,158,11,0.08)",
  err: "#EF4444", errBg: "rgba(239,68,68,0.08)",
  info: "#60A5FA", infoBg: "rgba(96,165,250,0.08)",
  purp: "#A78BFA", purpBg: "rgba(167,139,250,0.08)",
};
const F = {
  mono: "'JetBrains Mono','SF Mono','Fira Code','Cascadia Code',monospace",
  sans: "'DM Sans','Nunito Sans',system-ui,-apple-system,sans-serif",
};
const RISK_MAP = { green: { l: "Green", c: C.ok, i: "●" }, amber: { l: "Amber", c: C.warn, i: "▲" }, red: { l: "Red", c: C.err, i: "■" } };
const STATUS_MAP = { new: { l: "Новый", c: C.info, bg: C.infoBg }, downloaded: { l: "Скачан", c: C.warn, bg: C.warnBg }, extracted: { l: "Извлечён", c: C.ok, bg: C.okBg }, approved: { l: "Утверждён", c: C.purp, bg: C.purpBg }, failed: { l: "Ошибка", c: C.err, bg: C.errBg } };
const fmt = {
  d: s => s ? new Date(s).toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit", year: "numeric" }) : "—",
  t: s => s ? new Date(s).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" }) : "—",
  n: v => v == null ? "—" : v.toLocaleString("ru-RU"),
};

// ═══════════════════════ INITIAL DATA ═══════════════════════
const INIT_SOURCES = [
  { code: "moskva_tstisk", name: "ГКУ «ЦСТиСК» Москомспорта", region: "г. Москва", federal_subject: "77", source_type: "pdf_portal", risk_class: "amber", active: true, official_basis: "Домен mos.ru принадлежит Правительству Москвы", orders_ok: 42, orders_pending: 1, orders_failed: 0, total_assignments: 1847, last_order_date: "2026-02-17", last_checked_at: "2026-02-24T10:00:00Z", download: { method: "playwright", base_url: "https://www.mos.ru", antibot: "servicepipe", delay_min: 2, delay_max: 6, wait_selector: "a[href$='.pdf']" }, detect: { list_urls: ["https://www.mos.ru/moskomsport/documents/prisvoenie-sportivnykh-razryadov/", "https://www.mos.ru/moskomsport/documents/prisvoenie-kvalifikatsionnykh-kategoriy/"], link_regex: 'href=["\']([^"\']*view/\\d+[^"\']*)["\']', title_regex: '>([^<]*(?:Приказ|Распоряжение)[^<]*)<', order_date_regex: '', order_number_regex: '', pagination: "", max_pages: 1, js_var: "" }, meta: { issuing_body: "ГКУ «ЦСТиСК» Москомспорта", order_type: "приказ" } },
  { code: "spb_kfkis", name: "КФКиС Санкт-Петербурга", region: "г. Санкт-Петербург", federal_subject: "78", source_type: "pdf_portal", risk_class: "green", active: true, official_basis: "Домен kfis.gov.spb.ru", orders_ok: 38, orders_pending: 2, orders_failed: 0, total_assignments: 892, last_order_date: "2026-02-20", last_checked_at: "2026-02-24T09:55:00Z", download: { method: "httpx", base_url: "https://kfis.gov.spb.ru", antibot: "", delay_min: 1, delay_max: 3, wait_selector: "" }, detect: { list_urls: ["https://kfis.gov.spb.ru/docs/?type=54"], link_regex: 'href=["\']([^"\']*?/docs/\\d+[^"\']*)["\']', title_regex: '', order_date_regex: '', order_number_regex: '', pagination: "&page={n}", max_pages: 3, js_var: "" }, meta: { issuing_body: "Комитет по ФКиС Санкт-Петербурга", order_type: "распоряжение" } },
  { code: "mo_mosoblsport", name: "МОСОБЛСПОРТ", region: "Московская область", federal_subject: "50", source_type: "pdf_portal", risk_class: "red", active: true, official_basis: "Домен mst.mosreg.ru", orders_ok: 15, orders_pending: 0, orders_failed: 2, total_assignments: 312, last_order_date: "2026-02-13", last_checked_at: "2026-02-23T14:00:00Z", download: { method: "playwright", base_url: "https://mst.mosreg.ru", antibot: "servicepipe", delay_min: 3, delay_max: 8, wait_selector: "a.document-link" }, detect: { list_urls: ["https://mst.mosreg.ru/dokumenty/prisvoenie-sportivnykh-razryadov/"], link_regex: 'href=["\']([^"\']*(?:rasporiaz|prikaz)[^"\']*)["\']', title_regex: '', order_date_regex: '', order_number_regex: '', pagination: "?page={n}", max_pages: 3, js_var: "" }, meta: { issuing_body: "Минспорт Московской области", order_type: "распоряжение" } },
  { code: "krasnodar_minsport", name: "Минспорт Краснодарского края", region: "Краснодарский край", federal_subject: "23", source_type: "pdf_portal", risk_class: "green", active: true, official_basis: "Домен minsport.krasnodar.ru", orders_ok: 12, orders_pending: 0, orders_failed: 0, total_assignments: 134, last_order_date: "2026-01-28", last_checked_at: "2026-02-24T10:03:00Z", download: { method: "httpx", base_url: "https://minsport.krasnodar.ru", antibot: "", delay_min: 1, delay_max: 4, wait_selector: "" }, detect: { list_urls: ["https://minsport.krasnodar.ru/activities/sport/prisvoenie/"], link_regex: 'href=["\']([^"\']*\\.pdf)["\']', title_regex: '', order_date_regex: '', order_number_regex: '', pagination: "", max_pages: 1, js_var: "" }, meta: { issuing_body: "Минспорт Краснодарского края", order_type: "приказ" } },
  { code: "rf_minsport", name: "Минспорт РФ (msrfinfo.ru)", region: "Россия", federal_subject: "00", source_type: "json_embed", risk_class: "green", active: false, official_basis: "msrfinfo.ru — информационный ресурс Минспорта", orders_ok: 9, orders_pending: 0, orders_failed: 0, total_assignments: 47, last_order_date: "2026-02-01", last_checked_at: "2026-02-22T06:00:00Z", download: { method: "httpx", base_url: "https://msrfinfo.ru", antibot: "", delay_min: 1, delay_max: 2, wait_selector: "" }, detect: { list_urls: ["https://msrfinfo.ru/awards/"], link_regex: '', title_regex: '', order_date_regex: '', order_number_regex: '', pagination: "", max_pages: 1, js_var: "$obj" }, meta: { issuing_body: "Минспорт Российской Федерации", order_type: "приказ" } },
];

const INIT_ORDERS = [
  { id: "o1", order_number: "С-2/26", order_date: "2026-02-17", status: "extracted", source_code: "moskva_tstisk", record_count: 286, page_count: 16 },
  { id: "o2", order_number: "Р-128/2026", order_date: "2026-02-20", status: "new", source_code: "spb_kfkis", record_count: 0, page_count: 0 },
  { id: "o3", order_number: "Р-129/2026", order_date: "2026-02-21", status: "new", source_code: "spb_kfkis", record_count: 0, page_count: 0 },
  { id: "o4", order_number: "23-18-рп", order_date: "2026-02-13", status: "failed", source_code: "mo_mosoblsport", record_count: 0, page_count: 4 },
  { id: "o5", order_number: "П-12/26", order_date: "2026-01-28", status: "approved", source_code: "krasnodar_minsport", record_count: 48, page_count: 6 },
];

const INIT_LOGS = [
  { lvl: "info", stage: "change_detection", src: "spb_kfkis", msg: "Обнаружено 2 новых документа", t: "2026-02-24T10:00:12Z" },
  { lvl: "info", stage: "ocr", src: "moskva_tstisk", msg: "16 стр., метод=pypdf, confidence=0.92, 303ms", t: "2026-02-18T12:00:45Z" },
  { lvl: "info", stage: "extract", src: "moskva_tstisk", msg: "286 записей (rule_extractor), avg_conf=0.87", t: "2026-02-18T12:01:02Z" },
  { lvl: "error", stage: "download", src: "mo_mosoblsport", msg: "Servicepipe CAPTCHA detected — retry 3/3 failed", t: "2026-02-24T08:12:00Z" },
  { lvl: "warn", stage: "ocr", src: "mo_mosoblsport", msg: "Tesseract confidence=0.52 < threshold(0.6)", t: "2026-02-14T08:01:00Z" },
  { lvl: "info", stage: "scheduler", src: "system", msg: "check-all: 5 источников проверено за 12.4s", t: "2026-02-24T06:00:01Z" },
];

// ═══════════════════════ STATE REDUCER ═══════════════════════
function appReducer(state, action) {
  switch (action.type) {
    case "SET_SOURCES": return { ...state, sources: action.payload };
    case "ADD_SOURCE": return { ...state, sources: [...state.sources, action.payload] };
    case "UPDATE_SOURCE": return { ...state, sources: state.sources.map(s => s.code === action.code ? { ...s, ...action.payload } : s) };
    case "DELETE_SOURCE": return { ...state, sources: state.sources.filter(s => s.code !== action.code) };
    case "TOGGLE_SOURCE": return { ...state, sources: state.sources.map(s => s.code === action.code ? { ...s, active: !s.active } : s) };
    case "ADD_LOG": return { ...state, logs: [action.payload, ...state.logs].slice(0, 50) };
    default: return state;
  }
}

// ═══════════════════════ UI ATOMS ═══════════════════════
const Badge = ({ color, bg, children }) => <span style={{ display: "inline-block", padding: "2px 10px", borderRadius: 4, fontSize: 10, fontWeight: 600, letterSpacing: .5, color, background: bg || "rgba(255,255,255,0.04)", fontFamily: F.mono, whiteSpace: "nowrap" }}>{children}</span>;
const RiskBadge = ({ risk }) => { const r = RISK_MAP[risk] || RISK_MAP.green; return <span style={{ color: r.c, fontFamily: F.mono, fontSize: 11, fontWeight: 600 }}>{r.i} {r.l}</span>; };
const Btn = ({ children, onClick, color = C.acc, outline, sm, disabled, danger, style = {} }) => (
  <button disabled={disabled} onClick={onClick} style={{ background: outline ? "transparent" : (danger ? C.err : color), color: outline ? (danger ? C.err : color) : "#fff", border: outline ? `1px solid ${danger ? C.err : color}` : "none", borderRadius: 6, padding: sm ? "4px 10px" : "7px 16px", fontSize: sm ? 10 : 11, fontFamily: F.mono, cursor: disabled ? "not-allowed" : "pointer", fontWeight: 600, opacity: disabled ? .45 : 1, transition: "all .15s", letterSpacing: .3, ...style }}>{children}</button>
);

const Inp = ({ label, value, onChange, ph, mono, area, help, disabled }) => (
  <div style={{ marginBottom: 10 }}>
    {label && <label style={{ display: "block", fontSize: 9, fontFamily: F.mono, color: C.txMute, textTransform: "uppercase", letterSpacing: 1.2, marginBottom: 3 }}>{label}</label>}
    {area ? <textarea value={value || ""} onChange={e => onChange(e.target.value)} placeholder={ph} disabled={disabled} rows={3} style={{ width: "100%", padding: "7px 9px", background: C.bg1, border: `1px solid ${C.bdr}`, borderRadius: 5, color: C.tx, fontFamily: mono ? F.mono : F.sans, fontSize: 12, resize: "vertical", outline: "none", boxSizing: "border-box", opacity: disabled ? .5 : 1 }} />
      : <input value={value || ""} onChange={e => onChange(e.target.value)} placeholder={ph} disabled={disabled} style={{ width: "100%", padding: "7px 9px", background: C.bg1, border: `1px solid ${C.bdr}`, borderRadius: 5, color: C.tx, fontFamily: mono ? F.mono : F.sans, fontSize: 12, outline: "none", boxSizing: "border-box", opacity: disabled ? .5 : 1 }} />}
    {help && <div style={{ fontSize: 9, color: C.txMute, marginTop: 2, lineHeight: 1.3 }}>{help}</div>}
  </div>
);

const Sel = ({ label, value, onChange, opts }) => (
  <div style={{ marginBottom: 10 }}>
    {label && <label style={{ display: "block", fontSize: 9, fontFamily: F.mono, color: C.txMute, textTransform: "uppercase", letterSpacing: 1.2, marginBottom: 3 }}>{label}</label>}
    <select value={value} onChange={e => onChange(e.target.value)} style={{ width: "100%", padding: "7px 9px", background: C.bg1, border: `1px solid ${C.bdr}`, borderRadius: 5, color: C.tx, fontFamily: F.sans, fontSize: 12, outline: "none" }}>
      {opts.map(o => <option key={o.v} value={o.v}>{o.l}</option>)}
    </select>
  </div>
);

const Tog = ({ label, value, onChange }) => (
  <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10, cursor: "pointer", userSelect: "none" }} onClick={() => onChange(!value)}>
    <div style={{ width: 34, height: 18, borderRadius: 9, background: value ? C.ok : C.bdr, transition: "background .2s", position: "relative", flexShrink: 0 }}>
      <div style={{ width: 14, height: 14, borderRadius: 7, background: "#fff", position: "absolute", top: 2, left: value ? 18 : 2, transition: "left .2s" }} />
    </div>
    <span style={{ fontSize: 12, color: C.tx }}>{label}</span>
  </div>
);

const Card = ({ title, children, actions, noPad }) => (
  <div style={{ background: C.bg2, border: `1px solid ${C.bdr}`, borderRadius: 8, padding: noPad ? 0 : 18, marginBottom: 14 }}>
    {(title || actions) && <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12, padding: noPad ? "14px 18px 0" : 0 }}>
      {title && <div style={{ fontSize: 10, fontFamily: F.mono, color: C.txMute, textTransform: "uppercase", letterSpacing: 1.2 }}>{title}</div>}
      {actions && <div style={{ display: "flex", gap: 5 }}>{actions}</div>}
    </div>}
    {children}
  </div>
);

const Stat = ({ label, value, sub, color, icon }) => (
  <div style={{ background: C.bg2, border: `1px solid ${C.bdr}`, borderRadius: 8, padding: "14px 16px", flex: 1, minWidth: 120 }}>
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
      <span style={{ color: C.txMute, fontSize: 9, fontFamily: F.mono, textTransform: "uppercase", letterSpacing: 1 }}>{label}</span>
      {icon && <span style={{ fontSize: 14, opacity: .25 }}>{icon}</span>}
    </div>
    <div style={{ fontSize: 24, fontWeight: 700, color: color || C.tx, fontFamily: F.mono, marginTop: 3 }}>{value}</div>
    {sub && <div style={{ fontSize: 9, color: C.txDim, marginTop: 2 }}>{sub}</div>}
  </div>
);

// Toast component
const Toast = ({ toast }) => {
  if (!toast) return null;
  return <div style={{ position: "fixed", top: 14, right: 14, padding: "10px 18px", borderRadius: 6, background: toast.ok ? "#065F46" : "#7F1D1D", color: "#fff", fontFamily: F.mono, fontSize: 11, zIndex: 9999, boxShadow: "0 4px 20px rgba(0,0,0,.5)", maxWidth: 400, animation: "fadeIn .2s ease" }}>{toast.msg}</div>;
};

// Confirm dialog (replaces confirm())
const ConfirmDialog = ({ msg, onYes, onNo }) => (
  <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,.6)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 9999 }}>
    <div style={{ background: C.bg2, border: `1px solid ${C.bdr}`, borderRadius: 10, padding: 24, maxWidth: 400, minWidth: 300 }}>
      <div style={{ fontSize: 13, color: C.tx, marginBottom: 18, lineHeight: 1.5 }}>{msg}</div>
      <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
        <Btn outline onClick={onNo}>Отмена</Btn>
        <Btn danger onClick={onYes}>Подтвердить</Btn>
      </div>
    </div>
  </div>
);

// ═══════════════════════ PAGES ═══════════════════════

// --- Dashboard ---
const DashboardPage = ({ sources, orders, logs }) => {
  const active = sources.filter(s => s.active).length;
  const totalOrders = orders.length;
  const totalAssign = sources.reduce((s, x) => s + (x.total_assignments || 0), 0);
  const failedOrders = orders.filter(o => o.status === "failed").length;
  const errLogs = logs.filter(l => l.lvl === "error").slice(0, 3);

  return <div>
    <h2 style={{ fontSize: 17, fontWeight: 600, fontFamily: F.sans, color: C.tx, margin: "0 0 16px" }}>Обзор системы</h2>
    <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginBottom: 18 }}>
      <Stat label="Источники" value={active} sub={`${sources.length} всего`} icon="◉" />
      <Stat label="Приказы" value={totalOrders} sub={`${failedOrders} с ошибкой`} icon="📋" />
      <Stat label="Записи" value={fmt.n(totalAssign)} icon="👤" color={C.ok} />
      <Stat label="Ошибки" value={errLogs.length} icon="⚠" color={failedOrders ? C.err : C.txDim} />
    </div>
    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
      <Card title="Статус конвейера">
        <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
          {[{ l: "Очередь", v: orders.filter(o => o.status === "new").length, c: C.info }, { l: "Извлечено", v: orders.filter(o => o.status === "extracted").length, c: C.ok }, { l: "Утверждено", v: orders.filter(o => o.status === "approved").length, c: C.purp }, { l: "Ошибки", v: failedOrders, c: C.err }].map((s, i) =>
            <div key={i} style={{ textAlign: "center", minWidth: 55 }}>
              <div style={{ fontSize: 20, fontWeight: 700, color: s.c, fontFamily: F.mono }}>{s.v}</div>
              <div style={{ fontSize: 9, color: C.txDim, marginTop: 1 }}>{s.l}</div>
            </div>
          )}
        </div>
      </Card>
      <Card title="Последние ошибки">
        {errLogs.length === 0 ? <div style={{ fontSize: 11, color: C.txMute }}>Нет ошибок</div> : errLogs.map((e, i) => <div key={i} style={{ display: "flex", gap: 6, padding: "5px 0", fontSize: 11, alignItems: "flex-start" }}>
          <span style={{ color: C.err, flexShrink: 0 }}>⚠</span>
          <span style={{ color: C.txDim, fontFamily: F.mono, fontSize: 10, minWidth: 65, flexShrink: 0 }}>{e.src}</span>
          <span style={{ color: C.tx, flex: 1, fontFamily: F.sans, fontSize: 12 }}>{e.msg}</span>
        </div>)}
      </Card>
    </div>
  </div>;
};

// --- Source Form (Add/Edit — 5-step wizard) ---
const BLANK = {
  code: "", name: "", region: "", federal_subject: "", source_type: "pdf_portal", risk_class: "green", active: false, official_basis: "",
  download: { method: "httpx", base_url: "", antibot: "", delay_min: 1, delay_max: 3, wait_selector: "" },
  detect: { list_urls: [""], link_regex: 'href=["\']([^"\']*\\.pdf)["\']', title_regex: "", order_date_regex: "", order_number_regex: "", pagination: "", max_pages: 1, js_var: "" },
  meta: { issuing_body: "", order_type: "приказ" },
};

const SourceForm = ({ source, isNew, onSave, onCancel, onDelete }) => {
  const [f, setF] = useState(() => isNew ? JSON.parse(JSON.stringify(BLANK)) : JSON.parse(JSON.stringify(source)));
  const [step, setStep] = useState(0);
  const [rxTest, setRxTest] = useState({ html: "", results: null });
  const [confirmDel, setConfirmDel] = useState(false);
  const [saving, setSaving] = useState(false);

  const set = (k, v) => setF(p => ({ ...p, [k]: v }));
  const setDl = (k, v) => setF(p => ({ ...p, download: { ...p.download, [k]: v } }));
  const setDt = (k, v) => setF(p => ({ ...p, detect: { ...p.detect, [k]: v } }));
  const setMt = (k, v) => setF(p => ({ ...p, meta: { ...p.meta, [k]: v } }));
  const setUrl = (i, v) => { const u = [...f.detect.list_urls]; u[i] = v; setDt("list_urls", u); };
  const addUrl = () => setDt("list_urls", [...f.detect.list_urls, ""]);
  const rmUrl = i => setDt("list_urls", f.detect.list_urls.filter((_, j) => j !== i));

  const testRegex = () => {
    try {
      const re = new RegExp(f.detect.link_regex, "gi");
      const matches = [];
      let m;
      while ((m = re.exec(rxTest.html)) !== null && matches.length < 20) {
        const raw = m[1] || m[0];
        let resolved = raw;
        try { resolved = new URL(raw, f.download.base_url || "https://example.com").href; } catch {}
        matches.push({ raw, resolved });
      }
      setRxTest(p => ({ ...p, results: matches }));
    } catch (e) { setRxTest(p => ({ ...p, results: [{ raw: `Ошибка: ${e.message}`, resolved: "" }] })); }
  };

  const valid = f.code && f.name && f.region && f.meta.issuing_body && f.detect.list_urls.some(u => u);

  const handleSave = () => {
    setSaving(true);
    setTimeout(() => { onSave(f); setSaving(false); }, 300);
  };

  const steps = ["Основные данные", "Загрузка", "Обнаружение", "Метаданные", "Проверка"];

  return <div>
    {confirmDel && <ConfirmDialog msg={`Удалить источник «${f.code}»? Это действие необратимо.`} onYes={() => { setConfirmDel(false); onDelete(f.code); }} onNo={() => setConfirmDel(false)} />}

    <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 18 }}>
      <button onClick={onCancel} style={{ background: "none", border: "none", color: C.acc, fontFamily: F.mono, fontSize: 11, cursor: "pointer", padding: 0 }}>← Источники</button>
      <h2 style={{ fontSize: 17, fontWeight: 600, fontFamily: F.sans, color: C.tx, margin: 0, flex: 1 }}>{isNew ? "Новый источник" : `Редактирование: ${f.code}`}</h2>
      {!isNew && <Btn sm outline danger onClick={() => setConfirmDel(true)}>Удалить</Btn>}
    </div>

    {/* Steps */}
    <div style={{ display: "flex", gap: 3, marginBottom: 18 }}>
      {steps.map((s, i) => (
        <button key={i} onClick={() => setStep(i)} style={{ flex: 1, padding: "7px 3px", background: step === i ? C.bg3 : C.bg2, border: `1px solid ${step === i ? C.acc : C.bdr}`, borderRadius: 5, cursor: "pointer", textAlign: "center", transition: "all .15s" }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: step === i ? C.acc : C.txMute, fontFamily: F.mono }}>{"①②③④⑤"[i]}</div>
          <div style={{ fontSize: 9, color: step === i ? C.tx : C.txMute, fontFamily: F.mono, marginTop: 1 }}>{s}</div>
        </button>
      ))}
    </div>

    {/* Step 0: Basic */}
    {step === 0 && <Card title="Основные данные">
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0 14px" }}>
        <Inp label="Код (уникальный)" value={f.code} onChange={v => set("code", v.toLowerCase().replace(/[^a-zа-яё0-9_]/gi, "_"))} ph="novosibirsk_minsport" mono disabled={!isNew} help={isNew ? "Латиница, цифры, _" : "Нельзя менять"} />
        <Inp label="Название организации" value={f.name} onChange={v => set("name", v)} ph="Министерство спорта Новосибирской области" />
        <Inp label="Регион" value={f.region} onChange={v => set("region", v)} ph="Новосибирская область" />
        <Inp label="Код субъекта РФ" value={f.federal_subject} onChange={v => set("federal_subject", v)} ph="54" />
        <Sel label="Тип источника" value={f.source_type} onChange={v => set("source_type", v)} opts={[{ v: "pdf_portal", l: "PDF-портал" }, { v: "json_embed", l: "JSON embed" }, { v: "html_table", l: "HTML-таблица" }]} />
        <Sel label="Класс риска" value={f.risk_class} onChange={v => set("risk_class", v)} opts={[{ v: "green", l: "🟢 Green — httpx, без антибота" }, { v: "amber", l: "🟡 Amber — Playwright/JS" }, { v: "red", l: "🔴 Red — тяжёлый антибот" }]} />
      </div>
      <Inp label="Официальное основание (8-ФЗ)" value={f.official_basis} onChange={v => set("official_basis", v)} ph="Домен принадлежит органу власти субъекта РФ" />
      <Tog label="Активен (включить мониторинг)" value={f.active} onChange={v => set("active", v)} />
    </Card>}

    {/* Step 1: Download */}
    {step === 1 && <Card title="Параметры загрузки">
      <Sel label="Метод" value={f.download.method} onChange={v => setDl("method", v)} opts={[{ v: "httpx", l: "httpx — прямой HTTP" }, { v: "playwright", l: "Playwright — браузер/JS" }]} />
      <Inp label="Базовый URL (домен)" value={f.download.base_url} onChange={v => setDl("base_url", v)} ph="https://minsport.nso.ru" mono help="Для urljoin и SSRF-whitelist" />
      {f.download.method === "playwright" && <>
        <Sel label="Антибот" value={f.download.antibot} onChange={v => setDl("antibot", v)} opts={[{ v: "", l: "Нет" }, { v: "servicepipe", l: "Servicepipe" }, { v: "cloudflare", l: "Cloudflare" }, { v: "ddos-guard", l: "DDoS-Guard" }]} />
        <Inp label="CSS-селектор ожидания" value={f.download.wait_selector} onChange={v => setDl("wait_selector", v)} ph="a[href$='.pdf']" mono help="Playwright ждёт появления элемента" />
      </>}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0 14px" }}>
        <Inp label="Мин. задержка (с)" value={f.download.delay_min} onChange={v => setDl("delay_min", parseFloat(v) || 0)} />
        <Inp label="Макс. задержка (с)" value={f.download.delay_max} onChange={v => setDl("delay_max", parseFloat(v) || 0)} />
      </div>
    </Card>}

    {/* Step 2: Detection */}
    {step === 2 && <Card title="Обнаружение документов">
      <div style={{ marginBottom: 10 }}>
        <label style={{ display: "block", fontSize: 9, fontFamily: F.mono, color: C.txMute, textTransform: "uppercase", letterSpacing: 1.2, marginBottom: 5 }}>URL страниц с документами</label>
        {f.detect.list_urls.map((u, i) => (
          <div key={i} style={{ display: "flex", gap: 5, marginBottom: 4 }}>
            <input value={u} onChange={e => setUrl(i, e.target.value)} placeholder="https://..." style={{ flex: 1, padding: "7px 9px", background: C.bg1, border: `1px solid ${C.bdr}`, borderRadius: 5, color: C.tx, fontFamily: F.mono, fontSize: 11, outline: "none" }} />
            {f.detect.list_urls.length > 1 && <Btn sm outline color={C.err} onClick={() => rmUrl(i)}>✕</Btn>}
          </div>
        ))}
        <Btn sm outline onClick={addUrl}>+ URL</Btn>
      </div>

      {f.source_type === "json_embed" ?
        <Inp label="JS-переменная" value={f.detect.js_var} onChange={v => setDt("js_var", v)} ph="$obj" mono help="Переменная в <script> с JSON-данными" />
        : <>
          <Inp label="Regex ссылок на документы" value={f.detect.link_regex} onChange={v => setDt("link_regex", v)} mono help="Группа (1) = href. Применяется к HTML страницы." />
          <Inp label="Regex заголовка (опц.)" value={f.detect.title_regex} onChange={v => setDt("title_regex", v)} mono />
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0 14px" }}>
            <Inp label="Regex даты приказа" value={f.detect.order_date_regex} onChange={v => setDt("order_date_regex", v)} mono />
            <Inp label="Regex номера приказа" value={f.detect.order_number_regex} onChange={v => setDt("order_number_regex", v)} mono />
          </div>
        </>}

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0 14px" }}>
        <Inp label="Шаблон пагинации" value={f.detect.pagination} onChange={v => setDt("pagination", v)} ph="?page={n}" mono help="Пусто = без пагинации" />
        <Inp label="Макс. страниц" value={f.detect.max_pages} onChange={v => setDt("max_pages", parseInt(v) || 1)} />
      </div>

      {/* Regex tester */}
      {f.source_type !== "json_embed" && <div style={{ marginTop: 14, padding: 12, background: C.bg, borderRadius: 6, border: `1px solid ${C.bdr}` }}>
        <div style={{ fontSize: 10, fontFamily: F.mono, color: C.acc, marginBottom: 8 }}>🧪 Тестер regex</div>
        <Inp label="HTML-фрагмент" value={rxTest.html} onChange={v => setRxTest(p => ({ ...p, html: v, results: null }))} area mono ph={'<a href="/docs/123.pdf">Приказ 1</a>\n<a href="/docs/456.pdf">Приказ 2</a>'} />
        <Btn sm onClick={testRegex} style={{ marginBottom: 6 }}>▶ Тест</Btn>
        {rxTest.results && <div style={{ marginTop: 6 }}>
          <div style={{ fontSize: 10, fontFamily: F.mono, color: rxTest.results.length ? C.ok : C.err, marginBottom: 5 }}>
            {rxTest.results.length ? `✓ ${rxTest.results.length} совпадений` : "✗ Нет совпадений"}
          </div>
          {rxTest.results.slice(0, 10).map((r, i) => (
            <div key={i} style={{ fontSize: 10, fontFamily: F.mono, color: C.txDim, padding: "2px 0", borderBottom: `1px solid ${C.bdr}` }}>
              <span style={{ color: C.tx }}>{r.raw}</span>
              {r.resolved !== r.raw && <span style={{ color: C.acc, marginLeft: 6 }}>→ {r.resolved}</span>}
            </div>
          ))}
        </div>}
      </div>}
    </Card>}

    {/* Step 3: Meta */}
    {step === 3 && <Card title="Метаданные">
      <Inp label="Издающий орган" value={f.meta.issuing_body} onChange={v => setMt("issuing_body", v)} ph="Минспорт Новосибирской области" help="Передаётся в промпт экстрактора" />
      <Sel label="Тип документа" value={f.meta.order_type} onChange={v => setMt("order_type", v)} opts={[{ v: "приказ", l: "Приказ" }, { v: "распоряжение", l: "Распоряжение" }, { v: "постановление", l: "Постановление" }]} />
    </Card>}

    {/* Step 4: Review */}
    {step === 4 && <div>
      <Card title="Сводка конфигурации">
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
          <div>
            <div style={{ fontSize: 9, fontFamily: F.mono, color: C.txMute, marginBottom: 3 }}>ИСТОЧНИК</div>
            <div style={{ fontFamily: F.mono, fontSize: 13, color: C.tx, fontWeight: 600 }}>{f.code || "—"}</div>
            <div style={{ fontSize: 12, color: C.txDim, marginTop: 1 }}>{f.name || "—"}</div>
            <div style={{ fontSize: 11, color: C.txMute }}>{f.region} {f.federal_subject ? `(${f.federal_subject})` : ""}</div>
            <div style={{ marginTop: 6 }}><RiskBadge risk={f.risk_class} /></div>
          </div>
          <div>
            <div style={{ fontSize: 9, fontFamily: F.mono, color: C.txMute, marginBottom: 3 }}>ЗАГРУЗКА</div>
            <div style={{ fontSize: 11, fontFamily: F.mono, color: C.tx }}>{f.download.method} · {f.download.base_url || "—"}</div>
            {f.download.antibot && <div style={{ fontSize: 10, color: C.warn }}>Антибот: {f.download.antibot}</div>}
            <div style={{ fontSize: 10, color: C.txDim }}>Задержка: {f.download.delay_min}–{f.download.delay_max}с</div>
          </div>
          <div>
            <div style={{ fontSize: 9, fontFamily: F.mono, color: C.txMute, marginBottom: 3 }}>ОБНАРУЖЕНИЕ</div>
            <div style={{ fontSize: 10, fontFamily: F.mono, color: C.txDim }}>{f.detect.list_urls.filter(u => u).length} URL, {f.source_type}</div>
            {f.detect.link_regex && <div style={{ fontSize: 9, fontFamily: F.mono, color: C.acc, marginTop: 1, wordBreak: "break-all" }}>{f.detect.link_regex.substring(0, 55)}…</div>}
            {f.detect.pagination && <div style={{ fontSize: 10, color: C.txDim }}>Пагинация: {f.detect.pagination} (до {f.detect.max_pages} стр.)</div>}
          </div>
          <div>
            <div style={{ fontSize: 9, fontFamily: F.mono, color: C.txMute, marginBottom: 3 }}>МЕТАДАННЫЕ</div>
            <div style={{ fontSize: 11, color: C.tx }}>{f.meta.issuing_body || "—"}</div>
            <div style={{ fontSize: 10, color: C.txDim }}>{f.meta.order_type}</div>
          </div>
        </div>
      </Card>
      <Card title="Валидация полноты">
        {[
          { ok: !!f.code, m: "Код источника" },
          { ok: !!f.name, m: "Название" },
          { ok: !!f.region, m: "Регион" },
          { ok: f.detect.list_urls.some(u => u), m: "URL страниц" },
          { ok: !!f.download.base_url, m: "Базовый URL" },
          { ok: !!f.meta.issuing_body, m: "Издающий орган" },
          { ok: f.source_type === "json_embed" || !!f.detect.link_regex, m: "Regex ссылок (или json_embed)" },
        ].map((v, i) => (
          <div key={i} style={{ display: "flex", alignItems: "center", gap: 7, padding: "3px 0", fontSize: 11 }}>
            <span style={{ color: v.ok ? C.ok : C.err, fontSize: 13 }}>{v.ok ? "✓" : "✗"}</span>
            <span style={{ color: v.ok ? C.tx : C.err }}>{v.m}</span>
          </div>
        ))}
      </Card>
    </div>}

    {/* Nav */}
    <div style={{ display: "flex", justifyContent: "space-between", marginTop: 14 }}>
      <div>{step > 0 && <Btn outline onClick={() => setStep(step - 1)}>← Назад</Btn>}</div>
      <div style={{ display: "flex", gap: 6 }}>
        {step < 4 && <Btn onClick={() => setStep(step + 1)}>Далее →</Btn>}
        {step === 4 && <Btn disabled={!valid || saving} color={C.ok} onClick={handleSave}>
          {saving ? "…" : isNew ? "✓ Создать" : "✓ Сохранить"}
        </Btn>}
      </div>
    </div>
  </div>;
};

// --- Sources Page ---
const SourcesPage = ({ sources, onEdit, onAdd, onToggle, onCheck }) => (
  <div>
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
      <h2 style={{ fontSize: 17, fontWeight: 600, fontFamily: F.sans, color: C.tx, margin: 0 }}>Источники <span style={{ fontFamily: F.mono, fontSize: 12, color: C.txMute, fontWeight: 400 }}>({sources.length})</span></h2>
      <div style={{ display: "flex", gap: 6 }}>
        <Btn onClick={onAdd}>+ Новый</Btn>
        <Btn outline onClick={onCheck}>▶ Проверить все</Btn>
      </div>
    </div>
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: F.sans, fontSize: 12 }}>
        <thead><tr>
          {["", "Код", "Название", "Регион", "Метод", "Статус", "Приказы", "Записи", ""].map((h, i) =>
            <th key={i} style={{ textAlign: i >= 6 ? "right" : "left", padding: "8px 8px", color: C.txMute, fontSize: 9, fontFamily: F.mono, textTransform: "uppercase", letterSpacing: 1, borderBottom: `1px solid ${C.bdr}`, whiteSpace: "nowrap" }}>{h}</th>
          )}
        </tr></thead>
        <tbody>
          {sources.map((s, ri) => (
            <tr key={ri} style={{ borderBottom: `1px solid ${C.bdr}`, transition: "background .12s", cursor: "pointer" }} onMouseEnter={e => e.currentTarget.style.background = C.bgHov} onMouseLeave={e => e.currentTarget.style.background = "transparent"} onClick={() => onEdit(s)}>
              <td style={{ padding: "8px", width: 60 }}><RiskBadge risk={s.risk_class} /></td>
              <td style={{ padding: "8px", fontFamily: F.mono, fontSize: 11 }}>{s.code}</td>
              <td style={{ padding: "8px", maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{s.name}</td>
              <td style={{ padding: "8px", color: C.txDim, fontSize: 11 }}>{s.region}</td>
              <td style={{ padding: "8px" }}><Badge color={C.tx}>{s.download?.method || "?"}</Badge></td>
              <td style={{ padding: "8px" }}>
                <span onClick={e => { e.stopPropagation(); onToggle(s.code); }} style={{ cursor: "pointer" }}>
                  <Badge color={s.active ? C.ok : C.txMute} bg={s.active ? C.okBg : undefined}>{s.active ? "Active" : "Off"}</Badge>
                </span>
              </td>
              <td style={{ padding: "8px", textAlign: "right", fontFamily: F.mono, fontSize: 11 }}><span style={{ color: C.ok }}>{s.orders_ok || 0}</span><span style={{ color: C.txMute }}>/</span><span style={{ color: C.info }}>{s.orders_pending || 0}</span><span style={{ color: C.txMute }}>/</span><span style={{ color: C.err }}>{s.orders_failed || 0}</span></td>
              <td style={{ padding: "8px", textAlign: "right", fontFamily: F.mono, fontSize: 11 }}>{fmt.n(s.total_assignments || 0)}</td>
              <td style={{ padding: "8px" }}><Btn sm outline onClick={e => { e.stopPropagation(); onEdit(s); }}>✎</Btn></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  </div>
);

// --- Orders Page ---
const OrdersPage = ({ orders }) => {
  const [filter, setFilter] = useState("all");
  const shown = filter === "all" ? orders : orders.filter(o => o.status === filter);
  return <div>
    <h2 style={{ fontSize: 17, fontWeight: 600, fontFamily: F.sans, color: C.tx, margin: "0 0 14px" }}>Приказы</h2>
    <div style={{ display: "flex", gap: 4, marginBottom: 12 }}>
      {["all", "new", "extracted", "approved", "failed"].map(f => <button key={f} onClick={() => setFilter(f)} style={{ background: filter === f ? C.bg3 : C.bg2, color: filter === f ? C.tx : C.txDim, border: `1px solid ${filter === f ? C.acc : C.bdr}`, borderRadius: 5, padding: "4px 10px", fontSize: 10, fontFamily: F.mono, cursor: "pointer" }}>{f === "all" ? `Все (${orders.length})` : `${(STATUS_MAP[f] || {}).l || f} (${orders.filter(o => o.status === f).length})`}</button>)}
    </div>
    {shown.map((o, i) => <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 10px", borderBottom: `1px solid ${C.bdr}` }}>
      <Badge color={(STATUS_MAP[o.status] || {}).c || C.txDim} bg={(STATUS_MAP[o.status] || {}).bg}>{(STATUS_MAP[o.status] || {}).l || o.status}</Badge>
      <span style={{ fontFamily: F.mono, fontWeight: 600, color: C.acc, minWidth: 90 }}>{o.order_number}</span>
      <span style={{ fontFamily: F.mono, fontSize: 10, color: C.txDim, minWidth: 70 }}>{fmt.d(o.order_date)}</span>
      <span style={{ color: C.txDim, flex: 1, fontSize: 11, fontFamily: F.mono }}>{o.source_code}</span>
      <span style={{ fontFamily: F.mono, fontSize: 11, color: o.record_count ? C.ok : C.txMute }}>{o.record_count || "—"} зап.</span>
    </div>)}
  </div>;
};

// --- Logs Page ---
const LogsPage = ({ logs }) => (
  <div>
    <h2 style={{ fontSize: 17, fontWeight: 600, fontFamily: F.sans, color: C.tx, margin: "0 0 12px" }}>Журнал</h2>
    {logs.map((l, i) => <div key={i} style={{ display: "flex", gap: 6, padding: "6px 8px", borderBottom: `1px solid ${C.bdr}`, fontFamily: F.mono, fontSize: 11, background: l.lvl === "error" ? C.errBg : "transparent", alignItems: "flex-start" }}>
      <span style={{ color: l.lvl === "error" ? C.err : l.lvl === "warn" ? C.warn : C.ok, fontSize: 9, marginTop: 2 }}>●</span>
      <span style={{ color: C.txMute, minWidth: 80, fontSize: 9, flexShrink: 0, marginTop: 1 }}>{fmt.t(l.t)}</span>
      <span style={{ color: C.txDim, minWidth: 65, fontSize: 10, flexShrink: 0 }}>{l.src}</span>
      <Badge color={C.tx}>{l.stage}</Badge>
      <span style={{ color: C.tx, flex: 1, fontFamily: F.sans, fontSize: 12 }}>{l.msg}</span>
    </div>)}
  </div>
);

// ═══════════════════════ APP ═══════════════════════
const NAV = [
  { id: "dashboard", l: "Обзор", i: "◉" },
  { id: "sources", l: "Источники", i: "⊕" },
  { id: "orders", l: "Приказы", i: "📋" },
  { id: "logs", l: "Журнал", i: "⫶" },
];

export default function App() {
  const [state, dispatch] = useReducer(appReducer, {
    sources: INIT_SOURCES,
    orders: INIT_ORDERS,
    logs: INIT_LOGS,
  });
  const [page, setPage] = useState("sources");
  const [editSrc, setEditSrc] = useState(null);
  const [isNew, setIsNew] = useState(false);
  const [toast, setToast] = useState(null);

  const notify = (msg, ok = true) => { setToast({ msg, ok }); setTimeout(() => setToast(null), 3500); };

  const goEdit = (s) => { setEditSrc(s); setIsNew(false); setPage("form"); };
  const goNew = () => { setEditSrc(null); setIsNew(true); setPage("form"); };
  const goBack = () => setPage("sources");

  const handleSave = (formData) => {
    if (isNew) {
      const exists = state.sources.some(s => s.code === formData.code);
      if (exists) { notify(`Источник «${formData.code}» уже существует`, false); return; }
      dispatch({ type: "ADD_SOURCE", payload: { ...formData, orders_ok: 0, orders_pending: 0, orders_failed: 0, total_assignments: 0, last_order_date: null, last_checked_at: null } });
      dispatch({ type: "ADD_LOG", payload: { lvl: "info", stage: "admin", src: formData.code, msg: `Источник создан (${formData.risk_class})`, t: new Date().toISOString() } });
      notify(`Источник «${formData.code}» создан`);
    } else {
      dispatch({ type: "UPDATE_SOURCE", code: formData.code, payload: formData });
      dispatch({ type: "ADD_LOG", payload: { lvl: "info", stage: "admin", src: formData.code, msg: "Конфигурация обновлена", t: new Date().toISOString() } });
      notify(`Источник «${formData.code}» обновлён`);
    }
    setPage("sources");
  };

  const handleDelete = (code) => {
    const src = state.sources.find(s => s.code === code);
    if (src && (src.orders_ok || src.orders_pending)) {
      notify(`Нельзя удалить: у «${code}» есть приказы. Деактивируйте вместо удаления.`, false);
      return;
    }
    dispatch({ type: "DELETE_SOURCE", code });
    dispatch({ type: "ADD_LOG", payload: { lvl: "warn", stage: "admin", src: code, msg: "Источник удалён", t: new Date().toISOString() } });
    notify(`Источник «${code}» удалён`);
    setPage("sources");
  };

  const handleToggle = (code) => {
    dispatch({ type: "TOGGLE_SOURCE", code });
    const src = state.sources.find(s => s.code === code);
    const newActive = src ? !src.active : false;
    notify(`${code}: ${newActive ? "активирован" : "деактивирован"}`);
  };

  const handleCheckAll = () => {
    dispatch({ type: "ADD_LOG", payload: { lvl: "info", stage: "scheduler", src: "system", msg: `check-all запущен: ${state.sources.filter(s => s.active).length} активных источников`, t: new Date().toISOString() } });
    notify("Проверка всех источников запущена");
  };

  return <div style={{ display: "flex", height: "100vh", background: C.bg, fontFamily: F.sans, color: C.tx }}>
    <style>{`@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap');
    @keyframes fadeIn { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: translateY(0); } }
    * { scrollbar-width: thin; scrollbar-color: ${C.bdr} transparent; }
    ::-webkit-scrollbar { width: 6px; } ::-webkit-scrollbar-track { background: transparent; } ::-webkit-scrollbar-thumb { background: ${C.bdr}; border-radius: 3px; }
    `}</style>
    <Toast toast={toast} />

    {/* Sidebar */}
    <div style={{ width: 180, background: C.bg1, borderRight: `1px solid ${C.bdr}`, display: "flex", flexDirection: "column", flexShrink: 0 }}>
      <div style={{ padding: "16px 14px 18px", borderBottom: `1px solid ${C.bdr}` }}>
        <div style={{ fontFamily: F.mono, fontSize: 13, fontWeight: 700, letterSpacing: 1 }}><span style={{ color: C.acc }}>SPORT</span>RANK</div>
        <div style={{ fontFamily: F.mono, fontSize: 7, color: C.txMute, marginTop: 2, letterSpacing: 2, textTransform: "uppercase" }}>Admin v3.0</div>
      </div>
      <nav style={{ padding: "8px 5px", flex: 1 }}>
        {NAV.map(n => {
          const act = page === n.id || (n.id === "sources" && page === "form");
          return <button key={n.id} onClick={() => { setPage(n.id); setEditSrc(null); }} style={{ display: "flex", alignItems: "center", gap: 7, width: "100%", padding: "8px 9px", background: act ? C.bg3 : "transparent", color: act ? C.tx : C.txDim, border: "none", borderRadius: 4, cursor: "pointer", fontFamily: F.sans, fontSize: 12, fontWeight: act ? 600 : 400, textAlign: "left", borderLeft: act ? `2px solid ${C.acc}` : "2px solid transparent", marginBottom: 2 }}>
            <span style={{ fontSize: 12, opacity: act ? 1 : .35, width: 16, textAlign: "center" }}>{n.i}</span>{n.l}
            {n.id === "sources" && <span style={{ marginLeft: "auto", fontSize: 9, fontFamily: F.mono, color: C.txMute }}>{state.sources.length}</span>}
          </button>;
        })}
      </nav>
      <div style={{ padding: "12px 14px", borderTop: `1px solid ${C.bdr}` }}>
        <div style={{ display: "flex", alignItems: "center", gap: 5 }}><div style={{ width: 6, height: 6, borderRadius: 3, background: C.ok }} /><span style={{ fontFamily: F.mono, fontSize: 8, color: C.txDim }}>System OK</span></div>
        <div style={{ fontFamily: F.mono, fontSize: 8, color: C.txMute, marginTop: 3 }}>{state.sources.filter(s => s.active).length} active · {fmt.n(state.sources.reduce((s, x) => s + (x.total_assignments || 0), 0))} rec</div>
      </div>
    </div>

    {/* Main */}
    <div style={{ flex: 1, overflow: "auto", padding: 22 }}>
      {page === "dashboard" && <DashboardPage sources={state.sources} orders={state.orders} logs={state.logs} />}
      {page === "sources" && <SourcesPage sources={state.sources} onEdit={goEdit} onAdd={goNew} onToggle={handleToggle} onCheck={handleCheckAll} />}
      {page === "form" && <SourceForm source={editSrc} isNew={isNew} onSave={handleSave} onCancel={goBack} onDelete={handleDelete} />}
      {page === "orders" && <OrdersPage orders={state.orders} />}
      {page === "logs" && <LogsPage logs={state.logs} />}
    </div>
  </div>;
}
