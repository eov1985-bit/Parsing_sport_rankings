import { useState, useEffect, useCallback, useRef } from "react";

// ============================================================================
// DEMO DATA
// ============================================================================
const DEMO_SOURCES = [
  { code: "moskva_tstisk", name: "ГКУ «ЦСТиСК» Москомспорта", region: "г. Москва", source_type: "pdf_portal", risk_class: "amber", active: true, orders_ok: 42, orders_pending: 1, orders_failed: 0, total_assignments: 1847, last_order_date: "2026-02-17", last_checked_at: "2026-02-24T10:00:00", discovery_config: { download: { method: "playwright", base_url: "https://www.mos.ru", antibot: "servicepipe", delay_min: 2, delay_max: 6, wait_selector: "a[href$='.pdf']" }, detect: { list_urls: ["https://www.mos.ru/moskomsport/documents/prisvoenie-sportivnykh-razryadov-po-vidam-sporta/", "https://www.mos.ru/moskomsport/documents/prisvoenie-kvalifikatsionnykh-kategoriy-sportivnykh-sudey/"], link_regex: "href=[\"']([^\"']*view/\\d+[^\"']*)[\"']", title_regex: ">([^<]*(?:Приказ|Распоряжение)[^<]*)<", order_date_regex: "от\\s+(\\d{1,2}[.\\s]\\d{2}[.\\s]\\d{4})", order_number_regex: "[№N]\\s*(\\S+)", pagination: null, max_pages: 1 }, meta: { issuing_body: "ГКУ «ЦСТиСК» Москомспорта", order_type: "приказ" } } },
  { code: "spb_kfkis", name: "КФКиС Санкт-Петербурга", region: "г. Санкт-Петербург", source_type: "pdf_portal", risk_class: "green", active: true, orders_ok: 38, orders_pending: 2, orders_failed: 0, total_assignments: 892, last_order_date: "2026-02-20", last_checked_at: "2026-02-24T09:55:00", discovery_config: { download: { method: "httpx", base_url: "https://kfis.gov.spb.ru", delay_min: 1, delay_max: 3 }, detect: { list_urls: ["https://kfis.gov.spb.ru/docs/?type=54"], link_regex: "href=[\"']([^\"']*?(?:/docs/\\d+|/documents/\\d+)[^\"']*)[\"']", title_regex: "class=[\"']doc-title[\"'][^>]*>([^<]+)<", pagination: "&page={n}", max_pages: 3 }, meta: { issuing_body: "Комитет по физической культуре и спорту Санкт-Петербурга", order_type: "распоряжение" } } },
  { code: "mo_mособлспорт", name: "МОСОБЛСПОРТ", region: "Московская область", source_type: "pdf_portal", risk_class: "red", active: true, orders_ok: 15, orders_pending: 0, orders_failed: 2, total_assignments: 312, last_order_date: "2026-02-13", last_checked_at: "2026-02-23T14:00:00", discovery_config: { download: { method: "playwright", base_url: "https://mst.mosreg.ru", antibot: "servicepipe", delay_min: 3, delay_max: 8, wait_selector: "a.document-link, a[href$='.pdf']" }, detect: { list_urls: ["https://mst.mosreg.ru/dokumenty/prisvoenie-sportivnykh-razryadov-kandidat-v-mastera-sporta-i-pervyi-sportivnyi-razryad"], link_regex: "href=[\"']([^\"']*(?:rasporiaz|prikaz)[^\"']*)[\"']", pagination: "?page={n}", max_pages: 3 }, meta: { issuing_body: "Министерство физической культуры и спорта Московской области", order_type: "распоряжение" } } },
  { code: "krasnodar_minsport", name: "Минспорт Краснодар", region: "Краснодарский край", source_type: "pdf_portal", risk_class: "green", active: true, orders_ok: 12, orders_pending: 0, orders_failed: 0, total_assignments: 134, last_order_date: "2026-01-28", last_checked_at: "2026-02-24T10:03:00", discovery_config: { download: { method: "httpx", base_url: "https://minsport.krasnodar.ru", delay_min: 1, delay_max: 4 }, detect: { list_urls: ["https://minsport.krasnodar.ru/activities/sport/prisvoenie-sportivnyx-razryadov/"], link_regex: "href=[\"']([^\"']*\\.pdf)[\"']" }, meta: { issuing_body: "Министерство физической культуры и спорта Краснодарского края", order_type: "приказ" } } },
  { code: "rf_minsport", name: "Минспорт РФ (msrfinfo.ru)", region: "Россия", source_type: "json_embed", risk_class: "green", active: false, orders_ok: 9, orders_pending: 0, orders_failed: 0, total_assignments: 47, last_order_date: "2026-02-01", last_checked_at: "2026-02-22T06:00:00", discovery_config: { download: { method: "httpx", base_url: "https://msrfinfo.ru", delay_min: 1, delay_max: 2 }, detect: { list_urls: ["https://msrfinfo.ru/awards/"], link_regex: "", source_type: "json_embed", js_var: "$obj" }, meta: { issuing_body: "Министерство спорта Российской Федерации", order_type: "приказ" } } },
];

const DEMO_DASHBOARD = {
  sources: { total: 5, active: 4, green: 2, amber: 1, red: 1 },
  orders: { total: 147, new: 3, downloaded: 1, extracted: 128, approved: 12, failed: 3, last_24h: 5, last_7d: 23 },
  assignments: { total: 4218, sports_count: 67, unique_people: 3891 },
  recent_errors: [
    { source_code: "mo_mособлспорт", stage: "download", message: "Servicepipe CAPTCHA detected", created_at: "2026-02-24T08:12:00" },
  ],
};

// ============================================================================
// THEME
// ============================================================================
const T = {
  bg: "#0B0D11", bgCard: "#12151C", bgHover: "#191D27", bgActive: "#1D2231", bgInput: "#0F1118",
  border: "#232939", borderFocus: "#4A6FA5",
  text: "#DFE2E8", textDim: "#828898", textMuted: "#525768",
  accent: "#4A8BF5", accentDim: "#3868BA",
  green: "#2DD4A0", greenBg: "rgba(45,212,160,0.07)",
  amber: "#F0B429", amberBg: "rgba(240,180,41,0.07)",
  red: "#EF6B6B", redBg: "rgba(239,107,107,0.07)",
  blue: "#5CA4FC", blueBg: "rgba(92,164,252,0.07)",
  purple: "#A78BFA",
  mono: "'JetBrains Mono','SF Mono','Fira Code',monospace",
  sans: "'DM Sans',system-ui,-apple-system,sans-serif",
};

const RISK = { green: { label:"Green", color:T.green, icon:"●" }, amber: { label:"Amber", color:T.amber, icon:"▲" }, red: { label:"Red", color:T.red, icon:"■" } };
const STATUS = { new:{l:"Новый",c:T.blue,bg:T.blueBg}, downloaded:{l:"Скачан",c:T.amber,bg:T.amberBg}, extracted:{l:"Извлечён",c:T.green,bg:T.greenBg}, approved:{l:"Утверждён",c:T.purple,bg:"rgba(167,139,250,0.07)"}, failed:{l:"Ошибка",c:T.red,bg:T.redBg} };

const fmt = { date: s=>s?new Date(s).toLocaleDateString("ru-RU",{day:"2-digit",month:"2-digit",year:"numeric"}):"—", time: s=>s?new Date(s).toLocaleString("ru-RU",{day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"}):"—", num: n=>n==null?"—":n.toLocaleString("ru-RU") };

// ============================================================================
// SHARED COMPONENTS
// ============================================================================
const Badge = ({color,bg,children})=><span style={{display:"inline-block",padding:"2px 10px",borderRadius:4,fontSize:11,fontWeight:600,letterSpacing:.4,color,background:bg,fontFamily:T.mono}}>{children}</span>;
const RiskBadge = ({risk})=>{const r=RISK[risk]||RISK.green;return<span style={{color:r.color,fontFamily:T.mono,fontSize:12,fontWeight:600}}>{r.icon} {r.label}</span>};
const Btn = ({children,onClick,color=T.accent,bg,outline,small,disabled,style={}})=><button disabled={disabled} onClick={onClick} style={{background:outline?"transparent":(bg||color),color:outline?color:"#fff",border:outline?`1px solid ${color}`:"none",borderRadius:6,padding:small?"5px 12px":"8px 18px",fontSize:small?11:12,fontFamily:T.mono,cursor:disabled?"not-allowed":"pointer",fontWeight:600,opacity:disabled?.5:1,transition:"all .15s",...style}}>{children}</button>;

const Input = ({label,value,onChange,placeholder,mono,textarea,width,helpText})=>(
  <div style={{marginBottom:12}}>
    {label&&<label style={{display:"block",fontSize:10,fontFamily:T.mono,color:T.textMuted,textTransform:"uppercase",letterSpacing:1,marginBottom:4}}>{label}</label>}
    {textarea?
      <textarea value={value} onChange={e=>onChange(e.target.value)} placeholder={placeholder} rows={3} style={{width:width||"100%",padding:"8px 10px",background:T.bgInput,border:`1px solid ${T.border}`,borderRadius:5,color:T.text,fontFamily:mono?T.mono:T.sans,fontSize:13,resize:"vertical",outline:"none",boxSizing:"border-box"}} />
    : <input value={value} onChange={e=>onChange(e.target.value)} placeholder={placeholder} style={{width:width||"100%",padding:"8px 10px",background:T.bgInput,border:`1px solid ${T.border}`,borderRadius:5,color:T.text,fontFamily:mono?T.mono:T.sans,fontSize:13,outline:"none",boxSizing:"border-box"}} />}
    {helpText&&<div style={{fontSize:10,color:T.textMuted,marginTop:2}}>{helpText}</div>}
  </div>
);

const Select = ({label,value,onChange,options})=>(
  <div style={{marginBottom:12}}>
    {label&&<label style={{display:"block",fontSize:10,fontFamily:T.mono,color:T.textMuted,textTransform:"uppercase",letterSpacing:1,marginBottom:4}}>{label}</label>}
    <select value={value} onChange={e=>onChange(e.target.value)} style={{width:"100%",padding:"8px 10px",background:T.bgInput,border:`1px solid ${T.border}`,borderRadius:5,color:T.text,fontFamily:T.sans,fontSize:13,outline:"none"}}>
      {options.map(o=><option key={o.value} value={o.value}>{o.label}</option>)}
    </select>
  </div>
);

const Toggle = ({label,value,onChange})=>(
  <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:12,cursor:"pointer"}} onClick={()=>onChange(!value)}>
    <div style={{width:36,height:20,borderRadius:10,background:value?T.green:T.border,transition:"background .2s",position:"relative"}}>
      <div style={{width:16,height:16,borderRadius:8,background:"#fff",position:"absolute",top:2,left:value?18:2,transition:"left .2s"}}/>
    </div>
    <span style={{fontSize:12,color:T.text}}>{label}</span>
  </div>
);

const Card = ({title,children,actions})=>(
  <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:20,marginBottom:16}}>
    {(title||actions)&&<div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:14}}>
      {title&&<div style={{fontSize:11,fontFamily:T.mono,color:T.textMuted,textTransform:"uppercase",letterSpacing:1}}>{title}</div>}
      {actions&&<div style={{display:"flex",gap:6}}>{actions}</div>}
    </div>}
    {children}
  </div>
);

const StatCard = ({label,value,sub,color,icon})=>(
  <div style={{background:T.bgCard,border:`1px solid ${T.border}`,borderRadius:8,padding:"16px 18px",flex:1,minWidth:140}}>
    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
      <span style={{color:T.textDim,fontSize:10,fontFamily:T.mono,textTransform:"uppercase",letterSpacing:1}}>{label}</span>
      {icon&&<span style={{fontSize:16,opacity:.3}}>{icon}</span>}
    </div>
    <div style={{fontSize:26,fontWeight:700,color:color||T.text,fontFamily:T.mono,marginTop:4}}>{value}</div>
    {sub&&<div style={{fontSize:10,color:T.textDim,marginTop:3,fontFamily:T.sans}}>{sub}</div>}
  </div>
);

// ============================================================================
// PAGES
// ============================================================================

// --- Dashboard ---
const DashboardPage = () => {
  const d = DEMO_DASHBOARD;
  return <div>
    <h2 style={{fontSize:18,fontWeight:600,fontFamily:T.sans,color:T.text,margin:"0 0 18px"}}>Обзор системы</h2>
    <div style={{display:"flex",gap:10,flexWrap:"wrap",marginBottom:20}}>
      <StatCard label="Источники" value={d.sources.active} sub={`${d.sources.total} всего`} icon="◉" />
      <StatCard label="Приказы" value={d.orders.total} sub={`+${d.orders.last_24h} за 24ч`} icon="📋" />
      <StatCard label="Записи" value={fmt.num(d.assignments.total)} sub={`${d.assignments.unique_people} чел.`} icon="👤" color={T.green} />
      <StatCard label="Спорт" value={d.assignments.sports_count} icon="🏅" />
    </div>
    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:14,marginBottom:20}}>
      <Card title="Статус конвейера">
        <div style={{display:"flex",gap:14,flexWrap:"wrap"}}>
          {[{l:"Очередь",v:d.orders.new,c:T.blue},{l:"Извлечено",v:d.orders.extracted,c:T.green},{l:"Утверждено",v:d.orders.approved,c:T.purple},{l:"Ошибки",v:d.orders.failed,c:T.red}].map((s,i)=>
            <div key={i} style={{textAlign:"center",minWidth:60}}>
              <div style={{fontSize:20,fontWeight:700,color:s.c,fontFamily:T.mono}}>{s.v}</div>
              <div style={{fontSize:10,color:T.textDim,marginTop:2}}>{s.l}</div>
            </div>
          )}
        </div>
      </Card>
      <Card title="Последние ошибки">
        {d.recent_errors.map((e,i)=><div key={i} style={{display:"flex",gap:8,padding:"6px 0",fontFamily:T.mono,fontSize:11,alignItems:"center"}}>
          <span style={{color:T.red}}>⚠</span>
          <span style={{color:T.textMuted,minWidth:75}}>{e.source_code}</span>
          <span style={{color:T.text,flex:1,fontSize:12,fontFamily:T.sans}}>{e.message}</span>
          <span style={{color:T.textMuted,fontSize:10}}>{fmt.time(e.created_at)}</span>
        </div>)}
      </Card>
    </div>
  </div>;
};

// --- Source Form (Add / Edit) ---
const EMPTY_SOURCE = {
  code:"",name:"",region:"",federal_subject:"",source_type:"pdf_portal",risk_class:"green",active:false,official_basis:"",
  download_method:"httpx",base_url:"",antibot:"",delay_min:1,delay_max:3,wait_selector:"",
  list_urls:[""],link_regex:'href=["\']([^"\']*\\.pdf)["\']',title_regex:"",order_date_regex:"",order_number_regex:"",pagination:"",max_pages:1,js_var:"",
  issuing_body:"",order_type:"приказ",
};

function sourceToForm(src) {
  const dc = src.discovery_config || {};
  const dl = dc.download || {};
  const det = dc.detect || {};
  const mt = dc.meta || {};
  return {
    code:src.code, name:src.name, region:src.region||"", federal_subject:src.federal_subject||"",
    source_type:src.source_type||"pdf_portal", risk_class:src.risk_class||"green", active:!!src.active, official_basis:src.official_basis||"",
    download_method:dl.method||"httpx", base_url:dl.base_url||"", antibot:dl.antibot||"", delay_min:dl.delay_min||1, delay_max:dl.delay_max||3, wait_selector:dl.wait_selector||"",
    list_urls:(det.list_urls||[""]).length?det.list_urls:[""], link_regex:det.link_regex||"", title_regex:det.title_regex||"", order_date_regex:det.order_date_regex||"", order_number_regex:det.order_number_regex||"", pagination:det.pagination||"", max_pages:det.max_pages||1, js_var:det.js_var||"",
    issuing_body:mt.issuing_body||"", order_type:mt.order_type||"приказ",
  };
}

const SourceForm = ({ source, onSave, onCancel, isNew, onDelete }) => {
  const [f, setF] = useState(isNew ? {...EMPTY_SOURCE} : sourceToForm(source));
  const [step, setStep] = useState(0);
  const [regexTest, setRegexTest] = useState({ html: "", results: null });

  const set = (key, val) => setF(prev => ({...prev, [key]: val}));
  const setUrl = (idx, val) => { const u=[...f.list_urls]; u[idx]=val; set("list_urls",u); };
  const addUrl = () => set("list_urls", [...f.list_urls, ""]);
  const rmUrl = idx => set("list_urls", f.list_urls.filter((_,i)=>i!==idx));

  const testRegex = () => {
    try {
      const re = new RegExp(f.link_regex, "gi");
      const matches = [];
      let m;
      while ((m = re.exec(regexTest.html)) !== null && matches.length < 20) {
        const raw = m[1] || m[0];
        try { matches.push({ raw, resolved: new URL(raw, f.base_url).href }); } catch { matches.push({ raw, resolved: raw }); }
      }
      setRegexTest(p => ({...p, results: matches}));
    } catch (e) { setRegexTest(p => ({...p, results: [{raw: `Ошибка regex: ${e.message}`, resolved: ""}]})); }
  };

  const valid = f.code && f.name && f.region && f.issuing_body && f.list_urls.some(u=>u);

  const steps = [
    { title: "Основные данные", icon: "①" },
    { title: "Загрузка", icon: "②" },
    { title: "Обнаружение", icon: "③" },
    { title: "Метаданные", icon: "④" },
    { title: "Проверка", icon: "⑤" },
  ];

  return <div>
    <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:20}}>
      <button onClick={onCancel} style={{background:"none",border:"none",color:T.accent,fontFamily:T.mono,fontSize:12,cursor:"pointer",padding:0}}>← Назад</button>
      <h2 style={{fontSize:18,fontWeight:600,fontFamily:T.sans,color:T.text,margin:0}}>{isNew?"Новый источник":"Редактирование: "+f.code}</h2>
    </div>

    {/* Step indicator */}
    <div style={{display:"flex",gap:4,marginBottom:20}}>
      {steps.map((s,i)=>(
        <button key={i} onClick={()=>setStep(i)} style={{flex:1,padding:"8px 4px",background:step===i?T.bgActive:T.bgCard,border:`1px solid ${step===i?T.accent:T.border}`,borderRadius:6,cursor:"pointer",textAlign:"center",transition:"all .15s"}}>
          <div style={{fontSize:14,color:step===i?T.accent:T.textMuted}}>{s.icon}</div>
          <div style={{fontSize:10,color:step===i?T.text:T.textMuted,fontFamily:T.mono,marginTop:2}}>{s.title}</div>
        </button>
      ))}
    </div>

    {/* Step 0: Basic */}
    {step===0 && <Card title="Основные данные источника">
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"0 16px"}}>
        <Input label="Код (уникальный)" value={f.code} onChange={v=>set("code",v.toLowerCase().replace(/[^a-zа-яё0-9_]/gi,"_"))} placeholder="novosibirsk_minsport" mono helpText={isNew?"Латиница/кириллица, цифры, подчёркивание":"Нельзя изменить после создания"} />
        <Input label="Название организации" value={f.name} onChange={v=>set("name",v)} placeholder="Министерство спорта Новосибирской области" />
        <Input label="Регион" value={f.region} onChange={v=>set("region",v)} placeholder="Новосибирская область" />
        <Input label="Код субъекта РФ" value={f.federal_subject} onChange={v=>set("federal_subject",v)} placeholder="54" />
        <Select label="Тип источника" value={f.source_type} onChange={v=>set("source_type",v)} options={[{value:"pdf_portal",label:"PDF-портал (HTML + PDF-ссылки)"},{value:"json_embed",label:"JSON embed (данные в JS-переменной)"},{value:"html_table",label:"HTML-таблица"}]} />
        <Select label="Класс риска" value={f.risk_class} onChange={v=>set("risk_class",v)} options={[{value:"green",label:"🟢 Green — httpx, без антибота"},{value:"amber",label:"🟡 Amber — Playwright, JS-рендер"},{value:"red",label:"🔴 Red — тяжёлый антибот, ручная проверка"}]} />
      </div>
      <Input label="Официальное основание" value={f.official_basis} onChange={v=>set("official_basis",v)} placeholder="Домен принадлежит органу власти субъекта РФ (8-ФЗ)" />
      <Toggle label="Активен (включить мониторинг)" value={f.active} onChange={v=>set("active",v)} />
    </Card>}

    {/* Step 1: Download */}
    {step===1 && <Card title="Параметры загрузки">
      <Select label="Метод загрузки" value={f.download_method} onChange={v=>set("download_method",v)} options={[{value:"httpx",label:"httpx — прямой HTTP (для зелёных)"},{value:"playwright",label:"Playwright — браузер/JS-рендер (для amber/red)"}]} />
      <Input label="Базовый URL" value={f.base_url} onChange={v=>set("base_url",v)} placeholder="https://minsport.nso.ru" mono helpText="Корневой домен для urljoin и SSRF-whitelist" />
      {f.download_method==="playwright"&&<>
        <Select label="Тип антибота" value={f.antibot} onChange={v=>set("antibot",v)} options={[{value:"",label:"Нет"},{value:"servicepipe",label:"Servicepipe"},{value:"cloudflare",label:"Cloudflare"},{value:"ddos-guard",label:"DDoS-Guard"}]} />
        <Input label="CSS-селектор ожидания" value={f.wait_selector} onChange={v=>set("wait_selector",v)} placeholder="a[href$='.pdf'], a.document-link" mono helpText="Playwright будет ждать появления этого элемента" />
      </>}
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"0 16px"}}>
        <Input label="Мин. задержка (сек)" value={f.delay_min} onChange={v=>set("delay_min",parseFloat(v)||0)} />
        <Input label="Макс. задержка (сек)" value={f.delay_max} onChange={v=>set("delay_max",parseFloat(v)||0)} />
      </div>
    </Card>}

    {/* Step 2: Detection */}
    {step===2 && <Card title="Обнаружение документов">
      <div style={{marginBottom:12}}>
        <label style={{display:"block",fontSize:10,fontFamily:T.mono,color:T.textMuted,textTransform:"uppercase",letterSpacing:1,marginBottom:6}}>URL страниц со списками документов</label>
        {f.list_urls.map((u,i)=>(
          <div key={i} style={{display:"flex",gap:6,marginBottom:6}}>
            <input value={u} onChange={e=>setUrl(i,e.target.value)} placeholder="https://..." style={{flex:1,padding:"8px 10px",background:T.bgInput,border:`1px solid ${T.border}`,borderRadius:5,color:T.text,fontFamily:T.mono,fontSize:12,outline:"none"}} />
            {f.list_urls.length>1&&<Btn small outline color={T.red} onClick={()=>rmUrl(i)}>✕</Btn>}
          </div>
        ))}
        <Btn small outline onClick={addUrl}>+ Добавить URL</Btn>
      </div>

      {f.source_type==="json_embed"?
        <Input label="Имя JS-переменной" value={f.js_var} onChange={v=>set("js_var",v)} placeholder="$obj" mono helpText="Переменная в <script>, содержащая JSON с данными" />
      :<>
        <Input label="Regex для ссылок на документы" value={f.link_regex} onChange={v=>set("link_regex",v)} mono helpText="Группа захвата (1) = href документа. Применяется к HTML." />
        <Input label="Regex для заголовка" value={f.title_regex} onChange={v=>set("title_regex",v)} mono helpText="Опционально. Извлекает заголовок из контекста ссылки." />
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"0 16px"}}>
          <Input label="Regex даты приказа" value={f.order_date_regex} onChange={v=>set("order_date_regex",v)} mono />
          <Input label="Regex номера приказа" value={f.order_number_regex} onChange={v=>set("order_number_regex",v)} mono />
        </div>
      </>}

      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"0 16px"}}>
        <Input label="Шаблон пагинации" value={f.pagination} onChange={v=>set("pagination",v)} placeholder="?page={n} или &page={n}" mono helpText="Пусто = без пагинации" />
        <Input label="Макс. страниц" value={f.max_pages} onChange={v=>set("max_pages",parseInt(v)||1)} />
      </div>

      {/* Regex Tester */}
      {f.source_type!=="json_embed"&&<div style={{marginTop:16,padding:14,background:T.bg,borderRadius:6,border:`1px solid ${T.border}`}}>
        <div style={{fontSize:11,fontFamily:T.mono,color:T.accent,marginBottom:8}}>🧪 Тест regex-паттерна</div>
        <Input label="HTML-фрагмент для теста" value={regexTest.html} onChange={v=>setRegexTest(p=>({...p,html:v,results:null}))} textarea mono placeholder={'<a href="/docs/123/">Документ 1</a>\n<a href="/docs/456/">Документ 2</a>'} />
        <Btn small onClick={testRegex} style={{marginBottom:8}}>Проверить regex</Btn>
        {regexTest.results&&<div style={{marginTop:8}}>
          <div style={{fontSize:11,color:regexTest.results.length?T.green:T.red,fontFamily:T.mono,marginBottom:6}}>
            {regexTest.results.length?`✓ Найдено: ${regexTest.results.length} совпадений`:"✗ Совпадений не найдено"}
          </div>
          {regexTest.results.map((r,i)=>(
            <div key={i} style={{fontSize:11,fontFamily:T.mono,color:T.textDim,padding:"3px 0",borderBottom:`1px solid ${T.border}`}}>
              <span style={{color:T.text}}>{r.raw}</span>
              {r.resolved!==r.raw&&<span style={{color:T.accent,marginLeft:8}}>→ {r.resolved}</span>}
            </div>
          ))}
        </div>}
      </div>}
    </Card>}

    {/* Step 3: Meta */}
    {step===3 && <Card title="Метаданные для извлечения">
      <Input label="Издающий орган" value={f.issuing_body} onChange={v=>set("issuing_body",v)} placeholder="Министерство физической культуры и спорта Новосибирской области" helpText="Передаётся в промпт LLM/rule-экстрактора" />
      <Select label="Тип документа" value={f.order_type} onChange={v=>set("order_type",v)} options={[{value:"приказ",label:"Приказ"},{value:"распоряжение",label:"Распоряжение"},{value:"постановление",label:"Постановление"}]} />
    </Card>}

    {/* Step 4: Review */}
    {step===4 && <div>
      <Card title="Проверка конфигурации">
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
          <div>
            <div style={{fontSize:10,fontFamily:T.mono,color:T.textMuted,marginBottom:4}}>ИСТОЧНИК</div>
            <div style={{fontFamily:T.mono,fontSize:14,color:T.text,fontWeight:600}}>{f.code || "—"}</div>
            <div style={{fontSize:13,color:T.textDim,marginTop:2}}>{f.name || "—"}</div>
            <div style={{fontSize:12,color:T.textDim}}>{f.region}</div>
            <div style={{marginTop:8}}><RiskBadge risk={f.risk_class} /></div>
          </div>
          <div>
            <div style={{fontSize:10,fontFamily:T.mono,color:T.textMuted,marginBottom:4}}>ЗАГРУЗКА</div>
            <div style={{fontSize:12,fontFamily:T.mono,color:T.text}}>{f.download_method} · {f.base_url || "—"}</div>
            {f.antibot&&<div style={{fontSize:11,color:T.amber}}>Антибот: {f.antibot}</div>}
            <div style={{fontSize:11,color:T.textDim}}>Задержка: {f.delay_min}–{f.delay_max}с</div>
          </div>
          <div>
            <div style={{fontSize:10,fontFamily:T.mono,color:T.textMuted,marginBottom:4}}>ОБНАРУЖЕНИЕ</div>
            <div style={{fontSize:11,fontFamily:T.mono,color:T.textDim}}>{f.list_urls.filter(u=>u).length} URL, {f.source_type}</div>
            {f.link_regex&&<div style={{fontSize:10,fontFamily:T.mono,color:T.accent,marginTop:2,wordBreak:"break-all"}}>{f.link_regex.substring(0,60)}…</div>}
            {f.pagination&&<div style={{fontSize:11,color:T.textDim}}>Пагинация: {f.pagination} (до {f.max_pages} стр.)</div>}
          </div>
          <div>
            <div style={{fontSize:10,fontFamily:T.mono,color:T.textMuted,marginBottom:4}}>МЕТАДАННЫЕ</div>
            <div style={{fontSize:12,color:T.text}}>{f.issuing_body || "—"}</div>
            <div style={{fontSize:11,color:T.textDim}}>{f.order_type}</div>
          </div>
        </div>
      </Card>

      {/* Validation */}
      <Card title="Валидация">
        {[
          { ok: !!f.code, msg: "Код источника задан" },
          { ok: !!f.name, msg: "Название задано" },
          { ok: !!f.region, msg: "Регион задан" },
          { ok: f.list_urls.some(u=>u), msg: "Хотя бы один URL задан" },
          { ok: !!f.issuing_body, msg: "Издающий орган задан" },
          { ok: !!f.base_url, msg: "Базовый URL задан" },
          { ok: f.source_type==="json_embed" || !!f.link_regex, msg: "Regex для ссылок задан (или json_embed)" },
        ].map((v,i)=>(
          <div key={i} style={{display:"flex",alignItems:"center",gap:8,padding:"4px 0",fontSize:12}}>
            <span style={{color:v.ok?T.green:T.red,fontSize:14}}>{v.ok?"✓":"✗"}</span>
            <span style={{color:v.ok?T.text:T.red}}>{v.msg}</span>
          </div>
        ))}
      </Card>
    </div>}

    {/* Navigation */}
    {/* Navigation */}
    <div style={{display:"flex",justifyContent:"space-between",marginTop:16}}>
      <div style={{display:"flex",gap:8}}>
        {step>0&&<Btn outline onClick={()=>setStep(step-1)}>← Назад</Btn>}
        {!isNew&&step===4&&<Btn outline color={T.red} onClick={()=>onDelete?.(f.code)}>🗑 Удалить источник</Btn>}
      </div>
      <div style={{display:"flex",gap:8}}>
        {step<4&&<Btn onClick={()=>setStep(step+1)}>Далее →</Btn>}
        {step===4&&<Btn disabled={!valid} color={T.green} onClick={()=>onSave(f)}>
          {isNew?"✓ Создать источник":"✓ Сохранить изменения"}
        </Btn>}
      </div>
    </div>
  </div>;
};


// --- Sources Page ---
const SourcesPage = ({ onEdit, onAdd, onToggle, onCheck }) => (
  <div>
    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:16}}>
      <h2 style={{fontSize:18,fontWeight:600,fontFamily:T.sans,color:T.text,margin:0}}>Источники</h2>
      <div style={{display:"flex",gap:8}}>
        <Btn onClick={onAdd}>+ Новый источник</Btn>
        <Btn outline onClick={async()=>{try{await api("/api/actions/check-all",{method:"POST"});alert("Проверка запущена")}catch(e){alert("Ошибка: "+e.message)}}}>▶ Проверить все</Btn>
      </div>
    </div>
    <div style={{overflowX:"auto"}}>
      <table style={{width:"100%",borderCollapse:"collapse",fontFamily:T.sans,fontSize:13}}>
        <thead><tr>
          {["Риск","Код","Название","Регион","Метод","Статус","Приказы","Записи","Последний",""].map((h,i)=>
            <th key={i} style={{textAlign:i>=6?"right":"left",padding:"10px 10px",color:T.textMuted,fontSize:10,fontFamily:T.mono,textTransform:"uppercase",letterSpacing:1,borderBottom:`1px solid ${T.border}`,whiteSpace:"nowrap"}}>{h}</th>
          )}
        </tr></thead>
        <tbody>
          {DEMO_SOURCES.map((s,ri)=>(
            <tr key={ri} style={{borderBottom:`1px solid ${T.border}`,transition:"background .15s",cursor:"pointer"}} onMouseEnter={e=>e.currentTarget.style.background=T.bgHover} onMouseLeave={e=>e.currentTarget.style.background="transparent"} onClick={()=>onEdit(s)}>
              <td style={{padding:"10px"}}><RiskBadge risk={s.risk_class} /></td>
              <td style={{padding:"10px",fontFamily:T.mono,fontSize:12}}>{s.code}</td>
              <td style={{padding:"10px",maxWidth:200,overflow:"hidden",textOverflow:"ellipsis"}}>{s.name}</td>
              <td style={{padding:"10px",color:T.textDim}}>{s.region}</td>
              <td style={{padding:"10px"}}><Badge color={T.text} bg={T.bgHover}>{s.discovery_config?.download?.method||"?"}</Badge></td>
              <td style={{padding:"10px"}}><Badge color={s.active?T.green:T.textMuted} bg={s.active?T.greenBg:T.bgHover}>{s.active?"Active":"Off"}</Badge></td>
              <td style={{padding:"10px",textAlign:"right",fontFamily:T.mono}}><span style={{color:T.green}}>{s.orders_ok}</span> / <span style={{color:T.blue}}>{s.orders_pending}</span> / <span style={{color:T.red}}>{s.orders_failed}</span></td>
              <td style={{padding:"10px",textAlign:"right",fontFamily:T.mono}}>{fmt.num(s.total_assignments)}</td>
              <td style={{padding:"10px",fontFamily:T.mono,fontSize:11,color:T.textDim}}>{fmt.date(s.last_order_date)}</td>
              <td style={{padding:"10px",whiteSpace:"nowrap",textAlign:"right"}}>
                <Btn small outline onClick={e=>{e.stopPropagation();onCheck?.(s.code)}} title="Проверить">▶</Btn>{" "}
                <Btn small outline onClick={e=>{e.stopPropagation();onToggle?.(s.code,!s.active)}} title={s.active?"Отключить":"Включить"}>{s.active?"⏸":"▷"}</Btn>{" "}
                <Btn small outline onClick={e=>{e.stopPropagation();onEdit(s)}}>✎</Btn>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  </div>
);

// --- Orders Page (compact) ---
const OrdersPage = () => {
  const [filter, setFilter] = useState("all");
  const orders = [
    {order_number:"С-2/26",order_date:"2026-02-17",status:"extracted",source_name:"ЦСТиСК",record_count:286,page_count:16,ocr_method:"pypdf"},
    {order_number:"Р-128/2026",order_date:"2026-02-20",status:"new",source_name:"КФКиС СПб",record_count:0,page_count:0},
    {order_number:"23-18-рп",order_date:"2026-02-13",status:"failed",source_name:"МОСОБЛСПОРТ",record_count:0,page_count:4,error:"OCR confidence below threshold"},
  ];
  return <div>
    <h2 style={{fontSize:18,fontWeight:600,fontFamily:T.sans,color:T.text,margin:"0 0 16px"}}>Приказы</h2>
    <div style={{display:"flex",gap:6,marginBottom:14}}>
      {["all","new","extracted","approved","failed"].map(f=><button key={f} onClick={()=>setFilter(f)} style={{background:filter===f?T.bgActive:T.bgCard,color:filter===f?T.text:T.textDim,border:`1px solid ${filter===f?T.accent:T.border}`,borderRadius:6,padding:"5px 12px",fontSize:11,fontFamily:T.mono,cursor:"pointer"}}>{f==="all"?"Все":(STATUS[f]?.l||f)}</button>)}
    </div>
    {orders.map((o,i)=><div key={i} style={{display:"flex",alignItems:"center",gap:12,padding:"10px 12px",borderBottom:`1px solid ${T.border}`}}>
      <Badge color={(STATUS[o.status]||{}).c||T.textDim} bg={(STATUS[o.status]||{}).bg||T.bgHover}>{(STATUS[o.status]||{}).l||o.status}</Badge>
      <span style={{fontFamily:T.mono,fontWeight:600,color:T.accent,minWidth:100}}>{o.order_number}</span>
      <span style={{fontFamily:T.mono,fontSize:11,color:T.textDim,minWidth:75}}>{fmt.date(o.order_date)}</span>
      <span style={{color:T.textDim,flex:1}}>{o.source_name}</span>
      <span style={{fontFamily:T.mono,fontSize:12,color:o.record_count?T.green:T.textMuted}}>{o.record_count||"—"} записей</span>
    </div>)}
  </div>;
};

// --- Logs Page (compact) ---
const LogsPage = () => {
  const logs = [
    {level:"info",stage:"change_detection",src:"spb_kfkis",msg:"2 новых документа",t:"2026-02-24T10:00:12"},
    {level:"info",stage:"ocr",src:"moskva_tstisk",msg:"16 стр., метод=pypdf",t:"2026-02-18T12:00:45"},
    {level:"error",stage:"download",src:"mo_mособлспорт",msg:"Servicepipe CAPTCHA detected",t:"2026-02-24T08:12:00"},
    {level:"warn",stage:"ocr",src:"mo_mособлспорт",msg:"Tesseract confidence 0.52",t:"2026-02-14T08:01:00"},
  ];
  return <div>
    <h2 style={{fontSize:18,fontWeight:600,fontFamily:T.sans,color:T.text,margin:"0 0 14px"}}>Журнал</h2>
    {logs.map((l,i)=><div key={i} style={{display:"flex",gap:8,padding:"7px 10px",borderBottom:`1px solid ${T.border}`,fontFamily:T.mono,fontSize:12,background:l.level==="error"?T.redBg:"transparent"}}>
      <span style={{color:l.level==="error"?T.red:l.level==="warn"?T.amber:T.green,fontSize:10}}>●</span>
      <span style={{color:T.textMuted,minWidth:90,fontSize:10}}>{fmt.time(l.t)}</span>
      <span style={{color:T.textDim,minWidth:70,fontSize:11}}>{l.src}</span>
      <Badge color={T.text} bg={T.bgHover}>{l.stage}</Badge>
      <span style={{color:T.text,flex:1,fontFamily:T.sans}}>{l.msg}</span>
    </div>)}
  </div>;
};

// --- Assignments Page ---
const DEMO_ASSIGNMENTS = [
  { fio: "Иванов Иван Иванович", birth_date: "15.03.1990", sport: "Бокс", assignment_type: "judge_category", rank_category: "спортивный судья третьей категории", action: "assignment", source_code: "moskva_tstisk", order_number: "С-2/26" },
  { fio: "Петрова Мария Сергеевна", birth_date: "22.07.1985", sport: "Дзюдо", assignment_type: "sport_rank", rank_category: "кандидат в мастера спорта", action: "assignment", source_code: "spb_kfkis", order_number: "Р-45/26" },
  { fio: "Сидоров Алексей Петрович", birth_date: "03.11.1992", sport: "Плавание", assignment_type: "judge_category", rank_category: "спортивный судья второй категории", action: "confirmation", source_code: "krasnodar_minsport", order_number: "П-12/26" },
  { fio: "Козлова Елена Владимировна", birth_date: "18.09.1988", sport: "Художественная гимнастика", assignment_type: "specialist_category", rank_category: "специалист первой квалификационной категории", action: "assignment", source_code: "moskva_tstisk", order_number: "С-2/26" },
  { fio: "Морозов Дмитрий Анатольевич", birth_date: "07.05.1995", sport: "Лёгкая атлетика", assignment_type: "sport_rank", rank_category: "первый спортивный разряд", action: "assignment", source_code: "mo_mособлспорт", order_number: "Р-8/26" },
];
const TYPE_LABELS = { sport_rank: "Разряд", judge_category: "Судья", specialist_category: "Специалист", coach_category: "Тренер" };
const ACTION_LABELS = { assignment: "Присвоение", confirmation: "Подтверждение", refusal: "Отказ", revocation: "Лишение" };

const AssignmentsPage = () => {
  const [search, setSearch] = useState("");
  const filtered = DEMO_ASSIGNMENTS.filter(a =>
    !search || a.fio.toLowerCase().includes(search.toLowerCase()) || (a.sport||"").toLowerCase().includes(search.toLowerCase())
  );
  return <div>
    <h2 style={{fontSize:18,fontWeight:600,fontFamily:T.sans,color:T.text,margin:"0 0 16px"}}>Реестр присвоений</h2>
    <input value={search} onChange={e=>setSearch(e.target.value)} placeholder="Поиск по ФИО или виду спорта…" style={{width:"100%",maxWidth:400,padding:"8px 12px",background:T.bgInput||T.bgCard,border:`1px solid ${T.border}`,borderRadius:6,color:T.text,fontFamily:T.sans,fontSize:13,marginBottom:16,outline:"none"}} />
    <div style={{overflowX:"auto"}}>
      <table style={{width:"100%",borderCollapse:"collapse",fontFamily:T.sans,fontSize:13}}>
        <thead><tr>
          {["ФИО","Дата рожд.","Вид спорта","Тип","Категория/разряд","Действие","Источник","Приказ"].map((h,i)=>
            <th key={i} style={{textAlign:"left",padding:"10px",color:T.textMuted,fontSize:10,fontFamily:T.mono,textTransform:"uppercase",letterSpacing:1,borderBottom:`1px solid ${T.border}`,whiteSpace:"nowrap"}}>{h}</th>
          )}
        </tr></thead>
        <tbody>{filtered.length===0?<tr><td colSpan={8} style={{padding:20,textAlign:"center",color:T.textMuted}}>Нет записей</td></tr>:
          filtered.map((a,i)=>(
            <tr key={i} style={{borderBottom:`1px solid ${T.border}`}}>
              <td style={{padding:"10px",fontWeight:500}}>{a.fio}</td>
              <td style={{padding:"10px",fontFamily:T.mono,fontSize:11,color:T.textDim}}>{a.birth_date||"—"}</td>
              <td style={{padding:"10px"}}>{a.sport||"—"}</td>
              <td style={{padding:"10px"}}><Badge color={T.accent} bg={T.bgActive}>{TYPE_LABELS[a.assignment_type]||a.assignment_type}</Badge></td>
              <td style={{padding:"10px",fontSize:12,color:T.textDim,maxWidth:200,overflow:"hidden",textOverflow:"ellipsis"}}>{a.rank_category}</td>
              <td style={{padding:"10px"}}><Badge color={a.action==="refusal"?T.red:a.action==="revocation"?T.red:T.green} bg={T.bgHover}>{ACTION_LABELS[a.action]||a.action}</Badge></td>
              <td style={{padding:"10px",fontFamily:T.mono,fontSize:11}}>{a.source_code}</td>
              <td style={{padding:"10px",fontFamily:T.mono,fontSize:11}}>{a.order_number}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  </div>;
};


// ============================================================================
// APP
// ============================================================================
const NAV = [
  { id:"dashboard", label:"Обзор", icon:"◉" },
  { id:"sources", label:"Источники", icon:"⊕" },
  { id:"orders", label:"Приказы", icon:"📋" },
  { id:"assignments", label:"Реестр", icon:"👤" },
  { id:"logs", label:"Журнал", icon:"⫶" },
];

// API helper: подставляет базовый URL, обрабатывает ошибки
const API = window.SPORTRANK_API || "";
const api = async (path, opts = {}) => {
  const token = sessionStorage.getItem("sr_token") || "";
  const res = await fetch(`${API}${path}`, {
    ...opts,
    headers: { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}), ...opts.headers },
    body: opts.body ? (typeof opts.body === "string" ? opts.body : JSON.stringify(opts.body)) : undefined,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || err.message || `HTTP ${res.status}`);
  }
  return res.json();
};

export default function App() {
  const [page, setPage] = useState("sources");
  const [editSource, setEditSource] = useState(null);
  const [isNewSource, setIsNewSource] = useState(false);
  const [toast, setToast] = useState(null);

  const showToast = (msg, ok = true) => { setToast({ msg, ok }); setTimeout(() => setToast(null), 4000); };

  const handleEditSource = (src) => { setEditSource(src); setIsNewSource(false); setPage("source-form"); };
  const handleNewSource = () => { setEditSource(null); setIsNewSource(true); setPage("source-form"); };

  const handleSaveSource = async (formData) => {
    try {
      if (isNewSource) {
        await api("/api/sources", { method: "POST", body: formData });
        showToast(`Источник ${formData.code} создан`);
      } else {
        await api(`/api/sources/${formData.code}`, { method: "PUT", body: formData });
        showToast(`Источник ${formData.code} обновлён`);
      }
      setPage("sources");
    } catch (e) {
      showToast(`Ошибка: ${e.message}`, false);
    }
  };

  const handleDeleteSource = async (code) => {
    if (!confirm(`Удалить источник ${code}?`)) return;
    try {
      await api(`/api/sources/${code}`, { method: "DELETE" });
      showToast(`Источник ${code} удалён`);
      setPage("sources");
    } catch (e) {
      showToast(`Ошибка: ${e.message}`, false);
    }
  };

  const handleToggleSource = async (code, active) => {
    try {
      await api(`/api/sources/${code}`, { method: "PATCH", body: { active } });
      showToast(`${code}: ${active ? "включён" : "отключён"}`);
    } catch (e) {
      showToast(`Ошибка: ${e.message}`, false);
    }
  };

  const handleCheckSource = async (code) => {
    try {
      await api(`/api/sources/${code}/check`, { method: "POST" });
      showToast(`Проверка ${code} запущена`);
    } catch (e) {
      showToast(`Ошибка: ${e.message}`, false);
    }
  };

  const handleCancelForm = () => setPage("sources");

  return <div style={{display:"flex",height:"100vh",background:T.bg,fontFamily:T.sans,color:T.text}}>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet" />

    {/* Sidebar */}
    <div style={{width:200,background:T.bgCard,borderRight:`1px solid ${T.border}`,display:"flex",flexDirection:"column",flexShrink:0}}>
      <div style={{padding:"18px 16px 20px",borderBottom:`1px solid ${T.border}`}}>
        <div style={{fontFamily:T.mono,fontSize:14,fontWeight:700,letterSpacing:1}}><span style={{color:T.accent}}>SPORT</span>RANK</div>
        <div style={{fontFamily:T.mono,fontSize:8,color:T.textMuted,marginTop:3,letterSpacing:2,textTransform:"uppercase"}}>Admin v2.0</div>
      </div>
      <nav style={{padding:"10px 6px",flex:1}}>
        {NAV.map(n=>{
          const act=page===n.id||(n.id==="sources"&&page==="source-form");
          return <button key={n.id} onClick={()=>{setPage(n.id);setEditSource(null)}} style={{display:"flex",alignItems:"center",gap:8,width:"100%",padding:"9px 10px",background:act?T.bgActive:"transparent",color:act?T.text:T.textDim,border:"none",borderRadius:5,cursor:"pointer",fontFamily:T.sans,fontSize:13,fontWeight:act?600:400,textAlign:"left",borderLeft:act?`2px solid ${T.accent}`:"2px solid transparent"}}>
            <span style={{fontSize:13,opacity:act?1:.4,width:18,textAlign:"center"}}>{n.icon}</span>{n.label}
          </button>;
        })}
      </nav>
      <div style={{padding:"14px 16px",borderTop:`1px solid ${T.border}`}}>
        <div style={{display:"flex",alignItems:"center",gap:6}}><div style={{width:7,height:7,borderRadius:"50%",background:T.green}}/><span style={{fontFamily:T.mono,fontSize:9,color:T.textDim}}>System OK</span></div>
      </div>
    </div>

    {/* Main */}
    <div style={{flex:1,overflow:"auto",padding:24}}>
      {toast && <div style={{position:"fixed",top:16,right:16,padding:"10px 18px",borderRadius:6,background:toast.ok?"#15803d":"#b91c1c",color:"#fff",fontFamily:T.mono,fontSize:12,zIndex:999,boxShadow:"0 4px 16px rgba(0,0,0,.4)",transition:"opacity .2s"}}>{toast.msg}</div>}
      {page==="dashboard"&&<DashboardPage />}
      {page==="sources"&&<SourcesPage onEdit={handleEditSource} onAdd={handleNewSource} onToggle={handleToggleSource} onCheck={handleCheckSource} />}
      {page==="source-form"&&<SourceForm source={editSource} isNew={isNewSource} onSave={handleSaveSource} onCancel={handleCancelForm} onDelete={handleDeleteSource} />}
      {page==="orders"&&<OrdersPage />}
      {page==="assignments"&&<AssignmentsPage />}
      {page==="logs"&&<LogsPage />}
    </div>
  </div>;
}
