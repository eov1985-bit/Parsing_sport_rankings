import { useState, useReducer } from "react";

/* ═══════════════════════════════════════════════════════════════════════
   SPORTRANK Admin v3.1 — Dashboard with Metrics, KPI, Exchange Preview
   - No sessionStorage/localStorage (Claude artifact safe)
   - No confirm() — state-based dialogs
   - useReducer for centralized state
   - Metrics/KPI page with throughput, error rates, OCR stats
   - Exchange API preview (exact JSON for external IS)
   ═══════════════════════════════════════════════════════════════════════ */

// ═══ THEME ═══
const C = {
  bg:"#080A0E",bg1:"#0E1117",bg2:"#161B25",bg3:"#1C2333",bgH:"#222B3A",
  bd:"#2A3345",bdF:"#4A7ADB",
  tx:"#D8DCE4",txD:"#8893A6",txM:"#4E5A6E",
  ac:"#4B8DF8",acD:"#3565B5",
  ok:"#34D399",okB:"rgba(52,211,153,.08)",
  wn:"#F59E0B",wnB:"rgba(245,158,11,.08)",
  er:"#EF4444",erB:"rgba(239,68,68,.08)",
  in:"#60A5FA",inB:"rgba(96,165,250,.08)",
  pu:"#A78BFA",puB:"rgba(167,139,250,.08)",
};
const F={m:"'JetBrains Mono','SF Mono','Fira Code',monospace",s:"'DM Sans','Nunito Sans',system-ui,sans-serif"};
const RISK={green:{l:"Green",c:C.ok,i:"●"},amber:{l:"Amber",c:C.wn,i:"▲"},red:{l:"Red",c:C.er,i:"■"}};
const STAT={new:{l:"Новый",c:C.in,b:C.inB},downloaded:{l:"Скачан",c:C.wn,b:C.wnB},extracted:{l:"Извлечён",c:C.ok,b:C.okB},approved:{l:"Утверждён",c:C.pu,b:C.puB},failed:{l:"Ошибка",c:C.er,b:C.erB}};
const fm={d:s=>s?new Date(s).toLocaleDateString("ru-RU",{day:"2-digit",month:"2-digit",year:"numeric"}):"—",t:s=>s?new Date(s).toLocaleString("ru-RU",{day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"}):"—",n:v=>v==null?"—":typeof v==="number"?v.toLocaleString("ru-RU"):v,p:v=>v==null?"—":(v*100).toFixed(1)+"%"};

// ═══ SAMPLE DATA ═══
const SRC=[
  {code:"moskva_tstisk",name:"ГКУ «ЦСТиСК» Москомспорта",region:"г. Москва",federal_subject:"77",source_type:"pdf_portal",risk_class:"amber",active:true,official_basis:"Домен mos.ru",orders_ok:42,orders_pending:1,orders_failed:0,total_assignments:1847,last_order_date:"2026-02-17",last_checked_at:"2026-02-25T10:00:00Z",download:{method:"playwright",base_url:"https://www.mos.ru",antibot:"servicepipe",delay_min:2,delay_max:6,wait_selector:"a[href$='.pdf']"},detect:{list_urls:["https://www.mos.ru/moskomsport/documents/prisvoenie-sportivnykh-razryadov/"],link_regex:'href=["\']([^"\']*view/\\d+[^"\']*)["\']',title_regex:"",order_date_regex:"",order_number_regex:"",pagination:"",max_pages:1,js_var:""},meta:{issuing_body:"ГКУ «ЦСТиСК» Москомспорта",order_type:"приказ"}},
  {code:"spb_kfkis",name:"КФКиС Санкт-Петербурга",region:"г. Санкт-Петербург",federal_subject:"78",source_type:"pdf_portal",risk_class:"green",active:true,official_basis:"Домен kfis.gov.spb.ru",orders_ok:38,orders_pending:2,orders_failed:0,total_assignments:892,last_order_date:"2026-02-20",last_checked_at:"2026-02-25T09:55:00Z",download:{method:"httpx",base_url:"https://kfis.gov.spb.ru",antibot:"",delay_min:1,delay_max:3,wait_selector:""},detect:{list_urls:["https://kfis.gov.spb.ru/docs/?type=54"],link_regex:'href=["\']([^"\']*?/docs/\\d+[^"\']*)["\']',title_regex:"",order_date_regex:"",order_number_regex:"",pagination:"&page={n}",max_pages:3,js_var:""},meta:{issuing_body:"КФКиС Санкт-Петербурга",order_type:"распоряжение"}},
  {code:"mo_mosoblsport",name:"МОСОБЛСПОРТ",region:"Московская обл.",federal_subject:"50",source_type:"pdf_portal",risk_class:"red",active:true,official_basis:"mst.mosreg.ru",orders_ok:15,orders_pending:0,orders_failed:2,total_assignments:312,last_order_date:"2026-02-13",last_checked_at:"2026-02-24T14:00:00Z",download:{method:"playwright",base_url:"https://mst.mosreg.ru",antibot:"servicepipe",delay_min:3,delay_max:8,wait_selector:"a.document-link"},detect:{list_urls:["https://mst.mosreg.ru/dokumenty/prisvoenie/"],link_regex:'href=["\']([^"\']*prikaz[^"\']*)["\']',title_regex:"",order_date_regex:"",order_number_regex:"",pagination:"?page={n}",max_pages:3,js_var:""},meta:{issuing_body:"Минспорт МО",order_type:"распоряжение"}},
  {code:"krasnodar_minsport",name:"Минспорт Краснодарского края",region:"Краснодарский край",federal_subject:"23",source_type:"pdf_portal",risk_class:"green",active:true,official_basis:"minsport.krasnodar.ru",orders_ok:12,orders_pending:0,orders_failed:0,total_assignments:134,last_order_date:"2026-01-28",last_checked_at:"2026-02-25T10:03:00Z",download:{method:"httpx",base_url:"https://minsport.krasnodar.ru",antibot:"",delay_min:1,delay_max:4,wait_selector:""},detect:{list_urls:["https://minsport.krasnodar.ru/activities/sport/prisvoenie/"],link_regex:'href=["\']([^"\']*\\.pdf)["\']',title_regex:"",order_date_regex:"",order_number_regex:"",pagination:"",max_pages:1,js_var:""},meta:{issuing_body:"Минспорт Краснодарского края",order_type:"приказ"}},
  {code:"rf_minsport",name:"Минспорт РФ (msrfinfo.ru)",region:"Россия",federal_subject:"00",source_type:"json_embed",risk_class:"green",active:false,official_basis:"msrfinfo.ru",orders_ok:9,orders_pending:0,orders_failed:0,total_assignments:47,last_order_date:"2026-02-01",last_checked_at:"2026-02-23T06:00:00Z",download:{method:"httpx",base_url:"https://msrfinfo.ru",antibot:"",delay_min:1,delay_max:2,wait_selector:""},detect:{list_urls:["https://msrfinfo.ru/awards/"],link_regex:"",title_regex:"",order_date_regex:"",order_number_regex:"",pagination:"",max_pages:1,js_var:"$obj"},meta:{issuing_body:"Минспорт РФ",order_type:"приказ"}},
];
const ORD=[
  {id:"o1",order_number:"С-2/26",order_date:"2026-02-17",status:"extracted",source_code:"moskva_tstisk",record_count:286,page_count:16,ocr_confidence:0.92,ocr_method:"pypdf",signed_by:"Иванова И.И."},
  {id:"o2",order_number:"Р-128/2026",order_date:"2026-02-20",status:"new",source_code:"spb_kfkis",record_count:0,page_count:0},
  {id:"o3",order_number:"Р-129/2026",order_date:"2026-02-21",status:"new",source_code:"spb_kfkis",record_count:0,page_count:0},
  {id:"o4",order_number:"23-18-рп",order_date:"2026-02-13",status:"failed",source_code:"mo_mosoblsport",record_count:0,page_count:4,ocr_confidence:0.52,ocr_method:"tesseract"},
  {id:"o5",order_number:"П-12/26",order_date:"2026-01-28",status:"approved",source_code:"krasnodar_minsport",record_count:48,page_count:6,ocr_confidence:0.88,ocr_method:"pypdf",signed_by:"Петров П.П."},
  {id:"o6",order_number:"С-1/26",order_date:"2026-02-10",status:"approved",source_code:"moskva_tstisk",record_count:312,page_count:22,ocr_confidence:0.91,ocr_method:"pypdf",signed_by:"Иванова И.И."},
];
const LOG=[
  {lvl:"info",stage:"change_detection",src:"spb_kfkis",msg:"2 новых документа",t:"2026-02-25T10:00:12Z"},
  {lvl:"info",stage:"ocr",src:"moskva_tstisk",msg:"16 стр. pypdf, confidence=0.92, 303ms",t:"2026-02-18T12:00:45Z"},
  {lvl:"info",stage:"extract",src:"moskva_tstisk",msg:"286 записей (rule_extractor), avg_conf=0.87",t:"2026-02-18T12:01:02Z"},
  {lvl:"error",stage:"download",src:"mo_mosoblsport",msg:"Servicepipe CAPTCHA — retry 3/3 failed",t:"2026-02-25T08:12:00Z"},
  {lvl:"warn",stage:"ocr",src:"mo_mosoblsport",msg:"Confidence=0.52 < 0.6 threshold",t:"2026-02-14T08:01:00Z"},
  {lvl:"info",stage:"scheduler",src:"system",msg:"check-all: 5 источников за 12.4s",t:"2026-02-25T06:00:01Z"},
  {lvl:"info",stage:"exchange",src:"system",msg:"GET /api/exchange/orders — 2 приказа, 360 записей",t:"2026-02-25T09:30:00Z"},
];
const EXCHANGE_PREVIEW=[
  {order:"П-12/26",issueDate:"2026-01-28",issuedBy:2310034681,signedBy:"Петров П.П.",assignment:true,file:null,items:[
    {lastName:"Козлов",firstName:"Андрей",middleName:"Николаевич",birthDate:"2000-06-15",sport:"плавание",rank:"КМС"},
    {lastName:"Смирнова",firstName:"Ольга",middleName:"Дмитриевна",birthDate:"1998-12-12",sport:"бокс",rank:"первый юношеский спортивный разряд"},
  ]},
  {order:"С-1/26",issueDate:"2026-02-10",issuedBy:7710035680,signedBy:"Иванова И.И.",assignment:true,file:null,items:[
    {lastName:"Петров",firstName:"Пётр",middleName:"Петрович",birthDate:"2000-12-31",sport:"дзюдо",rank:"КМС"},
    {lastName:"Иванова",firstName:"Мария",middleName:"Сергеевна",birthDate:"1995-03-22",sport:"художественная гимнастика",rank:"мастер спорта"},
  ]},
];

// ═══ REDUCER ═══
function reducer(st,a){switch(a.type){
  case"ADD_SRC":return{...st,sources:[...st.sources,a.p]};
  case"UPD_SRC":return{...st,sources:st.sources.map(s=>s.code===a.code?{...s,...a.p}:s)};
  case"DEL_SRC":return{...st,sources:st.sources.filter(s=>s.code!==a.code)};
  case"TOG_SRC":return{...st,sources:st.sources.map(s=>s.code===a.code?{...s,active:!s.active}:s)};
  case"LOG":return{...st,logs:[a.p,...st.logs].slice(0,50)};
  default:return st;
}}

// ═══ ATOMS ═══
const Bd=({color,bg,children})=><span style={{display:"inline-block",padding:"2px 9px",borderRadius:4,fontSize:10,fontWeight:600,letterSpacing:.4,color,background:bg||"rgba(255,255,255,.04)",fontFamily:F.m,whiteSpace:"nowrap"}}>{children}</span>;
const RB=({r})=>{const x=RISK[r]||RISK.green;return<span style={{color:x.c,fontFamily:F.m,fontSize:11,fontWeight:600}}>{x.i} {x.l}</span>};
const Bt=({children,onClick,color=C.ac,outline,sm,disabled,danger,style={}})=><button disabled={disabled} onClick={onClick} style={{background:outline?"transparent":(danger?C.er:color),color:outline?(danger?C.er:color):"#fff",border:outline?`1px solid ${danger?C.er:color}`:"none",borderRadius:6,padding:sm?"4px 10px":"7px 16px",fontSize:sm?10:11,fontFamily:F.m,cursor:disabled?"not-allowed":"pointer",fontWeight:600,opacity:disabled?.45:1,transition:"all .15s",letterSpacing:.3,...style}}>{children}</button>;
const In=({label,value,onChange,ph,mono,area,help,disabled})=><div style={{marginBottom:10}}>{label&&<label style={{display:"block",fontSize:9,fontFamily:F.m,color:C.txM,textTransform:"uppercase",letterSpacing:1.2,marginBottom:3}}>{label}</label>}{area?<textarea value={value||""} onChange={e=>onChange(e.target.value)} placeholder={ph} disabled={disabled} rows={3} style={{width:"100%",padding:"7px 9px",background:C.bg1,border:`1px solid ${C.bd}`,borderRadius:5,color:C.tx,fontFamily:mono?F.m:F.s,fontSize:12,resize:"vertical",outline:"none",boxSizing:"border-box",opacity:disabled?.5:1}}/>:<input value={value||""} onChange={e=>onChange(e.target.value)} placeholder={ph} disabled={disabled} style={{width:"100%",padding:"7px 9px",background:C.bg1,border:`1px solid ${C.bd}`,borderRadius:5,color:C.tx,fontFamily:mono?F.m:F.s,fontSize:12,outline:"none",boxSizing:"border-box",opacity:disabled?.5:1}}/>}{help&&<div style={{fontSize:9,color:C.txM,marginTop:2}}>{help}</div>}</div>;
const Sl=({label,value,onChange,opts})=><div style={{marginBottom:10}}>{label&&<label style={{display:"block",fontSize:9,fontFamily:F.m,color:C.txM,textTransform:"uppercase",letterSpacing:1.2,marginBottom:3}}>{label}</label>}<select value={value} onChange={e=>onChange(e.target.value)} style={{width:"100%",padding:"7px 9px",background:C.bg1,border:`1px solid ${C.bd}`,borderRadius:5,color:C.tx,fontFamily:F.s,fontSize:12,outline:"none"}}>{opts.map(o=><option key={o.v} value={o.v}>{o.l}</option>)}</select></div>;
const Tg=({label,value,onChange})=><div style={{display:"flex",alignItems:"center",gap:10,marginBottom:10,cursor:"pointer",userSelect:"none"}} onClick={()=>onChange(!value)}><div style={{width:34,height:18,borderRadius:9,background:value?C.ok:C.bd,transition:"background .2s",position:"relative",flexShrink:0}}><div style={{width:14,height:14,borderRadius:7,background:"#fff",position:"absolute",top:2,left:value?18:2,transition:"left .2s"}}/></div><span style={{fontSize:12,color:C.tx}}>{label}</span></div>;
const Cd=({title,children,actions,np})=><div style={{background:C.bg2,border:`1px solid ${C.bd}`,borderRadius:8,padding:np?0:18,marginBottom:14}}>{(title||actions)&&<div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12,padding:np?"14px 18px 0":0}}>{title&&<div style={{fontSize:10,fontFamily:F.m,color:C.txM,textTransform:"uppercase",letterSpacing:1.2}}>{title}</div>}{actions&&<div style={{display:"flex",gap:5}}>{actions}</div>}</div>}{children}</div>;
const Stt=({label,value,sub,color,icon})=><div style={{background:C.bg2,border:`1px solid ${C.bd}`,borderRadius:8,padding:"14px 16px",flex:1,minWidth:120}}><div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}><span style={{color:C.txM,fontSize:9,fontFamily:F.m,textTransform:"uppercase",letterSpacing:1}}>{label}</span>{icon&&<span style={{fontSize:14,opacity:.25}}>{icon}</span>}</div><div style={{fontSize:24,fontWeight:700,color:color||C.tx,fontFamily:F.m,marginTop:3}}>{value}</div>{sub&&<div style={{fontSize:9,color:C.txD,marginTop:2}}>{sub}</div>}</div>;
const Toast=({toast})=>toast?<div style={{position:"fixed",top:14,right:14,padding:"10px 18px",borderRadius:6,background:toast.ok?"#065F46":"#7F1D1D",color:"#fff",fontFamily:F.m,fontSize:11,zIndex:9999,boxShadow:"0 4px 20px rgba(0,0,0,.5)",maxWidth:400,animation:"fadeIn .2s ease"}}>{toast.msg}</div>:null;
const Cfm=({msg,onYes,onNo})=><div style={{position:"fixed",inset:0,background:"rgba(0,0,0,.6)",display:"flex",alignItems:"center",justifyContent:"center",zIndex:9999}}><div style={{background:C.bg2,border:`1px solid ${C.bd}`,borderRadius:10,padding:24,maxWidth:400,minWidth:300}}><div style={{fontSize:13,color:C.tx,marginBottom:18,lineHeight:1.5}}>{msg}</div><div style={{display:"flex",gap:8,justifyContent:"flex-end"}}><Bt outline onClick={onNo}>Отмена</Bt><Bt danger onClick={onYes}>Да</Bt></div></div></div>;
const MiniBar=({data,maxH=40,color=C.ac})=>{const mx=Math.max(...data.map(d=>d.v),1);return<div style={{display:"flex",alignItems:"flex-end",gap:2,height:maxH}}>{data.map((d,i)=><div key={i} title={`${d.l}: ${d.v}`} style={{flex:1,minWidth:6,background:color,borderRadius:"2px 2px 0 0",height:Math.max(2,d.v/mx*maxH),opacity:.7+.3*(d.v/mx),transition:"height .3s"}}/>)}</div>};
const KPI=({label,value,target,unit="",good})=>{const pct=target?Math.min(value/target,1.5):1;const col=good===undefined?(pct>=1?C.ok:pct>.7?C.wn:C.er):(good?C.ok:C.er);return<div style={{background:C.bg2,border:`1px solid ${C.bd}`,borderRadius:8,padding:"12px 14px",flex:1,minWidth:140}}><div style={{fontSize:9,fontFamily:F.m,color:C.txM,textTransform:"uppercase",letterSpacing:1,marginBottom:4}}>{label}</div><div style={{display:"flex",alignItems:"baseline",gap:4}}><span style={{fontSize:22,fontWeight:700,fontFamily:F.m,color:col}}>{typeof value==="number"?value.toLocaleString("ru-RU"):value}</span><span style={{fontSize:10,color:C.txD}}>{unit}</span></div>{target!=null&&<div style={{marginTop:5,height:3,background:C.bg,borderRadius:2,overflow:"hidden"}}><div style={{height:"100%",width:`${Math.min(pct*100,100)}%`,background:col,borderRadius:2,transition:"width .5s"}}/></div>}{target!=null&&<div style={{fontSize:8,color:C.txM,marginTop:2}}>Цель: {target}{unit}</div>}</div>};

// ═══ PAGES ═══

const PgDash=({sources:ss,orders:oo,logs:ll})=>{
  const act=ss.filter(s=>s.active).length;const t=ss.reduce((a,s)=>a+(s.total_assignments||0),0);const fail=oo.filter(o=>o.status==="failed").length;
  return<div>
    <h2 style={{fontSize:17,fontWeight:600,fontFamily:F.s,color:C.tx,margin:"0 0 16px"}}>Обзор системы</h2>
    <div style={{display:"flex",gap:10,flexWrap:"wrap",marginBottom:18}}>
      <Stt label="Источники" value={act} sub={`${ss.length} всего`} icon="◉"/>
      <Stt label="Приказы" value={oo.length} sub={`${fail} ошибок`} icon="📋"/>
      <Stt label="Записи" value={fm.n(t)} icon="👤" color={C.ok}/>
      <Stt label="OCR avg" value={fm.p(oo.filter(o=>o.ocr_confidence).reduce((a,o,_,arr)=>a+o.ocr_confidence/arr.length,0))} icon="🔍"/>
    </div>
    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
      <Cd title="Конвейер">
        <div style={{display:"flex",gap:16,flexWrap:"wrap"}}>
          {[{l:"Очередь",v:oo.filter(o=>o.status==="new").length,c:C.in},{l:"Извлечено",v:oo.filter(o=>o.status==="extracted").length,c:C.ok},{l:"Утверждено",v:oo.filter(o=>o.status==="approved").length,c:C.pu},{l:"Ошибки",v:fail,c:C.er}].map((s,i)=><div key={i} style={{textAlign:"center",minWidth:55}}><div style={{fontSize:20,fontWeight:700,color:s.c,fontFamily:F.m}}>{s.v}</div><div style={{fontSize:9,color:C.txD}}>{s.l}</div></div>)}
        </div>
      </Cd>
      <Cd title="Ошибки">
        {ll.filter(l=>l.lvl==="error").slice(0,3).map((e,i)=><div key={i} style={{display:"flex",gap:6,padding:"5px 0",fontSize:11}}><span style={{color:C.er}}>⚠</span><span style={{color:C.txD,fontFamily:F.m,fontSize:10,minWidth:65}}>{e.src}</span><span style={{color:C.tx,flex:1,fontSize:12}}>{e.msg}</span></div>)}
      </Cd>
    </div>
  </div>;
};

const PgMetrics=({sources:ss,orders:oo})=>{
  const approved=oo.filter(o=>o.status==="approved");const totalRec=approved.reduce((a,o)=>a+(o.record_count||0),0);
  const totalPg=oo.filter(o=>o.page_count).reduce((a,o)=>a+o.page_count,0);
  const withConf=oo.filter(o=>o.ocr_confidence);const avgConf=withConf.length?withConf.reduce((a,o)=>a+o.ocr_confidence,0)/withConf.length:0;
  const failRate=oo.length?oo.filter(o=>o.status==="failed").length/oo.length:0;
  const withMethod=oo.filter(o=>o.ocr_method);const pypdfPct=withMethod.length?withMethod.filter(o=>o.ocr_method==="pypdf").length/withMethod.length:0;
  const bars=[{l:"19",v:42},{l:"20",v:38},{l:"21",v:15},{l:"22",v:28},{l:"23",v:33},{l:"24",v:45},{l:"25",v:12}];
  const srcStats=ss.map(s=>{const so=oo.filter(o=>o.source_code===s.code);const f=so.filter(o=>o.status==="failed").length;return{code:s.code,risk:s.risk_class,total:so.length,failed:f,pct:so.length?f/so.length:0,records:so.reduce((a,o)=>a+(o.record_count||0),0)}}).sort((a,b)=>b.pct-a.pct);

  return<div>
    <h2 style={{fontSize:17,fontWeight:600,fontFamily:F.s,color:C.tx,margin:"0 0 16px"}}>Метрики и KPI</h2>
    <div style={{display:"flex",gap:10,flexWrap:"wrap",marginBottom:18}}>
      <KPI label="Записей извлечено" value={totalRec} target={5000} unit=" зап."/>
      <KPI label="OCR Confidence" value={+(avgConf*100).toFixed(1)} target={85} unit="%"/>
      <KPI label="Fail Rate" value={+(failRate*100).toFixed(1)} target={5} unit="%" good={failRate<0.1}/>
      <KPI label="PyPDF (без OCR)" value={+(pypdfPct*100).toFixed(0)} target={70} unit="%"/>
    </div>
    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12,marginBottom:18}}>
      <Cd title="Throughput (приказов/день)"><MiniBar data={bars} maxH={50}/><div style={{display:"flex",justifyContent:"space-between",marginTop:4}}>{bars.map((b,i)=><span key={i} style={{fontSize:8,color:C.txM,fontFamily:F.m}}>{b.l}</span>)}</div><div style={{marginTop:8,fontSize:11,color:C.txD}}>~8.8с/стр × {totalPg} стр = {totalPg>0?Math.round(totalPg*8.8)+"с":"—"}</div></Cd>
      <Cd title="OCR Performance"><div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}><div><div style={{fontSize:9,fontFamily:F.m,color:C.txM}}>СТРАНИЦ</div><div style={{fontSize:18,fontWeight:700,fontFamily:F.m,color:C.tx}}>{totalPg}</div></div><div><div style={{fontSize:9,fontFamily:F.m,color:C.txM}}>CONFIDENCE</div><div style={{fontSize:18,fontWeight:700,fontFamily:F.m,color:avgConf>=0.8?C.ok:C.wn}}>{fm.p(avgConf)}</div></div><div><div style={{fontSize:9,fontFamily:F.m,color:C.txM}}>PYPDF</div><div style={{fontSize:14,fontWeight:600,fontFamily:F.m,color:C.ok}}>{fm.p(pypdfPct)}</div></div><div><div style={{fontSize:9,fontFamily:F.m,color:C.txM}}>TESSERACT</div><div style={{fontSize:14,fontWeight:600,fontFamily:F.m,color:C.wn}}>{fm.p(1-pypdfPct)}</div></div></div><div style={{marginTop:8,fontSize:10,color:C.txM}}>MAX_PDF_PAGES: 200 · MAX_PDF_SIZE: 50MB</div></Cd>
    </div>
    <Cd title="Error Rate по источникам"><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr>{["","Источник","Всего","Ошибки","Rate","Записей"].map((h,i)=><th key={i} style={{textAlign:i>=2?"right":"left",padding:"6px 8px",color:C.txM,fontSize:9,fontFamily:F.m,borderBottom:`1px solid ${C.bd}`}}>{h}</th>)}</tr></thead><tbody>{srcStats.map((s,i)=><tr key={i} style={{borderBottom:`1px solid ${C.bd}`}}><td style={{padding:"6px 8px"}}><RB r={s.risk}/></td><td style={{padding:"6px 8px",fontFamily:F.m,fontSize:11}}>{s.code}</td><td style={{padding:"6px 8px",textAlign:"right",fontFamily:F.m}}>{s.total}</td><td style={{padding:"6px 8px",textAlign:"right",fontFamily:F.m,color:s.failed?C.er:C.txD}}>{s.failed}</td><td style={{padding:"6px 8px",textAlign:"right"}}><Bd color={s.pct>0.1?C.er:s.pct>0?C.wn:C.ok} bg={s.pct>0.1?C.erB:s.pct>0?C.wnB:C.okB}>{(s.pct*100).toFixed(1)}%</Bd></td><td style={{padding:"6px 8px",textAlign:"right",fontFamily:F.m,color:C.ok}}>{fm.n(s.records)}</td></tr>)}</tbody></table></Cd>
    <Cd title="SLO Targets"><div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:10}}>{[{l:"API p95",v:"< 500ms",ok:true},{l:"OCR throughput",v:"10 doc/min ×4w",ok:true},{l:"Backlog",v:oo.filter(o=>o.status==="new").length+" в очереди",ok:oo.filter(o=>o.status==="new").length<10},{l:"Fail rate",v:(failRate*100).toFixed(1)+"%",ok:failRate<0.1},{l:"Confidence",v:fm.p(avgConf),ok:avgConf>=0.6},{l:"Exchange",v:approved.length+" приказов",ok:approved.length>0}].map((s,i)=><div key={i} style={{padding:"8px 10px",background:C.bg3,borderRadius:6,border:`1px solid ${s.ok?C.ok:C.er}22`}}><div style={{fontSize:9,fontFamily:F.m,color:C.txM}}>{s.l}</div><div style={{fontSize:12,fontWeight:600,color:s.ok?C.ok:C.er,fontFamily:F.m,marginTop:2}}>{s.v}</div></div>)}</div></Cd>
  </div>;
};

const PgExchange=()=>{
  const[sel,setSel]=useState(0);const ex=EXCHANGE_PREVIEW[sel];
  return<div>
    <h2 style={{fontSize:17,fontWeight:600,fontFamily:F.s,color:C.tx,margin:"0 0 6px"}}>Обменный формат</h2>
    <div style={{fontSize:11,color:C.txD,marginBottom:14}}>JSON для внешней ИС-потребителя. <span style={{fontFamily:F.m,color:C.ac}}>GET /api/exchange/orders</span></div>
    <div style={{display:"flex",gap:4,marginBottom:12}}>{EXCHANGE_PREVIEW.map((e,i)=><button key={i} onClick={()=>setSel(i)} style={{background:sel===i?C.bg3:C.bg2,color:sel===i?C.tx:C.txD,border:`1px solid ${sel===i?C.ac:C.bd}`,borderRadius:5,padding:"5px 12px",fontSize:11,fontFamily:F.m,cursor:"pointer"}}>{e.order}</button>)}</div>
    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
      <Cd title="JSON Response"><pre style={{background:C.bg,padding:12,borderRadius:6,overflow:"auto",maxHeight:400,fontSize:11,fontFamily:F.m,color:C.tx,lineHeight:1.5,margin:0,border:`1px solid ${C.bd}`,whiteSpace:"pre-wrap"}}>{JSON.stringify(ex,null,2)}</pre></Cd>
      <div>
        <Cd title="Заголовок"><div style={{display:"grid",gridTemplateColumns:"auto 1fr",gap:"4px 12px",fontSize:12}}>{[["order",ex.order],["issueDate",ex.issueDate],["issuedBy",ex.issuedBy+" (ИНН)"],["signedBy",ex.signedBy],["assignment",ex.assignment?"true":"false"]].map(([k,v])=>[<span key={k} style={{fontFamily:F.m,color:C.ac,fontSize:11}}>{k}</span>,<span key={k+"v"} style={{color:C.tx}}>{String(v)}</span>])}</div></Cd>
        <Cd title={`items (${ex.items.length})`}><table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}><thead><tr>{["lastName","firstName","middleName","birthDate","sport","rank"].map(h=><th key={h} style={{textAlign:"left",padding:"4px 6px",color:C.ac,fontFamily:F.m,fontSize:9,borderBottom:`1px solid ${C.bd}`}}>{h}</th>)}</tr></thead><tbody>{ex.items.map((it,i)=><tr key={i} style={{borderBottom:`1px solid ${C.bd}`}}><td style={{padding:"4px 6px",fontWeight:600}}>{it.lastName}</td><td style={{padding:"4px 6px"}}>{it.firstName}</td><td style={{padding:"4px 6px",color:C.txD}}>{it.middleName}</td><td style={{padding:"4px 6px",fontFamily:F.m,fontSize:10}}>{it.birthDate}</td><td style={{padding:"4px 6px"}}>{it.sport}</td><td style={{padding:"4px 6px"}}><Bd color={C.ok} bg={C.okB}>{it.rank}</Bd></td></tr>)}</tbody></table></Cd>
        <Cd title="Спецификация"><div style={{fontSize:10,color:C.txD,lineHeight:1.6}}><div><span style={{fontFamily:F.m,color:C.ac}}>issuedBy</span> — ИНН (BIGINT)</div><div><span style={{fontFamily:F.m,color:C.ac}}>file</span> — Base64 PDF (null в списке, заполняется в /exchange/orders/{"{id}"})</div><div><span style={{fontFamily:F.m,color:C.ac}}>birthDate</span> — ISO 8601 (YYYY-MM-DD)</div><div><span style={{fontFamily:F.m,color:C.ac}}>assignment</span> — true если action=assignment</div></div></Cd>
      </div>
    </div>
  </div>;
};

const BLANK={code:"",name:"",region:"",federal_subject:"",source_type:"pdf_portal",risk_class:"green",active:false,official_basis:"",download:{method:"httpx",base_url:"",antibot:"",delay_min:1,delay_max:3,wait_selector:""},detect:{list_urls:[""],link_regex:'href=["\']([^"\']*\\.pdf)["\']',title_regex:"",order_date_regex:"",order_number_regex:"",pagination:"",max_pages:1,js_var:""},meta:{issuing_body:"",order_type:"приказ"}};

const PgForm=({source,isNew,onSave,onCancel,onDelete})=>{
  const[f,setF]=useState(()=>JSON.parse(JSON.stringify(isNew?BLANK:source)));
  const[step,setStep]=useState(0);const[rx,setRx]=useState({html:"",res:null});const[cfm,setCfm]=useState(false);
  const set=(k,v)=>setF(p=>({...p,[k]:v}));const sDl=(k,v)=>setF(p=>({...p,download:{...p.download,[k]:v}}));const sDt=(k,v)=>setF(p=>({...p,detect:{...p.detect,[k]:v}}));const sMt=(k,v)=>setF(p=>({...p,meta:{...p.meta,[k]:v}}));const sU=(i,v)=>{const u=[...f.detect.list_urls];u[i]=v;sDt("list_urls",u)};
  const testRx=()=>{try{const re=new RegExp(f.detect.link_regex,"gi");const m=[];let x;while((x=re.exec(rx.html))!==null&&m.length<20){const r=x[1]||x[0];let rv=r;try{rv=new URL(r,f.download.base_url||"https://example.com").href}catch{}m.push({raw:r,rv})}setRx(p=>({...p,res:m}))}catch(e){setRx(p=>({...p,res:[{raw:`Err: ${e.message}`,rv:""}]}))}};
  const valid=f.code&&f.name&&f.region&&f.meta.issuing_body&&f.detect.list_urls.some(u=>u);
  const steps=["Основные","Загрузка","Обнаружение","Мета","Проверка"];
  return<div>
    {cfm&&<Cfm msg={`Удалить «${f.code}»?`} onYes={()=>{setCfm(false);onDelete(f.code)}} onNo={()=>setCfm(false)}/>}
    <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:18}}><button onClick={onCancel} style={{background:"none",border:"none",color:C.ac,fontFamily:F.m,fontSize:11,cursor:"pointer"}}>← Назад</button><h2 style={{fontSize:17,fontWeight:600,fontFamily:F.s,color:C.tx,margin:0,flex:1}}>{isNew?"Новый источник":f.code}</h2>{!isNew&&<Bt sm outline danger onClick={()=>setCfm(true)}>Удалить</Bt>}</div>
    <div style={{display:"flex",gap:3,marginBottom:18}}>{steps.map((s,i)=><button key={i} onClick={()=>setStep(i)} style={{flex:1,padding:"6px 3px",background:step===i?C.bg3:C.bg2,border:`1px solid ${step===i?C.ac:C.bd}`,borderRadius:5,cursor:"pointer",textAlign:"center"}}><div style={{fontSize:11,fontWeight:700,color:step===i?C.ac:C.txM,fontFamily:F.m}}>{"①②③④⑤"[i]}</div><div style={{fontSize:8,color:step===i?C.tx:C.txM,fontFamily:F.m}}>{s}</div></button>)}</div>
    {step===0&&<Cd title="Основные данные"><div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"0 14px"}}><In label="Код" value={f.code} onChange={v=>set("code",v.toLowerCase().replace(/[^a-zа-яё0-9_]/gi,"_"))} ph="novosibirsk_minsport" mono disabled={!isNew}/><In label="Название" value={f.name} onChange={v=>set("name",v)}/><In label="Регион" value={f.region} onChange={v=>set("region",v)}/><In label="Субъект" value={f.federal_subject} onChange={v=>set("federal_subject",v)} ph="54"/><Sl label="Тип" value={f.source_type} onChange={v=>set("source_type",v)} opts={[{v:"pdf_portal",l:"PDF"},{v:"json_embed",l:"JSON"},{v:"html_table",l:"HTML"}]}/><Sl label="Риск" value={f.risk_class} onChange={v=>set("risk_class",v)} opts={[{v:"green",l:"🟢 Green"},{v:"amber",l:"🟡 Amber"},{v:"red",l:"🔴 Red"}]}/></div><In label="Основание" value={f.official_basis} onChange={v=>set("official_basis",v)}/><Tg label="Активен" value={f.active} onChange={v=>set("active",v)}/></Cd>}
    {step===1&&<Cd title="Загрузка"><Sl label="Метод" value={f.download.method} onChange={v=>sDl("method",v)} opts={[{v:"httpx",l:"httpx"},{v:"playwright",l:"Playwright"}]}/><In label="Базовый URL" value={f.download.base_url} onChange={v=>sDl("base_url",v)} mono/>{f.download.method==="playwright"&&<><Sl label="Антибот" value={f.download.antibot} onChange={v=>sDl("antibot",v)} opts={[{v:"",l:"Нет"},{v:"servicepipe",l:"Servicepipe"},{v:"cloudflare",l:"Cloudflare"}]}/><In label="Wait selector" value={f.download.wait_selector} onChange={v=>sDl("wait_selector",v)} mono/></>}<div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"0 14px"}}><In label="Мин. задержка" value={f.download.delay_min} onChange={v=>sDl("delay_min",+v||0)}/><In label="Макс. задержка" value={f.download.delay_max} onChange={v=>sDl("delay_max",+v||0)}/></div></Cd>}
    {step===2&&<Cd title="Обнаружение"><div style={{marginBottom:10}}><label style={{display:"block",fontSize:9,fontFamily:F.m,color:C.txM,textTransform:"uppercase",letterSpacing:1.2,marginBottom:5}}>URL страниц</label>{f.detect.list_urls.map((u,i)=><div key={i} style={{display:"flex",gap:5,marginBottom:4}}><input value={u} onChange={e=>sU(i,e.target.value)} placeholder="https://..." style={{flex:1,padding:"7px 9px",background:C.bg1,border:`1px solid ${C.bd}`,borderRadius:5,color:C.tx,fontFamily:F.m,fontSize:11,outline:"none"}}/>{f.detect.list_urls.length>1&&<Bt sm outline color={C.er} onClick={()=>sDt("list_urls",f.detect.list_urls.filter((_,j)=>j!==i))}>✕</Bt>}</div>)}<Bt sm outline onClick={()=>sDt("list_urls",[...f.detect.list_urls,""])}>+</Bt></div>{f.source_type==="json_embed"?<In label="JS var" value={f.detect.js_var} onChange={v=>sDt("js_var",v)} mono/>:<><In label="Link regex" value={f.detect.link_regex} onChange={v=>sDt("link_regex",v)} mono/><div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"0 14px"}}><In label="Pagination" value={f.detect.pagination} onChange={v=>sDt("pagination",v)} mono/><In label="Max pages" value={f.detect.max_pages} onChange={v=>sDt("max_pages",+v||1)}/></div><div style={{marginTop:14,padding:12,background:C.bg,borderRadius:6,border:`1px solid ${C.bd}`}}><div style={{fontSize:10,fontFamily:F.m,color:C.ac,marginBottom:8}}>🧪 Regex тестер</div><In label="HTML" value={rx.html} onChange={v=>setRx(p=>({...p,html:v,res:null}))} area mono ph={'<a href="/d/1.pdf">Doc</a>'}/><Bt sm onClick={testRx}>▶ Тест</Bt>{rx.res&&<div style={{marginTop:6}}><div style={{fontSize:10,fontFamily:F.m,color:rx.res.length?C.ok:C.er}}>{rx.res.length||0} совпадений</div>{rx.res.slice(0,10).map((r,i)=><div key={i} style={{fontSize:10,fontFamily:F.m,color:C.txD,padding:"2px 0"}}>{r.raw}{r.rv!==r.raw&&<span style={{color:C.ac,marginLeft:6}}>→ {r.rv}</span>}</div>)}</div>}</div></>}</Cd>}
    {step===3&&<Cd title="Метаданные"><In label="Издающий орган" value={f.meta.issuing_body} onChange={v=>sMt("issuing_body",v)}/><Sl label="Тип документа" value={f.meta.order_type} onChange={v=>sMt("order_type",v)} opts={[{v:"приказ",l:"Приказ"},{v:"распоряжение",l:"Распоряжение"}]}/></Cd>}
    {step===4&&<div><Cd title="Сводка"><div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:14}}><div><div style={{fontSize:9,fontFamily:F.m,color:C.txM}}>ИСТОЧНИК</div><div style={{fontFamily:F.m,fontSize:13,fontWeight:600}}>{f.code||"—"}</div><div style={{fontSize:12,color:C.txD}}>{f.name}</div><RB r={f.risk_class}/></div><div><div style={{fontSize:9,fontFamily:F.m,color:C.txM}}>ЗАГРУЗКА</div><div style={{fontSize:11,fontFamily:F.m}}>{f.download.method} · {f.download.base_url||"—"}</div></div></div></Cd><Cd title="Валидация">{[{ok:!!f.code,m:"Код"},{ok:!!f.name,m:"Название"},{ok:!!f.region,m:"Регион"},{ok:f.detect.list_urls.some(u=>u),m:"URL"},{ok:!!f.download.base_url,m:"Base URL"},{ok:!!f.meta.issuing_body,m:"Издающий орган"}].map((v,i)=><div key={i} style={{display:"flex",alignItems:"center",gap:7,padding:"3px 0",fontSize:11}}><span style={{color:v.ok?C.ok:C.er}}>{v.ok?"✓":"✗"}</span><span style={{color:v.ok?C.tx:C.er}}>{v.m}</span></div>)}</Cd></div>}
    <div style={{display:"flex",justifyContent:"space-between",marginTop:14}}><div>{step>0&&<Bt outline onClick={()=>setStep(step-1)}>←</Bt>}</div><div style={{display:"flex",gap:6}}>{step<4&&<Bt onClick={()=>setStep(step+1)}>→</Bt>}{step===4&&<Bt disabled={!valid} color={C.ok} onClick={()=>onSave(f)}>{isNew?"✓ Создать":"✓ Сохранить"}</Bt>}</div></div>
  </div>;
};

const PgSrc=({sources:ss,onEdit,onAdd,onToggle,onCheck})=><div>
  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:14}}><h2 style={{fontSize:17,fontWeight:600,fontFamily:F.s,color:C.tx,margin:0}}>Источники ({ss.length})</h2><div style={{display:"flex",gap:6}}><Bt onClick={onAdd}>+ Новый</Bt><Bt outline onClick={onCheck}>▶ Проверить</Bt></div></div>
  <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><thead><tr>{["","Код","Название","Метод","","Приказы","Записи",""].map((h,i)=><th key={i} style={{textAlign:i>=5?"right":"left",padding:"8px",color:C.txM,fontSize:9,fontFamily:F.m,borderBottom:`1px solid ${C.bd}`}}>{h}</th>)}</tr></thead><tbody>{ss.map((s,i)=><tr key={i} style={{borderBottom:`1px solid ${C.bd}`,cursor:"pointer"}} onMouseEnter={e=>e.currentTarget.style.background=C.bgH} onMouseLeave={e=>e.currentTarget.style.background="transparent"} onClick={()=>onEdit(s)}><td style={{padding:8}}><RB r={s.risk_class}/></td><td style={{padding:8,fontFamily:F.m,fontSize:11}}>{s.code}</td><td style={{padding:8,maxWidth:180,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{s.name}</td><td style={{padding:8}}><Bd color={C.tx}>{s.download?.method}</Bd></td><td style={{padding:8}}><span onClick={e=>{e.stopPropagation();onToggle(s.code)}} style={{cursor:"pointer"}}><Bd color={s.active?C.ok:C.txM} bg={s.active?C.okB:undefined}>{s.active?"On":"Off"}</Bd></span></td><td style={{padding:8,textAlign:"right",fontFamily:F.m,fontSize:11}}><span style={{color:C.ok}}>{s.orders_ok||0}</span>/<span style={{color:C.er}}>{s.orders_failed||0}</span></td><td style={{padding:8,textAlign:"right",fontFamily:F.m}}>{fm.n(s.total_assignments||0)}</td><td style={{padding:8}}><Bt sm outline onClick={e=>{e.stopPropagation();onEdit(s)}}>✎</Bt></td></tr>)}</tbody></table>
</div>;

const PgOrd=({orders:oo})=>{const[fl,setFl]=useState("all");const sh=fl==="all"?oo:oo.filter(o=>o.status===fl);return<div><h2 style={{fontSize:17,fontWeight:600,fontFamily:F.s,color:C.tx,margin:"0 0 14px"}}>Приказы</h2><div style={{display:"flex",gap:4,marginBottom:12}}>{["all","new","extracted","approved","failed"].map(f=><button key={f} onClick={()=>setFl(f)} style={{background:fl===f?C.bg3:C.bg2,color:fl===f?C.tx:C.txD,border:`1px solid ${fl===f?C.ac:C.bd}`,borderRadius:5,padding:"4px 10px",fontSize:10,fontFamily:F.m,cursor:"pointer"}}>{f==="all"?`Все (${oo.length})`:`${(STAT[f]||{}).l} (${oo.filter(o=>o.status===f).length})`}</button>)}</div>{sh.map((o,i)=><div key={i} style={{display:"flex",alignItems:"center",gap:10,padding:"8px 10px",borderBottom:`1px solid ${C.bd}`}}><Bd color={(STAT[o.status]||{}).c} bg={(STAT[o.status]||{}).b}>{(STAT[o.status]||{}).l}</Bd><span style={{fontFamily:F.m,fontWeight:600,color:C.ac,minWidth:90}}>{o.order_number}</span><span style={{fontFamily:F.m,fontSize:10,color:C.txD,minWidth:70}}>{fm.d(o.order_date)}</span><span style={{color:C.txD,flex:1,fontSize:11,fontFamily:F.m}}>{o.source_code}</span>{o.ocr_confidence!=null&&<span style={{fontFamily:F.m,fontSize:10,color:o.ocr_confidence>=0.8?C.ok:o.ocr_confidence>=0.6?C.wn:C.er}}>{fm.p(o.ocr_confidence)}</span>}<span style={{fontFamily:F.m,fontSize:11,color:o.record_count?C.ok:C.txM}}>{o.record_count||"—"}</span></div>)}</div>};

const PgLog=({logs:ll})=><div><h2 style={{fontSize:17,fontWeight:600,fontFamily:F.s,color:C.tx,margin:"0 0 12px"}}>Журнал</h2>{ll.map((l,i)=><div key={i} style={{display:"flex",gap:6,padding:"6px 8px",borderBottom:`1px solid ${C.bd}`,fontFamily:F.m,fontSize:11,background:l.lvl==="error"?C.erB:"transparent"}}><span style={{color:l.lvl==="error"?C.er:l.lvl==="warn"?C.wn:C.ok,fontSize:9,marginTop:2}}>●</span><span style={{color:C.txM,minWidth:80,fontSize:9}}>{fm.t(l.t)}</span><span style={{color:C.txD,minWidth:60,fontSize:10}}>{l.src}</span><Bd color={C.tx}>{l.stage}</Bd><span style={{color:C.tx,flex:1,fontFamily:F.s,fontSize:12}}>{l.msg}</span></div>)}</div>;

// ═══ APP ═══
const NAV=[{id:"dashboard",l:"Обзор",i:"◉"},{id:"sources",l:"Источники",i:"⊕"},{id:"orders",l:"Приказы",i:"📋"},{id:"metrics",l:"Метрики",i:"◈"},{id:"exchange",l:"Exchange",i:"⇄"},{id:"logs",l:"Журнал",i:"⫶"}];

export default function App(){
  const[st,dispatch]=useReducer(reducer,{sources:SRC,orders:ORD,logs:LOG});
  const[pg,setPg]=useState("metrics");const[eSrc,setESrc]=useState(null);const[isN,setIsN]=useState(false);const[toast,setToast]=useState(null);
  const notify=(msg,ok=true)=>{setToast({msg,ok});setTimeout(()=>setToast(null),3500)};
  const goEdit=s=>{setESrc(s);setIsN(false);setPg("form")};const goNew=()=>{setESrc(null);setIsN(true);setPg("form")};const goBack=()=>setPg("sources");
  const handleSave=f=>{if(isN){if(st.sources.some(s=>s.code===f.code)){notify(`«${f.code}» уже есть`,false);return}dispatch({type:"ADD_SRC",p:{...f,orders_ok:0,orders_pending:0,orders_failed:0,total_assignments:0}});notify(`«${f.code}» создан`)}else{dispatch({type:"UPD_SRC",code:f.code,p:f});notify(`«${f.code}» обновлён`)}setPg("sources")};
  const handleDel=code=>{const s=st.sources.find(x=>x.code===code);if(s&&(s.orders_ok||s.orders_pending)){notify(`Есть приказы — деактивируйте`,false);return}dispatch({type:"DEL_SRC",code});notify(`Удалён`);setPg("sources")};
  const handleTog=code=>{dispatch({type:"TOG_SRC",code});notify(`Toggle ${code}`)};
  const handleCheck=()=>{dispatch({type:"LOG",p:{lvl:"info",stage:"scheduler",src:"system",msg:`check-all: ${st.sources.filter(s=>s.active).length} src`,t:new Date().toISOString()}});notify("Проверка запущена")};

  return<div style={{display:"flex",height:"100vh",background:C.bg,fontFamily:F.s,color:C.tx}}>
    <style>{`@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap');@keyframes fadeIn{from{opacity:0;transform:translateY(-8px)}to{opacity:1;transform:translateY(0)}}*{scrollbar-width:thin;scrollbar-color:${C.bd} transparent}::-webkit-scrollbar{width:6px}::-webkit-scrollbar-thumb{background:${C.bd};border-radius:3px}`}</style>
    <Toast toast={toast}/>
    <div style={{width:170,background:C.bg1,borderRight:`1px solid ${C.bd}`,display:"flex",flexDirection:"column",flexShrink:0}}>
      <div style={{padding:"16px 14px 18px",borderBottom:`1px solid ${C.bd}`}}><div style={{fontFamily:F.m,fontSize:13,fontWeight:700,letterSpacing:1}}><span style={{color:C.ac}}>SPORT</span>RANK</div><div style={{fontFamily:F.m,fontSize:7,color:C.txM,marginTop:2,letterSpacing:2}}>ADMIN v3.1</div></div>
      <nav style={{padding:"8px 5px",flex:1}}>{NAV.map(n=>{const a=pg===n.id||(n.id==="sources"&&pg==="form");return<button key={n.id} onClick={()=>{setPg(n.id);setESrc(null)}} style={{display:"flex",alignItems:"center",gap:7,width:"100%",padding:"7px 9px",background:a?C.bg3:"transparent",color:a?C.tx:C.txD,border:"none",borderRadius:4,cursor:"pointer",fontFamily:F.s,fontSize:12,fontWeight:a?600:400,textAlign:"left",borderLeft:a?`2px solid ${C.ac}`:"2px solid transparent",marginBottom:1}}><span style={{fontSize:12,opacity:a?1:.35,width:16,textAlign:"center"}}>{n.i}</span>{n.l}</button>})}</nav>
      <div style={{padding:"10px 14px",borderTop:`1px solid ${C.bd}`,fontSize:8,fontFamily:F.m,color:C.txM}}><div style={{display:"flex",alignItems:"center",gap:4}}><div style={{width:6,height:6,borderRadius:3,background:C.ok}}/> OK</div><div style={{marginTop:3}}>{st.sources.filter(s=>s.active).length} src · {fm.n(st.sources.reduce((a,s)=>a+(s.total_assignments||0),0))} rec</div></div>
    </div>
    <div style={{flex:1,overflow:"auto",padding:22}}>
      {pg==="dashboard"&&<PgDash sources={st.sources} orders={st.orders} logs={st.logs}/>}
      {pg==="sources"&&<PgSrc sources={st.sources} onEdit={goEdit} onAdd={goNew} onToggle={handleTog} onCheck={handleCheck}/>}
      {pg==="form"&&<PgForm source={eSrc} isNew={isN} onSave={handleSave} onCancel={goBack} onDelete={handleDel}/>}
      {pg==="orders"&&<PgOrd orders={st.orders}/>}
      {pg==="metrics"&&<PgMetrics sources={st.sources} orders={st.orders}/>}
      {pg==="exchange"&&<PgExchange/>}
      {pg==="logs"&&<PgLog logs={st.logs}/>}
    </div>
  </div>;
}
