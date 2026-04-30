"""
Dr. Amanda / Método ASF Dashboard — geração + deploy automático.
Roda: python generate_dr_amanda.py

Método ASF · Clínica de Estética
Conversão: WhatsApp (onsite_conversion.total_messaging_connection)
Moeda: BRL (R$)
"""
import json, os, re, subprocess, shutil
from collections import defaultdict
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# ══════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════
CLIENTE_NOME  = "Dr. Amanda"
CLIENTE_SUB   = "Método ASF · Clínica de Estética"
CF_SUBFOLDER  = "dr-amanda"

HAS_GOOGLE    = False
HAS_ROAS      = False

META_ACT      = "act_1286752865945018"
CLIENTE_SLUG  = "Dr Amanda"

CONV_ACTION   = "onsite_conversion.total_messaging_connection"
CONV_LABEL    = "Conversas WhatsApp"
CONV_UNIT     = "conversas iniciadas"
CONV_CPL_LBL  = "Custo / Conversa"
CONV_CPL_UNIT = "por conversa"

CURRENCY_SYM  = "R$"

META_CPA_BOM  = 15.0
META_CPA_OK   = 30.0
GOOGLE_CPA_BOM = 999.0
GOOGLE_CPA_OK  = 999.0
# ══════════════════════════════════════════════════════════════════════

CF_TOKEN   = os.getenv("CF_TOKEN")
CF_ACCOUNT = os.getenv("CF_ACCOUNT")
CF_PROJECT = os.getenv("CF_PROJECT")
DEPLOY_DIR = os.path.dirname(os.path.abspath(__file__))

GADS_ICON = '<svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" width="18" height="18"><path fill="#34A853" d="M3.9998 22.9291C1.7908 22.9291 0 21.1383 0 18.9293s1.7908-3.9998 3.9998-3.9998 3.9998 1.7908 3.9998 3.9998-1.7908 3.9998-3.9998 3.9998z"/><path fill="#4285F4" d="M23.4641 16.9287L15.4632 3.072C14.3586 1.1587 11.9121.5028 9.9988 1.6074S7.4295 5.1585 8.5341 7.0718l8.0009 13.8567c1.1046 1.9133 3.5511 2.5679 5.4644 1.4646 1.9134-1.1046 2.568-3.5511 1.4647-5.4644z"/><path fill="#FBBC04" d="M7.5137 4.8438L1.5645 15.1484A4.5 4.5 0 0 1 4 14.4297c2.5597-.0075 4.6248 2.1585 4.4941 4.7148l3.2168-5.5723-3.6094-6.25c-.4499-.7793-.6322-1.6394-.5878-2.4784z"/></svg>'

today         = date.today()
yesterday     = today - timedelta(days=1)
today_str     = today.strftime("%Y-%m-%d")
yesterday_str = yesterday.strftime("%Y-%m-%d")

# ══════════════════════════════════════════════════════════════════════
# UTILITÁRIOS
# ══════════════════════════════════════════════════════════════════════
_NOISE = {"site antigo", "site", "antigo", "novo teste", "teste"}

def clean_camp(raw):
    if not raw: return "Campanha"
    brackets = re.findall(r'\[([^\]]+)\]', raw)
    useful = [b.strip() for b in brackets if b.strip().lower() not in _NOISE]
    if useful: return " / ".join(useful)
    n = re.sub(r'\[.*?\]|\d{2}/\d{2}/\d{4}|#\d+', '', raw)
    n = re.sub(r'\s{2,}', ' ', n).strip(' -').strip()
    n = re.sub(rf'^{re.escape(CLIENTE_SLUG)}\s*[-–\s]*', '', n, flags=re.IGNORECASE).strip(' -').strip()
    return n or raw[:35]

_desc_map = {
    "RMKT":        "Remarketing — quem ja viu",
    "REMARKETING": "Remarketing — quem ja viu",
    "CONVERSAO":   "Conversao — objetivo WhatsApp",
    "CONVERSÃO":   "Conversao — objetivo WhatsApp",
    "DISTRIBUICAO":"Alcance para novo publico",
    "DISTRIBUIÇÃO":"Alcance para novo publico",
    "ASF":         "Método ASF — produto principal",
    "ESTETICA":    "Estetica facial",
}

# ══════════════════════════════════════════════════════════════════════
# 1. WINDSOR — carregar JSON gerado pelo Claude via MCP
# ══════════════════════════════════════════════════════════════════════
windsor_file = os.path.join(DEPLOY_DIR, f"windsor_{CF_SUBFOLDER}.json")
if not os.path.exists(windsor_file):
    raise FileNotFoundError(
        f"\nArquivo de dados nao encontrado: {windsor_file}\n"
        f"Peca ao Claude: 'busca dados do Windsor para o {CLIENTE_NOME} e gera o dashboard'\n"
    )

def fix_encoding(s):
    if not isinstance(s, str): return s
    try: return s.encode('windows-1252').decode('utf-8')
    except: return s
def fix_dict(obj):
    if isinstance(obj, str): return fix_encoding(obj)
    elif isinstance(obj, dict): return {k: fix_dict(v) for k, v in obj.items()}
    elif isinstance(obj, list): return [fix_dict(i) for i in obj]
    return obj

with open(windsor_file, "r", encoding="utf-8") as f:
    windsor = fix_dict(json.load(f))

meta_rows   = windsor.get("meta_rows", [])
google_rows = windsor.get("google_rows", [])
print(f"Windsor: {len(meta_rows)} linhas Meta | {len(google_rows)} linhas Google")

# Mapeamento CONV_ACTION (Meta) -> campo Windsor
_WINDSOR_CONV = {
    "onsite_conversion.total_messaging_connection": "actions_onsite_conversion_messaging_conversation_started_7d",
    "link_click":   "actions_link_click",
    "lead":         "actions_lead",
    "purchase":     "actions_purchase",
}
WINDSOR_CONV_FIELD = _WINDSOR_CONV.get(CONV_ACTION, "actions_lead")

# ══════════════════════════════════════════════════════════════════════
# 2. META — transformar rows Windsor -> META, CAMP_META, CAMP_DAILY
# ══════════════════════════════════════════════════════════════════════
META            = []
CAMP_META_INFO  = {}
CAMP_DAILY_LIST = []

daily_m = defaultdict(lambda: {"s":0.0,"r":0,"i":0,"c":0,"lk":0,"rv":0.0,"v25":0,"v50":0,"v75":0,"v95":0,"msg":0})

for row in meta_rows:
    d   = row.get("date", "")
    if not d: continue
    cid = row.get("campaign_name", "—")
    s   = float(row.get("spend") or 0)
    r   = int(row.get("reach") or 0)
    i   = int(row.get("impressions") or 0)
    lk  = int(row.get("actions_link_click") or 0)
    c   = int(row.get(WINDSOR_CONV_FIELD) or 0)
    msg = int(row.get("actions_onsite_conversion_messaging_conversation_started_7d") or 0)
    v25 = int(row.get("video_p25_watched_actions_video_view") or 0)
    v50 = int(row.get("video_p50_watched_actions_video_view") or 0)
    v75 = int(row.get("video_p75_watched_actions_video_view") or 0)
    v95 = int(row.get("video_p95_watched_actions_video_view") or 0)

    daily_m[d]["s"]   += s
    daily_m[d]["r"]   += r
    daily_m[d]["i"]   += i
    daily_m[d]["lk"]  += lk
    daily_m[d]["c"]   += c
    daily_m[d]["msg"] += msg
    daily_m[d]["v25"] += v25
    daily_m[d]["v50"] += v50
    daily_m[d]["v75"] += v75
    daily_m[d]["v95"] += v95

    if cid not in CAMP_META_INFO:
        desc = next((v for k, v in _desc_map.items() if cid and k in cid.upper()), "Campanha ativa")
        CAMP_META_INFO[cid] = {"nome": clean_camp(cid), "desc": desc}

    CAMP_DAILY_LIST.append({"d": d, "id": cid, "s": s, "r": r, "lk": lk, "c": c, "rv": 0.0})

META = [
    (d, v["s"], v["r"], v["i"], v["c"], v["lk"], v["rv"],
     v["v25"], v["v50"], v["v75"], v["v95"], v["msg"])
    for d, v in sorted(daily_m.items()) if v["s"] > 0 or v["i"] > 0
]
meta_last = META[-1][0] if META else today_str
print(f"  Meta: {len(META)} dias | ultimo: {meta_last} | {len(CAMP_META_INFO)} campanhas")

# AD_DRILL — nivel de anuncio com link do post
ad_rows = windsor.get("ad_rows", [])
AD_DRILL = {}
for row in ad_rows:
    cn  = row.get("campaign_name", "—")
    an  = row.get("adset_name", "—")
    adn = row.get("ad_name", "—")
    s   = float(row.get("spend") or 0)
    c   = int(row.get(WINDSOR_CONV_FIELD) or 0)
    sid = row.get("effective_object_story_id", "") or ""
    post_link = ""
    if sid and "_" in sid:
        parts = sid.split("_", 1)
        post_link = f"https://www.facebook.com/permalink.php?story_fbid={parts[1]}&id={parts[0]}"
    thu = row.get("thumbnail_url", "") or ""
    if cn not in AD_DRILL: AD_DRILL[cn] = {}
    if an not in AD_DRILL[cn]:
        AD_DRILL[cn][an] = {"name": an, "spend": 0.0, "conv": 0, "ads": {}}
    AD_DRILL[cn][an]["spend"] += s
    AD_DRILL[cn][an]["conv"]  += c
    if adn not in AD_DRILL[cn][an]["ads"]:
        AD_DRILL[cn][an]["ads"][adn] = {"name": adn, "spend": 0.0, "conv": 0, "thumb": thu, "link": post_link}
    AD_DRILL[cn][an]["ads"][adn]["spend"] += s
    AD_DRILL[cn][an]["ads"][adn]["conv"]  += c
    if thu and not AD_DRILL[cn][an]["ads"][adn]["thumb"]:
        AD_DRILL[cn][an]["ads"][adn]["thumb"] = thu
    if post_link and not AD_DRILL[cn][an]["ads"][adn]["link"]:
        AD_DRILL[cn][an]["ads"][adn]["link"] = post_link
for cn in AD_DRILL:
    for an in AD_DRILL[cn]:
        AD_DRILL[cn][an]["ads"] = sorted(
            AD_DRILL[cn][an]["ads"].values(), key=lambda x: x["spend"], reverse=True)
        AD_DRILL[cn][an]["spend"] = round(AD_DRILL[cn][an]["spend"], 2)
ad_drill_json = json.dumps(AD_DRILL, separators=(",", ":"))

# ══════════════════════════════════════════════════════════════════════
# 3. GOOGLE ADS — transformar rows Windsor -> GOOGLE, CAMPANHAS
# ══════════════════════════════════════════════════════════════════════
GOOGLE    = []
CAMPANHAS = []

daily_g = defaultdict(lambda: {"s":0.0,"imp":0,"cl":0,"cv":0.0})
camp_g  = defaultdict(lambda: {"s":0.0,"cl":0,"cv":0.0})

for row in google_rows:
    d  = row.get("date", "")
    if not d: continue
    cn = row.get("campaign_name", "—")
    s  = float(row.get("spend") or 0)
    im = int(row.get("impressions") or 0)
    cl = int(row.get("clicks") or 0)
    cv = float(row.get("conversions") or 0)
    daily_g[d]["s"]   += s
    daily_g[d]["imp"] += im
    daily_g[d]["cl"]  += cl
    daily_g[d]["cv"]  += cv
    camp_g[cn]["s"]   += s
    camp_g[cn]["cl"]  += cl
    camp_g[cn]["cv"]  += cv

GOOGLE = [
    (d, v["s"], v["imp"], v["cl"], int(v["cv"]), 0.0)
    for d, v in sorted(daily_g.items()) if v["s"] > 0
]

for cn, v in sorted(camp_g.items(), key=lambda x: x[1]["s"], reverse=True):
    if v["s"] == 0: continue
    conv = int(v["cv"])
    cpa  = v["s"] / conv if conv > 0 else 0
    cor  = ("green" if (conv > 0 and cpa < GOOGLE_CPA_BOM)
            else "yellow" if (conv > 0 and cpa < GOOGLE_CPA_OK)
            else "red" if conv > 0 else "yellow")
    CAMPANHAS.append({"nome": clean_camp(cn), "desc": "Campanha ativa",
                      "gasto": round(v["s"], 2), "cliques": v["cl"],
                      "contatos": conv, "cpa": round(cpa, 2), "cor": cor})

google_last = GOOGLE[-1][0] if GOOGLE else ""
print(f"  Google: {len(GOOGLE)} dias | ultimo: {google_last} | {len(CAMPANHAS)} campanhas")



# 3. JSON + DATAS
meta_first   = META[0][0] if META else today_str
google_first = GOOGLE[0][0] if GOOGLE else ""
min_date     = min(meta_first, google_first) if (META and GOOGLE) else (meta_first if META else google_first)
max_date     = yesterday_str
gcampmonth_label = (f"{google_first[8:10]}/{google_first[5:7]} -- {google_last[8:10]}/{google_last[5:7]}"
                    if GOOGLE and google_last else "--")

meta_js        = [{"d":d,"s":s,"r":r,"i":imp,"c":cv,"lk":lk,"rv":rv,"v25":v25,"v50":v50,"v75":v75,"v95":v95,"msg":msg}
                  for d,s,r,imp,cv,lk,rv,v25,v50,v75,v95,msg in META]
google_js      = [{"d":d,"s":s,"imp":imp,"cl":cl,"cv":cv} for d,s,imp,cl,cv,rv in GOOGLE]
meta_json      = json.dumps(meta_js,        separators=(",", ":"))
google_json    = json.dumps(google_js,      separators=(",", ":"))
camp_json       = json.dumps(CAMPANHAS,       separators=(",", ":"))
camp_meta_json  = json.dumps(CAMP_META_INFO,  separators=(",", ":"))
camp_daily_json = json.dumps(CAMP_DAILY_LIST, separators=(",", ":"))

_google_first_fmt = f"{google_first[8:10]}/{google_first[5:7]}" if google_first else "--"

HTML = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>{CLIENTE_NOME} — Painel de Marketing</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/themes/dark.css">
<script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
<script src="https://cdn.jsdelivr.net/npm/flatpickr/dist/l10n/pt.js"></script>
<style>
  body{{background:#0a0a0a;color:#e5e7eb;font-family:system-ui,-apple-system,sans-serif}}
  .card{{background:#141414;border:1px solid #252525;border-radius:12px}}
  .card-meta{{border-left:4px solid #3b82f6!important}}
  .title-meta{{color:#93c5fd!important}}
  .card-resumo{{border-left:4px solid #a855f7!important}}
  .title-resumo{{color:#c084fc!important}}
  .neon{{color:#00ff88}}
  .badge-blue{{background:rgba(59,130,246,.12);color:#60a5fa;border:1px solid rgba(59,130,246,.25);border-radius:6px}}
  .btn-period{{padding:6px 14px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;border:1px solid #2a2a2a;background:#161616;color:#9ca3af;transition:all .15s}}
  .btn-period:hover{{border-color:#444;color:#e5e7eb}}
  .btn-period.active{{background:rgba(0,255,136,.12);border-color:rgba(0,255,136,.4);color:#00ff88}}
  .flatpickr-input{{background:#161616!important;border:1px solid #2a2a2a!important;color:#9ca3af!important;border-radius:8px!important;padding:6px 12px!important;font-size:12px!important;cursor:pointer!important;outline:none!important;width:100%;box-sizing:border-box}}
  .flatpickr-input:focus{{border-color:#444!important}}
  .flatpickr-calendar{{background:#111!important;border:1px solid #222!important;box-shadow:0 8px 32px rgba(0,0,0,.6)!important}}
  .flatpickr-day{{color:#9ca3af!important}}
  .flatpickr-day:hover{{background:#1e1e1e!important;border-color:#333!important}}
  .flatpickr-day.selected,.flatpickr-day.startRange,.flatpickr-day.endRange{{background:#00ff88!important;border-color:#00ff88!important;color:#000!important}}
  .flatpickr-day.inRange{{background:rgba(0,255,136,.12)!important;border-color:transparent!important;color:#00ff88!important;box-shadow:-5px 0 0 rgba(0,255,136,.12),5px 0 0 rgba(0,255,136,.12)!important}}
  .flatpickr-day.today{{border-color:#333!important}}
  .flatpickr-day.flatpickr-disabled{{color:#374151!important}}
  .flatpickr-months .flatpickr-month,.flatpickr-weekdays,.span.flatpickr-weekday{{background:#111!important;color:#9ca3af!important;fill:#9ca3af!important}}
  .flatpickr-weekday{{color:#6b7280!important}}
  .flatpickr-prev-month,.flatpickr-next-month{{color:#e5e7eb!important;fill:#e5e7eb!important;opacity:1!important}}
  .flatpickr-prev-month svg,.flatpickr-next-month svg{{fill:#e5e7eb!important}}
  .flatpickr-prev-month svg path,.flatpickr-next-month svg path{{fill:#e5e7eb!important;stroke:#e5e7eb!important}}
  .flatpickr-prev-month:hover,.flatpickr-next-month:hover{{color:#00ff88!important;fill:#00ff88!important}}
  .flatpickr-prev-month:hover svg path,.flatpickr-next-month:hover svg path{{fill:#00ff88!important;stroke:#00ff88!important}}
  .flatpickr-current-month .flatpickr-monthDropdown-months{{background:#111!important;color:#e5e7eb!important}}
  .numInputWrapper input{{color:#e5e7eb!important;background:#111!important}}
  .kpi-val{{font-size:30px;font-weight:700;line-height:1.1}}
  .kpi-label{{font-size:12px;color:#d1d5db;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px;font-weight:600}}
  .kpi-sub{{font-size:11px;color:#9ca3af;margin-top:4px}}
  .sec-title{{font-size:14px;color:#e5e7eb;text-transform:uppercase;letter-spacing:.06em;font-weight:700;margin-bottom:14px}}
  .date-pill{{font-size:12px;color:#9ca3af;font-weight:500;text-transform:none;letter-spacing:normal;border:1px solid #333;border-radius:6px;padding:2px 10px;white-space:nowrap}}
  .tag-green{{color:#4ade80}} .tag-yellow{{color:#fbbf24}} .tag-red{{color:#f87171}}
  .video-bar-bg{{background:#1e1e1e;border-radius:4px;height:6px;overflow:hidden}}
  .video-bar{{background:#00ff88;height:6px;border-radius:4px;transition:width .4s}}
  .camp-row-click{{cursor:pointer;transition:background .1s}}
  .camp-row-click:hover td{{background:rgba(255,255,255,.015)}}
  .drill-row{{background:#0d0d0d}}
  .adset-hdr{{background:rgba(59,130,246,.06);border-left:3px solid rgba(59,130,246,.35);border-radius:6px;padding:8px 12px;margin-bottom:8px}}
  .ad-thumb{{width:52px;height:52px;object-fit:cover;border-radius:8px;flex-shrink:0;background:#1a1a1a}}
  .ad-thumb-ph{{width:52px;height:52px;border-radius:8px;background:#1e1e1e;border:1px solid #2a2a2a;display:flex;align-items:center;justify-content:center;flex-shrink:0;letter-spacing:.02em}}
  .drill-inner{{padding:12px 16px 16px;border-top:1px solid #1a1a1a}}
  .drill-note{{font-size:10px;color:#4b5563;margin-top:12px;font-style:italic}}
  @media(max-width:639px){{.kpi-val{{font-size:22px}}.kpi-sub{{font-size:10px}}.kpi-label{{font-size:10px}}}}
  @media(max-width:767px){{
    .mc-table td:nth-child(3),.mc-table th:nth-child(3),
    .mc-table td:nth-child(4),.mc-table th:nth-child(4){{display:none}}
    .btn-period{{padding:5px 10px;font-size:11px}}
  }}
</style>
</head>
<body class="min-h-screen">
<div class="max-w-5xl mx-auto px-4 py-8">

<!-- Header -->
<div class="flex flex-col sm:flex-row sm:items-start sm:justify-between mb-6 gap-3">
  <div>
    <h1 class="text-3xl font-bold text-white tracking-widest uppercase">{CLIENTE_NOME}</h1>
    <p class="text-xs text-gray-600 mt-1">{CLIENTE_SUB}</p>
  </div>
  <div class="text-left sm:text-right">
    <p class="text-xs text-gray-600">Dados ate <span class="text-gray-400 font-medium">{yesterday.strftime('%d/%m/%Y')}</span></p>
    <div class="flex sm:justify-end gap-1 mt-2 flex-wrap">
      <span class="badge-blue text-xs px-2 py-0.5 inline-flex items-center gap-1"><img src="https://cdn.simpleicons.org/meta/1877F2" width="12" height="12" alt="Meta"/> Meta Ads</span>
    </div>
  </div>
</div>

<!-- Period Selector -->
<div class="card p-4 mb-6">
  <div class="flex flex-wrap items-center gap-2">
    <span class="text-xs text-gray-500 mr-1">Periodo:</span>
    <div class="flex flex-wrap gap-1">
      <button class="btn-period" onclick="setPeriod('month')" id="btn-month">Este mes</button>
      <button class="btn-period" onclick="setPeriod('7')"     id="btn-7">7 dias</button>
      <button class="btn-period" onclick="setPeriod('14')"    id="btn-14">14 dias</button>
      <button class="btn-period" onclick="setPeriod('30')"    id="btn-30">30 dias</button>
      <button class="btn-period" onclick="setPeriod('90')"    id="btn-90">90 dias</button>
    </div>
    <div class="w-full sm:w-auto sm:ml-auto pt-1 sm:pt-0">
      <input type="text" id="dt-range" placeholder="Escolher datas..." readonly class="w-full"/>
    </div>
  </div>
</div>


<!-- Data Content -->
<div id="data-content">

<!-- RESUMO GERAL -->
<div class="card card-resumo p-5 mb-4">
  <p class="sec-title title-resumo">Resumo — Meta Ads</p>
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
    <div><p class="kpi-label">Investimento</p><p class="kpi-val neon" id="tot-spend">—</p><p class="kpi-sub">total no periodo</p></div>
    <div><p class="kpi-label">{CONV_LABEL}</p><p class="kpi-val neon" id="tot-leads">—</p><p class="kpi-sub">{CONV_UNIT}</p></div>
    <div><p class="kpi-label">CPA Medio</p><p class="kpi-val text-white" id="tot-cpa">—</p><p class="kpi-sub">{CONV_CPL_UNIT}</p></div>
    <div><p class="kpi-label">Impressoes</p><p class="kpi-val text-white" id="tot-imp">—</p><p class="kpi-sub">exibicoes totais</p></div>
  </div>
</div>

<!-- Chart -->
<div class="card card-meta p-5 mb-6">
  <h2 class="sec-title">Investimento diario + {CONV_LABEL}</h2>
  <canvas id="mainChart" height="90"></canvas>
</div>

<!-- META ADS -->
<div class="card card-meta p-5 mb-4">
  <div class="sec-title title-meta flex items-center gap-2 mb-3">
    <img src="https://cdn.simpleicons.org/meta/1877F2" width="18" height="18" alt="Meta"/>
    Meta Ads — Facebook &amp; Instagram
    <span class="date-pill" id="meta-date-label"></span>
  </div>
  <div class="grid grid-cols-2 md:grid-cols-6 gap-3 mb-4">
    <div><p class="kpi-label">Alcance</p><p class="kpi-val text-white" id="m-reach">—</p><p class="kpi-sub">pessoas unicas</p></div>
    <div><p class="kpi-label">Impressoes</p><p class="kpi-val text-white" id="m-imp">—</p><p class="kpi-sub">exibicoes totais</p></div>
    <div><p class="kpi-label">Frequencia</p><p class="kpi-val text-white" id="m-freq">—</p><p class="kpi-sub">media por pessoa</p></div>
    <div><p class="kpi-label">CPM</p><p class="kpi-val text-white" id="m-cpm">—</p><p class="kpi-sub">custo por 1.000</p></div>
    <div><p class="kpi-label">CTR</p><p class="kpi-val text-white" id="m-ctr">—</p><p class="kpi-sub">taxa de clique</p></div>
    <div><p class="kpi-label">Investimento</p><p class="kpi-val neon" id="m-spend">—</p><p class="kpi-sub">total no periodo</p></div>
  </div>
  <div class="border-t border-gray-800 my-3"></div>
  <div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
    <div><p class="kpi-label">Cliques</p><p class="kpi-val text-white" id="m-clicks">—</p><p class="kpi-sub">em links</p></div>
    <div><p class="kpi-label">CPC</p><p class="kpi-val text-white" id="m-cpc">—</p><p class="kpi-sub">custo por clique</p></div>
    <div><p class="kpi-label">{CONV_LABEL}</p><p class="kpi-val neon" id="m-conv">—</p><p class="kpi-sub">{CONV_UNIT}</p></div>
    <div><p class="kpi-label">{CONV_CPL_LBL}</p><p class="kpi-val text-white" id="m-cpl">—</p><p class="kpi-sub">{CONV_CPL_UNIT}</p></div>
    <div><p class="kpi-label">Conversas (Wpp + DM)</p><p class="kpi-val text-white" id="m-msg">—</p><p class="kpi-sub">WhatsApp + Instagram Direct</p></div>
  </div>
  <div class="border-t border-gray-800 my-3" id="video-divider"></div>
  <div id="video-block">
    <p class="kpi-label mb-3">Retencao de video</p>
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
      <div><div class="flex justify-between mb-1"><span class="text-xs text-gray-400">View 25%</span><span class="text-xs text-white" id="m-v25">—</span></div><div class="video-bar-bg"><div class="video-bar" id="bar-v25" style="width:0%"></div></div></div>
      <div><div class="flex justify-between mb-1"><span class="text-xs text-gray-400">View 50%</span><span class="text-xs text-white" id="m-v50">—</span></div><div class="video-bar-bg"><div class="video-bar" id="bar-v50" style="width:0%;background:#60a5fa"></div></div></div>
      <div><div class="flex justify-between mb-1"><span class="text-xs text-gray-400">View 75%</span><span class="text-xs text-white" id="m-v75">—</span></div><div class="video-bar-bg"><div class="video-bar" id="bar-v75" style="width:0%;background:#fbbf24"></div></div></div>
      <div><div class="flex justify-between mb-1"><span class="text-xs text-gray-400">View 95%</span><span class="text-xs text-white" id="m-v95">—</span></div><div class="video-bar-bg"><div class="video-bar" id="bar-v95" style="width:0%;background:#f87171"></div></div></div>
    </div>
    <p class="text-xs text-gray-700 mt-2" id="video-note"></p>
  </div>
</div>

<!-- META CAMPANHAS -->
<div class="card card-meta p-5 mb-6">
  <div class="sec-title title-meta flex items-center gap-2 mb-3">
    <img src="https://cdn.simpleicons.org/meta/1877F2" width="18" height="18" alt="Meta"/>
    Campanhas Meta
    <span class="date-pill" id="meta-camp-date-label"></span>
  </div>
  <div class="overflow-x-auto">
    <table class="mc-table w-full text-sm">
      <thead><tr class="text-xs text-gray-600 border-b border-gray-800">
        <th class="text-left pb-3 pr-2 w-6"></th>
        <th class="text-left pb-3 pr-4">Campanha</th>
        <th class="text-right pb-3 pr-4">Investido</th>
        <th class="text-right pb-3 pr-4">Alcance</th>
        <th class="text-right pb-3 pr-4">Cliques</th>
        <th class="text-right pb-3 pr-4">{CONV_LABEL}</th>
        <th class="text-right pb-3">{CONV_CPL_LBL}</th>
      </tr></thead>
      <tbody id="meta-camp-body"></tbody>
    </table>
  </div>
</div>

<div class="text-center text-xs text-gray-700 py-4">Atualizado em {today.strftime('%d/%m/%Y')}</div>

</div><!-- /data-content -->
</div><!-- /max-w-5xl -->

<script>
const META_DATA  = {meta_json};
const CAMP_META  = {camp_meta_json};
const CAMP_DAILY = {camp_daily_json};
const AD_DRILL   = {ad_drill_json};
const META_CPA_BOM_JS = {META_CPA_BOM};
const META_CPA_OK_JS  = {META_CPA_OK};

let chart = null;
const CUR    = n => 'R$ ' + n.toFixed(2);
const PCT    = n => n.toFixed(2) + '%';
const FMT    = n => n >= 1000000 ? (n/1000000).toFixed(1)+'M' : n >= 1000 ? (n/1000).toFixed(1)+'K' : String(n);
const fmtD = s => {{ const [y,m,d]=s.split('-'); return d+'/'+m; }};

function set(id,val){{const el=document.getElementById(id);if(el)el.textContent=val;}}

function toggleDrill(campId) {{
  const row = document.getElementById('drill-'+campId);
  const arrow = document.getElementById('arrow-'+campId);
  if(!row) return;
  const nowHidden = row.classList.toggle('hidden');
  if(arrow) arrow.textContent = nowHidden ? '▶' : '▼';
  if(!nowHidden && !row.dataset.rendered) {{
    renderDrill(campId, row);
    row.dataset.rendered = '1';
  }}
}}

function toggleAset(asetId) {{
  const el = document.getElementById(asetId);
  const arr = document.getElementById('arr-'+asetId);
  if(!el) return;
  const open = el.style.display === 'none';
  el.style.display = open ? 'block' : 'none';
  if(arr) arr.textContent = open ? '▼' : '▶';
}}

function renderDrill(campId, container) {{
  const data = AD_DRILL[campId];
  if(!data || Object.keys(data).length === 0) {{
    container.innerHTML = '<td colspan="7"><div class="drill-inner"><p class="text-gray-600 text-xs">Sem dados de criativo nos ultimos 30 dias.</p></div></td>';
    return;
  }}
  const CUR_D = n => 'R$ ' + n.toFixed(2);
  let html = '<td colspan="7"><div class="drill-inner">';
  const asets = Object.values(data).sort((a,b)=>b.spend-a.spend);
  asets.forEach((aset, ai) => {{
    const asetId = 'aset-'+campId+'-'+ai;
    const asetCpa = aset.conv>0 ? aset.spend/aset.conv : 0;
    const adCount = aset.ads ? aset.ads.length : 0;
    html += `<div class="adset-hdr cursor-pointer select-none" onclick="toggleAset('${{asetId}}')" style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0">
      <div style="display:flex;align-items:center;gap:8px">
        <span id="arr-${{asetId}}" style="font-size:10px;color:#6b7280">▶</span>
        <span class="text-xs font-bold text-blue-300 uppercase tracking-wide">${{aset.name.length>60?aset.name.substring(0,57)+'...':aset.name}}</span>
      </div>
      <span class="text-xs">${{
        `<span style="color:#fb923c;font-weight:600">${{CUR_D(aset.spend)}}</span>` +
        ` · <span style="color:#4ade80;font-weight:600">${{aset.conv}} conv.</span>` +
        (asetCpa>0 ? ` · <span style="color:#60a5fa;font-weight:600">${{CUR_D(asetCpa)}}/conv.</span>` : '') +
        ` · <span style="color:#6b7280">${{adCount}} anuncio${{adCount!==1?'s':''}}</span>`
      }}</span>
    </div>`;
    html += `<div id="${{asetId}}" style="display:none" class="space-y-2 mb-3 mt-2 pl-4 border-l border-blue-900/40">`;
    (aset.ads||[]).forEach(ad => {{
      const adCpa = ad.conv>0 ? ad.spend/ad.conv : 0;
      const cpaCls = ad.conv>0&&adCpa<META_CPA_BOM_JS?'text-green-400':ad.conv>0&&adCpa<META_CPA_OK_JS?'text-yellow-400':ad.conv>0?'text-red-400':'text-gray-600';
      const initials = ad.name.replace(/[^A-Za-z0-9]/g,' ').trim().split(/\s+/).slice(0,2).map(w=>w[0]||'').join('').toUpperCase()||'?';
      const thumbWrap = ad.thumb
        ? `<img src="${{ad.thumb}}" class="ad-thumb" alt="" onerror="this.parentNode.querySelector('.ad-thumb-ph').style.display='flex';this.style.display='none'"/><div class="ad-thumb-ph" style="display:none;font-size:11px;font-weight:700;color:#6b7280">${{initials}}</div>`
        : `<div class="ad-thumb-ph" style="font-size:11px;font-weight:700;color:#6b7280">${{initials}}</div>`;
      const eyeLink = ad.link || ad.thumb || '';
        ? `<a href="${{eyeLink}}" target="_blank" rel="noopener" title="Ver post no Meta" style="color:#6b7280;text-decoration:none;font-size:14px;line-height:1;flex-shrink:0" onmouseover="this.style.color='#a78bfa'" onmouseout="this.style.color='#6b7280'">👁</a>`
        : '';
      html += `<div class="flex items-center gap-3 p-2 rounded-lg hover:bg-white/5 transition-colors">
        ${{thumbWrap}}
        <div class="flex-1 min-w-0">
          <div style="display:flex;align-items:center;gap:6px">
            <p class="text-sm text-white font-medium truncate" title="${{ad.name}}" style="margin:0">${{ad.name.length>55?ad.name.substring(0,52)+'...':ad.name}}</p>
          </div>
          <p class="text-xs text-gray-500 mt-0.5">${{CUR_D(ad.spend)}} · <span class="${{cpaCls}} font-semibold">${{ad.conv}} conv.</span>${{adCpa>0?' · '+CUR_D(adCpa)+'/conv.':''}}</p>
        </div>
      </div>`;
    }});
    html += '</div>';
  }});
  html += '<p class="drill-note">* Dados dos ultimos 30 dias (fixo — nao muda com o seletor de periodo)</p>';
  html += '</div></td>';
  container.innerHTML = html;
}}

function update(from,to){{
  const mR = META_DATA.filter(x=>x.d>=from&&x.d<=to);

  const mSpend=mR.reduce((a,x)=>a+x.s,0);
  const mReach=mR.reduce((a,x)=>a+x.r,0);
  const mImp  =mR.reduce((a,x)=>a+x.i,0);
  const mConv =mR.reduce((a,x)=>a+x.c,0);
  const mClk  =mR.reduce((a,x)=>a+x.lk,0);
  const mFreq =mReach>0?mImp/mReach:0;
  const mCPM  =mImp>0?mSpend/mImp*1000:0;
  const mCTR  =mImp>0?mClk/mImp*100:0;
  const mCPC  =mClk>0?mSpend/mClk:0;
  const mCPL  =mConv>0?mSpend/mConv:0;
  const mMsg  =mR.reduce((a,x)=>a+(x.msg||0),0);

  const vRows=mR.filter(x=>x.v25>0);
  const v25=vRows.reduce((a,x)=>a+x.v25,0);
  const v50=vRows.reduce((a,x)=>a+x.v50,0);
  const v75=vRows.reduce((a,x)=>a+x.v75,0);
  const v95=vRows.reduce((a,x)=>a+x.v95,0);
  const hasVideo=v25>0;

  set('m-reach', FMT(mReach));
  set('m-imp',   FMT(mImp));
  set('m-freq',  mFreq>0?mFreq.toFixed(1)+'x':'—');
  set('m-cpm',   CUR(mCPM));
  set('m-ctr',   PCT(mCTR));
  set('m-spend', CUR(mSpend));
  set('m-clicks',mClk);
  set('m-cpc',   mClk>0?CUR(mCPC):'—');
  set('m-conv',  mConv);
  set('m-cpl',   mConv>0?CUR(mCPL):'—');
  set('m-msg',   mMsg > 0 ? FMT(mMsg) : '—');
  set('meta-date-label', fmtD(from)+' — '+fmtD(to));

  if(hasVideo){{
    set('m-v25',FMT(v25));set('m-v50',FMT(v50));set('m-v75',FMT(v75));set('m-v95',FMT(v95));
    const b=v25||1;
    document.getElementById('bar-v25').style.width='100%';
    document.getElementById('bar-v50').style.width=(v50/b*100)+'%';
    document.getElementById('bar-v75').style.width=(v75/b*100)+'%';
    document.getElementById('bar-v95').style.width=(v95/b*100)+'%';
    set('video-note','');
  }}else{{
    ['m-v25','m-v50','m-v75','m-v95'].forEach(id=>set(id,'—'));
    ['bar-v25','bar-v50','bar-v75','bar-v95'].forEach(id=>{{document.getElementById(id).style.width='0%';}});
    set('video-note','Sem dados de video no periodo selecionado.');
  }}

  // Resumo
  set('tot-spend', CUR(mSpend));
  set('tot-leads', mConv > 0 ? mConv : '—');
  set('tot-cpa',   mConv > 0 ? CUR(mSpend/mConv) : '—');
  set('tot-imp',   FMT(mImp));

  // Grafico
  const allDates=mR.map(x=>x.d).sort();
  if(chart)chart.destroy();
  Chart.defaults.color='#6b7280';
  chart=new Chart(document.getElementById('mainChart').getContext('2d'),{{
    data:{{
      labels: allDates.map(fmtD),
      datasets:[
        {{type:'bar',label:'Meta (R$)',data:allDates.map(d=>{{const r=mR.find(x=>x.d===d);return r?r.s:0;}}),backgroundColor:'rgba(59,130,246,.2)',borderColor:'#60a5fa',borderWidth:1,borderRadius:3,yAxisID:'y'}},
        {{type:'line',label:'{CONV_LABEL}',data:allDates.map(d=>{{const r=mR.find(x=>x.d===d);return r?r.c:0;}}),borderColor:'#f59e0b',backgroundColor:'rgba(245,158,11,.1)',borderWidth:2,pointRadius:allDates.length>30?2:4,tension:.3,fill:false,yAxisID:'y1'}}
      ]
    }},
    options:{{
      responsive:true,
      interaction:{{mode:'index',intersect:false}},
      plugins:{{
        legend:{{labels:{{color:'#9ca3af',font:{{size:11}},padding:16}}}},
        tooltip:{{backgroundColor:'#1a1a1a',borderColor:'#333',borderWidth:1,titleColor:'#e5e7eb',bodyColor:'#9ca3af',
          callbacks:{{label:c=>c.dataset.label==='{CONV_LABEL}'?'  {CONV_LABEL}: '+c.parsed.y:'  '+c.dataset.label+': R$ '+c.parsed.y.toFixed(2)}}}}
      }},
      scales:{{
        x:{{grid:{{color:'#1a1a1a'}},ticks:{{color:'#6b7280',font:{{size:10}},maxRotation:45}}}},
        y:{{grid:{{color:'#1a1a1a'}},ticks:{{color:'#6b7280',font:{{size:10}},callback:v=>'R$ '+v}}}},
        y1:{{position:'right',grid:{{drawOnChartArea:false}},ticks:{{color:'#fbbf24',font:{{size:10}},callback:v=>v+' conv.'}}}}
      }}
    }}
  }});

  // Campanhas Meta — agregar dinamicamente por período selecionado
  set('meta-camp-date-label', fmtD(from)+' — '+fmtD(to));
  const byId={{}};
  CAMP_DAILY.filter(x=>x.d>=from&&x.d<=to).forEach(row=>{{
    if(!byId[row.id]) byId[row.id]={{gasto:0,reach:0,lk:0,conv:0}};
    byId[row.id].gasto+=row.s;
    byId[row.id].reach+=row.r;
    byId[row.id].lk+=row.lk;
    byId[row.id].conv+=row.c;
  }});
  const camps=Object.entries(byId)
    .filter(([id,c])=>c.gasto>0)
    .map(([id,c])=>{{
      const info=CAMP_META[id]||{{nome:id,desc:'—'}};
      const cpa=c.conv>0?c.gasto/c.conv:0;
      const cor=c.conv>0&&cpa<{META_CPA_BOM}?'green':c.conv>0&&cpa<{META_CPA_OK}?'yellow':c.conv>0?'red':'blue';
      return {{id,...info,...c,cpa,cor}};
    }})
    .sort((a,b)=>b.gasto-a.gasto);

  const mbody=document.getElementById('meta-camp-body');
  mbody.innerHTML='';
  camps.forEach(c=>{{
    const cc=c.cor==='green'?'tag-green':c.cor==='blue'?'text-blue-400':c.cor==='yellow'?'tag-yellow':'tag-red';
    const cpaStr =c.conv>0?`<span class="${{cc}} font-semibold">R$ ${{c.cpa.toFixed(2)}}</span>`:`<span class="text-gray-600">—</span>`;
    const convStr=c.conv>0?`<span class="font-bold text-white">${{c.conv}}</span>`:`<span class="text-gray-600">—</span>`;
    const hasDrill = AD_DRILL[c.id] && Object.keys(AD_DRILL[c.id]).length > 0;
    const arrowEl = hasDrill ? `<span id="arrow-${{c.id}}" class="text-gray-500 text-xs mr-1 select-none">▶</span>` : `<span class="text-gray-800 text-xs mr-1">·</span>`;
    const rowClick = hasDrill ? `onclick="toggleDrill('${{c.id}}')" class="border-b border-gray-800/50 camp-row-click"` : `class="border-b border-gray-800/50"`;
    mbody.innerHTML+=`<tr ${{rowClick}}>
      <td class="py-3 pl-1 pr-2 w-6">${{arrowEl}}</td>
      <td class="py-3 pr-4"><p class="text-white font-medium">${{c.nome}}</p><p class="text-xs text-gray-500">${{c.desc}}</p></td>
      <td class="py-3 pr-4 text-right text-gray-300">R$ ${{c.gasto.toFixed(2)}}</td>
      <td class="py-3 pr-4 text-right text-gray-300">${{c.reach.toLocaleString('pt-BR')}}</td>
      <td class="py-3 pr-4 text-right text-gray-300">${{c.lk}}</td>
      <td class="py-3 pr-4 text-right">${{convStr}}</td>
      <td class="py-3 text-right">${{cpaStr}}</td>
    </tr>
    <tr id="drill-${{c.id}}" class="drill-row hidden"></tr>`;
  }});
  if(camps.length>=1){{
    const mt=camps.reduce((a,c)=>{{a.g+=c.gasto;a.r+=c.reach;a.lk+=c.lk;a.cv+=c.conv;return a;}},{{g:0,r:0,lk:0,cv:0}});
    const mcpa=mt.cv>0?mt.g/mt.cv:0;
    mbody.innerHTML+=`<tr class="border-t-2 border-gray-600 bg-white/[.02]">
      <td></td>
      <td class="py-2 pl-1 text-xs text-gray-400 font-semibold uppercase tracking-wide">Total</td>
      <td class="py-2 pr-4 text-right text-gray-200 font-semibold">R$ ${{mt.g.toFixed(2)}}</td>
      <td class="py-2 pr-4 text-right text-gray-200 font-semibold">${{mt.r.toLocaleString('pt-BR')}}</td>
      <td class="py-2 pr-4 text-right text-gray-200 font-semibold">${{mt.lk}}</td>
      <td class="py-2 pr-4 text-right text-white font-bold">${{mt.cv>0?mt.cv:'—'}}</td>
      <td class="py-2 text-right text-gray-200 font-semibold">${{mcpa>0?'R$ '+mcpa.toFixed(2):'—'}}</td>
    </tr>`;
  }}
}}

function clearActive(){{['month','7','14','30','90'].forEach(id=>document.getElementById('btn-'+id).classList.remove('active'));}}
function isoToDate(s){{const[y,m,d]=s.split('-');return new Date(+y,+m-1,+d);}}
function dateToIso(dt){{return dt.getFullYear()+'-'+String(dt.getMonth()+1).padStart(2,'0')+'-'+String(dt.getDate()).padStart(2,'0');}}

const MAX_DATE      = '{max_date}';
const MIN_DATE      = '{min_date}';
const TODAY_STR     = '{today_str}';
const YESTERDAY_STR = '{yesterday_str}';

const fp = flatpickr('#dt-range',{{
  mode:'range', dateFormat:'d/m/Y',
  minDate: MIN_DATE ? isoToDate(MIN_DATE) : null,
  maxDate: MAX_DATE ? isoToDate(MAX_DATE) : null,
  locale:'pt',
  showMonths: window.innerWidth>=640 ? 2 : 1,
  onOpen(sel,dateStr,instance){{setTimeout(()=>instance.jumpToDate(sel.length>0?sel[0]:isoToDate(MAX_DATE)),0);}},
  onClose(sel){{
    if(sel.length===2){{clearActive();update(dateToIso(sel[0]),dateToIso(sel[1]));}}
    else if(sel.length===1){{clearActive();const d=dateToIso(sel[0]);update(d,d);}}
  }}
}});

function setPeriod(p){{
  clearActive();
  document.getElementById('btn-'+p).classList.add('active');
  let from;
  if(p==='month'){{from=YESTERDAY_STR.substring(0,8)+'01';}}
  else{{const d=isoToDate(YESTERDAY_STR);d.setDate(d.getDate()-parseInt(p)+1);from=dateToIso(d);}}
  fp.setDate([isoToDate(from),isoToDate(YESTERDAY_STR)]);
  update(from,YESTERDAY_STR);
}}
setPeriod('30');
</script>
</body>
</html>"""

# ══════════════════════════════════════════════════════════════════════
# 4. SALVAR + DEPLOY
# ══════════════════════════════════════════════════════════════════════
out_dir  = os.path.join(DEPLOY_DIR, CF_SUBFOLDER)
out_file = os.path.join(out_dir, "index.html")
os.makedirs(out_dir, exist_ok=True)

with open(out_file, "w", encoding="utf-8") as f:
    f.write(HTML)
print(f"\nHTML gerado: {out_file}")

dist_dir     = os.path.join(DEPLOY_DIR, "dist")
dist_cliente = os.path.join(dist_dir, CF_SUBFOLDER)
os.makedirs(dist_cliente, exist_ok=True)
shutil.copy(out_file, dist_cliente)
print(f"Copiado para dist/{CF_SUBFOLDER}/index.html")

print("\nDeployando dist/ completo...")
env_deploy = os.environ.copy()
env_deploy["CLOUDFLARE_API_TOKEN"]  = CF_TOKEN
env_deploy["CLOUDFLARE_ACCOUNT_ID"] = CF_ACCOUNT

result = subprocess.run(
    f'npx wrangler pages deploy "{dist_dir}" --project-name={CF_PROJECT} --branch=main --commit-dirty=true',
    env=env_deploy, capture_output=False, shell=True, cwd=DEPLOY_DIR, timeout=120
)
if result.returncode == 0:
    print(f"\nURL: https://relatorio-performance.online/{CF_SUBFOLDER}/")
else:
    print(f"Erro no deploy (rc={result.returncode})")
