"""
Qualy USA Dashboard — geração + deploy automático.
Roda: python generate_qualy_usa.py

Ecom USA — conversão principal: purchase (pixel Meta)
Moeda: USD ($)
"""
import json, os, io, csv, re, subprocess, requests, shutil
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# ══════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════
CLIENTE_NOME  = "Qualy USA"
CF_SUBFOLDER  = "qualy-usa"

HAS_META      = True
HAS_GOOGLE    = True

META_ACT      = "act_828411085334344"
SHEET_ID      = "1xC_D39qiXuFaUueTdPBbo-sqHLhRIKk05XLWN5o3Efg"
CLIENTE_SLUG  = "Qualy"

CONV_ACTION   = "purchase"           # métrica de conversão Meta (ecom)
CONV_LABEL    = "Compras"
CONV_UNIT     = "pedidos realizados"
CONV_CPL_LBL  = "Custo / Compra"
CONV_CPL_UNIT = "por pedido"

CURRENCY_SYM  = "$"                  # símbolo monetário

# CPA thresholds (USD)
META_CPA_BOM   = 100.0
META_CPA_OK    = 200.0
GOOGLE_CPA_BOM =  50.0
GOOGLE_CPA_OK  = 120.0
# ══════════════════════════════════════════════════════════════════════

META_TOKEN = os.getenv("META_ACCESS_TOKEN")
CF_TOKEN   = os.getenv("CF_TOKEN")
CF_ACCOUNT = os.getenv("CF_ACCOUNT")
CF_PROJECT = os.getenv("CF_PROJECT")
DEPLOY_DIR = os.path.dirname(os.path.abspath(__file__))

META_API     = "https://graph.facebook.com/v22.0"
GRAPH_FIELDS = "date_start,spend,reach,impressions,clicks,actions"
VIDEO_FIELDS = "date_start,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions"

today         = date.today()
yesterday     = today - timedelta(days=1)
since_90      = today - timedelta(days=89)
since_str     = since_90.strftime("%Y-%m-%d")
today_str     = today.strftime("%Y-%m-%d")
yesterday_str = yesterday.strftime("%Y-%m-%d")
month_start   = today.replace(day=1).strftime("%Y-%m-%d")

GADS_ICON = '<svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" width="18" height="18"><path fill="#34A853" d="M3.9998 22.9291C1.7908 22.9291 0 21.1383 0 18.9293s1.7908-3.9998 3.9998-3.9998 3.9998 1.7908 3.9998 3.9998-1.7908 3.9998-3.9998 3.9998z"/><path fill="#4285F4" d="M23.4641 16.9287L15.4632 3.072C14.3586 1.1587 11.9121.5028 9.9988 1.6074S7.4295 5.1585 8.5341 7.0718l8.0009 13.8567c1.1046 1.9133 3.5511 2.5679 5.4644 1.4646 1.9134-1.1046 2.568-3.5511 1.4647-5.4644z"/><path fill="#FBBC04" d="M7.5137 4.8438L1.5645 15.1484A4.5 4.5 0 0 1 4 14.4297c2.5597-.0075 4.6248 2.1585 4.4941 4.7148l3.2168-5.5723-3.6094-6.25c-.4499-.7793-.6322-1.6394-.5878-2.4784z"/></svg>'

# ══════════════════════════════════════════════════════════════════════
# 1. META ADS
# ══════════════════════════════════════════════════════════════════════
META           = []
META_CAMPANHAS = []
meta_last      = ""

print("Puxando Meta Ads...")

def meta_get(fields, since, until):
    url = f"{META_API}/{META_ACT}/insights"
    params = {
        "fields": fields, "level": "account", "time_increment": 1,
        "time_range": json.dumps({"since": since, "until": until}),
        "limit": 100, "access_token": META_TOKEN,
    }
    rows, nxt = [], None
    while True:
        p = dict(params)
        if nxt: p["after"] = nxt
        r = requests.get(url, params=p, timeout=30)
        d = r.json()
        rows.extend(d.get("data", []))
        nxt = d.get("paging", {}).get("cursors", {}).get("after")
        if not nxt or not d.get("paging", {}).get("next"): break
    return rows

def action_val(actions, key):
    for a in (actions or []):
        if a["action_type"] == key: return int(float(a["value"]))
    return 0

raw_main  = meta_get(GRAPH_FIELDS, since_str, today_str)
since_30  = (today - timedelta(days=29)).strftime("%Y-%m-%d")
raw_video = meta_get(VIDEO_FIELDS, since_30, today_str)
video_by_date = {r["date_start"]: r for r in raw_video}

for row in sorted(raw_main, key=lambda x: x["date_start"]):
    d   = row["date_start"]
    s   = float(row.get("spend", 0))
    r   = int(row.get("reach", 0))
    imp = int(row.get("impressions", 0))
    conv= action_val(row.get("actions"), CONV_ACTION)
    lk  = action_val(row.get("actions"), "link_click")
    vd  = video_by_date.get(d, {})
    v25 = int(float((vd.get("video_p25_watched_actions") or [{}])[0].get("value", 0)))
    v50 = int(float((vd.get("video_p50_watched_actions") or [{}])[0].get("value", 0)))
    v75 = int(float((vd.get("video_p75_watched_actions") or [{}])[0].get("value", 0)))
    v95 = int(float((vd.get("video_p95_watched_actions") or [{}])[0].get("value", 0)))
    if s > 0 or imp > 0:
        META.append((d, s, r, imp, conv, lk, v25, v50, v75, v95))

meta_last = META[-1][0] if META else today_str
print(f"  Meta: {len(META)} dias | ultimo: {meta_last}")

# Campanhas Meta ativas
camp_r = requests.get(f"{META_API}/{META_ACT}/campaigns", params={
    "fields": "id,name,status", "effective_status": '["ACTIVE"]',
    "limit": 50, "access_token": META_TOKEN,
}, timeout=20).json()

for camp in camp_r.get("data", []):
    ci = requests.get(f"{META_API}/{camp['id']}/insights", params={
        "fields": "campaign_name,spend,reach,impressions,clicks,actions",
        "time_range": json.dumps({"since": month_start, "until": today_str}),
        "access_token": META_TOKEN,
    }, timeout=20).json()
    data = ci.get("data", [{}])
    if not data or float(data[0].get("spend", 0)) == 0: continue
    d0       = data[0]
    nome_raw = d0.get("campaign_name", camp["name"])
    gasto    = float(d0.get("spend", 0))
    alcance  = int(d0.get("reach", 0))
    cliques  = action_val(d0.get("actions"), "link_click")
    conv     = action_val(d0.get("actions"), CONV_ACTION)
    cpa      = round(gasto / conv, 2) if conv > 0 else 0
    cor      = "green" if conv > 0 and cpa < META_CPA_BOM else "yellow" if conv > 0 and cpa < META_CPA_OK else "red" if conv > 0 else "blue"
    nome     = re.sub(r"CP\s*-\s*", "", nome_raw)
    nome     = re.sub(r"\s*-\s*\d{2}/\d{2}/\d{4}.*$", "", nome)
    nome     = re.sub(r"\s*-\s*\d{2}-\d{2}.*$", "", nome)
    nome     = re.sub(r"\s*-\s*\d+.*$", "", nome).strip()
    desc_map = {
        "RMKT": "Remarketing — quem ja viu o produto",
        "REMARKETING": "Remarketing — quem ja viu o produto",
        "CONVERSAO": "Conversao — objetivo compra",
        "CONVERSÃO": "Conversao — objetivo compra",
        "DISTRIBUICAO": "Alcance para novo publico",
        "DISTRIBUIÇÃO": "Alcance para novo publico",
        "CATALOG": "Catalogo de produtos dinamico",
        "ADVANTAGE": "Advantage+ Shopping",
    }
    desc = next((v for k, v in desc_map.items() if k in nome.upper()), "Campanha ativa")
    META_CAMPANHAS.append({"nome": nome, "desc": desc, "gasto": gasto, "alcance": alcance,
                            "cliques": cliques, "conv": conv, "cpa": cpa, "cor": cor})

META_CAMPANHAS.sort(key=lambda x: -x["gasto"])
print(f"  Campanhas Meta ativas: {len(META_CAMPANHAS)}")

# ══════════════════════════════════════════════════════════════════════
# 2. GOOGLE ADS (planilha)
# ══════════════════════════════════════════════════════════════════════
GOOGLE    = []
CAMPANHAS = []
google_last = ""

print("Lendo planilha Google Ads...")

def fetch_sheet_csv(tab):
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={tab}"
    r = requests.get(url, timeout=20)
    r.encoding = "utf-8"
    return list(csv.reader(io.StringIO(r.text)))

rows_g = fetch_sheet_csv("Tendencia%20Diaria")
header_idx = next((i for i, row in enumerate(rows_g) if row and "Data" in row[0]), None)
if header_idx is not None:
    for row in rows_g[header_idx + 1:]:
        if not row or not row[0].strip(): break
        try:
            d   = row[0].strip()
            s   = float(row[1].replace(",", ".").replace("\xa0", ""))
            imp = int(float(row[2].replace(",", ".").replace("\xa0", "")))
            cl  = int(float(row[3].replace(",", ".").replace("\xa0", "")))
            cv  = int(float(row[6].replace(",", ".").replace("\xa0", "")))
            if re.match(r"\d{4}-\d{2}-\d{2}", d):
                GOOGLE.append((d, s, imp, cl, cv))
        except Exception:
            continue

GOOGLE.sort(key=lambda x: x[0])
if GOOGLE: google_last = GOOGLE[-1][0]
print(f"  Google: {len(GOOGLE)} dias | ultimo: {google_last}")

_NOISE = {"site antigo", "site", "antigo", "novo teste", "teste"}

def clean_camp(raw):
    brackets = re.findall(r'\[([^\]]+)\]', raw)
    useful = [b.strip() for b in brackets if b.strip().lower() not in _NOISE]
    if useful: return " / ".join(useful)
    n = re.sub(r'\[.*?\]|\d{2}/\d{2}/\d{4}|#\d+', '', raw)
    n = re.sub(r'\s{2,}', ' ', n).strip(' -').strip()
    n = re.sub(rf'^{re.escape(CLIENTE_SLUG)}\s*[-–\s]*', '', n, flags=re.IGNORECASE).strip(' -').strip()
    return n or raw[:35]

rows_c = fetch_sheet_csv("Campanhas")
h_idx  = next((i for i, row in enumerate(rows_c)
               if row and "Campanha" in row[0] and len(row) > 1 and "Status" in row[1]), None)
if h_idx is not None:
    for row in rows_c[h_idx + 1:]:
        if not row or not row[0].strip(): continue
        try:
            status = row[1].strip().lower()
            if status not in ("ativa", "ativo", "enabled", "active"): continue
            gasto  = float(row[4].replace(",", ".").replace("\xa0", ""))
            cliques= int(float(row[6].replace(",", ".").replace("\xa0", "")))
            conv   = int(float(row[9].replace(",", ".").replace("\xa0", "")))
            cpa    = float(row[10].replace(",", ".").replace("\xa0", "")) if conv > 0 else 0
            cor    = "green" if (conv > 0 and cpa < GOOGLE_CPA_BOM) else "yellow" if (conv > 0 and cpa < GOOGLE_CPA_OK) else "red" if conv > 0 else "yellow"
            nome   = clean_camp(row[0].strip())
            CAMPANHAS.append({"nome": nome, "desc": "Campanha ativa", "gasto": gasto,
                              "cliques": cliques, "contatos": conv, "cpa": cpa, "cor": cor})
        except Exception:
            continue

print(f"  Campanhas Google ativas: {len(CAMPANHAS)}")

# ══════════════════════════════════════════════════════════════════════
# 3. JSON + DATAS
# ══════════════════════════════════════════════════════════════════════
meta_first   = META[0][0] if META else since_str
google_first = GOOGLE[0][0] if GOOGLE else ""
min_date     = min(meta_first, google_first) if (META and GOOGLE) else (meta_first if META else google_first)
max_date     = meta_last if meta_last else (google_last if google_last else today_str)
camp_label   = f"{month_start[8:10]}/{month_start[5:7]} — {today_str[8:10]}/{today_str[5:7]}"
gcampmonth_label = (f"{google_first[8:10]}/{google_first[5:7]} — {google_last[8:10]}/{google_last[5:7]}"
                    if GOOGLE and google_last else "—")

meta_js        = [{"d":d,"s":s,"r":r,"i":imp,"c":cv,"lk":lk,"v25":v25,"v50":v50,"v75":v75,"v95":v95}
                  for d,s,r,imp,cv,lk,v25,v50,v75,v95 in META]
google_js      = [{"d":d,"s":s,"imp":imp,"cl":cl,"cv":cv} for d,s,imp,cl,cv in GOOGLE]
meta_json      = json.dumps(meta_js,        separators=(",", ":"))
google_json    = json.dumps(google_js,      separators=(",", ":"))
camp_json      = json.dumps(CAMPANHAS,      separators=(",", ":"))
meta_camp_json = json.dumps(META_CAMPANHAS, separators=(",", ":"))

_google_first_fmt = f"{google_first[8:10]}/{google_first[5:7]}" if google_first else "—"

# ══════════════════════════════════════════════════════════════════════
# 4. HTML
# ══════════════════════════════════════════════════════════════════════
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
  .card-google{{border-left:4px solid #00ff88!important}}
  .card-resumo{{border-left:4px solid #a855f7!important}}
  .title-meta{{color:#93c5fd!important}}
  .title-google{{color:#4ade80!important}}
  .title-resumo{{color:#c084fc!important}}
  .neon{{color:#00ff88}}
  .badge-blue{{background:rgba(59,130,246,.12);color:#60a5fa;border:1px solid rgba(59,130,246,.25);border-radius:6px}}
  .badge-green{{background:rgba(0,255,136,.1);color:#00ff88;border:1px solid rgba(0,255,136,.25);border-radius:6px}}
  .btn-period{{padding:6px 14px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;border:1px solid #2a2a2a;background:#161616;color:#9ca3af;transition:all .15s}}
  .btn-period:hover{{border-color:#444;color:#e5e7eb}}
  .btn-period.active{{background:rgba(0,255,136,.12);border-color:rgba(0,255,136,.4);color:#00ff88}}
  .flatpickr-input{{background:#161616!important;border:1px solid #2a2a2a!important;color:#9ca3af!important;border-radius:8px!important;padding:6px 12px!important;font-size:12px!important;cursor:pointer!important;outline:none!important;min-width:210px}}
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
</style>
</head>
<body class="min-h-screen">
<div class="max-w-5xl mx-auto px-4 py-8">

<!-- Header -->
<div class="flex flex-col sm:flex-row sm:items-start sm:justify-between mb-6 gap-3">
  <div>
    <h1 class="text-3xl font-bold text-white tracking-widest uppercase">{CLIENTE_NOME}</h1>
    <p class="text-xs text-gray-600 mt-1">E-commerce · USA</p>
  </div>
  <div class="text-left sm:text-right">
    <p class="text-xs text-gray-600">Dados ate <span class="text-gray-400 font-medium">{yesterday.strftime('%d/%m/%Y')}</span></p>
    <div class="flex sm:justify-end gap-1 mt-2 flex-wrap">
      <span class="badge-blue text-xs px-2 py-0.5 inline-flex items-center gap-1"><img src="https://cdn.simpleicons.org/meta/1877F2" width="12" height="12" alt="Meta"/> Meta Ads</span>
      <span class="badge-green text-xs px-2 py-0.5 inline-flex items-center gap-1">{GADS_ICON} Google Ads</span>
    </div>
  </div>
</div>

<!-- Period Selector -->
<div class="card p-4 mb-6">
  <div class="flex flex-wrap items-center gap-2">
    <span class="text-xs text-gray-500 mr-1">Periodo:</span>
    <button class="btn-period" onclick="setPeriod('month')" id="btn-month">Este mes</button>
    <button class="btn-period" onclick="setPeriod('7')"     id="btn-7">7 dias</button>
    <button class="btn-period" onclick="setPeriod('14')"    id="btn-14">14 dias</button>
    <button class="btn-period" onclick="setPeriod('30')"    id="btn-30">30 dias</button>
    <button class="btn-period" onclick="setPeriod('90')"    id="btn-90">90 dias</button>
    <input type="text" id="dt-range" placeholder="Escolher datas no calendario..." readonly class="ml-auto"/>
  </div>
</div>

<!-- Today Warning -->
<div id="today-warning" class="hidden card p-10 mb-6 flex flex-col items-center justify-center text-center gap-4">
  <span style="font-size:40px;line-height:1">⚠️</span>
  <p class="text-amber-400 font-bold text-xl">Periodo inclui hoje</p>
  <p class="text-gray-400 text-sm max-w-sm">Os dados de hoje ainda estao incompletos.<br/>Selecione um periodo ate <strong class="text-white">{yesterday.strftime('%d/%m')}</strong> (ontem) para ver os numeros finais.</p>
</div>

<!-- Data Content -->
<div id="data-content">

<!-- RESUMO GERAL -->
<div class="card card-resumo p-5 mb-4">
  <p class="sec-title title-resumo">Resumo Geral — Meta + Google</p>
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
    <div><p class="kpi-label">Investimento Total</p><p class="kpi-val neon" id="tot-spend">—</p><p class="kpi-sub">Meta + Google</p></div>
    <div><p class="kpi-label">Compras</p><p class="kpi-val neon" id="tot-leads">—</p><p class="kpi-sub">pedidos (Meta + Google)</p></div>
    <div><p class="kpi-label">Custo por Compra</p><p class="kpi-val text-white" id="tot-cpl">—</p><p class="kpi-sub">medio geral</p></div>
    <div><p class="kpi-label">Alcance</p><p class="kpi-val text-white" id="tot-reach">—</p><p class="kpi-sub">pessoas unicas (Meta)</p></div>
  </div>
</div>

<!-- Chart -->
<div class="card card-resumo p-5 mb-6">
  <h2 class="sec-title">Investimento diario + Compras</h2>
  <canvas id="mainChart" height="90"></canvas>
  <p class="text-xs text-gray-700 mt-2">* Google disponivel a partir de {_google_first_fmt} (linha verde empilhada).</p>
</div>

<!-- META ADS -->
<div class="card card-meta p-5 mb-4">
  <div class="sec-title title-meta flex items-center gap-2 mb-3">
    <img src="https://cdn.simpleicons.org/meta/1877F2" width="18" height="18" alt="Meta"/>
    Meta Ads — Facebook &amp; Instagram
    <span class="date-pill" id="meta-date-label"></span>
  </div>
  <div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
    <div><p class="kpi-label">Alcance</p><p class="kpi-val text-white" id="m-reach">—</p><p class="kpi-sub">pessoas unicas</p></div>
    <div><p class="kpi-label">Impressoes</p><p class="kpi-val text-white" id="m-imp">—</p><p class="kpi-sub">exibicoes totais</p></div>
    <div><p class="kpi-label">CPM</p><p class="kpi-val text-white" id="m-cpm">—</p><p class="kpi-sub">custo por 1.000</p></div>
    <div><p class="kpi-label">CTR</p><p class="kpi-val text-white" id="m-ctr">—</p><p class="kpi-sub">taxa de clique</p></div>
    <div><p class="kpi-label">Investimento</p><p class="kpi-val neon" id="m-spend">—</p><p class="kpi-sub">total no periodo</p></div>
  </div>
  <div class="border-t border-gray-800 my-3"></div>
  <div class="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
    <div><p class="kpi-label">Cliques</p><p class="kpi-val text-white" id="m-clicks">—</p><p class="kpi-sub">em links</p></div>
    <div><p class="kpi-label">CPC</p><p class="kpi-val text-white" id="m-cpc">—</p><p class="kpi-sub">custo por clique</p></div>
    <div><p class="kpi-label">Compras</p><p class="kpi-val neon" id="m-conv">—</p><p class="kpi-sub">pedidos realizados</p></div>
    <div><p class="kpi-label">Custo / Compra</p><p class="kpi-val text-white" id="m-cpl">—</p><p class="kpi-sub">por pedido</p></div>
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
<div class="card card-meta p-5 mb-4">
  <div class="sec-title title-meta flex items-center gap-2 mb-3">
    <img src="https://cdn.simpleicons.org/meta/1877F2" width="18" height="18" alt="Meta"/>
    Campanhas Meta
    <span class="date-pill">{camp_label}</span>
  </div>
  <div class="overflow-x-auto">
    <table class="w-full text-sm">
      <thead><tr class="text-xs text-gray-600 border-b border-gray-800">
        <th class="text-left pb-3 pr-4">Campanha</th>
        <th class="text-right pb-3 pr-4">Investido</th>
        <th class="text-right pb-3 pr-4">Alcance</th>
        <th class="text-right pb-3 pr-4">Cliques</th>
        <th class="text-right pb-3 pr-4">Compras</th>
        <th class="text-right pb-3">Custo/Compra</th>
      </tr></thead>
      <tbody id="meta-camp-body"></tbody>
    </table>
  </div>
</div>

<!-- GOOGLE ADS -->
<div class="card card-google p-5 mb-4">
  <div class="sec-title title-google flex items-center gap-2 mb-3">
    {GADS_ICON}Google Ads — Pesquisa
    <span class="date-pill" id="google-date-label"></span>
  </div>
  <div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
    <div><p class="kpi-label">Impressoes</p><p class="kpi-val text-white" id="g-imp">—</p><p class="kpi-sub">vezes exibido</p></div>
    <div><p class="kpi-label">CTR</p><p class="kpi-val text-white" id="g-ctr">—</p><p class="kpi-sub">taxa de clique</p></div>
    <div><p class="kpi-label">Cliques</p><p class="kpi-val text-white" id="g-clicks">—</p><p class="kpi-sub">visitas ao site</p></div>
    <div><p class="kpi-label">CPC</p><p class="kpi-val text-white" id="g-cpc">—</p><p class="kpi-sub">custo por clique</p></div>
    <div><p class="kpi-label">Investimento</p><p class="kpi-val neon" id="g-spend">—</p><p class="kpi-sub">total no periodo</p></div>
  </div>
  <div class="border-t border-gray-800 my-3"></div>
  <div class="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
    <div><p class="kpi-label">Conversoes</p><p class="kpi-val neon" id="g-conv">—</p><p class="kpi-sub">contatos gerados</p></div>
    <div><p class="kpi-label">CPA</p><p class="kpi-val text-white" id="g-cpa">—</p><p class="kpi-sub">custo por conversao</p></div>
    <div><p class="kpi-label">Taxa de Conv.</p><p class="kpi-val text-white" id="g-taxa">—</p><p class="kpi-sub">cliques → conversao</p></div>
    <div><p class="kpi-label">Investimento</p><p class="kpi-val text-gray-600" id="g-fat">—</p><p class="kpi-sub">ver coluna ao lado</p></div>
  </div>
  <p class="text-xs text-gray-600 mt-4 italic" id="google-note"></p>
</div>

<!-- GOOGLE CAMPANHAS -->
<div class="card card-google p-5 mb-6">
  <div class="sec-title title-google flex items-center gap-2 mb-3">
    {GADS_ICON}Campanhas Google
    <span class="date-pill">{gcampmonth_label}</span>
  </div>
  <div class="overflow-x-auto">
    <table class="w-full text-sm">
      <thead><tr class="text-xs text-gray-600 border-b border-gray-800">
        <th class="text-left pb-3 pr-4">Campanha</th>
        <th class="text-right pb-3 pr-4">Investido</th>
        <th class="text-right pb-3 pr-4">Cliques</th>
        <th class="text-right pb-3 pr-4">Conversoes</th>
        <th class="text-right pb-3">Custo/conversao</th>
      </tr></thead>
      <tbody id="camp-body"></tbody>
    </table>
  </div>
  <p class="text-xs text-gray-600 mt-3" id="camp-note"></p>
</div>

<div class="text-center text-xs text-gray-700 py-4">Atualizado em {today.strftime('%d/%m/%Y')}</div>

</div><!-- /data-content -->
</div><!-- /max-w-5xl -->

<script>
const META_DATA      = {meta_json};
const GOOGLE_DATA    = {google_json};
const CAMPANHAS      = {camp_json};
const META_CAMPANHAS = {meta_camp_json};

let chart = null;
const USD  = n => '$ ' + n.toFixed(2);
const PCT  = n => n.toFixed(2) + '%';
const FMT  = n => n >= 1000000 ? (n/1000000).toFixed(1)+'M' : n >= 1000 ? (n/1000).toFixed(1)+'K' : String(n);
const fmtD = s => {{ const [y,m,d]=s.split('-'); return d+'/'+m; }};

function filterM(from,to){{return META_DATA.filter(x=>x.d>=from&&x.d<=to);}}
function filterG(from,to){{return GOOGLE_DATA.filter(x=>x.d>=from&&x.d<=to);}}
function set(id,val){{const el=document.getElementById(id);if(el)el.textContent=val;}}

function update(from,to){{
  const todayWarn  = document.getElementById('today-warning');
  const dataContent= document.getElementById('data-content');
  if(to >= TODAY_STR){{
    todayWarn.classList.remove('hidden');
    dataContent.classList.add('hidden');
    return;
  }}
  todayWarn.classList.add('hidden');
  dataContent.classList.remove('hidden');

  const mR = filterM(from,to);
  const gEffTo  = (GOOGLE_LAST && to   > GOOGLE_LAST) ? GOOGLE_LAST : to;
  const gEffFrom= (GOOGLE_LAST && from > GOOGLE_LAST) ? GOOGLE_LAST : from;
  const gR = filterG(gEffFrom, gEffTo);

  // Meta totais
  const mSpend=mR.reduce((a,x)=>a+x.s,0);
  const mReach=mR.reduce((a,x)=>a+x.r,0);
  const mImp  =mR.reduce((a,x)=>a+x.i,0);
  const mConv =mR.reduce((a,x)=>a+x.c,0);
  const mClk  =mR.reduce((a,x)=>a+x.lk,0);
  const mCPM  =mImp>0?mSpend/mImp*1000:0;
  const mCTR  =mImp>0?mClk/mImp*100:0;
  const mCPC  =mClk>0?mSpend/mClk:0;
  const mCPL  =mConv>0?mSpend/mConv:0;

  const vRows=mR.filter(x=>x.v25>0);
  const v25=vRows.reduce((a,x)=>a+x.v25,0);
  const v50=vRows.reduce((a,x)=>a+x.v50,0);
  const v75=vRows.reduce((a,x)=>a+x.v75,0);
  const v95=vRows.reduce((a,x)=>a+x.v95,0);
  const hasVideo=v25>0;

  set('m-reach', FMT(mReach));
  set('m-imp',   FMT(mImp));
  set('m-cpm',   USD(mCPM));
  set('m-ctr',   PCT(mCTR));
  set('m-spend', USD(mSpend));
  set('m-clicks',mClk);
  set('m-cpc',   mClk>0?USD(mCPC):'—');
  set('m-conv',  mConv);
  set('m-cpl',   mConv>0?USD(mCPL):'—');
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

  // Google totais
  const gSpend =gR.reduce((a,x)=>a+x.s,0);
  const gImp   =gR.reduce((a,x)=>a+x.imp,0);
  const gClicks=gR.reduce((a,x)=>a+x.cl,0);
  const gConv  =gR.reduce((a,x)=>a+x.cv,0);
  const gCPA   =gConv>0?gSpend/gConv:0;
  const gCPC   =gClicks>0?gSpend/gClicks:0;
  const gCTR   =gImp>0?gClicks/gImp*100:0;
  const gTaxa  =gClicks>0?gConv/gClicks*100:0;
  const hasG   =gR.length>0&&gSpend>0;

  set('g-imp',    hasG?FMT(gImp):'—');
  set('g-ctr',    hasG&&gImp>0?PCT(gCTR):'—');
  set('g-clicks', hasG?gClicks:'—');
  set('g-cpc',    hasG&&gClicks>0?USD(gCPC):'—');
  set('g-spend',  hasG?USD(gSpend):'—');
  set('g-conv',   hasG?gConv:'—');
  set('g-cpa',    hasG&&gConv>0?USD(gCPA):'—');
  set('g-taxa',   hasG&&gClicks>0?PCT(gTaxa):'—');
  set('g-fat',    '—');
  set('google-note', hasG
    ? 'ROAS e Faturamento indisponiveis — rastreamento de receita via Shopify nao conectado ao relatorio.'
    : 'Sem dados Google no periodo selecionado.');

  const gDateLabel = (GOOGLE_LAST && to > GOOGLE_LAST)
    ? fmtD(gEffFrom)+' — '+fmtD(GOOGLE_LAST)+' (atraso 1 dia)' : fmtD(from)+' — '+fmtD(to);
  set('google-date-label', hasG ? gDateLabel : gDateLabel+' (sem dados)');

  // Resumo Geral
  const totSpend=mSpend+gSpend;
  const totLeads=mConv+gConv;
  set('tot-spend', USD(totSpend));
  set('tot-leads', totLeads>0?totLeads:'—');
  set('tot-cpl',   totLeads>0?USD(totSpend/totLeads):'—');
  set('tot-reach', FMT(mReach));

  // Grafico
  const allDates=[...new Set([...mR.map(x=>x.d),...gR.map(x=>x.d)])].sort();
  const mByD=Object.fromEntries(mR.map(x=>[x.d,x]));
  const gByD=Object.fromEntries(gR.map(x=>[x.d,x]));
  const labels  =allDates.map(fmtD);
  const dsMeta  =allDates.map(d=>mByD[d]?mByD[d].s:0);
  const dsGoogle=allDates.map(d=>gByD[d]?gByD[d].s:0);
  const dsConv  =allDates.map(d=>mByD[d]?mByD[d].c:0);

  if(chart)chart.destroy();
  Chart.defaults.color='#6b7280';
  chart=new Chart(document.getElementById('mainChart').getContext('2d'),{{
    data:{{
      labels,
      datasets:[
        {{type:'bar',label:'Meta ($)',data:dsMeta,backgroundColor:'rgba(59,130,246,.2)',borderColor:'#60a5fa',borderWidth:1,borderRadius:3,stack:'sp',yAxisID:'y'}},
        {{type:'bar',label:'Google ($)',data:dsGoogle,backgroundColor:'rgba(0,255,136,.18)',borderColor:'#00ff88',borderWidth:1,borderRadius:3,stack:'sp',yAxisID:'y'}},
        {{type:'line',label:'Compras',data:dsConv,borderColor:'#f59e0b',backgroundColor:'rgba(245,158,11,.1)',borderWidth:2,pointRadius:allDates.length>30?2:4,tension:.3,fill:false,yAxisID:'y1'}}
      ]
    }},
    options:{{
      responsive:true,
      interaction:{{mode:'index',intersect:false}},
      plugins:{{
        legend:{{labels:{{color:'#9ca3af',font:{{size:11}},padding:16}}}},
        tooltip:{{backgroundColor:'#1a1a1a',borderColor:'#333',borderWidth:1,titleColor:'#e5e7eb',bodyColor:'#9ca3af',
          callbacks:{{label:c=>c.dataset.label==='Compras'?'  Compras: '+c.parsed.y:'  '+c.dataset.label+': $ '+c.parsed.y.toFixed(2)}}}}
      }},
      scales:{{
        x:{{stacked:true,grid:{{color:'#1a1a1a'}},ticks:{{color:'#6b7280',font:{{size:10}},maxRotation:45}}}},
        y:{{stacked:true,grid:{{color:'#1a1a1a'}},ticks:{{color:'#6b7280',font:{{size:10}},callback:v=>'$ '+v}}}},
        y1:{{position:'right',grid:{{drawOnChartArea:false}},ticks:{{color:'#fbbf24',font:{{size:10}},callback:v=>v+' comp.'}}}}
      }}
    }}
  }});

  // Campanhas Google
  const body=document.getElementById('camp-body');
  body.innerHTML='';
  let noteText='';
  CAMPANHAS.forEach(c=>{{
    const cc=c.cor==='green'?'tag-green':c.cor==='yellow'?'tag-yellow':'tag-red';
    body.innerHTML+=`<tr class="border-b border-gray-800/50">
      <td class="py-3 pr-4"><p class="text-white font-medium">${{c.nome}}</p><p class="text-xs text-gray-500">${{c.desc}}</p></td>
      <td class="py-3 pr-4 text-right text-gray-300">$ ${{c.gasto.toFixed(2)}}</td>
      <td class="py-3 pr-4 text-right text-gray-300">${{c.cliques}}</td>
      <td class="py-3 pr-4 text-right font-bold text-white">${{c.contatos}}</td>
      <td class="py-3 text-right"><span class="${{cc}} font-semibold">${{c.cpa>0?'$ '+c.cpa.toFixed(2):'—'}}</span></td>
    </tr>`;
    if(c.cor==='red'&&c.cpa>0)noteText='Atencao: "'+c.nome+'" com custo elevado ($ '+c.cpa.toFixed(2)+').';
  }});
  document.getElementById('camp-note').textContent=noteText;

  // Campanhas Meta
  const mbody=document.getElementById('meta-camp-body');
  mbody.innerHTML='';
  META_CAMPANHAS.forEach(c=>{{
    const cc=c.cor==='green'?'tag-green':c.cor==='blue'?'text-blue-400':c.cor==='yellow'?'tag-yellow':'tag-red';
    const cpaStr =c.conv>0?`<span class="${{cc}} font-semibold">$ ${{c.cpa.toFixed(2)}}</span>`:`<span class="text-gray-600">—</span>`;
    const convStr=c.conv>0?`<span class="font-bold text-white">${{c.conv}}</span>`:`<span class="text-gray-600">—</span>`;
    mbody.innerHTML+=`<tr class="border-b border-gray-800/50">
      <td class="py-3 pr-4"><p class="text-white font-medium">${{c.nome}}</p><p class="text-xs text-gray-500">${{c.desc}}</p></td>
      <td class="py-3 pr-4 text-right text-gray-300">$ ${{c.gasto.toFixed(2)}}</td>
      <td class="py-3 pr-4 text-right text-gray-300">${{c.alcance.toLocaleString('en-US')}}</td>
      <td class="py-3 pr-4 text-right text-gray-300">${{c.cliques}}</td>
      <td class="py-3 pr-4 text-right">${{convStr}}</td>
      <td class="py-3 text-right">${{cpaStr}}</td>
    </tr>`;
  }});
}}

function clearActive(){{['month','7','14','30','90'].forEach(id=>document.getElementById('btn-'+id).classList.remove('active'));}}
function isoToDate(s){{const[y,m,d]=s.split('-');return new Date(+y,+m-1,+d);}}
function dateToIso(dt){{return dt.getFullYear()+'-'+String(dt.getMonth()+1).padStart(2,'0')+'-'+String(dt.getDate()).padStart(2,'0');}}

const MAX_DATE      = '{max_date}';
const MIN_DATE      = '{min_date}';
const GOOGLE_LAST   = '{google_last}';
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
setPeriod('month');
</script>
</body>
</html>"""

# ══════════════════════════════════════════════════════════════════════
# 5. SALVAR + DEPLOY
# ══════════════════════════════════════════════════════════════════════
out_dir  = os.path.join(DEPLOY_DIR, CF_SUBFOLDER)
out_file = os.path.join(out_dir, "index.html")
os.makedirs(out_dir, exist_ok=True)

assets_src = os.path.join(DEPLOY_DIR, "assets")
assets_dst = os.path.join(out_dir, "assets")
if os.path.isdir(assets_src):
    shutil.copytree(assets_src, assets_dst, dirs_exist_ok=True)

with open(out_file, "w", encoding="utf-8") as f:
    f.write(HTML)
print(f"\nHTML gerado: {out_file}")

print("\nDeployando para Cloudflare Pages...")
env_deploy = os.environ.copy()
env_deploy["CLOUDFLARE_API_TOKEN"]  = CF_TOKEN
env_deploy["CLOUDFLARE_ACCOUNT_ID"] = CF_ACCOUNT

result = subprocess.run(
    f'npx wrangler pages deploy "{out_dir}" --project-name={CF_PROJECT} --branch=main --commit-dirty=true',
    env=env_deploy, capture_output=False, shell=True, cwd=DEPLOY_DIR, timeout=120
)
if result.returncode == 0:
    print(f"\n✅ URL: https://formuladolucro.site/{CF_SUBFOLDER}/")
    print(f"   Alt:  https://veritasdigital.pages.dev/{CF_SUBFOLDER}/")
else:
    print(f"Erro no deploy (rc={result.returncode})")
