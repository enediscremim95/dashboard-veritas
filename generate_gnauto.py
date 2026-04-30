"""
GN Auto Dashboard — geração + deploy em controle.gnauto.com.br
Roda: python generate_gnauto.py

Grupo Negocia Auto · Carros antes do leilão
Paleta: fundo #080808, vermelho #E8280A, laranja #FF6B1A (igual ao site gnauto.com.br)
Fontes: Barlow Condensed (títulos) + Sora (corpo)
"""
import json, os, io, csv, re, subprocess, requests, shutil
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# ══════════════════════════════════════════════════════════════════════
#  CONFIG — preencher quando tiver as contas
# ══════════════════════════════════════════════════════════════════════
CLIENTE_NOME  = "GN Auto"
CLIENTE_SUB   = "Grupo Negocia Auto · Brasil"

HAS_META   = False   # mudar pra True quando tiver conta Meta
HAS_GOOGLE = False   # mudar pra True quando tiver conta Google Ads

META_ACT   = ""      # ex: "act_XXXXXXXXX"
SHEET_ID   = ""      # ID da planilha Google Ads

CONV_ACTION   = "onsite_conversion.total_messaging_connection"
CONV_LABEL    = "Conversas WhatsApp"
CONV_UNIT     = "conversas iniciadas"
CONV_CPL_LBL  = "Custo / Conversa"
CONV_CPL_UNIT = "por conversa"

CURRENCY_SYM  = "R$"

META_CPA_BOM   =  20.0
META_CPA_OK    =  45.0
GOOGLE_CPA_BOM =  20.0
GOOGLE_CPA_OK  =  45.0

# Deploy em projeto separado (não no veritasdigital)
CF_PROJECT_GNAUTO = "gnauto-controle"
GNAUTO_ZONE_ID    = "399d7f611e8083f670719520bacf39f9"
SUBDOMAIN         = "controle.gnauto.com.br"
# ══════════════════════════════════════════════════════════════════════

META_TOKEN = os.getenv("META_ACCESS_TOKEN")
CF_TOKEN   = os.getenv("CF_TOKEN")
CF_ACCOUNT = os.getenv("CF_ACCOUNT")
DEPLOY_DIR = os.path.dirname(os.path.abspath(__file__))

META_API     = "https://graph.facebook.com/v22.0"
GRAPH_FIELDS = "date_start,spend,reach,impressions,clicks,actions,action_values"
VIDEO_FIELDS = "date_start,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions"

GADS_ICON = '<svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" width="18" height="18"><path fill="#34A853" d="M3.9998 22.9291C1.7908 22.9291 0 21.1383 0 18.9293s1.7908-3.9998 3.9998-3.9998 3.9998 1.7908 3.9998 3.9998-1.7908 3.9998-3.9998 3.9998z"/><path fill="#4285F4" d="M23.4641 16.9287L15.4632 3.072C14.3586 1.1587 11.9121.5028 9.9988 1.6074S7.4295 5.1585 8.5341 7.0718l8.0009 13.8567c1.1046 1.9133 3.5511 2.5679 5.4644 1.4646 1.9134-1.1046 2.568-3.5511 1.4647-5.4644z"/><path fill="#FBBC04" d="M7.5137 4.8438L1.5645 15.1484A4.5 4.5 0 0 1 4 14.4297c2.5597-.0075 4.6248 2.1585 4.4941 4.7148l3.2168-5.5723-3.6094-6.25c-.4499-.7793-.6322-1.6394-.5878-2.4784z"/></svg>'

today         = date.today()
yesterday     = today - timedelta(days=1)
since_90      = today - timedelta(days=89)
since_str     = since_90.strftime("%Y-%m-%d")
today_str     = today.strftime("%Y-%m-%d")
yesterday_str = yesterday.strftime("%Y-%m-%d")
month_start   = today.replace(day=1).strftime("%Y-%m-%d")

# ══════════════════════════════════════════════════════════════════════
# 1. META ADS
# ══════════════════════════════════════════════════════════════════════
META            = []
CAMP_META_INFO  = {}
CAMP_DAILY_LIST = []
AD_DRILL        = {}
meta_last       = ""

if HAS_META and META_ACT:
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

    def action_float(vals, key):
        for a in (vals or []):
            if a["action_type"] == key: return float(a["value"])
        return 0.0

    _NOISE = {"site antigo", "site", "antigo", "novo teste", "teste"}
    CLIENTE_SLUG = "GN Auto"

    def clean_camp(raw):
        brackets = re.findall(r'\[([^\]]+)\]', raw)
        useful = [b.strip() for b in brackets if b.strip().lower() not in _NOISE]
        if useful: return " / ".join(useful)
        n = re.sub(r'\[.*?\]|\d{2}/\d{2}/\d{4}|#\d+', '', raw)
        n = re.sub(r'\s{2,}', ' ', n).strip(' -').strip()
        n = re.sub(rf'^{re.escape(CLIENTE_SLUG)}\s*[-–\s]*', '', n, flags=re.IGNORECASE).strip(' -').strip()
        return n or raw[:35]

    since_30 = (today - timedelta(days=29)).strftime("%Y-%m-%d")
    raw_main  = meta_get(GRAPH_FIELDS, since_str, today_str)
    raw_video = meta_get(VIDEO_FIELDS, since_30, today_str)
    video_by_date = {r["date_start"]: r for r in raw_video}

    for row in sorted(raw_main, key=lambda x: x["date_start"]):
        d   = row["date_start"]
        s   = float(row.get("spend", 0))
        r   = int(row.get("reach", 0))
        imp = int(row.get("impressions", 0))
        conv= action_val(row.get("actions"), CONV_ACTION)
        lk  = action_val(row.get("actions"), "link_click")
        rv  = action_float(row.get("action_values"), CONV_ACTION)
        vd  = video_by_date.get(d, {})
        v25 = int(float((vd.get("video_p25_watched_actions") or [{}])[0].get("value", 0)))
        v50 = int(float((vd.get("video_p50_watched_actions") or [{}])[0].get("value", 0)))
        v75 = int(float((vd.get("video_p75_watched_actions") or [{}])[0].get("value", 0)))
        v95 = int(float((vd.get("video_p95_watched_actions") or [{}])[0].get("value", 0)))
        msg = action_val(row.get("actions"), "onsite_conversion.total_messaging_connection")
        if s > 0 or imp > 0:
            META.append((d, s, r, imp, conv, lk, rv, v25, v50, v75, v95, msg))

    meta_last = META[-1][0] if META else today_str

    def meta_get_camps(since, until):
        url = f"{META_API}/{META_ACT}/insights"
        params = {
            "fields": "campaign_id,campaign_name,spend,reach,impressions,clicks,actions,action_values",
            "level": "campaign", "time_increment": 1,
            "time_range": json.dumps({"since": since, "until": until}),
            "limit": 500, "access_token": META_TOKEN,
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

    _desc_map = {
        "RMKT":        "Remarketing — quem ja viu",
        "REMARKETING": "Remarketing — quem ja viu",
        "CONVERSAO":   "Conversao — objetivo WhatsApp",
        "CONVERSÃO":   "Conversao — objetivo WhatsApp",
        "DISTRIBUICAO":"Alcance para novo publico",
        "DISTRIBUIÇÃO":"Alcance para novo publico",
    }

    camp_raw = meta_get_camps(since_str, today_str)
    for row in camp_raw:
        cid  = row["campaign_id"]
        nome = clean_camp(row["campaign_name"])
        if cid not in CAMP_META_INFO:
            desc = next((v for k, v in _desc_map.items() if k in row["campaign_name"].upper()), "Campanha ativa")
            CAMP_META_INFO[cid] = {"nome": nome, "desc": desc}
        CAMP_DAILY_LIST.append({
            "d": row["date_start"], "id": cid,
            "s": float(row.get("spend", 0)), "r": int(row.get("reach", 0)),
            "lk": action_val(row.get("actions"), "link_click"),
            "c": action_val(row.get("actions"), CONV_ACTION),
            "rv": action_float(row.get("action_values"), CONV_ACTION),
        })

    # Criativos
    since_30_drill = (today - timedelta(days=29)).strftime("%Y-%m-%d")

    def meta_get_ad_insights(since, until):
        url = f"{META_API}/{META_ACT}/insights"
        params = {
            "fields": "ad_id,ad_name,adset_id,adset_name,campaign_id,spend,actions",
            "level": "ad",
            "time_range": json.dumps({"since": since, "until": until}),
            "limit": 500, "access_token": META_TOKEN,
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

    ad_insights = meta_get_ad_insights(since_30_drill, yesterday_str)
    for row in ad_insights:
        cid   = row.get("campaign_id", "")
        asetid= row.get("adset_id", "")
        aname = row.get("adset_name", "Conjunto")
        adid  = row.get("ad_id", "")
        adname= row.get("ad_name", "Anuncio")
        spend = float(row.get("spend", 0))
        conv  = action_val(row.get("actions"), CONV_ACTION)
        if cid not in AD_DRILL: AD_DRILL[cid] = {}
        if asetid not in AD_DRILL[cid]:
            AD_DRILL[cid][asetid] = {"name": aname, "spend": 0.0, "conv": 0, "ads": {}}
        AD_DRILL[cid][asetid]["spend"] += spend
        AD_DRILL[cid][asetid]["conv"]  += conv
        if adid not in AD_DRILL[cid][asetid]["ads"]:
            AD_DRILL[cid][asetid]["ads"][adid] = {"id": adid, "name": adname, "spend": 0.0, "conv": 0, "thumb": "", "preview": ""}
        AD_DRILL[cid][asetid]["ads"][adid]["spend"] += spend
        AD_DRILL[cid][asetid]["ads"][adid]["conv"]  += conv

    def meta_get_ads_meta():
        url = f"{META_API}/{META_ACT}/ads"
        params = {
            "fields": "id,name,adset_id,campaign_id,preview_shareable_link,creative{thumbnail_url,image_url}",
            "limit": 500, "access_token": META_TOKEN,
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

    ads_meta = meta_get_ads_meta()
    for row in ads_meta:
        adid   = row.get("id", "")
        cid    = row.get("campaign_id", "")
        asetid = row.get("adset_id", "")
        preview = row.get("preview_shareable_link", "")
        creative = row.get("creative", {})
        thumb = creative.get("thumbnail_url") or creative.get("image_url") or ""
        if cid in AD_DRILL and asetid in AD_DRILL[cid] and adid in AD_DRILL[cid][asetid]["ads"]:
            AD_DRILL[cid][asetid]["ads"][adid]["thumb"]   = thumb
            AD_DRILL[cid][asetid]["ads"][adid]["preview"] = preview

    for cid in AD_DRILL:
        for asetid in AD_DRILL[cid]:
            AD_DRILL[cid][asetid]["ads"] = sorted(
                AD_DRILL[cid][asetid]["ads"].values(),
                key=lambda x: x["conv"], reverse=True
            )

    print(f"  Meta: {len(META)} dias | Campanhas: {len(CAMP_META_INFO)} | Criativos: {len(AD_DRILL)}")
else:
    print("HAS_META=False — pulando Meta Ads")

# ══════════════════════════════════════════════════════════════════════
# 2. GOOGLE ADS
# ══════════════════════════════════════════════════════════════════════
GOOGLE    = []
CAMPANHAS = []
google_last = ""

if HAS_GOOGLE and SHEET_ID:
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
                rv  = float(row[8].replace(",", ".").replace("\xa0", "")) if len(row) > 8 else 0.0
                if re.match(r"\d{4}-\d{2}-\d{2}", d):
                    GOOGLE.append((d, s, imp, cl, cv, rv))
            except Exception:
                continue

    GOOGLE.sort(key=lambda x: x[0])
    if GOOGLE: google_last = GOOGLE[-1][0]

    rows_c = fetch_sheet_csv("Campanhas")
    h_idx  = next((i for i, row in enumerate(rows_c)
                   if row and "Campanha" in row[0] and len(row) > 1 and "Status" in row[1]), None)
    if h_idx is not None:
        def clean_camp_g(raw):
            n = re.sub(r'\[.*?\]|\d{2}/\d{2}/\d{4}|#\d+', '', raw)
            return re.sub(r'\s{2,}', ' ', n).strip(' -').strip() or raw[:35]
        for row in rows_c[h_idx + 1:]:
            if not row or not row[0].strip(): continue
            try:
                status = row[1].strip().lower()
                if status not in ("ativa", "ativo", "enabled", "active"): continue
                gasto  = float(row[4].replace(",", ".").replace("\xa0", ""))
                cliques= int(float(row[6].replace(",", ".").replace("\xa0", "")))
                conv   = float(row[9].replace(",", ".").replace("\xa0", ""))
                cpa    = float(row[10].replace(",", ".").replace("\xa0", "")) if conv > 0 else 0
                cor    = "green" if (conv > 0 and cpa < GOOGLE_CPA_BOM) else "yellow" if (conv > 0 and cpa < GOOGLE_CPA_OK) else "red" if conv > 0 else "yellow"
                nome   = clean_camp_g(row[0].strip())
                CAMPANHAS.append({"nome": nome, "desc": "Campanha ativa", "gasto": gasto,
                                  "cliques": cliques, "contatos": conv, "cpa": cpa, "cor": cor})
            except Exception:
                continue
    print(f"  Google: {len(GOOGLE)} dias | Campanhas ativas: {len(CAMPANHAS)}")
else:
    print("HAS_GOOGLE=False — pulando Google Ads")

# ══════════════════════════════════════════════════════════════════════
# 3. JSON + DATAS
# ══════════════════════════════════════════════════════════════════════
meta_first   = META[0][0] if META else since_str
google_first = GOOGLE[0][0] if GOOGLE else ""
min_date     = min(meta_first, google_first) if (META and GOOGLE) else (meta_first if META else google_first)
if not min_date: min_date = since_str
max_date     = meta_last if meta_last else (google_last if google_last else yesterday_str)
gcampmonth_label = (f"{google_first[8:10]}/{google_first[5:7]} — {google_last[8:10]}/{google_last[5:7]}"
                    if GOOGLE and google_last else "—")

meta_js        = [{"d":d,"s":s,"r":r,"i":imp,"c":cv,"lk":lk,"rv":rv,"v25":v25,"v50":v50,"v75":v75,"v95":v95,"msg":msg}
                  for d,s,r,imp,cv,lk,rv,v25,v50,v75,v95,msg in META]
google_js      = [{"d":d,"s":s,"imp":imp,"cl":cl,"cv":cv} for d,s,imp,cl,cv,rv in GOOGLE]
meta_json      = json.dumps(meta_js,        separators=(",", ":"))
google_json    = json.dumps(google_js,      separators=(",", ":"))
camp_json      = json.dumps(CAMPANHAS,      separators=(",", ":"))
camp_meta_json = json.dumps(CAMP_META_INFO, separators=(",", ":"))
camp_daily_json= json.dumps(CAMP_DAILY_LIST,separators=(",", ":"))
ad_drill_json  = json.dumps(AD_DRILL,       separators=(",", ":"))

_google_first_fmt = f"{google_first[8:10]}/{google_first[5:7]}" if google_first else "—"

# Badge de status
meta_badge = "" if not HAS_META else '<span class="badge-red text-xs px-2 py-0.5 inline-flex items-center gap-1"><img src="https://cdn.simpleicons.org/meta/ffffff" width="12" height="12" alt="Meta"/> Meta Ads</span>'
google_badge = "" if not HAS_GOOGLE else f'<span class="badge-orange text-xs px-2 py-0.5 inline-flex items-center gap-1">{GADS_ICON} Google Ads</span>'
pending_badge = "" if (HAS_META or HAS_GOOGLE) else '<span class="badge-pending text-xs px-2 py-0.5">Em implantacao</span>'

# ══════════════════════════════════════════════════════════════════════
# 4. HTML — paleta GN Auto
# ══════════════════════════════════════════════════════════════════════
HTML = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>{CLIENTE_NOME} — Painel de Performance</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700;800;900&family=Sora:wght@300;400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/themes/dark.css">
<script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
<script src="https://cdn.jsdelivr.net/npm/flatpickr/dist/l10n/pt.js"></script>
<style>
  :root{{
    --red:#E8280A;--red2:#C41F06;--orange:#FF6B1A;
    --bg:#080808;--bg2:#111111;--bg3:#181818;
    --border:#242424;--text:#F0EDE8;--muted:#888;
    --font-display:'Barlow Condensed',sans-serif;
    --font-body:'Sora',sans-serif;
  }}
  *{{box-sizing:border-box}}
  body{{background:var(--bg);color:var(--text);font-family:var(--font-body);min-height:100vh;overflow-x:hidden;line-height:1.6}}
  h1{{font-family:var(--font-display);letter-spacing:.06em;text-transform:uppercase}}
  .card{{background:var(--bg2);border:1px solid var(--border);border-radius:12px}}
  .card-meta{{border-left:4px solid var(--red)!important}}
  .card-google{{border-left:4px solid var(--orange)!important}}
  .card-resumo{{border-left:4px solid var(--red)!important;background:linear-gradient(135deg,#110808 0%,var(--bg2) 60%)!important}}
  .title-meta{{color:#ff8a78!important;font-family:var(--font-display);letter-spacing:.04em;font-size:15px!important}}
  .title-google{{color:#ffaa66!important;font-family:var(--font-display);letter-spacing:.04em;font-size:15px!important}}
  .title-resumo{{color:#ff8a78!important;font-family:var(--font-display);letter-spacing:.04em;font-size:15px!important}}
  .accent{{color:var(--red)}}
  .badge-red{{background:rgba(232,40,10,.12);color:#ff6b50;border:1px solid rgba(232,40,10,.3);border-radius:6px}}
  .badge-orange{{background:rgba(255,107,26,.12);color:#ffaa66;border:1px solid rgba(255,107,26,.3);border-radius:6px}}
  .badge-pending{{background:rgba(255,255,255,.05);color:#666;border:1px solid #333;border-radius:6px}}
  .btn-period{{padding:6px 14px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;border:1px solid var(--border);background:var(--bg3);color:var(--muted);transition:all .15s;font-family:var(--font-body)}}
  .btn-period:hover{{border-color:#444;color:var(--text)}}
  .btn-period.active{{background:rgba(232,40,10,.12);border-color:rgba(232,40,10,.4);color:var(--red)}}
  .flatpickr-input{{background:var(--bg3)!important;border:1px solid var(--border)!important;color:var(--muted)!important;border-radius:8px!important;padding:6px 12px!important;font-size:12px!important;cursor:pointer!important;outline:none!important;width:100%;box-sizing:border-box;font-family:var(--font-body)}}
  .flatpickr-input:focus{{border-color:#444!important}}
  .flatpickr-calendar{{background:#0e0e0e!important;border:1px solid var(--border)!important;box-shadow:0 8px 32px rgba(0,0,0,.8)!important}}
  .flatpickr-day{{color:var(--muted)!important}}
  .flatpickr-day:hover{{background:#1a1a1a!important;border-color:#333!important}}
  .flatpickr-day.selected,.flatpickr-day.startRange,.flatpickr-day.endRange{{background:var(--red)!important;border-color:var(--red)!important;color:#fff!important}}
  .flatpickr-day.inRange{{background:rgba(232,40,10,.12)!important;border-color:transparent!important;color:var(--red)!important;box-shadow:-5px 0 0 rgba(232,40,10,.12),5px 0 0 rgba(232,40,10,.12)!important}}
  .flatpickr-day.today{{border-color:#333!important}}
  .flatpickr-day.flatpickr-disabled{{color:#333!important}}
  .flatpickr-months .flatpickr-month,.flatpickr-weekdays,.span.flatpickr-weekday{{background:#0e0e0e!important;color:var(--muted)!important;fill:var(--muted)!important}}
  .flatpickr-weekday{{color:#555!important}}
  .flatpickr-prev-month,.flatpickr-next-month{{color:var(--text)!important;fill:var(--text)!important;opacity:1!important}}
  .flatpickr-prev-month svg,.flatpickr-next-month svg{{fill:var(--text)!important}}
  .flatpickr-prev-month svg path,.flatpickr-next-month svg path{{fill:var(--text)!important;stroke:var(--text)!important}}
  .flatpickr-prev-month:hover,.flatpickr-next-month:hover{{color:var(--red)!important;fill:var(--red)!important}}
  .flatpickr-prev-month:hover svg path,.flatpickr-next-month:hover svg path{{fill:var(--red)!important;stroke:var(--red)!important}}
  .flatpickr-current-month .flatpickr-monthDropdown-months{{background:#0e0e0e!important;color:var(--text)!important}}
  .numInputWrapper input{{color:var(--text)!important;background:#0e0e0e!important}}
  .kpi-val{{font-size:30px;font-weight:700;line-height:1.1;font-family:var(--font-display)}}
  .kpi-label{{font-size:11px;color:#aaa;text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px;font-weight:600}}
  .kpi-sub{{font-size:11px;color:var(--muted);margin-top:4px}}
  .sec-title{{font-size:14px;text-transform:uppercase;letter-spacing:.08em;font-weight:800;margin-bottom:14px}}
  .date-pill{{font-size:12px;color:var(--muted);font-weight:500;text-transform:none;letter-spacing:normal;border:1px solid #333;border-radius:6px;padding:2px 10px;white-space:nowrap}}
  .tag-green{{color:#4ade80}} .tag-yellow{{color:#fbbf24}} .tag-red{{color:#f87171}} .tag-blue{{color:#60a5fa}}
  .video-bar-bg{{background:#1a1a1a;border-radius:4px;height:6px;overflow:hidden}}
  .video-bar{{background:var(--red);height:6px;border-radius:4px;transition:width .4s}}
  .camp-row-click{{cursor:pointer;transition:background .1s}}
  .camp-row-click:hover td{{background:rgba(255,255,255,.015)}}
  .drill-row{{background:#0a0a0a}}
  .adset-hdr{{background:rgba(232,40,10,.06);border-left:3px solid rgba(232,40,10,.35);border-radius:6px;padding:8px 12px;margin-bottom:8px}}
  .ad-thumb{{width:52px;height:52px;object-fit:cover;border-radius:8px;flex-shrink:0;background:#1a1a1a}}
  .ad-thumb-ph{{width:52px;height:52px;border-radius:8px;background:#1a1a1a;border:1px solid var(--border);display:flex;align-items:center;justify-content:center;flex-shrink:0;letter-spacing:.02em}}
  .drill-inner{{padding:12px 16px 16px;border-top:1px solid #181818}}
  .drill-note{{font-size:10px;color:#4b5563;margin-top:12px;font-style:italic}}
  .logo-mark{{display:inline-flex;align-items:center;gap:8px}}
  .logo-mark .dot{{width:10px;height:10px;background:var(--red);border-radius:50%;display:inline-block}}
  @media(max-width:639px){{.kpi-val{{font-size:22px}}.kpi-sub{{font-size:10px}}.kpi-label{{font-size:10px}}}}
  @media(max-width:767px){{
    .mc-table td:nth-child(3),.mc-table th:nth-child(3),
    .mc-table td:nth-child(4),.mc-table th:nth-child(4){{display:none}}
    .gc-table td:nth-child(3),.gc-table th:nth-child(3){{display:none}}
    .btn-period{{padding:5px 10px;font-size:11px}}
  }}
</style>
</head>
<body>
<div class="max-w-5xl mx-auto px-4 py-8">

<!-- Header -->
<div class="flex flex-col sm:flex-row sm:items-start sm:justify-between mb-6 gap-3">
  <div>
    <h1 class="text-4xl font-black tracking-widest" style="font-family:var(--font-display)">
      GN<span style="color:var(--red)">AUTO</span>
    </h1>
    <p class="text-xs mt-1" style="color:var(--muted)">{CLIENTE_SUB}</p>
  </div>
  <div class="text-left sm:text-right">
    <p class="text-xs" style="color:var(--muted)">Dados ate <span class="font-medium" style="color:#bbb">{yesterday.strftime('%d/%m/%Y')}</span></p>
    <div class="flex sm:justify-end gap-1 mt-2 flex-wrap">
      {meta_badge}
      {google_badge}
      {pending_badge}
    </div>
  </div>
</div>

<!-- Period Selector -->
<div class="card p-4 mb-6">
  <div class="flex flex-wrap items-center gap-2">
    <span class="text-xs mr-1" style="color:var(--muted)">Periodo:</span>
    <div class="flex flex-wrap gap-1">
      <button class="btn-period" onclick="setPeriod('month')" id="btn-month">Este mes</button>
      <button class="btn-period" onclick="setPeriod('7')"     id="btn-7">7 dias</button>
      <button class="btn-period" onclick="setPeriod('14')"    id="btn-14">14 dias</button>
      <button class="btn-period" onclick="setPeriod('30')"    id="btn-30">30 dias</button>
    </div>
    <div class="w-full sm:w-auto sm:ml-auto pt-1 sm:pt-0">
      <input type="text" id="dt-range" placeholder="Escolher datas..." readonly class="w-full"/>
    </div>
  </div>
</div>

<!-- Today Warning -->
<div id="today-warning" class="hidden card p-10 mb-6 flex flex-col items-center justify-center text-center gap-4">
  <span style="font-size:40px;line-height:1">⚠️</span>
  <p class="font-bold text-xl" style="color:var(--orange)">Periodo inclui hoje</p>
  <p class="text-sm max-w-sm" style="color:var(--muted)">Os dados de hoje ainda estao incompletos.<br/>Selecione um periodo ate <strong style="color:var(--text)">{yesterday.strftime('%d/%m')}</strong> (ontem) para ver os numeros finais.</p>
</div>

<!-- Sem dados (estado inicial) -->
<div id="no-data-msg" class="card p-10 mb-6 flex flex-col items-center justify-center text-center gap-4 {'hidden' if (HAS_META or HAS_GOOGLE) else ''}">
  <span style="font-size:48px;line-height:1">🚗</span>
  <p class="font-black text-2xl uppercase tracking-widest" style="font-family:var(--font-display);color:var(--text)">Em Implantacao</p>
  <p class="text-sm max-w-sm" style="color:var(--muted)">As contas de anuncio da GN Auto estao sendo configuradas.<br/>Este painel vai atualizar automaticamente quando os dados estiverem disponiveis.</p>
  <div class="flex gap-2 mt-2 flex-wrap justify-center">
    <span class="badge-pending text-xs px-3 py-1">Meta Ads — pendente</span>
    <span class="badge-pending text-xs px-3 py-1">Google Ads — pendente</span>
  </div>
</div>

<!-- Data Content -->
<div id="data-content" class="{'hidden' if not (HAS_META or HAS_GOOGLE) else ''}">

<!-- RESUMO GERAL -->
<div class="card card-resumo p-5 mb-4">
  <p class="sec-title title-resumo">Resumo Geral — Meta + Google</p>
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
    <div><p class="kpi-label">Investimento Total</p><p class="kpi-val accent" id="tot-spend">—</p><p class="kpi-sub">Meta + Google</p></div>
    <div><p class="kpi-label">{CONV_LABEL}</p><p class="kpi-val accent" id="tot-leads">—</p><p class="kpi-sub">{CONV_UNIT} (Meta + Google)</p></div>
    <div><p class="kpi-label">CPA Medio</p><p class="kpi-val" style="color:var(--text)" id="tot-cpa">—</p><p class="kpi-sub">custo por conversa</p></div>
    <div><p class="kpi-label">Impressoes</p><p class="kpi-val" style="color:var(--text)" id="tot-imp">—</p><p class="kpi-sub">Meta + Google</p></div>
  </div>
</div>

<!-- Chart -->
<div class="card card-resumo p-5 mb-6">
  <h2 class="sec-title title-resumo">Investimento diario + {CONV_LABEL}</h2>
  <canvas id="mainChart" height="90"></canvas>
  <p class="text-xs mt-2" style="color:#444">* Google disponivel a partir de {_google_first_fmt}. Dados Google com 1 dia de atraso.</p>
</div>

<!-- META ADS -->
<div class="card card-meta p-5 mb-4">
  <div class="sec-title title-meta flex items-center gap-2 mb-3">
    <img src="https://cdn.simpleicons.org/meta/ffffff" width="18" height="18" alt="Meta" style="opacity:.85"/>
    Meta Ads — Facebook &amp; Instagram
    <span class="date-pill" id="meta-date-label"></span>
  </div>
  <div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
    <div><p class="kpi-label">Alcance</p><p class="kpi-val" style="color:var(--text)" id="m-reach">—</p><p class="kpi-sub">pessoas unicas</p></div>
    <div><p class="kpi-label">Impressoes</p><p class="kpi-val" style="color:var(--text)" id="m-imp">—</p><p class="kpi-sub">exibicoes totais</p></div>
    <div><p class="kpi-label">CPM</p><p class="kpi-val" style="color:var(--text)" id="m-cpm">—</p><p class="kpi-sub">custo por 1.000</p></div>
    <div><p class="kpi-label">CTR</p><p class="kpi-val" style="color:var(--text)" id="m-ctr">—</p><p class="kpi-sub">taxa de clique</p></div>
    <div><p class="kpi-label">Investimento</p><p class="kpi-val accent" id="m-spend">—</p><p class="kpi-sub">total no periodo</p></div>
  </div>
  <div class="border-t my-3" style="border-color:#242424"></div>
  <div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
    <div><p class="kpi-label">Cliques</p><p class="kpi-val" style="color:var(--text)" id="m-clicks">—</p><p class="kpi-sub">em links</p></div>
    <div><p class="kpi-label">CPC</p><p class="kpi-val" style="color:var(--text)" id="m-cpc">—</p><p class="kpi-sub">custo por clique</p></div>
    <div><p class="kpi-label">{CONV_LABEL}</p><p class="kpi-val accent" id="m-conv">—</p><p class="kpi-sub">{CONV_UNIT}</p></div>
    <div><p class="kpi-label">{CONV_CPL_LBL}</p><p class="kpi-val" style="color:var(--text)" id="m-cpl">—</p><p class="kpi-sub">{CONV_CPL_UNIT}</p></div>
    <div><p class="kpi-label">Conversas (Wpp + DM)</p><p class="kpi-val" style="color:var(--text)" id="m-msg">—</p><p class="kpi-sub">WhatsApp + Instagram Direct</p></div>
  </div>
  <div class="border-t my-3" id="video-divider" style="border-color:#242424"></div>
  <div id="video-block">
    <p class="kpi-label mb-3">Retencao de video</p>
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
      <div><div class="flex justify-between mb-1"><span class="text-xs" style="color:#888">View 25%</span><span class="text-xs" style="color:var(--text)" id="m-v25">—</span></div><div class="video-bar-bg"><div class="video-bar" id="bar-v25" style="width:0%"></div></div></div>
      <div><div class="flex justify-between mb-1"><span class="text-xs" style="color:#888">View 50%</span><span class="text-xs" style="color:var(--text)" id="m-v50">—</span></div><div class="video-bar-bg"><div class="video-bar" id="bar-v50" style="width:0%;background:var(--orange)"></div></div></div>
      <div><div class="flex justify-between mb-1"><span class="text-xs" style="color:#888">View 75%</span><span class="text-xs" style="color:var(--text)" id="m-v75">—</span></div><div class="video-bar-bg"><div class="video-bar" id="bar-v75" style="width:0%;background:#fbbf24"></div></div></div>
      <div><div class="flex justify-between mb-1"><span class="text-xs" style="color:#888">View 95%</span><span class="text-xs" style="color:var(--text)" id="m-v95">—</span></div><div class="video-bar-bg"><div class="video-bar" id="bar-v95" style="width:0%;background:#f87171"></div></div></div>
    </div>
    <p class="text-xs mt-2" style="color:#444" id="video-note"></p>
  </div>
</div>

<!-- META CAMPANHAS -->
<div class="card card-meta p-5 mb-4">
  <div class="sec-title title-meta flex items-center gap-2 mb-3">
    <img src="https://cdn.simpleicons.org/meta/ffffff" width="18" height="18" alt="Meta" style="opacity:.85"/>
    Campanhas Meta
    <span class="date-pill" id="meta-camp-date-label"></span>
  </div>
  <div class="overflow-x-auto">
    <table class="mc-table w-full text-sm">
      <thead><tr class="text-xs border-b" style="color:#555;border-color:#242424">
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

<!-- GOOGLE ADS -->
<div class="card card-google p-5 mb-4">
  <div class="sec-title title-google flex items-center gap-2 mb-3">
    {GADS_ICON}Google Ads — Pesquisa
    <span class="date-pill" id="google-date-label"></span>
  </div>
  <div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
    <div><p class="kpi-label">Impressoes</p><p class="kpi-val" style="color:var(--text)" id="g-imp">—</p><p class="kpi-sub">vezes exibido</p></div>
    <div><p class="kpi-label">CTR</p><p class="kpi-val" style="color:var(--text)" id="g-ctr">—</p><p class="kpi-sub">taxa de clique</p></div>
    <div><p class="kpi-label">Cliques</p><p class="kpi-val" style="color:var(--text)" id="g-clicks">—</p><p class="kpi-sub">visitas ao site</p></div>
    <div><p class="kpi-label">CPC</p><p class="kpi-val" style="color:var(--text)" id="g-cpc">—</p><p class="kpi-sub">custo por clique</p></div>
    <div><p class="kpi-label">Investimento</p><p class="kpi-val" style="color:var(--orange)" id="g-spend">—</p><p class="kpi-sub">total no periodo</p></div>
  </div>
  <div class="border-t my-3" style="border-color:#242424"></div>
  <div class="grid grid-cols-2 md:grid-cols-3 gap-3 mb-4">
    <div><p class="kpi-label">Conversoes</p><p class="kpi-val" style="color:var(--orange)" id="g-conv">—</p><p class="kpi-sub">{CONV_UNIT}</p></div>
    <div><p class="kpi-label">CPA</p><p class="kpi-val" style="color:var(--text)" id="g-cpa">—</p><p class="kpi-sub">custo por conversao</p></div>
    <div><p class="kpi-label">Taxa de Conv.</p><p class="kpi-val" style="color:var(--text)" id="g-taxa">—</p><p class="kpi-sub">cliques → conversao</p></div>
  </div>
  <p class="text-xs mt-4 italic" style="color:#444" id="google-note"></p>
</div>

<!-- GOOGLE CAMPANHAS -->
<div class="card card-google p-5 mb-6">
  <div class="sec-title title-google flex items-center gap-2 mb-3">
    {GADS_ICON}Campanhas Google
    <span class="date-pill">{gcampmonth_label}</span>
  </div>
  <div class="overflow-x-auto">
    <table class="gc-table w-full text-sm">
      <thead><tr class="text-xs border-b" style="color:#555;border-color:#242424">
        <th class="text-left pb-3 pr-4">Campanha</th>
        <th class="text-right pb-3 pr-4">Investido</th>
        <th class="text-right pb-3 pr-4">Cliques</th>
        <th class="text-right pb-3 pr-4">Conversoes</th>
        <th class="text-right pb-3">Custo/conv.</th>
      </tr></thead>
      <tbody id="camp-body"></tbody>
    </table>
  </div>
  <p class="text-xs mt-3" style="color:#444" id="camp-note"></p>
</div>

<div class="text-center text-xs py-4" style="color:#333">controle.gnauto.com.br · Atualizado em {today.strftime('%d/%m/%Y')}</div>

</div><!-- /data-content -->
</div><!-- /max-w-5xl -->

<script>
const META_DATA    = {meta_json};
const GOOGLE_DATA  = {google_json};
const CAMPANHAS    = {camp_json};
const CAMP_META    = {camp_meta_json};
const CAMP_DAILY   = {camp_daily_json};
const AD_DRILL     = {ad_drill_json};
const HAS_META_JS  = {'true' if HAS_META else 'false'};
const HAS_GOOGLE_JS= {'true' if HAS_GOOGLE else 'false'};
const META_CPA_BOM_JS = {META_CPA_BOM};
const META_CPA_OK_JS  = {META_CPA_OK};

let chart = null;
const CUR = n => 'R$ ' + n.toFixed(2);
const PCT = n => n.toFixed(2) + '%';
const FMT = n => n >= 1000000 ? (n/1000000).toFixed(1)+'M' : n >= 1000 ? (n/1000).toFixed(1)+'K' : String(n);
const fmtD = s => {{ const [y,m,d]=s.split('-'); return d+'/'+m; }};

function filterM(from,to){{return META_DATA.filter(x=>x.d>=from&&x.d<=to);}}
function filterG(from,to){{return GOOGLE_DATA.filter(x=>x.d>=from&&x.d<=to);}}
function set(id,val){{const el=document.getElementById(id);if(el)el.textContent=val;}}

function toggleDrill(campId) {{
  const row = document.getElementById('drill-'+campId);
  const arrow = document.getElementById('arrow-'+campId);
  if(!row) return;
  const nowHidden = row.classList.toggle('hidden');
  if(arrow) arrow.textContent = nowHidden ? '▶' : '▼';
  if(!nowHidden && !row.dataset.rendered) {{ renderDrill(campId, row); row.dataset.rendered = '1'; }}
}}

function renderDrill(campId, container) {{
  const data = AD_DRILL[campId];
  if(!data || Object.keys(data).length === 0) {{
    container.innerHTML = '<td colspan="7"><div class="drill-inner"><p style="color:#555" class="text-xs">Sem dados de criativo nos ultimos 30 dias.</p></div></td>';
    return;
  }}
  let html = '<td colspan="7"><div class="drill-inner">';
  Object.values(data).sort((a,b)=>b.spend-a.spend).forEach(aset => {{
    const asetCpa = aset.conv>0 ? aset.spend/aset.conv : 0;
    html += `<div class="adset-hdr"><span class="text-xs font-bold uppercase tracking-wide" style="color:#ff8a78">${{aset.name}}</span><span class="text-xs ml-3" style="color:#666">R$ ${{aset.spend.toFixed(2)}} · ${{aset.conv}} conv.${{asetCpa>0?' · R$ '+asetCpa.toFixed(2)+'/conv.':''}}</span></div>`;
    html += '<div class="space-y-2 mb-4">';
    aset.ads.forEach(ad => {{
      const adCpa = ad.conv>0 ? ad.spend/ad.conv : 0;
      const cpaCls = ad.conv>0&&adCpa<META_CPA_BOM_JS?'tag-green':ad.conv>0&&adCpa<META_CPA_OK_JS?'tag-yellow':ad.conv>0?'tag-red':'';
      const initials = ad.name.replace(/[^A-Za-z0-9]/g,' ').trim().split(/\s+/).slice(0,2).map(w=>w[0]||'').join('').toUpperCase()||'?';
      const thumbHtml = ad.thumb ? `<img src="${{ad.thumb}}" class="ad-thumb" alt="" onerror="this.parentNode.querySelector('.ad-thumb-ph').style.display='flex';this.style.display='none'"/><div class="ad-thumb-ph" style="display:none;font-size:11px;font-weight:700;color:#6b7280">${{initials}}</div>` : `<div class="ad-thumb-ph" style="font-size:11px;font-weight:700;color:#6b7280">${{initials}}</div>`;
      const previewBtn = ad.preview ? `<a href="${{ad.preview}}" target="_blank" rel="noopener" style="color:#555" class="ml-2 hover:text-red-400 transition-colors" title="Ver anuncio">👁️</a>` : '';
      html += `<div class="flex items-center gap-3 p-2 rounded-lg" style="transition:background .15s" onmouseenter="this.style.background='rgba(255,255,255,.03)'" onmouseleave="this.style.background=''">
        ${{thumbHtml}}
        <div class="flex-1 min-w-0">
          <p class="text-sm font-medium truncate" style="color:var(--text)" title="${{ad.name}}">${{ad.name.length>55?ad.name.substring(0,52)+'...':ad.name}}</p>
          <p class="text-xs mt-0.5" style="color:#666">R$ ${{ad.spend.toFixed(2)}} · <span class="${{cpaCls}} font-semibold">${{ad.conv}} conv.</span>${{adCpa>0?' · R$ '+adCpa.toFixed(2)+'/conv.':''}}</p>
        </div>
        ${{previewBtn}}
      </div>`;
    }});
    html += '</div>';
  }});
  html += '<p class="drill-note">* Dados dos ultimos 30 dias (fixo)</p></div></td>';
  container.innerHTML = html;
}}

function update(from,to){{
  const todayWarn   = document.getElementById('today-warning');
  const dataContent = document.getElementById('data-content');
  const noDataMsg   = document.getElementById('no-data-msg');
  if(to >= TODAY_STR){{
    todayWarn.classList.remove('hidden');
    dataContent.classList.add('hidden');
    if(noDataMsg) noDataMsg.classList.add('hidden');
    return;
  }}
  todayWarn.classList.add('hidden');
  if(!HAS_META_JS && !HAS_GOOGLE_JS){{
    dataContent.classList.add('hidden');
    if(noDataMsg) noDataMsg.classList.remove('hidden');
    return;
  }}
  dataContent.classList.remove('hidden');
  if(noDataMsg) noDataMsg.classList.add('hidden');

  const mR = filterM(from,to);
  const gEffTo  = (GOOGLE_LAST && to   > GOOGLE_LAST) ? GOOGLE_LAST : to;
  const gEffFrom= (GOOGLE_LAST && from > GOOGLE_LAST) ? GOOGLE_LAST : from;
  const gR = filterG(gEffFrom, gEffTo);

  const mSpend=mR.reduce((a,x)=>a+x.s,0);
  const mReach=mR.reduce((a,x)=>a+x.r,0);
  const mImp  =mR.reduce((a,x)=>a+x.i,0);
  const mConv =mR.reduce((a,x)=>a+x.c,0);
  const mClk  =mR.reduce((a,x)=>a+x.lk,0);
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
  set('m-cpm',   CUR(mCPM));
  set('m-ctr',   PCT(mCTR));
  set('m-spend', CUR(mSpend));
  set('m-clicks',mClk);
  set('m-cpc',   mClk>0?CUR(mCPC):'—');
  set('m-conv',  mConv);
  set('m-cpl',   mConv>0?CUR(mCPL):'—');
  set('m-msg',   mMsg>0?FMT(mMsg):'—');
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
  set('g-cpc',    hasG&&gClicks>0?CUR(gCPC):'—');
  set('g-spend',  hasG?CUR(gSpend):'—');
  set('g-conv',   hasG?gConv:'—');
  set('g-cpa',    hasG&&gConv>0?CUR(gCPA):'—');
  set('g-taxa',   hasG&&gClicks>0?PCT(gTaxa):'—');
  set('google-note', hasG ? '' : 'Sem dados Google no periodo selecionado.');

  const gDateLabel = (GOOGLE_LAST && to > GOOGLE_LAST)
    ? fmtD(gEffFrom)+' — '+fmtD(GOOGLE_LAST)+' (atraso 1 dia)' : fmtD(from)+' — '+fmtD(to);
  set('google-date-label', hasG ? gDateLabel : gDateLabel+' (sem dados)');

  const totSpend=mSpend+gSpend;
  const totLeads=mConv+gConv;
  const totImp  =mImp+gImp;
  const totCPA  =totLeads>0?totSpend/totLeads:0;
  set('tot-spend', CUR(totSpend));
  set('tot-leads', totLeads>0?totLeads:'—');
  set('tot-cpa',   totCPA>0?CUR(totCPA):'—');
  set('tot-imp',   FMT(totImp));

  const allDates=[...new Set([...mR.map(x=>x.d),...gR.map(x=>x.d)])].sort();
  const mByD=Object.fromEntries(mR.map(x=>[x.d,x]));
  const gByD=Object.fromEntries(gR.map(x=>[x.d,x]));
  const labels  =allDates.map(fmtD);
  const dsMeta  =allDates.map(d=>mByD[d]?mByD[d].s:0);
  const dsGoogle=allDates.map(d=>gByD[d]?gByD[d].s:0);
  const dsConv  =allDates.map(d=>mByD[d]?mByD[d].c:0);

  if(chart)chart.destroy();
  Chart.defaults.color='#666';
  chart=new Chart(document.getElementById('mainChart').getContext('2d'),{{
    data:{{
      labels,
      datasets:[
        {{type:'bar',label:'Meta (R$)',data:dsMeta,backgroundColor:'rgba(232,40,10,.2)',borderColor:'#E8280A',borderWidth:1,borderRadius:3,stack:'sp',yAxisID:'y'}},
        {{type:'bar',label:'Google (R$)',data:dsGoogle,backgroundColor:'rgba(255,107,26,.18)',borderColor:'#FF6B1A',borderWidth:1,borderRadius:3,stack:'sp',yAxisID:'y'}},
        {{type:'line',label:'{CONV_LABEL}',data:dsConv,borderColor:'#fbbf24',backgroundColor:'rgba(251,191,36,.1)',borderWidth:2,pointRadius:allDates.length>30?2:4,tension:.3,fill:false,yAxisID:'y1'}}
      ]
    }},
    options:{{
      responsive:true,
      interaction:{{mode:'index',intersect:false}},
      plugins:{{
        legend:{{labels:{{color:'#666',font:{{size:11}},padding:16}}}},
        tooltip:{{backgroundColor:'#111',borderColor:'#242424',borderWidth:1,titleColor:'#F0EDE8',bodyColor:'#888',
          callbacks:{{label:c=>c.dataset.label==='{CONV_LABEL}'?'  {CONV_LABEL}: '+c.parsed.y:'  '+c.dataset.label+': R$ '+c.parsed.y.toFixed(2)}}}}
      }},
      scales:{{
        x:{{stacked:true,grid:{{color:'#181818'}},ticks:{{color:'#555',font:{{size:10}},maxRotation:45}}}},
        y:{{stacked:true,grid:{{color:'#181818'}},ticks:{{color:'#555',font:{{size:10}},callback:v=>'R$ '+v}}}},
        y1:{{position:'right',grid:{{drawOnChartArea:false}},ticks:{{color:'#aaa',font:{{size:10}},callback:v=>v+' conv.'}}}}
      }}
    }}
  }});

  const body=document.getElementById('camp-body');
  body.innerHTML='';
  let noteText='';
  CAMPANHAS.forEach(c=>{{
    const cc=c.cor==='green'?'tag-green':c.cor==='yellow'?'tag-yellow':'tag-red';
    body.innerHTML+=`<tr class="border-b" style="border-color:#1f1f1f">
      <td class="py-3 pr-4"><p class="font-medium" style="color:var(--text)">${{c.nome}}</p><p class="text-xs" style="color:#555">${{c.desc}}</p></td>
      <td class="py-3 pr-4 text-right" style="color:#aaa">R$ ${{c.gasto.toFixed(2)}}</td>
      <td class="py-3 pr-4 text-right" style="color:#aaa">${{c.cliques}}</td>
      <td class="py-3 pr-4 text-right font-bold" style="color:var(--text)">${{c.contatos}}</td>
      <td class="py-3 text-right"><span class="${{cc}} font-semibold">${{c.cpa>0?'R$ '+c.cpa.toFixed(2):'—'}}</span></td>
    </tr>`;
  }});
  document.getElementById('camp-note').textContent=noteText;

  set('meta-camp-date-label', fmtD(from)+' — '+fmtD(to));
  const byId={{}};
  CAMP_DAILY.filter(x=>x.d>=from&&x.d<=to).forEach(row=>{{
    if(!byId[row.id]) byId[row.id]={{gasto:0,reach:0,lk:0,conv:0}};
    byId[row.id].gasto+=row.s; byId[row.id].reach+=row.r;
    byId[row.id].lk+=row.lk; byId[row.id].conv+=row.c;
  }});
  const camps=Object.entries(byId)
    .filter(([id,c])=>c.gasto>0)
    .map(([id,c])=>{{
      const info=CAMP_META[id]||{{nome:id,desc:'—'}};
      const cpa=c.conv>0?c.gasto/c.conv:0;
      const cor=c.conv>0&&cpa<{META_CPA_BOM}?'green':c.conv>0&&cpa<{META_CPA_OK}?'yellow':c.conv>0?'red':'blue';
      return {{id,...info,...c,cpa,cor}};
    }}).sort((a,b)=>b.gasto-a.gasto);

  const mbody=document.getElementById('meta-camp-body');
  mbody.innerHTML='';
  camps.forEach(c=>{{
    const cc=c.cor==='green'?'tag-green':c.cor==='blue'?'tag-blue':c.cor==='yellow'?'tag-yellow':'tag-red';
    const cpaStr =c.conv>0?`<span class="${{cc}} font-semibold">R$ ${{c.cpa.toFixed(2)}}</span>`:`<span style="color:#444">—</span>`;
    const convStr=c.conv>0?`<span class="font-bold" style="color:var(--text)">${{c.conv}}</span>`:`<span style="color:#444">—</span>`;
    const hasDrill = AD_DRILL[c.id] && Object.keys(AD_DRILL[c.id]).length > 0;
    const arrowEl = hasDrill ? `<span id="arrow-${{c.id}}" style="color:#555;font-size:11px;margin-right:4px">▶</span>` : `<span style="color:#333;font-size:11px;margin-right:4px">·</span>`;
    const rowClick = hasDrill ? `onclick="toggleDrill('${{c.id}}')" class="border-b camp-row-click" style="border-color:#1f1f1f"` : `class="border-b" style="border-color:#1f1f1f"`;
    mbody.innerHTML+=`<tr ${{rowClick}}>
      <td class="py-3 pl-1 pr-2 w-6">${{arrowEl}}</td>
      <td class="py-3 pr-4"><p class="font-medium" style="color:var(--text)">${{c.nome}}</p><p class="text-xs" style="color:#555">${{c.desc}}</p></td>
      <td class="py-3 pr-4 text-right" style="color:#aaa">R$ ${{c.gasto.toFixed(2)}}</td>
      <td class="py-3 pr-4 text-right" style="color:#aaa">${{c.reach.toLocaleString('pt-BR')}}</td>
      <td class="py-3 pr-4 text-right" style="color:#aaa">${{c.lk}}</td>
      <td class="py-3 pr-4 text-right">${{convStr}}</td>
      <td class="py-3 text-right">${{cpaStr}}</td>
    </tr>
    <tr id="drill-${{c.id}}" class="drill-row hidden"></tr>`;
  }});
}}

function clearActive(){{['month','7','14','30'].forEach(id=>{{const el=document.getElementById('btn-'+id);if(el)el.classList.remove('active');}});}}
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
  const btn=document.getElementById('btn-'+p);
  if(btn)btn.classList.add('active');
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
# 5. GERAR HTML
# ══════════════════════════════════════════════════════════════════════
dist_dir  = os.path.join(DEPLOY_DIR, "dist-gnauto")
out_file  = os.path.join(dist_dir, "index.html")
os.makedirs(dist_dir, exist_ok=True)

with open(out_file, "w", encoding="utf-8") as f:
    f.write(HTML)
print(f"\nHTML gerado: {out_file}")

# ══════════════════════════════════════════════════════════════════════
# 6. DEPLOY → projeto separado gnauto-controle
# ══════════════════════════════════════════════════════════════════════
print(f"\nDeployando para projeto '{CF_PROJECT_GNAUTO}'...")
env_deploy = os.environ.copy()
env_deploy["CLOUDFLARE_API_TOKEN"]  = CF_TOKEN
env_deploy["CLOUDFLARE_ACCOUNT_ID"] = CF_ACCOUNT

result = subprocess.run(
    f'npx wrangler pages deploy "{dist_dir}" --project-name={CF_PROJECT_GNAUTO} --branch=main --commit-dirty=true',
    env=env_deploy, capture_output=False, shell=True, cwd=DEPLOY_DIR, timeout=120
)

if result.returncode == 0:
    print(f"\nDeploy OK!")
    print(f"  URL Pages: https://{CF_PROJECT_GNAUTO}.pages.dev")
    print(f"  URL final: https://{SUBDOMAIN}")
else:
    print(f"Erro no deploy (rc={result.returncode})")
