"""
refresh_windsor_data.py
Busca TODOS os dados Windsor de uma vez e separa por cliente.

Fuso: America/Sao_Paulo — data_preset=last_90dT inclui hoje.

Uso:
  python refresh_windsor_data.py             # todos
  python refresh_windsor_data.py tratorval   # so um cliente
"""
import json, os, sys, urllib.request, urllib.parse, time
from zoneinfo import ZoneInfo
from datetime import datetime
from collections import defaultdict

DASH    = os.path.dirname(os.path.abspath(__file__))
API_KEY = os.getenv("WINDSOR_API_KEY", "b4b0be35ce4f55c84603d64ea55096ee3506")
BASE    = "https://connectors.windsor.ai/all"
SP_TZ   = ZoneInfo("America/Sao_Paulo")

today_sp = datetime.now(SP_TZ).strftime("%Y-%m-%d")
print(f"Data hoje SP: {today_sp}\n")

# ─── Fields ─────────────────────────────────────────────────────────────────
META_FIELDS = ",".join([
    "date", "account_name", "campaign", "campaign_id", "adset_name",
    "spend", "impressions", "reach", "clicks",
    "actions_lead", "actions_link_click", "actions_landing_page_view",
    "actions_purchase", "action_values_purchase",
    "actions_offsite_conversion_fb_pixel_contact",
    "actions_onsite_conversion_messaging_conversation_started_7d",
    "video_p25_watched_actions_video_view",
    "video_p50_watched_actions_video_view",
    "video_p75_watched_actions_video_view",
    "video_p95_watched_actions_video_view",
])

AD_FIELDS = META_FIELDS + ",ad_name,thumbnail_url,effective_object_story_id"

def normalize(rows):
    """Renomeia 'campaign' -> 'campaign_name' pra compatibilidade com generate scripts."""
    for r in rows:
        if "campaign" in r and "campaign_name" not in r:
            r["campaign_name"] = r.pop("campaign")
        elif "campaign" in r:
            r.pop("campaign")
    return rows

GOOGLE_FIELDS = ",".join([
    "date", "account_name", "campaign_name", "campaign_id",
    "clicks", "conversions", "conversion_value", "cost", "impressions",
])

# ─── Mapeamento account_name Windsor -> slug ─────────────────────────────────
# Fonte de verdade: nomes exatos como aparecem na API Windsor
META_MAP = {
    "TRATORVAL - CA1":                        "tratorval",
    "CA01 - Kooldent":                        "kooldent",
    "CA01 - Dra Amanda Feltrim":              "dr-amanda",
    "CA2 - DFORTES CLINIC":                   "dfort",
    "CA1 - Dentuga - Dr Pedro":               "dentuga",
    "BM - Felt - CA02 - imobiliaria Alphaville": "felt",
    "BM - Felt - CA03 -  COD - FELT":        "felt",
    "CA 01 - Kensington Olympia Dental Clinic": "kensington",
    "CA01 - QUALY USA":                       "qualy-usa",
    "CA ' 01 ' PORTO SMILE":                  "porto-smile",
    "All Clinique":                            "all-clinique",
    "CA01 - BiVAR":                           "bivar",
    "CA 01 - Lar e Cia":                      "lar-e-cia",
    "CA1 - Mediconverte":                     "mediconvert",
    "CA01 - CIOALGES":                        "cioalges",
}

GOOGLE_MAP = {
    "tratorval":                              "tratorval",
    "Kensington Olympia":                     "kensington",
    "QUALY USA":                              "qualy-usa",
    "Porto Smile Clinic":                     "porto-smile",
    "All Clinique | Estetica Integrativa":    "all-clinique",
    "All Clinique | Estética Integrativa":    "all-clinique",
    "bivarclinic.pt":                         "bivar",
    "Lar e Cia Decoracao":                    "lar-e-cia",
    "Lar e Cia Decoração":                    "lar-e-cia",
    "CIOALGES":                               "cioalges",
    "IOT RONDONIA":                           "cioalges",  # verificar
}

# ─── Fetch ───────────────────────────────────────────────────────────────────
def fetch_all(fields, retries=2):
    params = {"api_key": API_KEY, "date_preset": "last_90dT", "fields": fields}
    url = BASE + "?" + urllib.parse.urlencode(params)
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=180) as r:
                data = json.loads(r.read())
                return data.get("data", data.get("result", []))
        except Exception as e:
            if attempt < retries:
                print(f"  Tentativa {attempt+1} falhou ({e}). Aguardando 5s...")
                time.sleep(5)
            else:
                raise

# ─── Fetch global ───────────────────────────────────────────────────────────
print("Buscando Meta (adset level)...")
all_meta = normalize(fetch_all(META_FIELDS))
print(f"  {len(all_meta)} rows")

print("Buscando Meta (ad level)...")
all_ads  = normalize(fetch_all(AD_FIELDS))
print(f"  {len(all_ads)} rows")

print("Buscando Google Ads...")
all_google = fetch_all(GOOGLE_FIELDS)
print(f"  {len(all_google)} rows\n")

# ─── Separar por slug ────────────────────────────────────────────────────────
meta_by_slug   = defaultdict(list)
ads_by_slug    = defaultdict(list)
google_by_slug = defaultdict(list)

for row in all_meta:
    acct = row.get("account_name", "")
    if acct in META_MAP:
        meta_by_slug[META_MAP[acct]].append(row)

for row in all_ads:
    acct = row.get("account_name", "")
    if acct in META_MAP:
        ads_by_slug[META_MAP[acct]].append(row)

for row in all_google:
    acct = row.get("account_name", "")
    if acct in GOOGLE_MAP:
        google_by_slug[GOOGLE_MAP[acct]].append(row)

# ─── Slugs a processar ───────────────────────────────────────────────────────
all_slugs = set(META_MAP.values()) | set(GOOGLE_MAP.values())
targets   = sys.argv[1:] if len(sys.argv) > 1 else sorted(all_slugs)
targets   = [t for t in targets if t in all_slugs]
if not targets:
    print(f"Slugs invalidos: {sys.argv[1:]}. Disponiveis: {sorted(all_slugs)}")
    sys.exit(1)

# Contas que a API unificada nao retorna campaign_name — precisam de MCP manual
MCP_REQUIRED = {"tratorval", "qualy-usa"}

# ─── Salvar JSON por cliente ─────────────────────────────────────────────────
print("=== Salvando windsor JSON files ===")
mcp_needed = []
for slug in targets:
    meta_rows   = meta_by_slug.get(slug, [])
    ad_rows     = ads_by_slug.get(slug, [])
    google_rows = google_by_slug.get(slug, [])

    # Detectar campaign_name nulo (API nao retornou breakdown correto)
    if meta_rows:
        pct_null = sum(1 for r in meta_rows if not r.get("campaign_name")) / len(meta_rows)
        if pct_null > 0.5:
            mcp_needed.append(slug)
            print(f"  AVISO {slug}: {pct_null:.0%} sem campaign_name — precisa MCP (dados anteriores mantidos)")
            continue  # nao sobrescreve o JSON que tem dados corretos

    out = {
        "fetched_at":  today_sp,
        "date_preset": "last_90dT",
        "meta_rows":   meta_rows,
        "ad_rows":     ad_rows,
        "google_rows": google_rows,
    }
    path = os.path.join(DASH, f"windsor_{slug}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)
    print(f"  OK windsor_{slug}.json  Meta:{len(meta_rows)} | Ads:{len(ad_rows)} | Google:{len(google_rows)}")

if mcp_needed:
    print(f"\nATENCAO: {len(mcp_needed)} cliente(s) precisam de atualizacao via MCP pelo Claude:")
    for s in mcp_needed:
        print(f"  - {s}")
    print("Diga ao Claude: 'atualiza via MCP: " + ", ".join(mcp_needed) + "'")

print(f"\n=== Concluido! SP: {today_sp} ===")
