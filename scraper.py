# ============================================================
# OCASA — Scraper + Enricher (pipeline unificado)
# Etapa 1: Scraping Google Maps + TuQuejaSuma → raw_reviews
# Etapa 2: Enriquecimiento Azure OpenAI → reviews_enriquecidas
# Entorno: Auto-detecta Colab vs GitHub Actions
# ============================================================

# ── CELDA 1 — Instalación (correr solo una vez por sesión) ──
import subprocess
subprocess.run(["pip", "install", "playwright", "openai", "gspread", "google-auth", 
                "beautifulsoup4", "lxml", "tqdm", "pandas", "nest_asyncio", "-q"], check=True)
subprocess.run(["playwright", "install", "chromium"], check=True)
subprocess.run(["playwright", "install-deps", "chromium"], check=True)
print("✅ Todo instalado")

import re, os, json, math, time, hashlib
import nest_asyncio
nest_asyncio.apply()
asyncio.run(main())
import pandas as pd
import gspread
from datetime import datetime, date
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from openai import AzureOpenAI

# ── Detectar entorno ───────────────────────────────────────────
try:
    import google.colab
    EN_COLAB = True
except ImportError:
    EN_COLAB = False

print(f"🌐 Entorno: {'Google Colab' if EN_COLAB else 'GitHub Actions'}")


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════
def safe(val):
    if val is None:
        return ""
    try:
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return ""
    except Exception:
        pass
    return val if isinstance(val, str) else str(val)

def generar_hash(texto, fuente, origen):
    raw = f"{str(texto).strip()}|{str(fuente).strip()}|{str(origen).strip()}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ══════════════════════════════════════════════════════════════
# AUTENTICACIÓN GOOGLE
# ══════════════════════════════════════════════════════════════
if EN_COLAB:
    from google.colab import auth
    auth.authenticate_user()
    from google.auth import default
    creds, _ = default()
    gc = gspread.authorize(creds)
else:
    from google.oauth2.service_account import Credentials
    sa_info = json.loads(os.environ["GOOGLE_SA_KEY"])
    creds   = Credentials.from_service_account_info(
        sa_info,
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    gc = gspread.authorize(creds)

sh               = gc.open("reviews_ocasa")
ws_raw           = sh.worksheet("raw_reviews")
RAW_HEADERS      = ["fecha_scraping","autor","texto_original","estrellas",
                    "fecha_review","fuente","origen","url_fuente","procesado"]
ENRICH_HEADERS   = ["hash_review","fecha_estimada","autor","review","estrellas",
                    "palabras_clave","cliente","servicio","punto_de_dolor",
                    "sentimiento","sucursal","fuente","resumen_review","url_fuente"]
print(f"✅ Google Sheets conectado: {sh.title}")


# ══════════════════════════════════════════════════════════════
# AZURE OPENAI
# ══════════════════════════════════════════════════════════════
if EN_COLAB:
    try:
        from google.colab import userdata
        API_KEY = userdata.get("AZURE_OPENAI_KEY")
        print("✅ API Key desde Colab Secrets")
    except Exception:
        API_KEY = "3poVK637WZcUsMi6F3yCli3z7CF1ylIeqo61GIHeCDnxbIaVFJFxJQQJ99CDACYeBjFXJ3w3AAABACOGpwgp"
        print("⚠️  API Key hardcodeada")
    ENDPOINT    = "https://oai-customer-experience.openai.azure.com/"
    DEPLOYMENT  = "gpt-5.4-mini-testing-anders-graffe"
    API_VERSION = "2025-04-01-preview"
else:
    API_KEY     = os.environ["AZURE_OPENAI_KEY"]
    ENDPOINT    = os.environ.get("AZURE_OPENAI_ENDPOINT",    "https://oai-customer-experience.openai.azure.com/")
    DEPLOYMENT  = os.environ.get("AZURE_OPENAI_DEPLOYMENT",  "gpt-5.4-mini-testing-anders-graffe")
    API_VERSION = os.environ.get("AZURE_OPENAI_API_VERSION", "2025-04-01-preview")

oai = AzureOpenAI(
    azure_endpoint=ENDPOINT,
    api_key=API_KEY,
    api_version=API_VERSION,
    timeout=60.0,
    max_retries=2,
)
print("✅ Azure OpenAI inicializado")


# ══════════════════════════════════════════════════════════════
# CONFIGURACIÓN SCRAPER
# ══════════════════════════════════════════════════════════════
SUCURSALES_MAPS = [
    ("Iriarte",    "https://www.google.com/maps/place/OCASA+Iriarte/@-34.6523788,-58.3930127,17z/data=!4m8!3m7!1s0x95bccb5c94b8b01d:0x5a4e81af054824d4!8m2!3d-34.6523832!4d-58.3904378!9m1!1b1!16s%2Fg%2F11bxg1l9hv?entry=ttu&g_ep=EgoyMDI2MDUyMC4wIKXMDSoASAFQAw%3D%3D"),
    ("Soldati",    "https://www.google.com/maps/place/OCASA+SOLDATI/@-34.6718192,-58.4397183,17z/data=!4m8!3m7!1s0x95bccb1d848c3c71:0xf1726a297dd6b1aa!8m2!3d-34.6718193!4d-58.4348527!9m1!1b1!16s%2Fg%2F11lhzbv5qk?entry=ttu&g_ep=EgoyMDI2MDUyNS4wIKXMDSoASAFQAw%3D%3D"),
    ("Echeverría", "https://www.google.com/maps/place/OCASA/@-34.5557448,-58.4465084,17z/data=!4m8!3m7!1s0x95bcb5cb33d2855b:0x984181d02bb35b83!8m2!3d-34.5557448!4d-58.4439281!9m1!1b1!16s%2Fg%2F11g6xqq0t7?entry=ttu&g_ep=EgoyMDI2MDUyNS4wIKXMDSoASAFQAw%3D%3D"),
    ("Sarandí",    "https://www.google.com/maps/place/Ocasa+Sarand%C3%AD/@-34.6701739,-58.3265334,17z/data=!4m8!3m7!1s0x95a333bfbd6eb737:0x31b54046b2f0c628!8m2!3d-34.6701739!4d-58.3239531!9m1!1b1!16s%2Fg%2F11k02c43mv?entry=ttu&g_ep=EgoyMDI2MDUyNS4wIKXMDSoASAFQAw%3D%3D"),
    ("Avellaneda", "https://www.google.com/maps/place/Ocasa+Avellaneda/@-34.6590219,-58.3824913,17z/data=!4m8!3m7!1s0x95bccca9abc34c61:0x18ac53c51cc00e61!8m2!3d-34.6590219!4d-58.3799164!9m1!1b1!16s%2Fg%2F11gf9f9p8x?entry=ttu&g_ep=EgoyMDI2MDUyNi4wIKXMDSoASAFQAw%3D%3D"),
]
TQS_MAX_PAGINAS = 5


# ══════════════════════════════════════════════════════════════
# ETAPA 1 — SCRAPER
# ══════════════════════════════════════════════════════════════
async def scrape_maps_diario(url, sucursal, textos_existentes,
                              max_scrolls=40, parar_si_duplicados=5):
    reviews = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, args=["--no-sandbox","--disable-dev-shm-usage"]
        )
        ctx  = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="es-AR"
        )
        page = await ctx.new_page()
        try:
            await page.goto(url, timeout=40000, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)

            for sel in ['button[aria-label*="Reseña"]','button[aria-label*="reseña"]','[data-tab-index="1"]']:
                try:
                    await page.click(sel, timeout=3000)
                    await page.wait_for_timeout(2000)
                    break
                except: continue

            try:
                await page.click('button[aria-label*="Ordenar"]', timeout=3000)
                await page.wait_for_timeout(1000)
                await page.click('li[data-index="1"]', timeout=3000)
                await page.wait_for_timeout(2000)
            except: pass

            panel = None
            for sel in ['.m6QErb.DxyBCb.kA9KIf','.m6QErb.DxyBCb','div[role="feed"]']:
                try:
                    found = await page.query_selector(sel)
                    if found:
                        panel = found
                        break
                except: continue

            if not panel:
                return reviews

            textos_vistos     = set()
            duplicados_consec = 0
            scroll_count      = 0

            while scroll_count < max_scrolls:
                items = await page.query_selector_all('[data-review-id]')
                for item in items:
                    try:
                        try:
                            btn = await item.query_selector('button.w8nwRe')
                            if btn:
                                await btn.click()
                                await page.wait_for_timeout(80)
                        except: pass

                        texto_el  = await item.query_selector('.wiI7pd')
                        texto_val = (await texto_el.inner_text()).strip() if texto_el else ""

                        if not texto_val or texto_val in textos_vistos:
                            continue
                        textos_vistos.add(texto_val)

                        if texto_val in textos_existentes:
                            duplicados_consec += 1
                            if duplicados_consec >= parar_si_duplicados:
                                return reviews
                            continue
                        else:
                            duplicados_consec = 0

                        autor     = await item.query_selector('.d4r55')
                        estrellas = await item.query_selector('.kvMYJc')
                        fecha     = await item.query_selector('.rsqaWe')

                        reviews.append({
                            "fecha_scraping": datetime.now().strftime("%Y-%m-%d"),
                            "autor":          (await autor.inner_text()).strip() if autor else "anónimo",
                            "texto_original": texto_val,
                            "estrellas":      await estrellas.get_attribute("aria-label") if estrellas else "",
                            "fecha_review":   (await fecha.inner_text()).strip() if fecha else "",
                            "fuente":         "google_maps",
                            "origen":         sucursal,
                            "url_fuente":     url,
                            "procesado":      "NO"
                        })
                    except: continue

                await panel.evaluate("el => el.scrollTop += 800")
                await page.wait_for_timeout(700)
                scroll_count += 1

        except Exception as e:
            print(f"  ❌ Error Maps {sucursal}: {e}")
        finally:
            await browser.close()
    return reviews


async def scrape_tqs_pagina(url):
    DOMINIO = "https://www.tuquejasuma.com"
    items   = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, args=["--no-sandbox","--disable-dev-shm-usage"]
        )
        ctx  = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
            locale="es-AR"
        )
        page = await ctx.new_page()
        await page.goto(url, timeout=40000, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)
        for _ in range(15):
            await page.evaluate("window.scrollBy(0, 800)")
            await page.wait_for_timeout(300)
        html   = await page.content()
        titulo = await page.title()
        await browser.close()

    if "moment" in titulo.lower() or "checking" in titulo.lower():
        return []

    soup = BeautifulSoup(html, "lxml")
    for art in soup.find_all("article"):
        h          = art.find(["h2","h3"])
        titulo_val = h.get_text(strip=True) if h else ""
        parr       = " ".join(p.get_text(strip=True) for p in art.find_all("p"))
        cont       = f"{titulo_val} | {parr}".strip(" |")
        if not cont or len(cont) < 5:
            continue

        fecha = ""
        el = art.find("time")
        if el:
            fecha = el.get("datetime","").strip() or el.get_text(strip=True)
        if not fecha:
            for tag in art.find_all(["small","span","p"]):
                txt = tag.get_text(strip=True)
                if "publicado" in txt.lower() or "solucionado" in txt.lower():
                    match = re.search(r"(\d{1,2}\s+de\s+\w+(?:\s+de\s+\d{4})?)", txt, re.IGNORECASE)
                    fecha = match.group(0).strip() if match else txt
                    break
        if not fecha:
            for cls in ["text-muted","fecha","date"]:
                el = art.find(class_=cls)
                if el:
                    fecha = el.get_text(strip=True)
                    if fecha: break

        a    = art.find("a", href=True)
        href = a["href"] if a else ""
        link = href if href.startswith("http") else f"{DOMINIO}{href}"
        items.append({"contenido": cont, "fecha": fecha, "url": link or url})
    return items


async def scrape_tuquejasuma_diario(textos_existentes, max_paginas=5):
    DOMINIO = "https://www.tuquejasuma.com"
    posts   = []
    textos_vistos = set()

    for num_pag in range(1, max_paginas + 1):
        url = (f"{DOMINIO}/reclamos/buscar?query=ocasa" if num_pag == 1
               else f"{DOMINIO}/reclamos/buscar?query=ocasa&page={num_pag}")
        print(f"  TQS página {num_pag}")
        items = await scrape_tqs_pagina(url)

        if not items:
            print(f"  ⚠️ Sin resultados — fin tuQuejaSuma")
            break

        nuevos_en_pag = 0
        for item in items:
            if item["contenido"] in textos_vistos:
                continue
            textos_vistos.add(item["contenido"])
            if item["contenido"] in textos_existentes:
                continue
            nuevos_en_pag += 1
            posts.append({
                "fecha_scraping": datetime.now().strftime("%Y-%m-%d"),
                "autor":          "anónimo",
                "texto_original": item["contenido"],
                "estrellas":      "",
                "fecha_review":   item["fecha"],
                "fuente":         "tuquejasuma",
                "origen":         "tuQuejaSuma OCASA",
                "url_fuente":     item["url"],
                "procesado":      "NO"
            })

        print(f"  → +{nuevos_en_pag} nuevos | acumulado TQS: {len(posts)}")
        if nuevos_en_pag == 0:
            print(f"  ✅ Página completa duplicada — fin tuQuejaSuma")
            break

    return posts


async def etapa_scraping():
    """Carga textos existentes, scrapea fuentes y guarda nuevos en raw_reviews."""
    print("\n" + "="*55)
    print("ETAPA 1 — SCRAPING")
    print("="*55)

    print("Cargando registros existentes del sheet...")
    existing_data = ws_raw.get_all_values()
    textos_existentes = set()
    if len(existing_data) > 1:
        col = RAW_HEADERS.index("texto_original")
        textos_existentes = {
            row[col] for row in existing_data[1:] if len(row) > col
        }
    print(f"✅ {len(textos_existentes)} registros ya en el sheet")

    todos  = []
    inicio = datetime.now()
    print(f"🕐 Inicio scraping: {inicio.strftime('%Y-%m-%d %H:%M')}")

    for nombre, url in SUCURSALES_MAPS:
        print(f"\n📍 Google Maps → {nombre}")
        try:
            reviews = await scrape_maps_diario(
                url, nombre, textos_existentes,
                max_scrolls=40, parar_si_duplicados=5
            )
            todos.extend(reviews)
            print(f"  → {len(reviews)} reseñas nuevas")
        except Exception as e:
            print(f"  ❌ Error: {e}")

    print(f"\n🟠 tuQuejaSuma")
    try:
        posts = await scrape_tuquejasuma_diario(textos_existentes, max_paginas=TQS_MAX_PAGINAS)
        todos.extend(posts)
        print(f"  → {len(posts)} reclamos nuevos")
    except Exception as e:
        print(f"  ❌ Error: {e}")

    fin = datetime.now()
    dur = (fin - inicio).seconds
    print(f"\n{'='*55}")
    print(f"✅ Total nuevos scrapeados: {len(todos)}")
    print(f"⏱️  Duración scraping: {dur//60}m {dur%60}s")

    if todos:
        for i in range(0, len(todos), 500):
            chunk = todos[i:i+500]
            ws_raw.append_rows(pd.DataFrame(chunk)[RAW_HEADERS].values.tolist())
            print(f"→ Guardados {min(i+500, len(todos))}/{len(todos)} en raw_reviews")
        print(f"✅ {len(todos)} registros guardados en raw_reviews")
    else:
        print("ℹ️ Sin registros nuevos — raw_reviews al día")

    return len(todos)


# ══════════════════════════════════════════════════════════════
# ETAPA 2 — ENRICHER
# ══════════════════════════════════════════════════════════════
SCRAPING_DATE = date.today()

MES_MAP = {
    "enero":1,"febrero":2,"marzo":3,"abril":4,
    "mayo":5,"junio":6,"julio":7,"agosto":8,
    "septiembre":9,"octubre":10,"noviembre":11,"diciembre":12
}

def meses_pasados(n):
    year  = SCRAPING_DATE.year
    month = SCRAPING_DATE.month - n
    while month <= 0:
        month += 12
        year  -= 1
    return date(year, month, 1)

def inferir_anio_tuqueja(mes_num):
    return SCRAPING_DATE.year if mes_num <= SCRAPING_DATE.month else SCRAPING_DATE.year - 1

def normalizar_fecha(raw, fuente):
    raw = str(raw).strip().lower()
    if fuente == "google_maps":
        raw = raw.replace("editado ", "")
        m = re.search(r"hace (\d+) mes", raw)
        if m: return meses_pasados(int(m.group(1))).strftime("%Y-%m-01")
        if "hace un mes" in raw: return meses_pasados(1).strftime("%Y-%m-01")
        m = re.search(r"hace (\d+) a.o", raw)
        if m: return f"{SCRAPING_DATE.year - int(m.group(1))}-01-01"
        if "hace un a" in raw: return f"{SCRAPING_DATE.year - 1}-01-01"
        if "semana" in raw or re.search(r"hace \d+ d", raw) or "ayer" in raw:
            return SCRAPING_DATE.strftime("%Y-%m-01")
        return SCRAPING_DATE.strftime("%Y-%m-01")
    elif fuente == "tuquejasuma":
        m = re.search(r"(\d+) de ([a-záéíóúñ]+)", raw)
        if m:
            mes_num = MES_MAP.get(m.group(2).lower())
            if mes_num:
                return f"{inferir_anio_tuqueja(mes_num)}-{mes_num:02d}-01"
        return SCRAPING_DATE.strftime("%Y-%m-01")
    return SCRAPING_DATE.strftime("%Y-%m-01")


SYSTEM_PROMPT = """
Eres un analista experto en experiencia de cliente para OCASA, empresa argentina de logística last-mile.
Tu tarea es analizar reseñas de clientes y devolver ÚNICAMENTE un JSON con los campos exactos indicados.
No incluyas texto adicional, markdown ni explicaciones. Solo el objeto JSON.

CAMPOS:
{
  "palabras_clave": [lista de 3-5 strings en minúsculas],
  "cliente": string o null,
  "servicio": string o null,
  "punto_de_dolor": string (EXACTAMENTE uno de los tópicos listados abajo),
  "sentimiento": "Positivo" | "Negativo" | "Neutro",
  "resumen_review": string (máximo 50 palabras en español)
}

REGLAS PARA `cliente`:
- Renaper, RENAPER, DNI → "Renaper / DNI"
- MercadoLibre, Mercado Libre, Meli, ML → "MercadoLibre"
- Otros: Temu, Shein, Adidas, Nike, ANSES, banco, tarjeta, OCA, Andreani, etc.
- Si no se menciona ninguna: null.

REGLAS PARA `servicio` (elegir EXACTAMENTE uno):
- Paquetería ecommerce | DNI / Documentos | Tarjetas bancarias | Postal / Correo
- Importaciones | Logística empresas | Licencia de conducir
- Gestión administrativa | Reclamos generales | null

TÓPICOS FIJOS PARA `punto_de_dolor` (elegir EXACTAMENTE uno):
- Entrega no realizada / visita falsa
- Demoras y retrasos
- Falta de comunicación y notificaciones
- Atención al cliente deficiente
- Pérdida o robo de paquete
- Error en tracking / seguimiento
- Dificultad para retirar en sucursal
- Retorno al remitente sin aviso
- Error en dirección de entrega
- Trato descortés del personal de sucursal
- Trato descortés del transportista
- Trato descortés de atención telefónica
- Cobro incorrecto / problema de pago
- Experiencia positiva
"""

def analizar_review(texto):
    try:
        response = oai.chat.completions.create(
            model=DEPLOYMENT,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": f"Analiza esta reseña de OCASA:\n\n{texto[:1500]}"}
            ],
            max_completion_tokens=400,
            temperature=0.1,
        )
        raw = response.choices[0].message.content.strip()
        raw = raw.replace("```json","").replace("```","").strip()
        return json.loads(raw)
    except Exception as e:
        return {
            "palabras_clave": [],
            "cliente": None,
            "servicio": "Reclamos generales",
            "punto_de_dolor": "Atención al cliente deficiente",
            "sentimiento": "Negativo",
            "resumen_review": f"Error al procesar: {str(e)[:80]}"
        }


def etapa_enrichment():
    """Lee raw_reviews, enriquece los no procesados y escribe en reviews_enriquecidas."""
    print("\n" + "="*55)
    print("ETAPA 2 — ENRICHMENT")
    print("="*55)

    # ── Leer hashes ya procesados ──────────────────────────────
    print(f"🔍 Leyendo reviews_enriquecidas...")
    ya_procesados = set()
    primera_vez   = False

    try:
        ws_out = sh.worksheet("reviews_enriquecidas")
        datos_existentes = ws_out.get_all_values()
        if len(datos_existentes) > 1:
            headers_ex = datos_existentes[0]
            if "hash_review" in headers_ex:
                col_hash = headers_ex.index("hash_review")
                ya_procesados = {
                    row[col_hash] for row in datos_existentes[1:]
                    if len(row) > col_hash and row[col_hash]
                }
        print(f"   📋 {len(ya_procesados)} reviews ya enriquecidas")
    except gspread.exceptions.WorksheetNotFound:
        ws_out      = sh.add_worksheet(title="reviews_enriquecidas", rows=5000, cols=15)
        primera_vez = True
        print("   ➕ Pestaña 'reviews_enriquecidas' creada")

    # ── Leer raw_reviews frescos via API ───────────────────────
    print(f"\n📥 Leyendo raw_reviews (API directa)...")
    datos_raw   = ws_raw.get_all_values()
    if len(datos_raw) <= 1:
        print("⚠️ Sin datos en raw_reviews")
        return 0

    headers_raw = datos_raw[0]
    df = pd.DataFrame(datos_raw[1:], columns=headers_raw).fillna("")
    print(f"📦 {len(df)} reviews en raw_reviews")

    df["hash_review"] = df.apply(
        lambda r: generar_hash(r.get("texto_original",""), r.get("fuente",""), r.get("origen","")),
        axis=1
    )

    df_nuevos = df[~df["hash_review"].isin(ya_procesados)].reset_index(drop=True)
    print(f"🆕 {len(df_nuevos)} reviews nuevas para enriquecer")

    if len(df_nuevos) == 0:
        print("✅ Sin registros nuevos — reviews_enriquecidas al día")
        return 0

    # ── Procesar con Azure OpenAI ──────────────────────────────
    if EN_COLAB:
        from tqdm.notebook import tqdm
    else:
        from tqdm import tqdm

    resultados = []
    for _, row in tqdm(df_nuevos.iterrows(), total=len(df_nuevos), desc="Enriqueciendo"):
        hash_id = row["hash_review"]
        texto   = safe(row.get("texto_original",""))
        fuente  = safe(row.get("fuente",""))
        analisis = analizar_review(texto)

        resultados.append({
            "hash_review":    hash_id,
            "fecha_estimada": normalizar_fecha(safe(row.get("fecha_review","")), fuente),
            "autor":          safe(row.get("autor","")),
            "review":         texto,
            "estrellas":      safe(row.get("estrellas","")),
            "palabras_clave": ", ".join(analisis.get("palabras_clave",[])),
            "cliente":        safe(analisis.get("cliente")),
            "servicio":       safe(analisis.get("servicio")),
            "punto_de_dolor": safe(analisis.get("punto_de_dolor")),
            "sentimiento":    safe(analisis.get("sentimiento")),
            "sucursal":       safe(row.get("origen","")),
            "fuente":         fuente,
            "resumen_review": safe(analisis.get("resumen_review")),
            "url_fuente":     safe(row.get("url_fuente","")),
        })
        time.sleep(0.3)

    print(f"\n✅ {len(resultados)} reviews enriquecidas")

    # ── Escribir en Google Sheets ──────────────────────────────
    print(f"📤 Escribiendo en reviews_enriquecidas...")

    if primera_vez or len(ya_procesados) == 0:
        ws_out.append_row(ENRICH_HEADERS, value_input_option="RAW")
        ws_out.format("A1:N1", {
            "backgroundColor": {"red": 0.0, "green": 0.643, "blue": 0.6},
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "horizontalAlignment": "CENTER"
        })
        ws_out.freeze(rows=1)

    filas = [[safe(r.get(k,"")) for k in ENRICH_HEADERS] for r in resultados]
    for i in range(0, len(filas), 500):
        chunk = filas[i:i+500]
        ws_out.append_rows(chunk, value_input_option="RAW")
        print(f"   ✅ Bloque {i+1}–{i+len(chunk)} appendeado")
        time.sleep(1.5)

    print(f"🎉 {len(resultados)} registros nuevos en reviews_enriquecidas")
    return len(resultados)


# ══════════════════════════════════════════════════════════════
# MAIN — corre las dos etapas en secuencia
# ══════════════════════════════════════════════════════════════
async def main():
    inicio_total = datetime.now()
    print(f"\n🚀 Pipeline OCASA — {inicio_total.strftime('%Y-%m-%d %H:%M')}")
    print(f"🔗 Sheet: https://docs.google.com/spreadsheets/d/{sh.id}")

    # Etapa 1: Scraping
    nuevos_scrapeados = await etapa_scraping()

    # Etapa 2: Enrichment (siempre corre, no solo si hay nuevos)
    # así procesa cualquier pendiente de corridas anteriores también
    nuevos_enriquecidos = etapa_enrichment()

    fin_total = datetime.now()
    dur = (fin_total - inicio_total).seconds
    print(f"\n{'='*55}")
    print(f"🏁 Pipeline completado")
    print(f"   Scrapeados:    {nuevos_scrapeados}")
    print(f"   Enriquecidos:  {nuevos_enriquecidos}")
    print(f"   Duración total: {dur//60}m {dur%60}s")
    print(f"{'='*55}")


asyncio.run(main())
