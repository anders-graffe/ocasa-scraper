# ============================================================
# SCRAPER OCASA — versión GitHub Actions
# Igual al script de Colab pero sin dependencias de google.colab
# ============================================================

import re, os, json, asyncio
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime             import datetime
from bs4                  import BeautifulSoup
from playwright.async_api import async_playwright


# ── Autenticación (Service Account en vez de auth.authenticate_user) ──
sa_info = json.loads(os.environ["GOOGLE_SA_KEY"])
creds   = Credentials.from_service_account_info(
    sa_info,
    scopes=[
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc        = gspread.authorize(creds)
sh        = gc.open("reviews_ocasa")
worksheet = sh.worksheet("raw_reviews")

HEADERS = ["fecha_scraping","autor","texto_original","estrellas",
           "fecha_review","fuente","origen","url_fuente","procesado"]

print("Cargando registros existentes del sheet...")
existing_data = worksheet.get_all_values()
TEXTOS_EXISTENTES = set()
if len(existing_data) > 1:
    col = HEADERS.index("texto_original")
    TEXTOS_EXISTENTES = {
        row[col] for row in existing_data[1:] if len(row) > col
    }
print(f"✅ {len(TEXTOS_EXISTENTES)} registros ya en el sheet")


# ── Configuración ───────────────────────────────────────────
SUCURSALES_MAPS = [
    ("Iriarte",    "https://www.google.com/maps/place/OCASA+Iriarte/@-34.6523788,-58.3930127,17z/data=!4m8!3m7!1s0x95bccb5c94b8b01d:0x5a4e81af054824d4!8m2!3d-34.6523832!4d-58.3904378!9m1!1b1!16s%2Fg%2F11bxg1l9hv?entry=ttu&g_ep=EgoyMDI2MDUyMC4wIKXMDSoASAFQAw%3D%3D"),
    ("Soldati",    "https://www.google.com/maps/place/OCASA+SOLDATI/@-34.6718192,-58.4397183,17z/data=!4m8!3m7!1s0x95bccb1d848c3c71:0xf1726a297dd6b1aa!8m2!3d-34.6718193!4d-58.4348527!9m1!1b1!16s%2Fg%2F11lhzbv5qk?entry=ttu&g_ep=EgoyMDI2MDUyNS4wIKXMDSoASAFQAw%3D%3D"),
    ("Echeverría", "https://www.google.com/maps/place/OCASA/@-34.5557448,-58.4465084,17z/data=!4m8!3m7!1s0x95bcb5cb33d2855b:0x984181d02bb35b83!8m2!3d-34.5557448!4d-58.4439281!9m1!1b1!16s%2Fg%2F11g6xqq0t7?entry=ttu&g_ep=EgoyMDI2MDUyNS4wIKXMDSoASAFQAw%3D%3D"),
    ("Sarandí",    "https://www.google.com/maps/place/Ocasa+Sarand%C3%AD/@-34.6701739,-58.3265334,17z/data=!4m8!3m7!1s0x95a333bfbd6eb737:0x31b54046b2f0c628!8m2!3d-34.6701739!4d-58.3239531!9m1!1b1!16s%2Fg%2F11k02c43mv?entry=ttu&g_ep=EgoyMDI2MDUyNS4wIKXMDSoASAFQAw%3D%3D"),
    ("Avellaneda", "https://www.google.com/maps/place/Ocasa+Avellaneda/@-34.6590219,-58.3824913,17z/data=!4m8!3m7!1s0x95bccca9abc34c61:0x18ac53c51cc00e61!8m2!3d-34.6590219!4d-58.3799164!9m1!1b1!16s%2Fg%2F11gf9f9p8x?entry=ttu&g_ep=EgoyMDI2MDUyNi4wIKXMDSoASAFQAw%3D%3D"),
]
TQS_MAX_PAGINAS = 5


# ── Scraper Google Maps ─────────────────────────────────────
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


# ── Scraper tuQuejaSuma ─────────────────────────────────────
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
                    match = re.search(
                        r"(\d{1,2}\s+de\s+\w+(?:\s+de\s+\d{4})?)",
                        txt, re.IGNORECASE
                    )
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


# ── Main ────────────────────────────────────────────────────
async def main():
    todos  = []
    inicio = datetime.now()
    print(f"\n🕐 Inicio: {inicio.strftime('%Y-%m-%d %H:%M')}")

    for nombre, url in SUCURSALES_MAPS:
        print(f"\n📍 Google Maps → {nombre}")
        try:
            reviews = await scrape_maps_diario(
                url, nombre, TEXTOS_EXISTENTES,
                max_scrolls=40, parar_si_duplicados=5
            )
            todos.extend(reviews)
            print(f"  → {len(reviews)} reseñas nuevas")
        except Exception as e:
            print(f"  ❌ Error: {e}")

    print(f"\n🟠 tuQuejaSuma")
    try:
        posts = await scrape_tuquejasuma_diario(TEXTOS_EXISTENTES, max_paginas=TQS_MAX_PAGINAS)
        todos.extend(posts)
        print(f"  → {len(posts)} reclamos nuevos")
    except Exception as e:
        print(f"  ❌ Error: {e}")

    fin = datetime.now()
    dur = (fin - inicio).seconds
    print(f"\n{'='*50}")
    print(f"✅ Total nuevos: {len(todos)}")
    print(f"⏱️  Duración: {dur//60}m {dur%60}s")

    # Guardar en Sheets
    if todos:
        for i in range(0, len(todos), 500):
            chunk = todos[i:i+500]
            worksheet.append_rows(pd.DataFrame(chunk)[HEADERS].values.tolist())
            print(f"→ Guardados {min(i+500, len(todos))}/{len(todos)}")
        print(f"✅ {len(todos)} registros guardados")
    else:
        print("ℹ️ Sin registros nuevos — sheet al día")


# ── Entry point ─────────────────────────────────────────────
asyncio.run(main())
