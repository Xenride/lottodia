import re
import warnings
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import os, json
import time
import random
from gspread.exceptions import APIError, WorksheetNotFound

def get_creds():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if os.getenv("GSERVICE_JSON"):
        info = json.loads(os.environ["GSERVICE_JSON"])
        return Credentials.from_service_account_info(info, scopes=scopes)
    return Credentials.from_service_account_file("service_account.json", scopes=scopes)


# Suprime warnings ruidosos (ej: LibreSSL vs OpenSSL en macOS)
warnings.filterwarnings("ignore")

# Fuentes
URL_LOTERIA = "https://www.tuazar.com/loteria/resultados/"
URL_ANIMALITOS = "https://www.tuazar.com/loteria/animalitos/resultados/"

# Google Sheets
SHEET_ID = "1c4FhmgoR-PfNa9Z-iNkvI1s-zeZTTsVLDZK7xgUkWVQ"
WORKSHEET_NAME = "Resultados"
SERVICE_ACCOUNT_JSON = "service_account.json"

TIME_RE = re.compile(r"\d{1,2}:\d{2}\s?(AM|PM)", re.IGNORECASE)

# --- Google Sheets API: retries for transient errors (429/5xx) ---
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

def _status_code_from_api_error(err: Exception):
    resp = getattr(err, 'response', None)
    return getattr(resp, 'status_code', None)

def with_backoff(fn, *, max_attempts=6, base_delay=2.0, max_delay=60.0, label='gspread'):
    """
    Ejecuta `fn()` con reintentos y exponential backoff + jitter para errores transitorios.
    Útil para 503 'service unavailable' que puede ocurrir al leer/escribir en Google Sheets.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except APIError as e:
            status = _status_code_from_api_error(e)
            if status not in RETRYABLE_STATUS or attempt == max_attempts:
                raise

            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            jitter = delay * (0.3 + random.random() * 0.4)  # 30% - 70%
            sleep_s = delay + jitter
            print(f'[{label}] Retryable error {status}. Sleeping {sleep_s:.1f}s (attempt {attempt}/{max_attempts})')
            time.sleep(sleep_s)

def safe_ws_update(ws, values, *, range_name='A1'):
    """Compatibilidad entre versiones de gspread: intenta distintas firmas de update()."""
    def _call():
        try:
            return ws.update(range_name, values, value_input_option='RAW')
        except TypeError:
            try:
                return ws.update(values, range_name=range_name, value_input_option='RAW')
            except TypeError:
                return ws.update(values, range_name)
    return with_backoff(_call, label='update')

# Schema fijo por tipo de lotería (para /loteria/resultados/)
LOTTERY_SCHEMA = {
    "TRIPLE FÁCIL": ["triple"],
    "TRIPLE ZAMORANO": ["triple"],
    "TRIPLE POPULAR": ["triple"],

    "TERMINALES": ["terminal_a_b", "terminal_c"],

    "CHANCE": ["numero", "signo"],
    "CHANCE CON CACHO": ["numero", "signo", "cacho"],

    "LOTTO ACTIVO": ["numero"],
}


def fetch_html(url: str) -> str:
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def extract_date(soup: BeautifulSoup) -> str:
    # Ej: "Sorteos ... del Domingo, 15/02/2026"
    m = re.search(r"\b\d{2}/\d{2}/\d{4}\b", soup.get_text())
    return m.group(0) if m else ""


def collect_section_texts(h2):
    """
    Recolecta textos desde un <h2> hasta el siguiente <h2>.
    Parche clave: separa textos tipo "2:00 PM -" en tokens ["2:00 PM", "-"].
    """
    texts = []
    el = h2

    while True:
        el = el.find_next()
        if not el or el.name == "h2":
            break

        raw = clean(el.get_text(" ", strip=True))
        if not raw:
            continue

        # 🔧 PARCHE CLAVE
        m = re.match(r"^(\d{1,2}:\d{2}\s?(?:AM|PM))\s+(.*)$", raw, flags=re.IGNORECASE)
        if m:
            texts.append(clean(m.group(1)))  # hora
            tail = clean(m.group(2))         # resto
            if tail:
                texts.append(tail)
        else:
            texts.append(raw)

    return texts


def parse_loteria(html: str):
    """
    Parser de https://www.tuazar.com/loteria/resultados/
    Devuelve lista de dicts con esquema normalizado.
    """
    soup = BeautifulSoup(html, "html.parser")
    date = extract_date(soup)
    rows = []

    for h2 in soup.find_all("h2"):
        loteria = clean(h2.get_text()).upper()
        if loteria not in LOTTERY_SCHEMA:
            continue

        schema = LOTTERY_SCHEMA[loteria]
        texts = collect_section_texts(h2)

        i = 0
        while i < len(texts):
            if not TIME_RE.fullmatch(texts[i]):
                i += 1
                continue

            horario = texts[i].upper().replace("AM", " AM").replace("PM", " PM")
            values = []
            j = i + 1

            while j < len(texts) and not TIME_RE.fullmatch(texts[j]):
                if texts[j] not in ("", "-"):
                    values.append(texts[j])
                j += 1

            row = {
                "categoria": "loteria",
                "fecha": date,
                "loteria": loteria,
                "horario": horario,
            }

            for idx, col in enumerate(schema):
                row[col] = values[idx] if idx < len(values) else ""

            rows.append(row)
            i = j

    return rows


def parse_animalitos(html: str):
    """
    Parser de https://www.tuazar.com/loteria/animalitos/resultados/
    Devuelve lista de dicts:
      categoria, fecha, loteria, horario, numero, animal
    """
    soup = BeautifulSoup(html, "html.parser")
    date = extract_date(soup)

    rows = []
    # En esta página cada "juego" viene como h2 (ej: EL GUACHARITO MILLONARIO),
    # y dentro hay repeticiones tipo:
    #   4 - / ALACRÁN / 8:30 AM
    # o en una sola línea: "43 - MARIPOSA" luego la hora.
    for h2 in soup.find_all("h2"):
        loteria = clean(h2.get_text()).upper()
        if not loteria or loteria in ("RESULTADOS DE ANIMALITOS",):
            continue

        texts = collect_section_texts(h2)

        pending_num = None
        pending_animal = None

        for t in texts:
            tt = clean(t)

            # Caso: "43 - MARIPOSA"
            m = re.match(r"^(\d{1,2})\s*-\s*(.+)$", tt)
            if m:
                pending_num = m.group(1).zfill(2)
                pending_animal = clean(m.group(2)).upper()
                continue

            # Caso: "4 -" (número y guión separados)
            m2 = re.match(r"^(\d{1,2})\s*-$", tt)
            if m2:
                pending_num = m2.group(1).zfill(2)
                pending_animal = None
                continue

            # Si tenemos número pendiente y este token parece el nombre del animal
            if pending_num and not pending_animal:
                if tt and not TIME_RE.fullmatch(tt) and tt not in ("-", "ANIMALITO"):
                    pending_animal = tt.upper()
                    continue

            # Si llega una hora y tenemos un animalito pendiente, cerramos fila
            if TIME_RE.fullmatch(tt) and pending_num and pending_animal:
                horario = tt.upper().replace("AM", " AM").replace("PM", " PM")
                rows.append({
                    "categoria": "animalitos",
                    "fecha": date,
                    "loteria": loteria,
                    "horario": horario,
                    "numero": pending_num,
                    "animal": pending_animal,
                })
                pending_num = None
                pending_animal = None

    # Dedup por seguridad
    seen = set()
    out = []
    for r in rows:
        key = (r.get("fecha"), r.get("loteria"), r.get("horario"), r.get("numero"), r.get("animal"))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def write(rows):
    creds = get_creds()
    gc = gspread.authorize(creds)

    # 503/5xx pueden ocurrir de forma transitoria en la API de Google Sheets.
    sh = with_backoff(lambda: gc.open_by_key(SHEET_ID), label="open_by_key")

    try:
        ws = with_backoff(lambda: sh.worksheet(WORKSHEET_NAME), label="worksheet")
    except WorksheetNotFound:
        ws = with_backoff(lambda: sh.add_worksheet(title=WORKSHEET_NAME, rows=4000, cols=30), label="add_worksheet")

    with_backoff(lambda: ws.clear(), label="clear")

    # Columnas finales (unión de ambos mundos)
    columns = ["categoria", "fecha", "loteria", "horario"]

    # columnas de lotería
    for cols in LOTTERY_SCHEMA.values():
        for c in cols:
            if c not in columns:
                columns.append(c)

    # columnas extra para animalitos
    for c in ["animal"]:
        if c not in columns:
            columns.append(c)

    data = [[r.get(c, "") for c in columns] for r in rows]
    values = [columns] + data

    # Un solo update (más eficiente y menos propenso a errores que append_* repetidos)
    safe_ws_update(ws, values, range_name="A1")


def main():
    # 1) Loterías "normales"
    html_l = fetch_html(URL_LOTERIA)
    rows_l = parse_loteria(html_l)
    print(f"Filas encontradas (lotería): {len(rows_l)}")

    # 2) Animalitos
    html_a = fetch_html(URL_ANIMALITOS)
    rows_a = parse_animalitos(html_a)
    print(f"Filas encontradas (animalitos): {len(rows_a)}")

    rows = rows_l + rows_a
    print(f"Total filas a escribir: {len(rows)}")

    write(rows)
    print("Listo ✅")


if __name__ == "__main__":
    main()
