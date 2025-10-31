import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import logging

logger = logging.getLogger("extraction")

# === Constantes ===
ENTITY_VALUE = 'Agencia Nacional de Infraestructura'
FIXED_CLASSIFICATION_ID = 13
COMPONENT_ID = 7  # ID fijo del componente en la tabla regulations_component

URL_BASE = (
    "https://www.ani.gov.co/informacion-de-la-ani/normatividad"
    "?field_tipos_de_normas__tid=12&title=&body_value=&field_fecha__value%5Bvalue%5D%5Byear%5D="
)

CLASSIFICATION_KEYWORDS = {
    'resolución': 15,
    'resolucion': 15,
    'decreto': 14,
}
DEFAULT_RTYPE_ID = 14


# === Utilidades ===
def clean_quotes(text):
    """Elimina comillas y caracteres raros."""
    if not text:
        return text
    quotes_pattern = r'["\'\u201C\u201D\u2018\u2019\u00AB\u00BB\u201E\u201A\u2039\u203A\u2032\u2033´`′″]'
    cleaned_text = re.sub(quotes_pattern, '', text)
    cleaned_text = cleaned_text.strip()
    return ' '.join(cleaned_text.split())


def get_rtype_id(title):
    """Obtiene el tipo de regulación basado en palabras clave."""
    title_lower = title.lower()
    for keyword, rtype_id in CLASSIFICATION_KEYWORDS.items():
        if keyword in title_lower:
            return rtype_id
    return DEFAULT_RTYPE_ID


def is_valid_created_at(created_at_value):
    """Valida que la fecha tenga formato correcto."""
    if not created_at_value:
        return False
    if isinstance(created_at_value, str):
        return bool(created_at_value.strip())
    if isinstance(created_at_value, datetime):
        return True
    return False


# === Extracción de campos ===
def extract_title_and_link(row, norma_data):
    """Extrae título y enlace de una fila HTML."""
    title_cell = row.find('td', class_='views-field views-field-title')
    if not title_cell:
        return False

    title_link = title_cell.find('a')
    if not title_link:
        return False

    raw_title = title_link.get_text(strip=True)
    cleaned_title = clean_quotes(raw_title)

    if len(cleaned_title) > 65:
        return False

    norma_data['title'] = cleaned_title

    external_link = title_link.get('href')
    if external_link and not external_link.startswith('http'):
        external_link = 'https://www.ani.gov.co' + external_link

    norma_data['external_link'] = external_link
    norma_data['gtype'] = 'link' if external_link else None

    return True


def extract_summary(row, norma_data):
    """Extrae el resumen de la fila."""
    summary_cell = row.find('td', class_='views-field views-field-body')
    if summary_cell:
        raw_summary = summary_cell.get_text(strip=True)
        cleaned_summary = clean_quotes(raw_summary)
        norma_data['summary'] = cleaned_summary.capitalize()
    else:
        norma_data['summary'] = None


def extract_creation_date(row, norma_data):
    """Extrae la fecha de creación."""
    fecha_cell = row.find('td', class_='views-field views-field-field-fecha--1')
    if fecha_cell:
        fecha_span = fecha_cell.find('span', class_='date-display-single')
        if fecha_span:
            created_at_raw = fecha_span.get('content', fecha_span.get_text(strip=True))
            if 'T' in created_at_raw:
                norma_data['created_at'] = created_at_raw.split('T')[0]
            elif '/' in created_at_raw:
                try:
                    day, month, year = created_at_raw.split('/')
                    norma_data['created_at'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except:
                    norma_data['created_at'] = created_at_raw
            else:
                norma_data['created_at'] = created_at_raw
        else:
            norma_data['created_at'] = fecha_cell.get_text(strip=True)
    else:
        norma_data['created_at'] = None

    return is_valid_created_at(norma_data['created_at'])


# === Scraping principal ===
def scrape_page(page_num=0):
    """Extrae los registros de una página específica."""
    page_url = f"{URL_BASE}&page={page_num}" if page_num > 0 else URL_BASE
    response = requests.get(page_url, timeout=15)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')
    tbody = soup.find('tbody')
    if not tbody:
        return []

    rows = tbody.find_all('tr')
    page_data = []

    for i, row in enumerate(rows, 1):
        norma_data = {
            'created_at': None,
            'update_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'is_active': True,
            'title': None,
            'gtype': None,
            'entity': ENTITY_VALUE,
            'external_link': None,
            'rtype_id': None,
            'summary': None,
            'classification_id': FIXED_CLASSIFICATION_ID,
        }

        if not extract_title_and_link(row, norma_data):
            continue
        extract_summary(row, norma_data)
        if not extract_creation_date(row, norma_data):
            continue

        norma_data['rtype_id'] = get_rtype_id(norma_data['title'])
        page_data.append(norma_data)

    logger.info(f"Página {page_num}: {len(page_data)} filas extraídas.")
    return page_data


# === Función principal ===
def extract(num_pages=3):
    """
    Extrae regulaciones y crea la lista de componentes asociada.
    Retorna un dict: {'regulations': [...], 'components': [...]}
    """
    all_regs = []
    for p in range(num_pages):
        all_regs.extend(scrape_page(p))

    # Generar componentes asociados (uno por regulación)
    components = []
    for _ in all_regs:
        components.append({
            "components_id": COMPONENT_ID,  # Componente fijo (ej. 7)
        })

    logger.info(f"Total extraído: {len(all_regs)} regulaciones y {len(components)} componentes.")

    return {
        "regulations": all_regs,
        "components": components
    }
