from docx import Document
from docx.shared import Pt
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from .helpers import get_region_language
from .text_templates import text_templates

def render_title(doc: Document, country_code: str, year: int):
    lang = get_region_language(country_code)
    text = text_templates["title"][lang].format(year=year)
    p = doc.add_paragraph()
    p.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    run = p.add_run(text)
    run.bold = True
    run.font.size = Pt(16)

def render_intro_and_table(
    doc: Document,
    country_code: str,
    farmername: str,
    data: dict  # p.ej. {"farmercode":"CR-001", ...}
):
    lang = get_region_language(country_code)

    # --- INTRO: convierte <br> en párrafos ---
    intro_tpl = text_templates["intro"][lang]
    intro_text = intro_tpl.format(farmername=farmername)
    # Creamos la tabla 4×2
    table = doc.add_table(rows=4, cols=2, style="Table Grid")
    # Merge de columna izquierda
    left = table.cell(0, 0).merge(table.cell(3, 0))
    # Añadimos sólo los párrafos útiles, cortando por doble salto
    for section in [s.strip() for s in intro_text.split("<br><br>") if s.strip()]:
        left.add_paragraph(section)

    # --- CELDAS DERECHA con placeholders ya en text_templates ---
    for i, key in enumerate(("farmercode","contractcode","planting_year","contract_trees")):
        tpl = text_templates["cells_right"][key][lang]
        text = tpl.format(**data)
        cell = table.cell(i, 1)
        cell.text = text

    # un salto después de la tabla
    doc.add_paragraph()
    return table
