import os
import argparse
from docx.shared import Pt, Inches
from docx import Document
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.dynamic_text_blocks import fetch_dynamic_values, format_paragraphs
from GeneradordeReportes.graficadorG1Mortalidad import generar_mortalidad
from GeneradordeReportes.graficadorG2Altura import generar_altura
from GeneradordeReportes.graficadorG3Crecimiento import generar_crecimiento
from GeneradordeReportes.graficadorG4DefectosyPlagas import generar_tabla_sanidad

# Rutas de plantilla y salidas
BASE_DIR = os.path.dirname(__file__)
TEMPLATE = os.path.join(BASE_DIR, 'templates', 'Template.docx')
OUTPUT_DIR = os.path.join(BASE_DIR, 'outputs', 'reports')
IMG_TMP_DIR = os.path.join(OUTPUT_DIR, 'temp_images')

# Crear directorios de salida si no existen
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(IMG_TMP_DIR, exist_ok=True)

def add_titulo(doc: Document, year: int):
    """Agrega el título centrado con el año del reporte."""
    p = doc.add_paragraph()
    run = p.add_run(f'Reporte {year}')
    run.bold = True
    run.font.size = Pt(16)
    p.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER


def add_intro_table(
    doc: Document,
    farmer_name: str,
    producer_code: str,
    contract_code: str,
    planting_year: int,
    contract_trees: int
):
    """Crea una tabla bilingüe de introducción (español / inglés) con metadatos."""
    table = doc.add_table(rows=5, cols=2)
    table.style = 'Table Grid'

    datos = [
        (f'Estimado productor: {farmer_name}', f'Dear Producer: {farmer_name}'),
        (f'Código de productor: {producer_code}', f'Producer Code: {producer_code}'),
        (f'Contrato: {contract_code}', f'Contract: {contract_code}'),
        (f'Año de plantación: {planting_year}', f'Planting Year: {planting_year}'),
        (f'Árboles contratados: {contract_trees}', f'Contracted Trees: {contract_trees}')
    ]

    for idx, (es, en) in enumerate(datos):
        row = table.rows[idx]
        row.cells[0].text = es
        row.cells[1].text = en

    doc.add_paragraph()


def crear_reporte(code: str, country: str, year: int) -> str:
    engine = get_engine()
    # Generación de gráficas y tabla de sanidad
    generar_mortalidad(code, country, year)
    generar_altura(code, country, year)
    generar_crecimiento(code, country, year)
    df_sanidad = generar_tabla_sanidad(code, country, year)

    # Crear documento
    doc = Document(TEMPLATE)
    add_titulo(doc, year)

    # Valores dinámicos
    values = fetch_dynamic_values()
    farmer_name = values.get('farmername', '')
    contract_trees = values.get('contract_trees', 0)

    # Tabla de introducción bilingüe
    add_intro_table(
        doc,
        farmer_name,
        code,
        code,
        year,
        contract_trees
    )

    # Texto dinámico adicional
    paragraphs = format_paragraphs(values)
    for p in paragraphs:
        doc.add_paragraph(p)
    doc.add_page_break()

    # Insertar gráficas
    resumen_dir = os.path.join(BASE_DIR, 'outputs', code, 'Resumen')
    imgs = [
        os.path.join(resumen_dir, f'G1_Mortality_{code}.png'),
        os.path.join(resumen_dir, f'G2_Altura_{code}.png'),
        os.path.join(resumen_dir, f'G3_Crecimiento_{code}.png')
    ]
    for img in imgs:
        if os.path.exists(img):
            doc.add_picture(img, width=Inches(6.125))
            doc.add_page_break()
        else:
            print(f'⚠️ Imagen no encontrada: {img}')

    # Tabla de sanidad
    if df_sanidad is not None:
        doc.add_heading('Distribución de Plagas, Defectos y Enfermedades', level=2)
        table = doc.add_table(rows=1, cols=len(df_sanidad.columns))
        table.style = 'Table Grid'
        hdr_cells = table.rows[0].cells
        for i, col in enumerate(df_sanidad.columns):
            run = hdr_cells[i].paragraphs[0].add_run(str(col))
            run.bold = True
            run.font.size = Pt(10)
        for _, row in df_sanidad.iterrows():
            cells = table.add_row().cells
            for i, cell in enumerate(row):
                cells[i].text = str(cell)

    # Guardar archivo
    out_name = f'Reporte_{code}.docx'
    out_path = os.path.join(OUTPUT_DIR, out_name)
    doc.save(out_path)
    print(f'✅ Reporte creado: {out_path}')
    return out_path


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('code')
    parser.add_argument('--country', '-c', required=True)
    parser.add_argument('--year', '-y', type=int, required=True)
    args = parser.parse_args()
    crear_reporte(args.code, args.country, args.year)
