import os
from docx.shared import Pt
from docx import Document
from docx.shared import Inches
from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.helpers import get_region_language
from GeneradordeReportes.utils.dynamic_text_blocks import fetch_dynamic_values, format_paragraphs
from GeneradordeReportes.graficadorG1Mortalidad import generar_mortalidad
from GeneradordeReportes.graficadorG2Altura import generar_altura
from GeneradordeReportes.graficadorG3Crecimiento import generar_crecimiento
from GeneradordeReportes.graficadorG4DefectosyPlagas import generar_tabla_sanidad


# Rutas de plantilla y salidas
BASE_DIR    = os.path.dirname(__file__)
TEMPLATE    = os.path.join(BASE_DIR, 'templates', 'Template.docx')
OUTPUT_DIR  = os.path.join(BASE_DIR, 'outputs', 'reports')
IMG_TMP_DIR = os.path.join(OUTPUT_DIR, 'temp_images')

# Crear directorios de salida si no existen
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(IMG_TMP_DIR, exist_ok=True)


def crear_reporte(code: str, country: str, year: int) -> str:
    engine = get_engine()

    # Generar gráficas y tabla
    generar_mortalidad(code, country, year)
    generar_altura(code, country, year)
    generar_crecimiento(code, country, year)
    df_sanidad = generar_tabla_sanidad(code, country, year)


    resumen_dir = os.path.join("outputs", code, "Resumen")
    imgs = [
        os.path.join(resumen_dir, f"G1_Mortality_{code}.png"),
        os.path.join(resumen_dir, f"G2_Altura_{code}.png"),
        os.path.join(resumen_dir, f"G3_Crecimiento_{code}.png"),
    ]

    # Texto dinámico
    values = fetch_dynamic_values()
    paragraphs = format_paragraphs(values)

    # Documento base
    doc = Document(TEMPLATE)
    for p in paragraphs:
        doc.add_paragraph(p)
    doc.add_page_break()

    # Insertar imágenes
    for img in imgs:
        if os.path.exists(img):
            doc.add_picture(img, width=Inches(6.125))  # 15.56 cm ≈ 6.125 in
            doc.add_page_break()
        else:
            print(f"⚠️ Imagen no encontrada: {img}")

    if df_sanidad is not None:
        # Usamos add_heading para evitar KeyError si 'Heading 2' no existe en estilos
        doc.add_heading("Distribución de Plagas, Defectos y Enfermedades", level=2)
        table = doc.add_table(rows=1, cols=len(df_sanidad.columns))
        table.style = 'Table Grid'

        # Encabezados en negritas
        hdr_cells = table.rows[0].cells
        for i, col_name in enumerate(df_sanidad.columns):
            run = hdr_cells[i].paragraphs[0].add_run(str(col_name))
            run.bold = True
            run.font.size = Pt(10)

        # Rellenar filas
        for _, row in df_sanidad.iterrows():
            row_cells = table.add_row().cells
            for i, cell in enumerate(row):
                row_cells[i].text = str(cell)

    # Guardar documento
    out_name = f"Reporte_{code}.docx"
    out_path = os.path.join(OUTPUT_DIR, out_name)
    doc.save(out_path)
    print(f"✅ Reporte creado: {out_path}")
    return out_path


if __name__ == '__main__':
    # Ejemplo de uso: pasar country y year
    crear_reporte('CR0030', 'cr', 2025)
