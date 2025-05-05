# report_writer.py
import os
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


def crear_reporte(code: str) -> str:
    """
    Genera un .docx con texto dinámico y gráficas para un contrato.

    Args:
        code: Código de contrato (ej. 'CR0030').
    Returns:
        Ruta al archivo .docx generado.
    """
    # Conexión a la base de datos usando utils/libs.py
    engine = get_engine()

    # 1) Generar gráficas y guardarlas
    tmp_dir = os.path.join(IMG_TMP_DIR, code)
    os.makedirs(tmp_dir, exist_ok=True)
    imgs = []
    for fn, name in [
        (generar_mortalidad, 'mortalidad'),
        (generar_altura,     'altura'),
        (generar_crecimiento,'crecimiento'),
        (generar_tabla_sanidad, 'plagas_defectos'),
    ]:
        path = os.path.join(tmp_dir, f"{name}.png")
        fn(code, save_path=path)
        imgs.append(path)

    # 2) Texto dinámico
    values = fetch_dynamic_values()
    paragraphs = format_paragraphs(values)

    # 3) Montar documento a partir de la plantilla
    doc = Document(TEMPLATE)

    # Insertar párrafos dinámicos
    for p in paragraphs:
        doc.add_paragraph(p)
    doc.add_page_break()

    # Insertar imágenes de gráficas
    for img in imgs:
        doc.add_picture(img, width=Inches(6))
        doc.add_page_break()

    # 4) Guardar documento final
    out_name = f"Reporte_{code}.docx"
    out_path = os.path.join(OUTPUT_DIR, out_name)
    doc.save(out_path)
    print(f"✅ Reporte creado: {out_path}")
    return out_path


if __name__ == '__main__':
    # Prueba rápida con un contrato de ejemplo
    crear_reporte('CR0030')
