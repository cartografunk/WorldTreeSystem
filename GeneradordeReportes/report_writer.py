from GeneradordeReportes.utils.libs import pd, os
from docx import Document
from docx.shared import Pt, Inches
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT  # 👈 Importación faltante
from docx2pdf import convert
from GeneradordeReportes.utils.dynamic_text_blocks import fetch_dynamic_values, format_paragraphs
from GeneradordeReportes.utils.docx_helpers import render_title, render_intro_and_table
from GeneradordeReportes.graficadorG1Mortalidad import generar_mortalidad
from GeneradordeReportes.graficadorG2Altura import generar_altura
from GeneradordeReportes.graficadorG3Crecimiento import generar_crecimiento
from GeneradordeReportes.graficadorG4DefectosyPlagas import generar_tabla_sanidad
import argparse

# Configuración de rutas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPLATE = os.path.join(BASE_DIR, "GeneradordeReportes", "templates", "Template.docx")
OUTPUT_DIR = os.path.join(BASE_DIR, "GeneradordeReportes", "outputs", "reports")
IMG_TMP_DIR = os.path.join(BASE_DIR, "GeneradordeReportes", "temp_images")

# Crear directorios necesarios
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(IMG_TMP_DIR, exist_ok=True)


def crear_reporte(code: str, country: str, year: int, engine) -> str:
    """Función principal para generar el reporte completo"""

    # 1. Generación de gráficas
    output_root = os.path.join(BASE_DIR, "GeneradordeReportes", "outputs")  # 👈 Ruta unificada
    paths = {
        'G1': generar_mortalidad(code, country, year, engine, output_root=output_root),
        'G2': generar_altura(code, country, year, engine, output_root=output_root),
        'G3': generar_crecimiento(code, country, year, engine, output_root=output_root),
    }

    # 2. Validación de gráficas generadas
    for key, path in paths.items():
        if not path or not os.path.isfile(path):
            raise FileNotFoundError(f"Gráfica {key} no generada: {path}")

    # 3. Generar tabla de sanidad
    df_sanidad = generar_tabla_sanidad(code, country, year, engine)

    # 4. Crear documento Word
    doc = Document(TEMPLATE)

    # 5. Sección de título
    render_title(doc, country, year)

    # 6. Sección introductoria
    values = fetch_dynamic_values()
    datos = {
        "farmercode": values.get("farmercode", code),
        "contractcode": values.get("contractcode", code),
        "planting_year": values.get("planting_year", year),
        "contract_trees": values.get("contract_trees", 0),
    }
    render_intro_and_table(
        doc,
        country,
        farmer_name=values.get("farmername", ""),
        datos=datos,
        code=code
    )

    # 7. Texto dinámico
    for paragraph in format_paragraphs(values):
        doc.add_paragraph(paragraph)

    # — 8) Insertar solo las imágenes G1-G3
    resumen_dir = os.path.join(BASE_DIR, "GeneradordeReportes", "outputs", code, "Resumen")
    grafs = [
        ("G1", "G1_Mortality_", "Mortalidad"),
        ("G2", "G2_Altura_", "Altura"),
        ("G3", "G3_Crecimiento_", "Crecimiento"),
    ]

    for key, prefix, title in grafs:
        img_path = os.path.join(resumen_dir, f"{prefix}{code}.png")
        if os.path.exists(img_path):
            # Añadir título antes de la imagen
            doc.add_heading(f"Gráfico {key}: {title}", level=2)
            doc.add_picture(img_path, width=Inches(6.125))
            # Añadir descripción opcional (si es necesario)
            doc.add_paragraph(f"Figura {key} - {title} del contrato {code}")
            doc.add_page_break()
        else:
            print(f"⚠️ Gráfica {key} no encontrada: {img_path}")

    # 9. Tabla de sanidad
    if df_sanidad is not None and not df_sanidad.empty:
        doc.add_heading('Distribución de Plagas, Defectos y Enfermedades', level=2)
        table = doc.add_table(rows=1, cols=len(df_sanidad.columns), style='Table Grid')

        # Encabezados
        hdr_cells = table.rows[0].cells
        for idx, col_name in enumerate(df_sanidad.columns):
            run = hdr_cells[idx].paragraphs[0].add_run(str(col_name))
            run.bold = True
            run.font.size = Pt(10)

        # Datos
        for _, row in df_sanidad.iterrows():
            cells = table.add_row().cells
            for idx, val in enumerate(row):
                cells[idx].text = str(val)
    else:
        print(f"⚠️ No hay datos de sanidad para {code}")

    import time

    def guardar_documento(doc, out_path: str, intentos=3):
        for i in range(intentos):
            try:
                doc.save(out_path)
                return True
            except PermissionError:
                print(f"⚠️ Error de permisos (intento {i + 1}/{intentos}). Cerrando Word...")
                time.sleep(2)
                # Forzar cierre de Word
                try:
                    import win32com.client
                    word = win32com.client.Dispatch("Word.Application")
                    word.Quit()
                except:
                    pass
        return False

    # En tu código:
    out_name = f"Reporte_{code}.docx"
    out_path = os.path.join(OUTPUT_DIR, out_name)
    if guardar_documento(doc, out_path):
        print(f"✅ Reporte creado: {out_path}")
    else:
        print(f"❌ Error crítico: No se pudo guardar {out_path}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('code')
    parser.add_argument('--country', '-c', required=True)
    parser.add_argument('--year', '-y', type=int, required=True)
    args = parser.parse_args()
    crear_reporte(args.code, args.country, args.year)