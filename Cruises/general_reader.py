# Cruises/reader.py
from core.libs import argparse, json
from Cruises.xlsx_read_and_merge import combine_files
from Cruises.import_summary import generate_summary_from_df
from core.paths import resolve_inventory_paths

def get_args():
    parser = argparse.ArgumentParser(description="Cargar inventario forestal.")
    parser.add_argument("--files", nargs="+", help="Archivos XLSX a procesar", required=False)
    parser.add_argument("--tabla_destino", help="Nombre corto del lote", required=True)
    parser.add_argument("--tabla_sql", help="Nombre de la tabla SQL", required=False)
    parser.add_argument("--batch_imports_path", help="Ruta a batch_imports.json", default="Cruises/batch_imports.json")
    parser.add_argument("--country_code", help="Código del país (mx, cr, gt, us)", required=False)
    parser.add_argument("--year", help="Año del inventario", required=False)
    parser.add_argument("--save_summary", action="store_true", help="Guardar resumen en archivo")
    return parser.parse_args()


def load_batch_config(tabla_destino, path="Cruises/batch_imports.json"):
    with open(path, encoding="utf-8") as f:
        lotes = json.load(f)

    for lote in lotes:
        if lote.get("tabla_destino") == tabla_destino:
            return lote
    raise ValueError(f"❌ No se encontró lote con tabla_destino={tabla_destino}")


def load_and_prepare_data():
    args = get_args()

    if not args.files:
        lote = load_batch_config(args.tabla_destino, args.batch_imports_path)
        args.files = lote.get("archivos", [])
        args.country_code = args.country_code or lote.get("pais", "")[:2].lower()
        args.year = args.year or lote.get("año")

        # 🔧 CORREGIDO: ya no mete country ni year
        args.files = resolve_inventory_paths(args.files)

        print(f"📦 Archivos desde lote: {args.files}")

        df_combined = combine_files(explicit_files=args.files)

        if df_combined is not None and not df_combined.empty:
            generate_summary_from_df(df_combined)
        else:
            print("⚠️ No se pudo generar el resumen porque el dataframe está vacío.")

    return args, df_combined

