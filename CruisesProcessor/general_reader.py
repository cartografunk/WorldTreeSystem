# CruisesProcessor/reader.py
from core.libs import argparse, json
from CruisesProcessor.xlsx_read_and_merge import combine_files
from CruisesProcessor.import_summary import generate_summary_from_df
from CruisesProcessor.general_preparation import contracts_missing_metrics
from core.paths import resolve_inventory_paths

def get_args():
    parser = argparse.ArgumentParser(description="Cargar inventario forestal.")
    parser.add_argument("--files", nargs="+", help="Archivos XLSX a procesar", required=False)
    parser.add_argument("--tabla_destino", help="Nombre corto del lote", required=True)
    parser.add_argument("--tabla_sql", help="Nombre de la tabla SQL", required=False)
    parser.add_argument("--batch_imports_path", help="Ruta a batch_imports.json", default="CruisesProcessor/batch_imports.json")
    parser.add_argument("--country_code", help="Código del país (mx, cr, gt, us)", required=False)
    parser.add_argument("--year", help="Año del inventario", required=False)
    parser.add_argument("--save_summary", action="store_true", help="Guardar resumen en archivo")
    parser.add_argument("--recreate-table", action="store_true", help="Si se especifica, recrea la tabla SQL antes de volcar datos")
    return parser.parse_args()


def load_batch_config(tabla_destino, path="CruisesProcessor/batch_imports.json"):
    with open(path, encoding="utf-8") as f:
        lotes = json.load(f)

    for lote in lotes:
        if lote.get("tabla_destino") == tabla_destino:
            return lote
    raise ValueError(f"❌ No se encontró lote con tabla_destino={tabla_destino}")


def load_and_prepare_data():
    args = get_args()

    # 1. Cargar archivos desde batch si no vienen por argumento
    if not args.files:
        lote = load_batch_config(args.tabla_destino, args.batch_imports_path)
        args.files = lote.get("archivos", [])
        args.country_code = args.country_code or lote.get("pais", "")[:2].lower()
        args.year = args.year or lote.get("año")

    # 2. Resolver paths absolutos
    args.files = resolve_inventory_paths(args.files)

    # 3. Filtrar archivos si hay año definido
    if args.year:
        missing_contracts = contracts_missing_metrics(args.year)
        files_filtered = []
        for f in args.files:
            cc = f.split('/')[-1].split('_')[0]
            if cc in missing_contracts:
                files_filtered.append(f)
            else:
                print(f"⏩ SKIP {f} (ya métricado)")
        args.files = files_filtered

    # 4. Cargar los archivos (si quedaron)
    if not args.files:
        print("🎉 Todos los archivos ya están métricados. Nada que procesar.")
        return args, None

    df_combined = combine_files(explicit_files=args.files)

    if df_combined is not None and not df_combined.empty:
        generate_summary_from_df(df_combined, args.files)
    else:
        print("⚠️ No se pudo generar el resumen porque el dataframe está vacío.")

    return args, df_combined


