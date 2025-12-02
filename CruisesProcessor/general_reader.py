# CruisesProcessor/general_reader.py
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
    parser.add_argument("--no-sql", action="store_true", help="NO guardar en BD (solo procesar y exportar a Excel)") # ✅ NUEVO
    return parser.parse_args()


# ============================================================================
# EJEMPLOS DE USO
# ============================================================================

"""
MODO NORMAL (guarda en tabla original):
---------------------------------------
python -m CruisesProcessor --tabla_destino inventory_cr_2024

Resultado:
✅ Guarda en: inventory_cr_2024
⚠️  Filtra solo archivos que necesitan métrica (salta los ya procesados)


MODO SEGURO (guarda en tabla paralela con sufijo 'b'):
------------------------------------------------------
python -m CruisesProcessor --tabla_destino inventory_cr_2024 --no-sql

Resultado:
✅ Guarda en: inventory_cr_2024b  ← TABLA NUEVA
⚠️  NO toca: inventory_cr_2024   ← ORIGINAL INTACTA
✅ Procesa TODOS los archivos (sin filtrar por métrica)


DIFERENCIAS CLAVE:
------------------
MODO NORMAL:
- Solo procesa archivos que faltan en inventory_metrics
- Guarda en tabla original
- Puede saltar archivos ya métricados

MODO SEGURO (--no-sql):
- Procesa TODOS los archivos del batch
- NO filtra por métricas
- Guarda en tabla paralela (nombre + 'b')
- Ideal para reprocesar todo o hacer pruebas


VENTAJAS DEL MODO SEGURO:
------------------------
1. Crea tabla paralela (inventory_cr_2024b)
2. La tabla original queda intacta
3. Procesa todos los archivos sin excepción
4. Puedes comparar ambas versiones
5. Si todo está OK, puedes renombrar después
6. Si algo sale mal, simplemente DROP inventory_cr_2024b


COMPARAR DESPUÉS:
----------------
SELECT COUNT(*) FROM inventory_cr_2024;   -- Original
SELECT COUNT(*) FROM inventory_cr_2024b;  -- Nueva versión

-- Comparar contratos:
SELECT DISTINCT contractcode FROM inventory_cr_2024 ORDER BY contractcode;
SELECT DISTINCT contractcode FROM inventory_cr_2024b ORDER BY contractcode;

-- Si todo OK, reemplazar:
DROP TABLE inventory_cr_2024;
ALTER TABLE inventory_cr_2024b RENAME TO inventory_cr_2024;

-- Si salió mal:
DROP TABLE inventory_cr_2024b;  -- Borras la prueba
"""


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

    # 3. ✅ NUEVO: Filtrar archivos SOLO si NO está en modo --no-sql
    if args.year and not args.no_sql:
        # MODO NORMAL: filtrar solo archivos que necesitan métricas
        missing_contracts = contracts_missing_metrics(args.year)
        files_filtered = []
        for f in args.files:
            cc = f.split('/')[-1].split('_')[0]
            if cc in missing_contracts:
                files_filtered.append(f)
            else:
                print(f"⏩ SKIP {f} (ya métricado)")
        args.files = files_filtered
    elif args.no_sql:
        # MODO SEGURO: procesar TODOS los archivos sin filtrar
        print("\n" + "="*80)
        print("⚠️  MODO SEGURO: Se procesarán TODOS los archivos sin filtro")
        print(f"   Total archivos a procesar: {len(args.files)}")
        print("="*80 + "\n")

    # 4. Cargar los archivos (si quedaron)
    if not args.files:
        print("🎉 Todos los archivos ya están métricados. Nada que procesar.")
        return args, None

    df_combined = combine_files(explicit_files=args.files)
    print(f"🔍 [load_and_prepare_data DESPUÉS de combine_files] Columnas: {df_combined.columns.tolist()}")

    if df_combined is not None and not df_combined.empty:
        generate_summary_from_df(df_combined, args.files)
    else:
        print("⚠️ No se pudo generar el resumen porque el dataframe está vacío.")

    print(f"🔍 [load_and_prepare_data ANTES de return] Columnas: {df_combined.columns.tolist()}")
    return args, df_combined