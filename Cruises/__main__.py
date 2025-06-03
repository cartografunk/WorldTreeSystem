#WorldTreeSystem/Cruises/main.py

print("🌎 Hello World Tree!")
from core.libs import argparse, pd, Path, json
from core.db import get_engine
from core.doyle_calculator import calculate_doyle
from core.paths import INVENTORY_BASE

from Cruises.utils.cleaners import clean_cruise_dataframe, standardize_units, remove_blank_rows
from Cruises.utils.sql_helpers import prepare_df_for_sql
from Cruises.catalog_normalizer import normalize_catalogs
from Cruises.union import combine_files
from Cruises.filters import create_filter_func
from Cruises.inventory_importer import ensure_table

from Cruises.inventory_importer import save_inventory_to_sql
from Cruises.inventory_catalog import create_inventory_catalog
from Cruises.audit_pipeline import run_audit
from Cruises.dead_alive_calculator import calculate_dead_alive
from Cruises.dead_tree_imputer import add_imputed_dead_rows
from Cruises.filldown import forward_fill_headers
from Cruises.tree_id import split_by_id_validity
from Cruises.import_summary import generate_summary_from_df
from Cruises.status_para_batch_imports import marcar_lote_completado


print("🌎 Iniciando...")

def main():
    # Paso 1: parse preliminar para batch_imports_path y tabla_destino
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--batch_imports_path")
    pre_parser.add_argument("--tabla_destino")
    pre_parser.add_argument("--batch_id")
    pre_args, _ = pre_parser.parse_known_args()

    # Paso 2: parser completo con todos los argumentos
    parser = argparse.ArgumentParser(conflict_handler="resolve")
    parser.add_argument("--batch_imports_path", help="Ruta a batch_imports.json")
    parser.add_argument("--tabla_destino", help="Nombre de tabla_destino a buscar en batch_imports.json")
    parser.add_argument("--cruises_path", required=False)
    parser.add_argument("--table_name", required=False)
    parser.add_argument("--country_code", required=False)
    parser.add_argument("--year", type=int, required=False)
    parser.add_argument("--output_file", required=True)
    parser.add_argument("--files", nargs="+")
    parser.add_argument("--allowed_codes", nargs="*")
    parser.add_argument("--recreate_table", action="store_true")
    parser.add_argument("--batch_id", help="ID del lote definido en batch_imports.json")

    args = parser.parse_args()

    # Paso 3: cargar lote si corresponde
    # Cargar lote por batch_id, NO por tabla_destino
    if pre_args.batch_imports_path and pre_args.batch_id:
        with open(pre_args.batch_imports_path, encoding="utf-8") as f:
            lotes = json.load(f)

        lote = next((l for l in lotes if l.get("batch_id") == pre_args.batch_id), None)

        if not lote:
            print(f"❌ No se encontró batch_id={pre_args.batch_id} en {pre_args.batch_imports_path}")
            exit(1)

        # Asignar a args
        args.cruises_path = lote["carpeta"]
        args.table_name = lote["tabla_destino"]
        args.country_code = lote["pais"]
        args.year = lote["año"]

        # Convertir rutas relativas a absolutas si es necesario
        args.files = [
            str(Path(f)) if Path(f).is_absolute() else str(INVENTORY_BASE / Path(f))
            for f in lote["archivos"]
        ]

    parser.add_argument("--batch_imports_path", help="Ruta a batch_imports.json")
    parser.add_argument("--tabla_destino", help="Nombre de tabla_destino a buscar en batch_imports.json")


    # ========================================
    # ✅ Soporte para carga por lote desde JSON
    # ========================================
    if args.batch_imports_path and args.tabla_destino:

        batch_imports_path = Path(args.batch_imports_path)
        with open(batch_imports_path, encoding="utf-8") as f:
            lotes = json.load(f)

        lote = next((l for l in lotes if l["tabla_destino"] == args.tabla_destino), None)

        if not lote:
            print(f"❌ No se encontró tabla_destino={args.tabla_destino} en {args.batch_imports_path}")
            exit(1)

        # Rellenar los argumentos faltantes
        args.cruises_path = lote["carpeta"]
        args.table_name = lote["tabla_destino"]
        args.country_code = lote["pais"]
        args.year = lote["año"]
        args.files = lote["archivos"]  # ✅ así conserva las subcarpetas

        print(f"🚀 Cargando lote {args.table_name} ({args.year}) desde {args.batch_imports_path}")
        print(f"📂 Carpeta: {args.cruises_path}")
        print(f"📄 Archivos: {len(args.files)} archivos")


    # Crear filtro opcional
    filter_func = None
    if args.allowed_codes and args.allowed_codes != ["ALL"]:
        filter_func = create_filter_func(args.allowed_codes)


    # Combinar todos los datos
    #print("\n📂 Combinando archivos en DataFrame...")
    df_combined = combine_files(
        args.cruises_path,
        filter_func=filter_func,
        explicit_files=args.files if args.files else None
    )

    if df_combined is None or df_combined.empty:
        print("⚠️ No se encontraron datos combinados.")
        return

    # Reemplaza el summary antiguo por este:
    df_sum = generate_summary_from_df(df_combined, args.files)
    if not df_sum.empty:
        print("\n=== Resumen de archivos procesados ===")
        print(df_sum.to_string(index=False))
    else:
        print("⚠️ Resumen no disponible.")

    # Obtener engine
    engine = get_engine()

    # Limpieza general de columnas y forward fill
    df_combined = clean_cruise_dataframe(df_combined)
    df_combined = remove_blank_rows(df_combined)

    # Completar encabezados repetidos
    df_combined = forward_fill_headers(df_combined)

    # Convertir unidades ya con campos normalizados
    df_combined = standardize_units(df_combined)

    # Completar encabezados repetidos
    df_combined = forward_fill_headers(df_combined)

    # Convertir unidades ya con campos normalizados
    df_combined = standardize_units(df_combined)

    # 💾 Guardar el valor original antes de que lo pise normalize_catalogs
    df_combined["status_text_raw"] = df_combined["Status"]

    # Normalizar catálogos a ID (incluye status_id)
    df_combined = normalize_catalogs(
        df_combined,
        engine,
        logical_keys=["Status", "Species", "Defect", "Disease", "Pests", "Coppiced", "Permanent Plot"],
        country_code=args.country_code
    )


    # Calcular volumen Doyle
    df_combined = calculate_doyle(df_combined)

    # Calcular árboles vivos/muertos usando status_id
    df_combined = calculate_dead_alive(df_combined, engine)

    # Subsanar árboles muertos
    df_combined = add_imputed_dead_rows(
        df_combined,
        contract_col="contractcode",
        plot_col="plot",
        dead_col="dead_tree"
    )

    # Eliminar columnas de status innecesarias para el insert final
    for col in ["Status", "status_id", "status_text_raw"]:
        if col in df_combined.columns:
            df_combined.drop(columns=col, inplace=True)

    # Crear IDs de árbol
    df_good, df_bad = split_by_id_validity(df_combined)

    if not df_bad.empty:
        print(f"⚠️  {len(df_bad)} filas ignoradas por ID inválido.")

        # columnas que EXISTEN en df_bad (evita KeyError)
        diag_cols = [c for c in ("contractcode", "plot", "tree_number", "tree") if c in df_bad.columns]
        print(df_bad[diag_cols].head().to_string(index=False))

        # Nombre del archivo en la raíz del repo
        bad_report = f"bad_rows_{args.table_name}.xlsx"
        # Guardar el Excel (UTF-8 ya viene soportado)
        df_bad.to_excel(bad_report, index=False)
        print(f"📄 Reporte de filas excluidas → {bad_report}")

    #Chequeo de duplicados
    duplicated_ids = df_good[df_good['id'].duplicated(keep=False)].copy()

    if not duplicated_ids.empty:
        print(f"\n🚨 Detectados {duplicated_ids.shape[0]} registros con IDs duplicados reales.")
        print(duplicated_ids[['id', 'contractcode', 'plot', 'tree_number']].head(20))
        duplicated_ids.to_csv("duplicated_ids_found.csv", index=False)
        print("📄 Exportados duplicados a duplicated_ids_found.csv")
    else:
        print("\n✅ No hay IDs duplicados reales. Todo OK.")

    # Insertar en SQL
    df_sql, dtype_for_sql = prepare_df_for_sql(df_good)

    # 🔄 1) Quita la parte de hora y fuerza datetime64[ns]
    from pandas import to_datetime
    df_sql["CruiseDate"] = (
        to_datetime(df_sql["CruiseDate"], errors="coerce")
        .dt.date  # 2025-06-14 00:00:00 → 2025-06-14
    )

    # 🎯 2) Alinea todos los dtypes contra schema.py
    from core.schema import cast_dataframe
    df_sql = cast_dataframe(df_sql)
    
    df_sql = df_sql.loc[:, ~df_sql.columns.duplicated()]
    ensure_table(
        df_sql,
        engine,
        args.table_name,
        recreate=args.recreate_table
    )

    df_sql = df_sql.replace({pd.NA: None})

    #Normaliza dtypes una sola vez
    df_sql = cast_dataframe((df_sql))

    save_inventory_to_sql(
        df_sql,
        engine,
        args.table_name,
        if_exists="append",  # la tabla ya existe
        dtype=dtype_for_sql,
        progress=True,
        pre_cleaned=True
    )

    try:
        # Proceso principal...
        df_combined.to_excel(args.output_file, index=False)
        print(f"✅ Guardado Excel combinado: {args.output_file}")

        run_audit(engine, args.table_name, args.output_file)
        create_inventory_catalog(df_combined, engine, f"cat_{args.table_name}")
        print("✅ Proceso completo.")

        # Marcar como completado
        if getattr(args, "batch_imports_path", None) and getattr(args, "tabla_destino", None):
            tabla_sql = f"public.{args.table_name}"
            marcar_lote_completado(args.batch_imports_path, args.tabla_destino, tabla_sql)

    except Exception as e:
        print(f"❌ Error fatal durante el procesamiento: {e}")


if __name__ == '__main__':
    main()
