#!/usr/bin/env python
print("🌎 Hello World Tree!")
from core.libs import argparse, pd, inspect
from utils.cleaners import clean_cruise_dataframe, standardize_units, get_column
from utils.db import get_engine
from utils.sql_helpers import prepare_df_for_sql
from utils.summary import generate_summary

from catalog_normalizer import normalize_catalogs
from union import combine_files
from filters import create_filter_func
from inventory_importer import ensure_table

from inventory_importer import save_inventory_to_sql
from inventory_catalog import create_inventory_catalog
from audit_pipeline import run_audit
from core.doyle_calculator import calculate_doyle
from dead_alive_calculator import calculate_dead_alive
from dead_tree_imputer import add_imputed_dead_rows
from filldown import forward_fill_headers
from tree_id import split_by_id_validity

print("🌎 Iniciando...")

def main():

    parser = argparse.ArgumentParser(
        description="Procesa inventario forestal: combina, limpia, normaliza y guarda en SQL."
    )
    parser.add_argument("--cruises_path", required=True)
    parser.add_argument("--output_file", required=True)
    parser.add_argument("--allowed_codes", nargs="+", default=None)
    parser.add_argument("--table_name", required=True,
                        help="Nombre de la tabla SQL en la que se almacenará el inventario, por ejemplo 'inventory_gt_2025'.")
    parser.add_argument("--country_code", required=True, help="Código de país, por ejemplo GT, US, etc.")
    parser.add_argument(
        "--recreate_table",
        action="store_true",
        help="Drop & recreate table before bulk-insert"
    )

    args = parser.parse_args()

    # Crear filtro opcional
    filter_func = None
    if args.allowed_codes and args.allowed_codes != ["ALL"]:
        filter_func = create_filter_func(args.allowed_codes)


    # Combinar todos los datos
    print("\n📂 Combinando archivos en DataFrame...")
    df_combined = combine_files(args.cruises_path, filter_func=filter_func)
    if df_combined is None or df_combined.empty:
        print("⚠️ No se encontraron datos combinados.")
        return

    df_sum = generate_summary(args.cruises_path)
    if not df_sum.empty:
        print("\n=== Resumen de archivos procesados ===")
        print(df_sum.to_string(index=False))
    else:
        print("⚠️ Ningún archivo válido procesado.")

    # Obtener engine
    engine = get_engine()

    # Limpieza general de columnas y forward fill
    df_combined = clean_cruise_dataframe(df_combined)

    # Completar encabezados repetidos
    df_combined = forward_fill_headers(df_combined)

    # Convertir unidades ya con campos normalizados
    df_combined = standardize_units(df_combined)

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
    df_sql = df_sql.loc[:, ~df_sql.columns.duplicated()]
    ensure_table(
        df_sql,
        engine,
        args.table_name,
        recreate=args.recreate_table
    )

    df_sql = df_sql.replace({pd.NA: None})

    save_inventory_to_sql(
        df_sql,
        engine,
        args.table_name,
        if_exists="append",  # la tabla ya existe
        dtype=dtype_for_sql,
        progress=True,
        pre_cleaned=True
    )

    # Exportar Excel combinado
    df_combined.to_excel(args.output_file, index=False)
    print(f"✅ Guardado Excel combinado: {args.output_file}")

    # Ejecutar auditoría y catálogo adicional
    run_audit(engine, args.table_name, args.output_file)
    create_inventory_catalog(df_combined, engine, f"cat_{args.table_name}")
    print("✅ Proceso completo.")

def _safe_get_column(df, logical_name):
    try:
        get_column(df, logical_name)
        return True
    except KeyError:
        return False

if __name__ == '__main__':
    main()
