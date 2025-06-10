#WorldTreeSystem/Cruises/main.py

print("🌎 Hello World Tree!")
from core.libs import pd
from core.db import get_engine
from core.schema_helpers import FINAL_ORDER

from Cruises.catalog_normalizer import parse_country_code
from Cruises.general_reader import load_and_prepare_data
from Cruises.general_importer import prepare_df_for_sql, ensure_table, save_inventory_to_sql, create_inventory_catalog, marcar_lote_completado, cast_dataframe
from Cruises.general_processing import process_inventory_dataframe
from Cruises.audit_pipeline import run_audit
from Cruises.import_summary import generate_summary_from_df


print("🌎 Iniciando...")

def main():
    args, df_combined = load_and_prepare_data()

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

    country_code = parse_country_code(args.tabla_destino)
    df_good, df_bad = process_inventory_dataframe(df_combined, engine, country_code)

    if not df_bad.empty:
        print(f"⚠️  {len(df_bad)} filas ignoradas por ID inválido.")

        # Diagnóstico (esto lo puedes dejar tal cual)
        diag_cols = [c for c in ("contractcode", "plot", "tree_number", "tree") if c in df_bad.columns]
        print(df_bad[diag_cols].head().to_string(index=False))

        # Reordena columnas para exportar el archivo bad_rows en el orden de FINAL_ORDER
        cols_in_final = [c for c in FINAL_ORDER if c in df_bad.columns]
        extra_cols = [c for c in df_bad.columns if c not in FINAL_ORDER]
        df_bad_export = df_bad[cols_in_final + extra_cols]

        bad_report = f"bad_rows_{args.tabla_destino}.xlsx"
        df_bad_export.to_excel(bad_report, index=False)
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

    # 🎯 2) Alinea todos los dtypes contra schema.py

    df_sql = df_sql.loc[:, ~df_sql.columns.duplicated()]
    ensure_table(
        df_sql,
        engine,
        args.tabla_destino,
        recreate=args.recreate_table
    )

    # 👇 Solo haz replace en columnas que NO son fecha
    for col in df_sql.columns:
        if "date" not in col.lower():
            df_sql[col] = df_sql[col].replace({pd.NA: None})

    # 🔒 Castea fechas a datetime.date, por si algo se perdió antes
    if 'cruisedate' in df_sql.columns:
        df_sql['cruisedate'] = pd.to_datetime(df_sql['cruisedate'], errors="coerce").dt.date

    #Normaliza dtypes una sola vez
    df_sql = cast_dataframe((df_sql))

    save_inventory_to_sql(
        df_sql,
        engine,
        args.tabla_destino,
        if_exists="append",  # la tabla ya existe
        dtype=dtype_for_sql,
        progress=True
    )

if __name__ == '__main__':
    main()


