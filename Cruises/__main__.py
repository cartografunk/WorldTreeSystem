#WorldTreeSystem/Cruises/main.py

print("🌎 Hello World Tree!")
from core.libs import argparse, pd, Path, json
from core.db import get_engine

from Cruises.reader import load_and_prepare_data
from Cruises.global_importer import prepare_df_for_sql, ensure_table, save_inventory_to_sql, create_inventory_catalog, marcar_lote_completado
from Cruises.processing import process_inventory_dataframe
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

    df_good, df_bad = process_inventory_dataframe(df_combined, engine, args.country_code)

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
    from Cruises.general_importer from Cruises.global_importer import prepare_df_for_sqldataframe
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
