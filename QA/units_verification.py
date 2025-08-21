#QA/unit_verification.py

from core.db import get_engine
from InventoryMetrics.inventory_retriever import get_inventory_tables
from core.schema_helpers import get_column
import pandas as pd


def has_weird_decimals(series, n=5):
    # Busca decimales con más de n dígitos, por ejemplo 0.3937007874015748
    return series.dropna().astype(str).str.contains(r'\.\d{' + str(n + 1) + ',}')


def sample_and_check(engine, table, columns=["dbh_in", "tht_ft"], sample_size=100):
    results = []
    for col in columns:
        col_name = get_column(col)
        sql = f"SELECT {col_name} FROM public.{table} WHERE {col_name} IS NOT NULL"
        try:
            df = pd.read_sql(sql, engine)
            if df.empty:
                continue
            # Muestreo
            sample = df.sample(min(sample_size, len(df)), random_state=42)
            # Decimales raros
            weirds = sample[sample[col_name].apply(lambda x: isinstance(x, float) and (abs(x - round(x, 2)) > 1e-5))]
            weirds_long = sample[has_weird_decimals(sample[col_name], n=5)]
            # Junta ambos criterios
            merged = pd.concat([weirds, weirds_long]).drop_duplicates()
            if not merged.empty:
                # Guarda solo algunos ejemplos para revisar
                example_values = merged[col_name].head(5).tolist()
                results.append({
                    "table": table,
                    "field": col_name,
                    "weird_examples": example_values,
                    "total_weird_found": merged.shape[0],
                    "total_sampled": sample.shape[0],
                })
        except Exception as e:
            print(f"Error en {table} campo {col}: {e}")
    return results


def main():
    engine = get_engine()
    tables = get_inventory_tables(engine)
    all_results = []

    for table in tables:
        print(f"Chequeando {table}...")
        res = sample_and_check(engine, table)
        all_results.extend(res)

    # Tabla de corroboración
    df_corrob = pd.DataFrame(all_results)
    print("\nTABLA DE CORROBORACIÓN:")
    print(df_corrob)
    df_corrob.to_excel("corroboracion_unidades_decimalessospechosos.xlsx", index=False)
    print("\n✅ Archivo generado: corroboracion_unidades_decimalessospechosos.xlsx")


if __name__ == "__main__":
    main()
