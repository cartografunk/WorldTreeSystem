import os
import time
import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text
import schedule

# Importar engine y mapeo de campos desde main_database_mapping
from GeneradordeReportes.utils.db import get_engine
from MainDatabase.main_database_mapping import field_schema

# Configuración de rutas y tablas
EXCEL_PATH = os.environ.get(
    'CONTRACT_DB_EXCEL',
    r'C:\Users\HeyCe\World Tree Technologies Inc\Operations - Documentos\Main Database\Contract Database - All Regions.xlsx'
)
SHEET_NAME = 'Master Data Results'
MAIN_TABLE = 'masterdatabase.contract_tree_information'
HISTORY_TABLE = 'masterdatabase.contract_tree_information_history'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def import_contract_info():
    """
    Importa Contract Database al histórico y tabla principal, ajustando esquema dinámicamente.
    Mejora: trata SMALLINT como INTEGER para evitar desbordes.
    """
    try:
        logging.info(f"Leyendo Excel {EXCEL_PATH} hoja {SHEET_NAME}")
        # Cargar todas las columnas para normalizar cabeceras multilinea
        df_raw = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, dtype={'Contract Code': str})
        # Normalizar nombres de encabezado
        df_raw.columns = df_raw.columns.str.replace(r"\s+", " ", regex=True).str.strip()
        # Mapear y extraer columnas definidas
        col_map = {}
        for orig, (col, sql_type) in field_schema.items():
            norm = " ".join(orig.split())
            col_map[norm] = col
        available = [h for h in df_raw.columns if h in col_map]
        df = df_raw[available].rename(columns=col_map)
        # Convertir tipos numéricos
        for orig, (col, sql_type) in field_schema.items():
            if col in df.columns and sql_type.upper() in ('SMALLINT','DECIMAL','DOUBLE PRECISION'):
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df['loaded_at'] = datetime.now(timezone.utc)
        df = df[df['contract_code'].notna()]
        # Preparar registros y NaN->None
        records = [{k: (None if pd.isna(v) else v) for k,v in rec.items()} for rec in df.to_dict(orient='records')]

        engine = get_engine()
        with engine.begin() as conn:
            # Crear/alterar esquema histórico
            # SMALLINT -> INTEGER
            cols_hist = []
            for col, sql_type in field_schema.values():
                sql_type_db = 'INTEGER' if sql_type.upper() == 'SMALLINT' else sql_type
                cols_hist.append(f"{col} {sql_type_db}")
                conn.execute(text(f"ALTER TABLE {HISTORY_TABLE} ADD COLUMN IF NOT EXISTS {col} {sql_type_db};"))
            conn.execute(text(f"CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} ({', '.join(cols_hist)}, loaded_at TIMESTAMPTZ);"))
            conn.execute(text(f"ALTER TABLE {HISTORY_TABLE} ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMPTZ;"))

            # Crear/alterar esquema principal
            for col, sql_type in field_schema.values():
                sql_type_db = 'INTEGER' if sql_type.upper() == 'SMALLINT' else sql_type
                conn.execute(text(f"ALTER TABLE {MAIN_TABLE} ADD COLUMN IF NOT EXISTS {col} {sql_type_db};"))
            conn.execute(text(f"ALTER TABLE {MAIN_TABLE} ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMPTZ;"))

            # Bulk insert historial
            df.to_sql(
                name=HISTORY_TABLE.split('.')[-1],
                schema=HISTORY_TABLE.split('.')[0],
                con=conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            # Upsert principal
            cols = [col for col, _ in field_schema.values()] + ['loaded_at']
            insert_cols = ','.join(cols)
            insert_vals = ','.join(f":{c}" for c in cols)
            update_sets = ','.join(f"{c}=EXCLUDED.{c}" for c in cols if c!='contract_code')
            upsert_sql = f"""
                INSERT INTO {MAIN_TABLE} ({insert_cols}) VALUES ({insert_vals})
                ON CONFLICT (contract_code) DO UPDATE SET {update_sets};
            """
            conn.execute(text(upsert_sql), records)

        logging.info("Importación completada.")
    except Exception:
        logging.exception("Error al importar Contract Database:")


def main():
    import_contract_info()
    schedule.every().hour.do(import_contract_info)
    while True:
        schedule.run_pending()
        time.sleep(10)

if __name__ == '__main__':
    main()
