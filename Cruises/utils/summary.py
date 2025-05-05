# utils/summary.py

import os
import pandas as pd
from union import read_metadata_and_input
from utils.schema import COLUMNS
from utils.cleaners import clean_column_name

def generate_summary(cruises_path: str) -> pd.DataFrame:
    """
    Recorre todos los .xlsx, extrae metadata + DataFrame (renombrado según schema),
    y construye un DataFrame con:
      file, contract_code, farmer_name, cruise_date,
      matched_columns, total_trees
    """
    summary = []

    # sólo los campos de input en tu esquema
    expected_fields = [col["key"] for col in COLUMNS if col.get("source")=="input"]

    for root, _, files in os.walk(cruises_path):
        for fname in files:
            if not fname.lower().endswith(".xlsx") or fname.startswith("~$"):
                continue
            path = os.path.join(root, fname)

            # tu función que abre Input + Summary y RENOMBRA con schema
            df, meta = read_metadata_and_input(path)
            if df is None or df.empty:
                continue

            # Asegúrate de limpiar nombres de columna (en caso de que falte)
            df.columns = [clean_column_name(c) for c in df.columns]

            # ahora contamos cuántos de los expected_fields están en df
            matched = sum(1 for logical in expected_fields if logical in df.columns)
            total_trees = len(df)

            summary.append({
                "file": fname,
                "contract": meta.get("contract_code", ""),
                "farmer":   meta.get("farmer_name", ""),
                "cruise_date": (
                    meta.get("cruise_date").date()
                    if pd.notna(meta.get("cruise_date")) else ""
                ),
                "matched_columns": matched,
                "total_trees": total_trees
            })

    return pd.DataFrame(summary)
