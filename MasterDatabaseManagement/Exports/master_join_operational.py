"""
master_join_operational.py
Une CTI + FPI + CA + IMC (+ Survival_Current opcional)
y exporta un único archivo CSV/Excel con la tabla final.
"""

from core.db import get_engine
from core.libs import pd, text
from core.paths import DATABASE_EXPORTS_DIR, safe_mkdir
from datetime import datetime

# === CONFIG ===
SCHEMA = "masterdatabase"
EXPORT_DIR = DATABASE_EXPORTS_DIR
safe_mkdir(EXPORT_DIR)

TABLES = {
    "CTI": "contract_tree_information",
    "FPI": "farmer_personal_information",
    "CA": "contract_allocation",
    "IMC": "inventory_metrics_current",
    "SURV": "survival_current"
}


def extract_table(engine, table_name):
    """Lee una tabla del esquema masterdatabase."""
    query = f'SELECT * FROM {SCHEMA}."{table_name}";'
    with engine.begin() as conn:
        return pd.read_sql(text(query), conn)


def create_master_join(dfs):
    """
    Une progresivamente las tablas cargadas, tolerando farmers sin número asignado.
    """
    df_fpi = dfs.get("FPI", pd.DataFrame()).copy()
    df_cti = dfs.get("CTI", pd.DataFrame()).copy()
    df_ca  = dfs.get("CA",  pd.DataFrame()).copy()
    df_imc = dfs.get("IMC", pd.DataFrame()).copy()
    df_surv = dfs.get("SURV", pd.DataFrame()).copy()

    if df_cti.empty:
        raise ValueError("CTI vacío — no se puede generar el join maestro")

    # --- 🔧 Normalización de contract_codes y farmer_number ---
    if "contract_codes" in df_fpi.columns:
        df_fpi = df_fpi.explode("contract_codes").rename(columns={"contract_codes": "contract_code"})
    if "farmer_number" in df_fpi.columns:
        df_fpi["farmer_number"] = df_fpi["farmer_number"].fillna("NO_FARMER")
        df_fpi.loc[df_fpi["farmer_number"].astype(str).str.strip() == "", "farmer_number"] = "NO_FARMER"

    # --- base del join ---
    master = df_fpi.copy()

    # joins progresivos (siempre por contract_code)
    for key, df in [("CTI", df_cti), ("CA", df_ca), ("IMC", df_imc), ("SURV", df_surv)]:
        if df is None or df.empty:
            print(f"⚠️  {key} vacío — se omite.")
            continue
        if "contract_code" not in df.columns:
            print(f"⚠️  {key} sin columna contract_code — se omite.")
            continue
        master = master.merge(df, on="contract_code", how="outer", suffixes=("", f"_{key.lower()}"))
        print(f"🔗 JOIN con {key} completado")

    # --- 🔧 Post-procesamiento ---
    # Si hay contratos sin farmer_number (p. ej. Olymar CR0014), se marcan explícitamente
    master["farmer_number"] = master["farmer_number"].fillna("NO_FARMER")

    # Ordenar si las columnas existen
    sort_cols = [c for c in ["farmer_number", "contract_code"] if c in master.columns]
    if sort_cols:
        master = master.sort_values(sort_cols)

    return master



def main():
    print(f"\n{'='*60}")
    print("🌳 MASTER JOIN OPERACIONAL: FPI + CTI + CA + IMC + SURV")
    print(f"{'='*60}\n")

    engine = get_engine()
    dfs = {k: extract_table(engine, v) for k, v in TABLES.items()}

    df_master = create_master_join(dfs)

    for col in df_master.select_dtypes(include=["datetimetz"]).columns:
        df_master[col] = df_master[col].dt.tz_localize(None)

    csv_path = EXPORT_DIR / "master_join_operational.csv"
    xlsx_path = EXPORT_DIR / "master_join_operational.xlsx"

    #df_master.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df_master.to_excel(xlsx_path, index=False)

    print(f"\n✅ Exportado:")
    print(f"  CSV:  {csv_path}")
    print(f"  XLSX: {xlsx_path}")
    print(f"  Filas: {len(df_master)}  Columnas: {len(df_master.columns)}\n")


if __name__ == "__main__":
    main()
