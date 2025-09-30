import pandas as pd
from sqlalchemy import text
from core.db import get_engine

engine = get_engine()

path = r"C:\Users\HeyCe\World Tree Technologies Inc\Operations - Documentos\WorldTreeSystem\DatabaseExports\new_ca.xlsx"
df = pd.read_excel(path, sheet_name="new_ca")

# Asegurar numéricos en las ajustadas
for c in [
    "Adjusted_Contracted_COP",
    "Adjusted_Planted_COP",
    "Adjusted_Contracted_ETP",
    "Adjusted_ETP_Planted",
]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

with engine.begin() as conn:
    for _, row in df.iterrows():
        q = text("""
            UPDATE masterdatabase.contract_allocation
            SET etp_type       = :etp_type,
                contracted_cop = :adj_cop_con,
                planted_cop    = :adj_cop_pla,
                contracted_etp = :adj_etp_con,
                planted_etp    = :adj_etp_pla
            WHERE contract_code = :contract_code
        """)

        conn.execute(q, {
            "contract_code": row["Contract Code"],
            "etp_type": row.get("etp_type"),
            "adj_cop_con": row.get("Adjusted_Contracted_COP"),
            "adj_cop_pla": row.get("Adjusted_Planted_COP"),
            "adj_etp_con": row.get("Adjusted_Contracted_ETP"),
            "adj_etp_pla": row.get("Adjusted_ETP_Planted"),
        })

