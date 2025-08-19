# MasterDatabaseManagement/Changes/new_contract_input_activation.py

from pathlib import Path
from sqlalchemy import text
from core.libs import pd, openpyxl
from openpyxl import load_workbook
from core.db import get_engine
from core.paths import DATABASE_EXPORTS_DIR
from tqdm import tqdm

CATALOG_FILE = Path(DATABASE_EXPORTS_DIR) / "changelog.xlsx"
SHEET_NAME = "NewContractInputLog"
engine = get_engine()

COUNTRY_PREFIX = {
    "USA": "US",
    "United States": "US",
    "Mexico": "MX",
    "México": "MX",
    "Costa Rica": "CR",
    "Guatemala": "GT",
}

def get_prefix(region):
    if not region:
        return None
    return COUNTRY_PREFIX.get(str(region).strip(), None)

def fetch_max_per_prefix(engine, prefix):
    sql = """
    SELECT MAX(CAST(SUBSTRING(contract_code, 3) AS INT)) AS maxnum
    FROM masterdatabase.contract_farmer_information
    WHERE contract_code LIKE :pref
    """
    df = pd.read_sql(sql, engine, params={"pref": f"{prefix}%"})
    return int(df.iloc[0]["maxnum"]) if not df.empty and pd.notna(df.iloc[0]["maxnum"]) else 0

def main():
    wb = load_workbook(CATALOG_FILE)
    ws = wb[SHEET_NAME]

    # Map headers
    header_row = [c.value for c in ws[1]]
    header_map = {col: idx+1 for idx, col in enumerate(header_row)}

    # Asegura columnas Contract Code y DON
    if "Contract Code" not in header_map:
        ws.cell(row=1, column=len(header_row)+1, value="Contract Code")
        header_map["Contract Code"] = len(header_row)+1
    if "DON" not in header_map:
        ws.cell(row=1, column=len(header_row)+1, value="DON")
        header_map["DON"] = len(header_row)+1

    # Preparar contador por prefijo
    counters = {}

    applied = 0
    for i, row in enumerate(ws.iter_rows(min_row=2, max_row=ws.max_row), start=2):
        region = row[header_map["Region"]-1].value
        contract_name = row[header_map["Contract Name"]-1].value
        planting_year = row[header_map["Planting Year"]-1].value
        trees_contract = row[header_map["#TreesContract"]-1].value

        if not region or not contract_name or not planting_year or not trees_contract:
            continue  # fila incompleta

        # Contract Code
        cc_cell = ws.cell(row=i, column=header_map["Contract Code"])
        if not cc_cell.value:
            prefix = get_prefix(region)
            if not prefix:
                continue
            if prefix not in counters:
                counters[prefix] = fetch_max_per_prefix(engine, prefix)
            counters[prefix] += 1
            cc_cell.value = f"{prefix}{counters[prefix]:04d}"

        contract_code = cc_cell.value

        # DON
        don_cell = ws.cell(row=i, column=header_map["DON"])
        if str(don_cell.value).strip().lower() == "done":
            continue  # ya cargado antes

        # Aquí irían los INSERTs a CFI y CTI usando contract_code
        # (ejemplo, simplificado)
        with engine.begin() as conn:
            stmt = text("""
                INSERT INTO masterdatabase.contract_farmer_information (contract_code, contract_name, region)
                VALUES (:cc, :name, :reg)
                ON CONFLICT (contract_code) DO NOTHING
            """)
            conn.execute(stmt, {"cc": contract_code, "name": contract_name, "reg": region})

        # Marca DON
        don_cell.value = "Done"
        applied += 1

    wb.save(CATALOG_FILE)
    print(f"✅ Nuevos contratos aplicados: {applied}. Solo se actualizaron columnas 'Contract Code' y 'DON'.")

if __name__ == "__main__":
    main()
