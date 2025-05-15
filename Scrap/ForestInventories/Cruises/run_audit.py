#!/usr/bin/env python

from pathlib import Path
from core.libs import pd
from utils.db import get_engine
from inventory_importer import save_inventory_to_sql


def create_audit_table(engine, table_name, output_excel_folder=None):
    parts = table_name.split("_")
    if len(parts) != 3:
        raise ValueError("El nombre de la tabla debe ser del tipo: inventory_<país>_<año>")

    country_code = parts[1].upper()
    year = parts[2]
    inventory_catalog = f"cat_inventory_{country_code.lower()}_{year}"
    audit_table = f"audit_{country_code.lower()}_{year}"

    df_inventory = pd.read_sql_table(table_name, engine)
    df_catalog = pd.read_sql_table(inventory_catalog, engine)
    allowed_contracts = df_catalog["ContractCode"].unique()
    df_inventory = df_inventory[df_inventory["ContractCode"].isin(allowed_contracts)]

    df_status = pd.read_sql('SELECT id, "DeadTreeValue", "AliveTree" FROM cat_status', engine)
    df_merged = pd.merge(df_inventory, df_status, left_on="status_id", right_on="id", how="left")

    grouped = df_merged.groupby("ContractCode").agg(
        Total_Deads=("DeadTreeValue", "sum"),
        Total_Alive=("AliveTree", "sum"),
        Trees_Sampled=("ContractCode", "count")
    ).reset_index()

    df_farmers = pd.read_sql('SELECT "ContractCode", "FarmerName", "PlantingYear", "#TreesContract" FROM cat_farmers', engine)
    df_farmers.rename(columns={"#TreesContract": "Contracted_Trees"}, inplace=True)

    audit = pd.merge(df_farmers, grouped, on="ContractCode", how="inner")
    audit.fillna({"Total_Deads": 0, "Total_Alive": 0, "Trees_Sampled": 0}, inplace=True)

    audit["Sample Size"] = (audit["Trees_Sampled"] / audit["Contracted_Trees"]).fillna(0)
    audit["Mortality"] = (audit["Total_Deads"] / audit["Trees_Sampled"]).replace([float("inf"), float("nan")], 0)
    audit["Survival"] = (audit["Total_Alive"] / audit["Trees_Sampled"]).replace([float("inf"), float("nan")], 0)
    audit["Remaining Trees"] = audit["Contracted_Trees"] - audit["Total_Alive"]

    audit.rename(columns={
        "FarmerName": "Farmer Name",
        "ContractCode": "Contract Code",
        "PlantingYear": "Planting Year",
        "Trees_Sampled": "Trees Sampled",
        "Total_Deads": "Total Deads",
        "Total_Alive": "Total Alive",
        "Contracted_Trees": "Contracted Trees"
    }, inplace=True)

    audit["Sample Size"] = (audit["Sample Size"] * 100).round(1).astype(str) + "%"
    audit["Mortality"] = (audit["Mortality"] * 100).round(1).astype(str) + "%"
    audit["Survival"] = (audit["Survival"] * 100).round(1).astype(str) + "%"

    save_inventory_to_sql(audit, engine, audit_table, if_exists="replace")
    print(f"✅ Tabla de auditoría guardada en SQL: {audit_table}")

    if output_excel_folder:
        output_path = Path(output_excel_folder) / f"{audit_table}.xlsx"
        audit.to_excel(output_path, index=False)
        print(f"📁 Exportada también a Excel: {output_path}")

    return audit


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Genera tabla de auditoría por contrato.")
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--output_folder", default=None)

    args = parser.parse_args()
    engine = get_engine()
    create_audit_table(engine, args.table_name, args.output_folder)
