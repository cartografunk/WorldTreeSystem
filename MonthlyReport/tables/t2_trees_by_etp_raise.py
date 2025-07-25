# MonthlyReport/tables/t2_trees_by_etp_raise.py

from core.libs import pd

from core.libs import pd

def build_etp_trees_table2(engine, df_survival):
    # 1. Leer datos bases (puedes ajustar nombres de columnas si cambian)
    ca = pd.read_sql("SELECT * FROM masterdatabase.contract_allocation", engine)
    cti = pd.read_sql("SELECT contract_code, etp_year, region, trees_contract, planted FROM masterdatabase.contract_tree_information", engine)

    # 2. Merge para tener region, etp_year en ca
    ca = ca.merge(cti[["contract_code", "etp_year", "region"]], on="contract_code", how="left")

    # 3. Preparar allocation_types a procesar
    allocation_types = ['COP', 'ETP']
    records = []

    for allocation_type in allocation_types:
        if allocation_type == "COP":
            grouped = ca.groupby(["etp_year", "region"], dropna=False)["total_can_allocation"].sum().reset_index()
            grouped["contract_trees_status"] = "Contracted"
            grouped.rename(columns={"total_can_allocation": "value"}, inplace=True)
        elif allocation_type == "ETP":
            grouped = ca.groupby(["etp_year", "region"], dropna=False)["usa_trees_planted"].sum().reset_index()
            grouped["contract_trees_status"] = "Planted"
            grouped.rename(columns={"usa_trees_planted": "value"}, inplace=True)
        else:
            continue
        grouped["allocation_type"] = allocation_type
        records.append(grouped)

    df_long = pd.concat(records, ignore_index=True)

    # 4. Unir Contracted y Planted para hacer Surviving
    # Pivot para juntar Contracted y Planted en columnas
    temp = df_long.pivot_table(index=["etp_year", "allocation_type", "region"], columns="contract_trees_status", values="value", fill_value=0).reset_index()

    # 5. Agregar Survival desde df_survival
    # Asegúrate que survival esté en formato numérico (sin %), con columnas: etp_year, region, Survival
    df_survival["Survival_pct"] = pd.to_numeric(df_survival["Survival"].str.replace("%",""), errors="coerce") / 100
    temp = temp.merge(df_survival[["etp_year", "region", "Survival_pct"]], on=["etp_year", "region"], how="left")

    # 6. Calcular Surviving
    temp["Surviving"] = (temp["Planted"] * temp["Survival_pct"]).round(0).astype("Int64")

    # 7. Convierte de wide a long, para tener contract_trees_status (Contracted, Planted, Surviving)
    final_long = temp.melt(
        id_vars=["etp_year", "allocation_type", "region"],
        value_vars=["Contracted", "Planted", "Surviving"],
        var_name="contract_trees_status",
        value_name="value"
    )

    # 8. Crea columna etp (tipo + año, sin decimales)
    final_long["etp"] = final_long["allocation_type"] + " " + final_long["etp_year"].astype(int).astype(str)

    # 9. Pivotea para regiones como columnas
    df_pivot = final_long.pivot_table(
        index=["etp", "contract_trees_status"],
        columns="region",
        values="value",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    # 10. Ordena columnas y suma total
    region_cols = [col for col in ["Costa Rica", "Guatemala", "Mexico", "USA"] if col in df_pivot.columns]
    df_pivot["Total"] = df_pivot[region_cols].sum(axis=1)
    cols = ["etp", "contract_trees_status"] + region_cols + ["Total"]
    df_pivot = df_pivot[cols]

    return df_pivot

# Ejemplo de uso:
# engine = get_engine()
# df1 = build_etp_summary(engine)  # Tu tabla 1, ya calculada y lista
# df2 = build_etp_trees_table2(engine, df1)
# df2.to_excel("etp_trees_pivot.xlsx", index=False)
