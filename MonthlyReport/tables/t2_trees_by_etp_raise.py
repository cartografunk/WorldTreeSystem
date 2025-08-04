# MonthlyReport/tables/t2_trees_by_etp_raise.py
from core.libs import pd
from MonthlyReport.tables_process import get_allocation_type, get_survival_data, format_survival_summary

def build_etp_trees_table2(engine):
    # Cargar tablas
    ca = pd.read_sql("SELECT * FROM masterdatabase.contract_allocation", engine)
    cti = pd.read_sql("SELECT contract_code, etp_year, region, trees_contract, planted FROM masterdatabase.contract_tree_information", engine)

    # 🚨 Hacer merge de inmediato para asegurar que ca ya tenga etp_year y region
    ca = ca.merge(cti[["contract_code", "etp_year", "region"]], on="contract_code", how="left")

    records = []

    df_survival = get_survival_data(engine)


    for year in df_survival["etp_year"].unique():
        allocation_types = get_allocation_type(year)

        if allocation_types == ['COP']:
            df = ca[ca["etp_year"] == year].copy()
            df_grouped = df.groupby("region", dropna=False)[
                ["canada_trees_contracted", "total_can_allocation"]
            ].sum().reset_index()

            df_grouped["etp_year"] = year
            df_grouped["allocation_type"] = "COP"
            df_grouped.rename(columns={
                "canada_trees_contracted": "Contracted",
                "total_can_allocation": "Planted"
            }, inplace=True)

        elif allocation_types == ['ETP']:
            df = cti[cti["etp_year"] == year].copy()
            df_grouped = df.groupby("region", dropna=False)[
                ["trees_contract", "planted"]
            ].sum().reset_index()

            df_grouped["etp_year"] = year
            df_grouped["allocation_type"] = "ETP"
            df_grouped.rename(columns={
                "trees_contract": "Contracted",
                "planted": "Planted"
            }, inplace=True)

        elif allocation_types == ['COP', 'ETP']:
            df = ca[ca["etp_year"] == year].copy()
            df_grouped = df.groupby("region", dropna=False)[
                ["canada_trees_contracted", "usa_trees_contracted", "total_can_allocation", "usa_trees_planted"]
            ].sum().reset_index()

            df_grouped["etp_year"] = year
            df_grouped["allocation_type"] = "COP/ETP"
            df_grouped["Contracted"] = df_grouped["canada_trees_contracted"] + df_grouped["usa_trees_contracted"]
            df_grouped["Planted"] = df_grouped["total_can_allocation"] + df_grouped["usa_trees_planted"]
            df_grouped = df_grouped[["region", "etp_year", "allocation_type", "Contracted", "Planted"]]

        else:
            continue

        records.append(df_grouped)

    df_long = pd.concat(records, ignore_index=True)

    # 1. Agrega Survival desde df_survival
    df_survival["Survival_pct"] = pd.to_numeric(df_survival["Survival"].str.replace("%", ""), errors="coerce") / 100
    df_long = df_long.merge(df_survival[["etp_year", "region", "Survival_pct"]], on=["etp_year", "region"], how="left")

    # 2. Calcular Surviving
    df_long["Surviving"] = (df_long["Planted"] * df_long["Survival_pct"]).round(0).astype("Int64")

    # 3. Convertir a formato largo (Contracted, Planted, Surviving)
    final_long = df_long.melt(
        id_vars=["etp_year", "allocation_type", "region"],
        value_vars=["Contracted", "Planted", "Surviving"],
        var_name="contract_trees_status",
        value_name="value"
    )

    # 4. Crear campo etp
    final_long["etp"] = final_long["allocation_type"] + " " + final_long["etp_year"].astype(str)

    # 5. Pivoteo
    df_pivot = final_long.pivot_table(
        index=["etp", "contract_trees_status"],
        columns="region",
        values="value",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    # 6. Extraer año y tipo
    df_pivot["type"] = df_pivot["etp"].str.extract(r"^(COP|ETP|COP/ETP)")
    df_pivot["year"] = df_pivot["etp"].str.extract(r"(\d{4})").astype(int)
    df_pivot.drop(columns=["etp"], inplace=True)

    # 7. Total y orden
    region_cols = [c for c in ["Costa Rica", "Guatemala", "Mexico", "USA"] if c in df_pivot.columns]
    df_pivot["Total"] = df_pivot[region_cols].sum(axis=1)

    cols = ["year", "type", "contract_trees_status"] + region_cols + ["Total"]
    df_pivot = df_pivot[cols]

    df_pivot.rename(columns={"type": "etp"}, inplace=True)
    df_pivot["contract_trees_status"] = pd.Categorical(
        df_pivot["contract_trees_status"],
        categories=["Contracted", "Planted", "Surviving"],
        ordered=True
    )
    df_pivot.sort_values(by=["year", "etp", "contract_trees_status"], inplace=True, ignore_index=True)

    return df_pivot

