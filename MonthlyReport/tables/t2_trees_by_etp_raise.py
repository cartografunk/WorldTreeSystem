# MonthlyReport/tables/t2_trees_by_etp_raise.py
from core.libs import pd
from MonthlyReport.tables_process import get_allocation_type

def build_etp_trees_table2(engine):
    # --- 1) Cargar bases (REGIÓN DESDE FPI) ---
    ca = pd.read_sql("SELECT * FROM masterdatabase.contract_allocation", engine)

    cti = pd.read_sql(
        """
        SELECT contract_code, etp_year, trees_contract, planted
        FROM masterdatabase.contract_tree_information
        """,
        engine
    )

    fpi = pd.read_sql(
        """
        SELECT contract_code, region
        FROM masterdatabase.farmer_personal_information
        """,
        engine
    )

    # Dimensión por contrato: etp_year (CTI) + region (FPI)
    dim = (
        cti[["contract_code", "etp_year"]].drop_duplicates()
           .merge(fpi, on="contract_code", how="left")
    )
    dim["region"] = dim["region"].astype("string").str.strip()
    dim["etp_year"] = pd.to_numeric(dim["etp_year"], errors="coerce").astype("Int64")

    # CTI enriquecido con región (desde FPI)
    cti_en = cti.merge(fpi, on="contract_code", how="left")
    cti_en["region"] = cti_en["region"].astype("string").str.strip()
    cti_en["etp_year"] = pd.to_numeric(cti_en["etp_year"], errors="coerce").astype("Int64")

    # CA enriquecido con etp_year + región (desde dim)
    ca_en = ca.merge(dim[["contract_code", "etp_year", "region"]], on="contract_code", how="left")
    ca_en["region"] = ca_en["region"].astype("string").str.strip()
    ca_en["etp_year"] = pd.to_numeric(ca_en["etp_year"], errors="coerce").astype("Int64")

    # --- 2) Años disponibles (no dependas de survival) ---
    years = sorted(pd.concat([cti_en["etp_year"], ca_en["etp_year"]], ignore_index=True).dropna().unique())

    records = []

    # --- 3) Agregar por tipo de asignación ---
    for year in years:
        allocation_types = get_allocation_type(year)

        if allocation_types == ['COP']:
            df = ca_en[ca_en["etp_year"] == year].copy()
            df_grouped = (
                df.groupby("region", dropna=False)[["canada_trees_contracted", "total_can_allocation"]]
                  .sum(min_count=1)
                  .reset_index()
            )
            df_grouped["etp_year"] = year
            df_grouped["allocation_type"] = "COP"
            df_grouped.rename(columns={
                "canada_trees_contracted": "Contracted",
                "total_can_allocation": "Planted"
            }, inplace=True)

        elif allocation_types == ['ETP']:
            df = cti_en[cti_en["etp_year"] == year].copy()
            df_grouped = (
                df.groupby("region", dropna=False)[["trees_contract", "planted"]]
                  .sum(min_count=1)
                  .reset_index()
            )
            df_grouped["etp_year"] = year
            df_grouped["allocation_type"] = "ETP"
            df_grouped.rename(columns={
                "trees_contract": "Contracted",
                "planted": "Planted"
            }, inplace=True)

        elif allocation_types == ['COP', 'ETP']:
            df = ca_en[ca_en["etp_year"] == year].copy()
            df_grouped = (
                df.groupby("region", dropna=False)[
                    ["canada_trees_contracted", "usa_trees_contracted", "total_can_allocation", "usa_trees_planted"]
                ]
                .sum(min_count=1)
                .reset_index()
            )
            df_grouped["etp_year"] = year
            df_grouped["allocation_type"] = "COP/ETP"
            df_grouped["Contracted"] = df_grouped["canada_trees_contracted"] + df_grouped["usa_trees_contracted"]
            df_grouped["Planted"] = df_grouped["total_can_allocation"] + df_grouped["usa_trees_planted"]
            df_grouped = df_grouped[["region", "etp_year", "allocation_type", "Contracted", "Planted"]]
        else:
            continue

        df_grouped["region"] = df_grouped["region"].astype("string").str.strip()
        records.append(df_grouped)

    if not records:
        return pd.DataFrame(columns=["year", "etp", "contract_trees_status", "Total"])

    df_long = pd.concat(records, ignore_index=True)
    df_long["etp_year"] = pd.to_numeric(df_long["etp_year"], errors="coerce").astype("Int64")
    df_long["region"] = df_long["region"].astype("string").str.strip()

    # --- 4) Surviving por año+región (REGIÓN DESDE FPI) ---
    surviving_agg = pd.read_sql(
        """
        SELECT 
            cti.etp_year::int AS etp_year,
            fpi.region,
            SUM(sc.current_surviving_trees)::bigint AS "Surviving"
        FROM masterdatabase.survival_current sc
        JOIN masterdatabase.contract_tree_information       cti ON sc.contract_code = cti.contract_code
        JOIN masterdatabase.farmer_personal_information     fpi ON sc.contract_code = fpi.contract_code
        GROUP BY cti.etp_year, fpi.region
        """,
        engine
    )
    if not surviving_agg.empty:
        surviving_agg["etp_year"] = pd.to_numeric(surviving_agg["etp_year"], errors="coerce").astype("Int64")
        surviving_agg["region"] = surviving_agg["region"].astype("string").str.strip()
    else:
        surviving_agg = df_long[["etp_year", "region"]].drop_duplicates().copy()
        surviving_agg["Surviving"] = pd.Series([pd.NA] * len(surviving_agg), dtype="Int64")

    df_long = df_long.merge(surviving_agg[["etp_year", "region", "Surviving"]], on=["etp_year", "region"], how="left")
    if "Surviving" not in df_long.columns:
        df_long["Surviving"] = pd.Series(pd.NA, index=df_long.index, dtype="Int64")
    df_long["Surviving"] = pd.to_numeric(df_long["Surviving"], errors="coerce").fillna(0).astype("Int64")

    # --- 5) Largo (Contracted, Planted, Surviving) ---
    final_long = df_long.melt(
        id_vars=["etp_year", "allocation_type", "region"],
        value_vars=["Contracted", "Planted", "Surviving"],
        var_name="contract_trees_status",
        value_name="value"
    )

    # --- 6) Pivot final ---
    final_long["etp"] = final_long["allocation_type"] + " " + final_long["etp_year"].astype(str)
    df_pivot = final_long.pivot_table(
        index=["etp", "contract_trees_status"],
        columns="region",
        values="value",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    df_pivot["type"] = df_pivot["etp"].str.extract(r"^(COP|ETP|COP/ETP)")
    df_pivot["year"] = df_pivot["etp"].str.extract(r"(\d{4})").astype(int)
    df_pivot.drop(columns=["etp"], inplace=True)

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
