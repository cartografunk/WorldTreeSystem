# MonthlyReport/tables/t2_trees_by_etp_raise.py
from core.libs import pd
from MonthlyReport.tables_process import get_allocation_type

def build_etp_trees_table2(engine):
    # --- 1) Cargar bases ---
    ca = pd.read_sql("SELECT * FROM masterdatabase.contract_allocation", engine)
    cti = pd.read_sql(
        """
        SELECT contract_code, etp_year, region, trees_contract, planted
        FROM masterdatabase.contract_tree_information
        """,
        engine
    )

    # --- 2) Normalizar llaves (región/año) ---
    cti["region"] = cti["region"].astype("string").str.strip()
    # Asegura dtype numérico para el año
    cti["etp_year"] = pd.to_numeric(cti["etp_year"], errors="coerce").astype("Int64")
    # ✅ después del merge con sufijos, fuerza a usar etp_year/region de cti
    ca = ca.merge(
        cti[["contract_code", "etp_year", "region"]],
        on="contract_code",
        how="left",
        suffixes=("", "_cti")
    )
    if "etp_year_cti" in ca.columns:
        ca["etp_year"] = ca["etp_year_cti"]
        ca.drop(columns=["etp_year_cti"], inplace=True, errors="ignore")
    if "region_cti" in ca.columns:
        ca["region"] = ca["region_cti"].astype("string").str.strip()
        ca.drop(columns=["region_cti"], inplace=True, errors="ignore")

    # ✅ lista de años: SOLO desde cti
    years = sorted(cti["etp_year"].dropna().unique())

    # Merge ca + (etp_year, region) desde cti con sufijos controlados
    ca = ca.merge(
        cti[["contract_code", "etp_year", "region"]],
        on="contract_code",
        how="left",
        suffixes=("", "_cti")
    )
    if "region_cti" in ca.columns:
        ca["region"] = ca["region_cti"].astype("string").str.strip()
        ca.drop(columns=["region_cti"], inplace=True, errors="ignore")
    if "etp_year_cti" in ca.columns:
        ca["etp_year"] = ca["etp_year_cti"]
        ca.drop(columns=["etp_year_cti"], inplace=True, errors="ignore")

    # --- 3) Años disponibles (no dependas de survival) ---
    years = sorted(pd.concat([cti["etp_year"], ca["etp_year"]], ignore_index=True).dropna().unique())

    records = []

    # --- 4) Agregar por tipo de asignación ---
    for year in years:
        allocation_types = get_allocation_type(year)

        if allocation_types == ['COP']:
            df = ca[ca["etp_year"] == year].copy()
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
            df = cti[cti["etp_year"] == year].copy()
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
            df = ca[ca["etp_year"] == year].copy()
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

        # Normaliza región aquí también
        df_grouped["region"] = df_grouped["region"].astype("string").str.strip()
        records.append(df_grouped)

    if not records:
        return pd.DataFrame(columns=["year", "etp", "contract_trees_status", "Total"])

    df_long = pd.concat(records, ignore_index=True)
    df_long["etp_year"] = pd.to_numeric(df_long["etp_year"], errors="coerce").astype("Int64")
    df_long["region"] = df_long["region"].astype("string").str.strip()

    # --- 5) Surviving BRUTO (sumado por año+región) ---
    surviving_agg = pd.read_sql(
        """
        SELECT 
            cti.etp_year::int AS etp_year,
            cti.region,
            SUM(sc.current_surviving_trees)::bigint AS "Surviving"
        FROM masterdatabase.survival_current sc
        JOIN masterdatabase.contract_tree_information cti
          ON sc.contract_code = cti.contract_code
        GROUP BY cti.etp_year, cti.region
        """,
        engine
    )
    # Asegura dtypes/espacios compatibles con df_long
    if not surviving_agg.empty:
        surviving_agg["etp_year"] = pd.to_numeric(surviving_agg["etp_year"], errors="coerce").astype("Int64")
        surviving_agg["region"] = surviving_agg["region"].astype("string").str.strip()

    # Si el query regresó vacío, crea un DF vacío con las columnas esperadas para que el merge SÍ cree 'Surviving'
    if surviving_agg.empty:
        surviving_agg = df_long[["etp_year", "region"]].drop_duplicates().copy()
        surviving_agg["Surviving"] = pd.Series([pd.NA] * len(surviving_agg), dtype="Int64")

    # Merge (etp_year, region)
    df_long = df_long.merge(surviving_agg[["etp_year", "region", "Surviving"]], on=["etp_year", "region"], how="left")

    # Asegura existencia/numérico
    if "Surviving" not in df_long.columns:
        df_long["Surviving"] = pd.Series(pd.NA, index=df_long.index, dtype="Int64")

    df_long["Surviving"] = pd.to_numeric(df_long["Surviving"], errors="coerce").fillna(0).astype("Int64")

    # --- 6) Largo (Contracted, Planted, Surviving) ---
    final_long = df_long.melt(
        id_vars=["etp_year", "allocation_type", "region"],
        value_vars=["Contracted", "Planted", "Surviving"],
        var_name="contract_trees_status",
        value_name="value"
    )

    # --- 7) Pivot final ---
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
