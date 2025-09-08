# MonthlyReport/excel_composer.py

from MonthlyReport.tables.t1_etp_summary import build_etp_summary
from MonthlyReport.tables.t2_trees_by_etp_raise import build_etp_trees_table2
from MonthlyReport.tables.t2a_trees_by_etp_stats_obligation import enrich_with_obligations_and_stats
from MonthlyReport.tables_process import get_allocation_type
from core.libs import pd, datetime
from core.paths import MONTHLY_REPORT_DIR, ensure_all_paths_exist
from core.db import get_engine
from MonthlyReport.tables.t3_trees_by_planting_year import build_t3_trees_by_planting_year
from core.db_objects import ensure_fpi_expanded_view   # 👈 NUEVO

def generate_monthly_excel_report():
    engine = get_engine()
    ensure_all_paths_exist()

    # 👇 Garantiza la vista: masterdatabase.fpi_contracts_expanded
    ensure_fpi_expanded_view(engine)

    today_str = datetime.today().strftime("%Y-%m-%d")
    output_filename = f"monthly_report_{today_str}.xlsx"
    output_path = MONTHLY_REPORT_DIR / output_filename

    # T1
    df1 = build_etp_summary(engine)

    # T2 base
    df2 = build_etp_trees_table2(engine)

    # US / CAN (T2A)
    years_us = [y for y in df2["year"].unique() if "ETP" in get_allocation_type(y)]
    df2_us = df2[df2["year"].isin(years_us)].copy()
    df2a_us = enrich_with_obligations_and_stats(df2_us, engine)

    years_can = [y for y in df2["year"].unique() if "COP" in get_allocation_type(y)]
    df2_can = df2[df2["year"].isin(years_can)].copy()
    df2a_can = enrich_with_obligations_and_stats(df2_can, engine)

    # T3
    t3 = build_t3_trees_by_planting_year(engine)

    with pd.ExcelWriter(output_path) as writer:
        df1.to_excel(writer, sheet_name="ETP Summary", index=False)
        df2a_us.to_excel(writer, sheet_name="Trees by ETP US Raise", index=False)
        df2a_can.to_excel(writer, sheet_name="Trees by Canadian ETP Raise", index=False)
        t3.to_excel(writer, sheet_name="Trees by Planting Year", index=False)

    print("✅ Reporte generado en:\n")
    print(f'"{output_path}"')
