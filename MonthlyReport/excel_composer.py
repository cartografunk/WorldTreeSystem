# MonthlyReport/excel_composer.py

from MonthlyReport.tables.t1_etp_summary import build_etp_summary
from MonthlyReport.tables.t1a_etp_summary_by_allocation_type import build_etp_summary_by_allocation
from MonthlyReport.tables.t2_trees_by_etp_raise import build_etp_trees_table2
from MonthlyReport.tables.t2a_trees_by_etp_stats_obligation import enrich_with_obligations_and_stats
from MonthlyReport.tables.t3_trees_by_planting_year import build_t3_trees_by_planting_year
from MonthlyReport.tables_process import get_allocation_type, _merge_back_geo_columns, apply_aliases
from MonthlyReport.utils_monthly_base import build_monthly_base_table
from MonthlyReport.stats import survival_stats  # disponible por si lo usas en otras tablas
from MonthlyReport.tables.t4_change_by_etp import build_t4_change_by_etp, format_t4_matrix
from MonthlyReport.tables.t5_change_by_planting_year import build_t5_change_by_planting_year, format_t5_matrix

from core.libs import pd, datetime
from core.paths import MONTHLY_REPORT_DIR, ensure_all_paths_exist
from core.db import get_engine
from core.db_objects import ensure_fpi_expanded_view

def generate_monthly_excel_report():
    engine = get_engine()
    ensure_all_paths_exist()
    ensure_fpi_expanded_view(engine)

    # Base (en memoria por si alguna tabla la necesita)
    mbt = build_monthly_base_table()

    # Mes de corte basado en TODAY (primer día del mes)
    today = datetime.today()
    run_month_str = today.strftime("%Y-%m-01")

    today_str = today.strftime("%Y-%m-%d")
    output_filename = f"monthly_report_{today_str}.xlsx"
    output_path = MONTHLY_REPORT_DIR / output_filename

    # =========================
    # T1
    # =========================
    df1 = build_etp_summary(engine)
    t1a = build_etp_summary_by_allocation(engine)
    # =========================
    # T2 (crudo → alias justo antes de exportar)
    # =========================
    df2 = build_etp_trees_table2(engine)
    df2_xls = apply_aliases(df2)

    # =========================
    # T2A (US)
    # =========================
    years_us = [y for y in df2["year"].unique() if "ETP" in get_allocation_type(y)]
    df2_us = df2[df2["year"].isin(years_us)].copy()
    df2a_us = enrich_with_obligations_and_stats(df2_us, engine)
    df2a_us = _merge_back_geo_columns(df2a_us, df2_us)
    df2a_us["etp"] = "ETP"
    df2a_us_xls = apply_aliases(df2a_us)

    # =========================
    # T2A (CAN)
    # =========================
    years_can = [y for y in df2["year"].unique() if "COP" in get_allocation_type(y)]
    df2_can = df2[df2["year"].isin(years_can)].copy()
    df2a_can = enrich_with_obligations_and_stats(df2_can, engine)
    df2a_can = _merge_back_geo_columns(df2a_can, df2_can)
    df2a_can["etp"] = "COP"
    df2a_can_xls = apply_aliases(df2a_can)

    # =========================
    # T3 (por año de plantación)
    # =========================
    t3 = build_t3_trees_by_planting_year(engine)

    # =========================
    # T4/T5 DIFERENCIALES (basados en TODAY) + materialización en DB
    #   T4 = T2(calc) - T4_hist(<corte)
    #   T5 = T3(calc) - T5_hist(<corte)
    # =========================
    t4_long = build_t4_change_by_etp(engine=engine, run_month=run_month_str, materialize=True)
    t5_long = build_t5_change_by_planting_year(engine=engine, run_month=run_month_str, materialize=True)

    # Matrices estilo Excel
    t4_matrix = format_t4_matrix(t4_long)
    t5_matrix = format_t5_matrix(t5_long)

    # =========================
    # Exportar a Excel
    # =========================
    with pd.ExcelWriter(output_path) as writer:
        # mbt.to_excel(writer, sheet_name="01_monthly_base_table", index=False)  # útil para troubleshooting
        df1.to_excel(writer, sheet_name="ETP Summary", index=False)
        t1a.to_excel(writer, sheet_name="ETP Summary by Allocation", index=False)

        # df2_xls.to_excel(writer, sheet_name="T2 (raw)", index=False)
        df2a_us_xls.to_excel(writer, sheet_name="Trees by ETP US Raise", index=False)
        df2a_can_xls.to_excel(writer, sheet_name="Trees by Canadian COP Raise", index=False)
        t3.to_excel(writer, sheet_name="Trees by Planting Year", index=False)

        # T4/T5 (diferenciales definitivos)
        t4_matrix.to_excel(writer, sheet_name="Change of Trees by ETP US Raise", index=False)
        t5_matrix.to_excel(writer, sheet_name="Change of Trees by PlantingYear", index=False)

    print("✅ Reporte generado en:\n")
    print(f'"{output_path}"')
