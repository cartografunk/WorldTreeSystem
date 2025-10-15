# MonthlyReport/excel_composer.py

from core.libs import pd, datetime
from core.paths import MONTHLY_REPORT_DIR, ensure_all_paths_exist
from core.db import get_engine
from core.db_objects import ensure_fpi_expanded_view

from MonthlyReport.utils_monthly_base import build_monthly_base_table
from MonthlyReport.tables_process import apply_aliases, _merge_back_geo_columns, get_allocation_type

# TODOS los builders deben aceptar mbt
from MonthlyReport.tables.t1_etp_summary import build_etp_summary
from MonthlyReport.tables.t1a_etp_summary_by_allocation_type import build_etp_summary_by_allocation
from MonthlyReport.tables.t2_trees_by_etp_raise import build_etp_trees_table2
from MonthlyReport.tables.t2a_trees_by_cop_raise import build_cop_trees_table2
from MonthlyReport.tables.t3_trees_by_planting_year import build_t3_trees_by_planting_year

def generate_monthly_excel_report():
    engine = get_engine()
    ensure_all_paths_exist()
    ensure_fpi_expanded_view(engine)

    # ========= ÚNICA fase de BD =========
    mbt = build_monthly_base_table()

    # === series_obligation: una sola lectura y en memoria ===
    so_df = pd.read_sql("SELECT etp_year, series_obligation FROM masterdatabase.series_obligation", get_engine())
    so_df["etp_year"] = pd.to_numeric(so_df["etp_year"], errors="coerce")
    so_df["series_obligation"] = pd.to_numeric(so_df["series_obligation"], errors="coerce")
    so_by_year = (
        so_df.dropna(subset=["etp_year"])
        .drop_duplicates(subset=["etp_year"])
        .set_index("etp_year")["series_obligation"]
        .to_dict()
    )

    # ========= Fechas / salida =========
    today = datetime.today()
    today_str = today.strftime("%Y-%m-%d")
    output_path = MONTHLY_REPORT_DIR / f"monthly_report_{today_str}.xlsx"

    # ========= Tablas (todas desde MBT) =========
    df1 = build_etp_summary(mbt)
    # t1a = build_etp_summary_by_allocation(mbt)

    # T2 (ETP, con splits 2016 y 2018 ETP>0)
    df2 = build_etp_trees_table2(mbt, so_by_year=so_by_year)

    # T2a (COP, con splits 2015 / 2017-canada_2017_trees / 2016-2018 con COP>0)
    t2a = build_cop_trees_table2(mbt, so_by_year=so_by_year)  # ⬅️ NUEVO


    t3 = build_t3_trees_by_planting_year(mbt)

    # Aliases para export
    df2_xls = apply_aliases(df2)
    t2a_xls = apply_aliases(t2a)  # ⬅️ NUEVO

    # ========= Export =========
    with pd.ExcelWriter(output_path) as writer:
        mbt.to_excel(writer, sheet_name="01_monthly_base_table", index=False)
        df1.to_excel(writer, sheet_name="ETP Summary", index=False)
        # t1a.to_excel(writer, sheet_name="ETP Summary by Allocation", index=False)
        df2_xls.to_excel(writer, sheet_name="Trees by ETP (Summary)", index=False)
        t2a_xls.to_excel(writer, sheet_name="Trees by Canadian COP Raise", index=False)  # ⬅️ NUEVO
        t3.to_excel(writer, sheet_name="Trees by Planting Year", index=False)

    print("✅ Reporte generado en:\n")
    print(f'"{output_path}"')
